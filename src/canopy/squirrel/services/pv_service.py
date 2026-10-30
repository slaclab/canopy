"""PV registry + meta-PV domain service."""

from __future__ import annotations

import logging

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from canopy.core.errors import BadRequest, FacilityNotFound, InsufficientFacilityAccess
from canopy.core.facilities import gateway as facilities_gateway
from canopy.squirrel.errors import PvNotFound, MetaPvNotFound, PvAlreadyExists
from canopy.squirrel.schemas.pv import (
    PVElementDTO,
    NewPVElementDTO,
    PVSearchResponse,
    UpdatePVElementDTO,
)
from canopy.contracts.pagination import PagedResult
from canopy.squirrel.data.models import Pv, Tag
from canopy.squirrel.schemas.tag import TagDTO
from canopy.squirrel.schemas.meta_pv import MetaPVDTO, DisplayMetaPVDTO
from canopy.squirrel.data.repositories import (
    PvRepository,
    TagRepository,
    MetaPvRepository,
)

logger = logging.getLogger(__name__)

IDENTITY_KINDS: tuple[str, ...] = ("setpoint", "readback", "config")

# How many refused rows a multi-create's 403 names before summarizing the rest
_REFUSED_ROWS_SHOWN = 10


def _identity(
    setpoint_address: str | None,
    readback_address: str | None,
    config_address: str | None,
) -> tuple[str, str]:
    """Resolve the identifying address of a PV.

    A PV is identified by its first non-null address in the order setpoint, readback,
    config. Two PVs are duplicates when their identities are equal, so a PV that owns a
    setpoint never collides with a setpoint-less PV sharing its readback.

    Parameters
    ----------
    setpoint_address : str | None
        Normalized setpoint address.
    readback_address : str | None
        Normalized readback address.
    config_address : str | None
        Normalized config address.

    Returns
    -------
    tuple[str, str]
        The identity kind (``"setpoint"``, ``"readback"`` or ``"config"``) and the address
        carrying it.

    Raises
    ------
    BadRequest
        If every address is ``None``.
    """
    for kind, address in zip(IDENTITY_KINDS, (setpoint_address, readback_address, config_address)):
        if address:
            return kind, address
    raise BadRequest("At least one address (setpoint, readback, or config) is required")


class PvService:
    """PV registry + meta-PV management. Owns no transaction — the caller commits."""

    def __init__(self, session: AsyncSession, facility_scope: set[str] | None = None):
        """``facility_scope`` is the calling key's granted facility ids, hierarchy-
        expanded, resolved per request by ``squirrel.dependencies``; ``None`` means
        unrestricted, which is what the worker gets."""
        self.session = session
        self.facility_scope = facility_scope
        self.pv_repo = PvRepository(session)
        self.meta_pv_repo = MetaPvRepository(session)
        self.tag_repo = TagRepository(session)

    def _to_dto(self, pv: Pv, facilities: list[str]) -> PVElementDTO:
        return PVElementDTO(
            id=pv.id,
            setpoint_address=pv.setpoint_address,
            readback_address=pv.readback_address,
            config_address=pv.config_address,
            device=pv.device,
            description=pv.description,
            abs_tolerance=pv.abs_tolerance,
            rel_tolerance=pv.rel_tolerance,
            tags=[TagDTO(id=t.id, name=t.name, description=t.description) for t in pv.tags],
            facilities=facilities,
            created_date=pv.created_at,
            last_modified_date=pv.updated_at,
        )

    async def _to_dtos(self, pvs: list[Pv], ids_by_pv: dict[str, set[str]] | None = None) -> list[PVElementDTO]:
        """``_to_dto`` over a result set. Facility ids are stored but reported as slugs,
        resolved in one batched lookup for the whole set. ``ids_by_pv`` defaults to
        reading ``pv_facility`` for these PVs."""
        if ids_by_pv is None:
            ids_by_pv = await self.pv_repo.facility_ids_for_many([pv.id for pv in pvs])
        slug_by_id = await facilities_gateway.slugs_by_id({fid for ids in ids_by_pv.values() for fid in ids})
        return [
            self._to_dto(pv, sorted(slug_by_id[fid] for fid in ids_by_pv.get(pv.id, ()) if fid in slug_by_id))
            for pv in pvs
        ]

    def _can_write(self, row_facility_ids: set[str]) -> bool:
        """Whether the calling key may write an *existing* row with these facilities.

        Allowed when the key is unrestricted, or when the row's facilities overlap the
        grant *at all* — holding one of a PV's facilities is enough. An *unassigned* row
        (``row_facility_ids`` empty) is refused for a restricted key: no grant can cover
        a row with no facility, so only an unrestricted key may touch one, mirroring the
        read side once ``_in_facility_scope``-style filtering excludes it from a
        restricted key's list. Mint-time assignment is a separate, looser check — see
        ``_require_facility_for_create`` — since minting new unscoped data is a
        different act from mutating an existing unassigned row.
        """
        return self.facility_scope is None or bool(row_facility_ids & self.facility_scope)

    async def _require_facility_write(self, row_facility_ids: set[str], what: str) -> None:
        """Gate a write on the calling key's facility grant (see ``_can_write``).

        The refusal names the row's facilities by slug, so the caller can see which
        grant it would need. Naming them leaks nothing a restricted key can't already
        reach: ``GET /v1/facilities`` lists every facility to any read key. An
        unassigned row has no facility to name, so the message says so instead.
        """
        if self._can_write(row_facility_ids):
            return
        if not row_facility_ids:
            raise InsufficientFacilityAccess(
                f"API key is scoped to specific facilities; {what} is unassigned and only an unrestricted key may write it"
            )
        slugs = sorted((await facilities_gateway.slugs_by_id(row_facility_ids)).values())
        raise InsufficientFacilityAccess(f"API key is not scoped to {what}'s facility(s): {', '.join(slugs)}")

    async def _require_facility_for_create(self, facility_ids: set[str], what: str) -> None:
        """Gate the facilities a *new* row is being created with.

        A restricted key must name at least one facility it holds — unlike
        ``_require_facility_write``, an empty ``facility_ids`` is refused outright here
        rather than treated as "no row to check," since a restricted key minting an
        unassigned row would then be unable to reach it again under
        ``_require_facility_write``'s unrestricted-only rule for unassigned rows.
        Mirrors the equivalent gate in ``SnapshotService`` routes for snapshot creation.
        The refusal names the requested facilities by slug, as the other gates do.
        """
        if self.facility_scope is None:
            return
        if not (facility_ids & self.facility_scope):
            if not facility_ids:
                raise InsufficientFacilityAccess(
                    f"API key is scoped to specific facilities; {what} must name at least one"
                )
            slugs = sorted((await facilities_gateway.slugs_by_id(facility_ids)).values())
            raise InsufficientFacilityAccess(f"API key is not scoped to {what}'s facility(s): {', '.join(slugs)}")

    async def _require_facility_containment(self, facility_ids: set[str], what: str) -> None:
        """Gate a specific set of facilities against the grant, requiring every one of
        them to be held — stricter than ``_require_facility_write``'s "any overlap"
        test. Used where ``facility_ids`` describes a delta (facilities being added or
        removed) rather than "does this row belong to me at all": a key holding only
        one of several facilities must not be able to add or remove the others just
        because it can touch the row through one it does hold.
        """
        if self.facility_scope is None or not facility_ids:
            return
        if outside := facility_ids - self.facility_scope:
            slugs = sorted((await facilities_gateway.slugs_by_id(outside)).values())
            raise InsufficientFacilityAccess(f"API key is not scoped to {what}'s facility(s): {', '.join(slugs)}")

    async def _resolve_slugs(self, slugs: list[str] | set[str]) -> dict[str, str]:
        """Facility slugs -> ``{slug: id}``, in one round trip regardless of how many
        slugs are named (a CSV import can name dozens).

        An unknown slug is a hard ``FacilityNotFound``: degrading to "matches nothing"
        is right for a *filter*, wrong when the facility is the field being written.
        Every unknown slug is named at once, not just the first alphabetically.
        """
        resolved = await facilities_gateway.ids_by_slug(slugs)
        if unknown := set(slugs) - resolved.keys():
            raise FacilityNotFound(f"Facility(s) {sorted(unknown)} not found")
        return resolved

    @staticmethod
    def _register(
        pv: Pv,
        setpoint_address: str | None,
        readback_address: str | None,
        config_address: str | None,
        data: NewPVElementDTO,
        tags: list[Tag],
    ) -> Pv:
        """Write a registration onto ``pv``: a new row, or a deleted one being brought back.

        A deleted PV comes back with its id and ``created_at``, so its values in past
        snapshots belong to it again; every other field, tags included, comes from the
        new registration.
        """
        pv.setpoint_address = setpoint_address
        pv.readback_address = readback_address
        pv.config_address = config_address
        pv.device = data.device
        pv.description = data.description
        pv.abs_tolerance = data.abs_tolerance
        pv.rel_tolerance = data.rel_tolerance
        pv.tags = tags
        pv.deleted_at = None
        return pv

    @staticmethod
    def _normalize_address(address: str | None) -> str | None:
        """Trim an optional address from an API payload; empty becomes ``None``."""
        if address is None:
            return None
        normalized = address.strip()
        return normalized or None

    async def search_paged(
        self,
        search: str | None = None,
        page_size: int = 100,
        continuation_token: str | None = None,
        tag_filters: dict[str, list[str]] | None = None,
        facility: str | None = None,
    ) -> PagedResult[PVElementDTO]:
        facility_ids = await facilities_gateway.resolve_effective_scope(facility, self.facility_scope)
        pvs, next_token, total_count = await self.pv_repo.search_by_name(
            search=search,
            limit=page_size,
            continuation_token=continuation_token,
            tag_filters=tag_filters,
            facility_ids=facility_ids,
        )
        return PagedResult(
            results=await self._to_dtos(pvs),
            continuation_token=next_token,
            total_count=total_count,
        )

    async def get_by_id(self, pv_id: str) -> PVElementDTO | None:
        # Route through get_by_ids so tags (and their group) are eager-loaded: _to_dto reads
        # pv.tags, and a bare session.get() would trigger an async lazy-load (MissingGreenlet).
        pvs = await self.pv_repo.get_by_ids([pv_id])
        if not pvs:
            return None
        return (await self._to_dtos(pvs))[0]

    async def _find_by_identity(self, kind: str, address: str) -> Pv | None:
        """Look up the PV already holding an identity, live or deleted.

        Parameters
        ----------
        kind : str
            Identity kind as returned by ``_identity``.
        address : str
            Address carrying the identity.

        Returns
        -------
        Pv | None
            The PV owning that identity, or ``None`` when no PV ever held it.
        """
        if kind == "setpoint":
            return await self.pv_repo.find_by_setpoint(address, include_deleted=True)
        if kind == "readback":
            return await self.pv_repo.find_by_readback_without_setpoint(address, include_deleted=True)
        return await self.pv_repo.find_by_config_only(address, include_deleted=True)

    async def create(self, data: NewPVElementDTO) -> PVElementDTO:
        """Register a single PV.

        A deleted PV holding the same identifying address is brought back instead of a
        new row being created.

        Parameters
        ----------
        data : NewPVElementDTO
            Wire payload for the new PV.

        Returns
        -------
        PVElementDTO
            The created or restored PV.

        Raises
        ------
        BadRequest
            If no address is given, or a tag ID is unknown.
        PvAlreadyExists
            If another PV already holds the same identifying address.
        """
        setpoint_address = self._normalize_address(data.setpoint_address)
        readback_address = self._normalize_address(data.readback_address)
        config_address = self._normalize_address(data.config_address)
        kind, identity_address = _identity(setpoint_address, readback_address, config_address)

        # Gate the facility before the uniqueness check: otherwise a restricted key
        # probing guessed addresses learns an out-of-scope PV exists from the
        # `PvAlreadyExists` message before it's ever told it lacks facility access.
        facility_ids = set((await self._resolve_slugs(data.facilities)).values())
        await self._require_facility_for_create(facility_ids, "the requested PV")

        # A deleted PV comes back only for a key that could write it: bringing back
        # another facility's PV would move its past values into this key's facility.
        existing = await self._find_by_identity(kind, identity_address)
        if existing is not None and (
            existing.deleted_at is None or not self._can_write(await self.pv_repo.facility_ids_for(existing.id))
        ):
            raise PvAlreadyExists(f"PV with {kind} address '{identity_address}' already exists")

        tags = []
        if data.tags:
            tags = await self.tag_repo.get_by_ids(data.tags)
            if len(tags) != len(data.tags):
                raise BadRequest("One or more tag IDs are invalid")

        try:
            if existing is not None:
                pv = await self.pv_repo.update(
                    self._register(existing, setpoint_address, readback_address, config_address, data, tags)
                )
            else:
                pv = await self.pv_repo.create(
                    self._register(Pv(), setpoint_address, readback_address, config_address, data, tags)
                )
        except IntegrityError as e:
            raise PvAlreadyExists(f"PV addresses violate uniqueness constraints: {e}") from e
        if facility_ids or existing is not None:
            # A PV brought back takes the new registration's facilities, like its tags.
            await self.pv_repo.set_facilities(pv.id, facility_ids)
        return (await self._to_dtos([pv], {pv.id: facility_ids}))[0]

    async def create_many(self, data_list: list[NewPVElementDTO]) -> list[PVElementDTO]:
        """Register many PVs in one import.

        Parameters
        ----------
        data_list : list[NewPVElementDTO]
            Wire payloads for the PVs to import.

        Returns
        -------
        list[PVElementDTO]
            The created PVs, in input order. A row whose identifying address belongs to a
            deleted PV brings that PV back, as ``create`` does.

        Raises
        ------
        BadRequest
            If a row has no address, a tag ID is unknown, or two rows in the import share an
            identifying address.
        PvAlreadyExists
            If a row's identifying address is already held by a registered PV.
        """
        normalized_records: list[tuple[str, str, str | None, str | None, str | None, NewPVElementDTO]] = []
        identities: dict[str, set[str]] = {kind: set() for kind in IDENTITY_KINDS}
        duplicates: dict[str, set[str]] = {kind: set() for kind in IDENTITY_KINDS}
        for data in data_list:
            setpoint_address = self._normalize_address(data.setpoint_address)
            readback_address = self._normalize_address(data.readback_address)
            config_address = self._normalize_address(data.config_address)
            kind, identity_address = _identity(setpoint_address, readback_address, config_address)
            if identity_address in identities[kind]:
                duplicates[kind].add(identity_address)
            identities[kind].add(identity_address)
            normalized_records.append(
                (kind, identity_address, setpoint_address, readback_address, config_address, data)
            )

        for kind in IDENTITY_KINDS:
            if duplicates[kind]:
                raise BadRequest(f"Duplicate {kind} addresses in import: {sorted(duplicates[kind])[:10]}")

        # Resolve the whole import's distinct slugs up front — one gateway round trip
        # each instead of one per (row, slug) pair — but gate per row: collapsing the
        # import into one set and requiring only that it overlap the grant would let a
        # row assigned solely to an out-of-scope facility ride in alongside one the key
        # can reach. Gated before the existence checks below, for the same reason as
        # `create`: an out-of-scope row's existence must not leak through a
        # `PvAlreadyExists` message before the facility refusal.
        #
        # Check every row before refusing, so one 403 names all the facilities the import
        # needs instead of only the first row's — a CSV fix is one pass, not one per row.
        # A row naming no facility at all is refused the same as one naming only an
        # out-of-scope facility: import creates new rows, so the create-time "must name
        # a facility you hold" rule applies, not the looser existing-row write rule.
        ids_by_slug = await self._resolve_slugs({slug for *_, data in normalized_records for slug in data.facilities})
        refused: list[tuple[int, str, list[str]]] = []
        for row, (_, _, setpoint_address, readback_address, config_address, data) in enumerate(normalized_records, 1):
            row_facility_ids = {ids_by_slug[slug] for slug in data.facilities}
            if self.facility_scope is not None and not (row_facility_ids & self.facility_scope):
                address = setpoint_address or readback_address or config_address
                refused.append((row, address, sorted(data.facilities)))
        if refused:
            unreachable = sorted({slug for _, _, slugs in refused for slug in slugs})
            rows = "; ".join(
                f"row {row} ({address}): {', '.join(slugs) or '(none named)'}"
                for row, address, slugs in refused[:_REFUSED_ROWS_SHOWN]
            )
            if len(refused) > _REFUSED_ROWS_SHOWN:
                rows += f"; and {len(refused) - _REFUSED_ROWS_SHOWN} more"
            logger.warning(
                "PV import refused: %d of %d rows name only facilities outside the key's grant (%s)",
                len(refused),
                len(normalized_records),
                ", ".join(unreachable) or "(none named)",
                extra={"refused_rows": len(refused), "total_rows": len(normalized_records), "facilities": unreachable},
            )
            raise InsufficientFacilityAccess(
                f"API key is not scoped to facility(s) {', '.join(unreachable) or '(none named)'}. "
                f"{len(refused)} of {len(normalized_records)} rows name no facility it can write: {rows}"
            )

        existence_lookups = (
            ("setpoint", self.pv_repo.get_existing_setpoints),
            ("readback", self.pv_repo.get_existing_readbacks_without_setpoint),
            ("config", self.pv_repo.get_existing_config_only),
        )
        for kind, lookup in existence_lookups:
            existing = await lookup(sorted(identities[kind]))
            if existing:
                raise PvAlreadyExists(f"{kind.capitalize()} addresses already exist: {sorted(existing)[:10]}")

        deleted: dict[tuple[str, str], Pv] = {}
        for kind in IDENTITY_KINDS:
            for address, pv in (await self.pv_repo.get_deleted_by_identity(kind, sorted(identities[kind]))).items():
                deleted[(kind, address)] = pv
        # As in `create`, a deleted PV comes back only for a key that could write it.
        if deleted and self.facility_scope is not None:
            old_ids = await self.pv_repo.facility_ids_for_many([pv.id for pv in deleted.values()])
            if held := sorted(
                address for (_, address), pv in deleted.items() if not self._can_write(old_ids.get(pv.id, set()))
            ):
                raise PvAlreadyExists(f"Addresses already exist: {held[:10]}")

        all_tag_ids: set[str] = set()
        for *_, data in normalized_records:
            all_tag_ids.update(data.tags)

        tags_by_id = {}
        if all_tag_ids:
            tags = await self.tag_repo.get_by_ids(list(all_tag_ids))
            tags_by_id = {t.id: t for t in tags}

        pvs = []
        for kind, identity_address, setpoint_address, readback_address, config_address, data in normalized_records:
            pv_tags = [tags_by_id[tid] for tid in data.tags if tid in tags_by_id]
            pv = deleted.get((kind, identity_address)) or Pv()
            pvs.append(self._register(pv, setpoint_address, readback_address, config_address, data, pv_tags))

        try:
            # Restored rows are already in the session; adding them again is a no-op, and
            # the flush writes them with the new ones.
            pvs = await self.pv_repo.bulk_create(pvs)
        except IntegrityError as e:
            raise PvAlreadyExists(f"PV addresses violate uniqueness constraints: {e}") from e

        # `bulk_create` returns in input order, so zipping back against the normalized
        # records is what maps each new id to the slugs its payload asked for.
        assignments = [
            (pv.id, {ids_by_slug[slug] for slug in data.facilities})
            for pv, (*_, data) in zip(pvs, normalized_records)
            if data.facilities
        ]
        # A PV brought back takes the new registration's facilities, like its tags.
        await self.pv_repo.clear_facilities_many([pv.id for pv in deleted.values()])
        await self.pv_repo.add_facilities_many(assignments)
        return await self._to_dtos(pvs, dict(assignments))

    async def update(self, pv_id: str, data: UpdatePVElementDTO) -> PVElementDTO | None:
        pv = await self.pv_repo.get_by_id(pv_id)
        if not pv:
            return None
        facility_ids = await self.pv_repo.facility_ids_for(pv_id)
        await self._require_facility_write(facility_ids, "this PV")
        if data.facilities is not None:
            new_ids = set((await self._resolve_slugs(data.facilities)).values())
            # A restricted key may not strip a PV down to unassigned: that would hide it
            # from every facility-filtered read and hand write access to every other
            # write-scoped key, including ones with no overlap with the key doing this.
            # Holding every one of the PV's *current* facilities is not enough to grant
            # that — unassigning is its own act, refused for any restricted key.
            if not new_ids and self.facility_scope is not None:
                raise InsufficientFacilityAccess(
                    "API key is scoped to specific facilities; cannot unassign this PV's last facility"
                )
            # Every facility being added or removed must be in the grant: "any overlap
            # with the new set" would let a key holding just one of a PV's several
            # facilities silently drop the others on an otherwise-permitted update.
            await self._require_facility_containment(facility_ids ^ new_ids, "the requested change")
            await self.pv_repo.set_facilities(pv_id, new_ids)
            facility_ids = new_ids
        if data.device is not None:
            pv.device = data.device
        if data.description is not None:
            pv.description = data.description
        if data.abs_tolerance is not None:
            pv.abs_tolerance = data.abs_tolerance
        if data.rel_tolerance is not None:
            pv.rel_tolerance = data.rel_tolerance
        if data.tags is not None:
            tags = await self.tag_repo.get_by_ids(data.tags)
            if len(tags) != len(data.tags):
                raise BadRequest("One or more tag IDs are invalid")
            pv.tags = tags
        pv = await self.pv_repo.update(pv)
        return (await self._to_dtos([pv], {pv.id: facility_ids}))[0]

    async def delete(self, pv_id: str) -> bool:
        """Delete a PV by setting ``deleted_at``.

        The row stays, with its tags, so its values in past snapshots keep their PV, and
        registering its identifying address again brings it back. Returns ``False`` when
        no live PV has ``pv_id``.

        Raises
        ------
        BadRequest
            If the PV is still a meta-PV; its marker has to be removed first.
        """
        if await self.pv_repo.get_by_id(pv_id) is None:
            return False
        await self._require_facility_write(await self.pv_repo.facility_ids_for(pv_id), "this PV")
        if await self.meta_pv_repo.get_one(pv_id) is not None:
            raise BadRequest(
                "Cannot delete a PV that is still registered as a meta-PV; remove the meta-PV marker first"
            )
        return await self.pv_repo.soft_delete(pv_id)

    async def get_all_for_snapshot(self) -> list[tuple[str, str | None, str | None]]:
        addresses = await self.pv_repo.get_all_addresses()
        return [(pv_id, setpoint, readback) for pv_id, setpoint, readback, _ in addresses]

    async def search_filtered(
        self,
        q: str | None = None,
        devices: list[str] | None = None,
        tag_ids: list[str] | None = None,
        limit: int = 100,
        offset: int = 0,
        facility: str | None = None,
    ) -> PVSearchResponse:
        """Offset-paginated, server-side filtered search (the source's live-value join is dropped)."""
        facility_ids = await facilities_gateway.resolve_effective_scope(facility, self.facility_scope)
        pvs, total = await self.pv_repo.search_filtered(
            search_term=q,
            devices=devices or None,
            tag_ids=tag_ids or None,
            limit=limit,
            offset=offset,
            facility_ids=facility_ids,
        )
        return PVSearchResponse(results=await self._to_dtos(pvs), total_count=total, limit=limit, offset=offset)

    async def get_all_devices(self) -> list[str]:
        """All distinct device names, for the frontend's device filter."""
        return await self.pv_repo.get_all_devices()

    # --- meta PVs ------------------------------------------------------------ #
    #
    # Deliberately not facility-gated: which PVs are meta-PVs is a global display setting
    # shared by every snapshot, not facility data. See docs/deployment/api-keys.md.

    async def enable_meta_pv(self, id: str) -> MetaPVDTO:
        meta_pv = await self.meta_pv_repo.enable(id)
        if meta_pv is None:
            raise MetaPvNotFound(f"Meta PV {id} not found")
        return MetaPVDTO(pv_id=meta_pv.pv_id, display_name=meta_pv.display_name, is_active=meta_pv.is_active)

    async def disable_meta_pv(self, id: str) -> MetaPVDTO:
        meta_pv = await self.meta_pv_repo.disable(id)
        if meta_pv is None:
            raise MetaPvNotFound(f"Meta PV {id} not found")
        return MetaPVDTO(pv_id=meta_pv.pv_id, display_name=meta_pv.display_name, is_active=meta_pv.is_active)

    async def set_meta_display_name(self, id: str, display_name: str) -> MetaPVDTO:
        meta_pv = await self.meta_pv_repo.set_display_name(id, display_name)
        if meta_pv is None:
            raise MetaPvNotFound(f"Meta PV {id} not found")
        return MetaPVDTO(pv_id=meta_pv.pv_id, display_name=meta_pv.display_name, is_active=meta_pv.is_active)

    async def add_meta_pv(self, pv_name: str, display_name: str) -> MetaPVDTO:
        pv = await self.pv_repo.find_by_address(pv_name)
        if pv is None:
            raise PvNotFound(f"PV {pv_name} could not be found")
        try:
            new_pv = await self.meta_pv_repo.add(pv.id, display_name)
        except IntegrityError as e:
            if getattr(e.orig, "sqlstate", None) == "23505":
                raise PvAlreadyExists(
                    f"{pv.setpoint_address or pv.readback_address} ({pv.id}) is already a meta PV"
                ) from e
            raise
        return MetaPVDTO(pv_id=new_pv.pv_id, display_name=new_pv.display_name, is_active=new_pv.is_active)

    async def get_meta_pv(self, id: str) -> MetaPVDTO:
        meta_pv = await self.meta_pv_repo.get_one(id)
        if meta_pv is None:
            raise MetaPvNotFound(f"Meta PV {id} not found")
        return MetaPVDTO(pv_id=meta_pv.pv_id, display_name=meta_pv.display_name, is_active=meta_pv.is_active)

    async def get_meta_pvs(self, active: bool | None = None) -> list[DisplayMetaPVDTO]:
        meta_pv_metadata = await self.meta_pv_repo.get(active=active)
        pv_ids = [pv.pv_id for pv in meta_pv_metadata]
        pvs = {pv.id: pv for pv in await self.pv_repo.get_by_ids(pv_ids, include_deleted=True)}
        return [
            DisplayMetaPVDTO(
                pv_id=meta_pv.pv_id,
                pv_name=pvs[meta_pv.pv_id].setpoint_address or pvs[meta_pv.pv_id].readback_address or "",
                display_name=meta_pv.display_name,
                is_active=meta_pv.is_active,
            )
            for meta_pv in meta_pv_metadata
        ]

    async def delete_meta_pv(self, id: str) -> None:
        rowcount = await self.meta_pv_repo.delete(id)
        if rowcount == 0:
            raise MetaPvNotFound(f"Meta PV {id} not found")
