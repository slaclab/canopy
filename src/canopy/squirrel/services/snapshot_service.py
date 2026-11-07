"""Snapshot domain service: create / restore / compare + listing."""

from __future__ import annotations

import json
import math
import base64
import logging
import binascii
from uuid import UUID, uuid4
from typing import Any, Literal
from datetime import UTC, datetime
from dataclasses import asdict, dataclass
from collections.abc import Callable

from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from canopy.core.errors import BadRequest, FacilityNotFound, InsufficientFacilityAccess
from canopy.core.ownership import require_owned, local_instance, require_create_owner
from canopy.core.facilities import gateway as facilities_gateway
from canopy.core.time_parse import to_utc
from canopy.squirrel.errors import (
    ImportWouldSkip,
    NothingToImport,
    SnapshotNotFound,
    SnapshotIdConflict,
)
from canopy.contracts.pagination import PagedResult
from canopy.squirrel.data.models import Snapshot
from canopy.squirrel.restore_hooks import (
    RestoreHook,
    RestoreHookError,
    get_restore_hooks,
)
from canopy.squirrel.pv_cache.reader import PVCacheReader
from canopy.squirrel.schemas.snapshot import (
    BUNDLE_FORMAT,
    PVValueDTO,
    TagInfoDTO,
    SnapshotDTO,
    EpicsValueDTO,
    BundleValueDTO,
    MetaPVValueDTO,
    NewSnapshotDTO,
    SkippedValueDTO,
    RestoreResultDTO,
    RestoreRequestDTO,
    SnapshotBundleDTO,
    SnapshotSummaryDTO,
    ComparisonResultDTO,
    SnapshotImportResultDTO,
)
from canopy.squirrel.adapters.epics_io import EpicsIO
from canopy.squirrel.data.repositories import (
    PvRepository,
    MetaPvRepository,
    SnapshotRepository,
    SnapshotValueRepository,
)
from canopy.squirrel.services.bulk_insert import bulk_insert_snapshot_values

logger = logging.getLogger(__name__)

PRIMARY_PHASE = "Setpoints"

# Title prefix of a snapshot created by ``import_bundle``.
IMPORTED_TITLE_PREFIX = "Restored from history: "
# How many skipped PVs a strict import's refusal names; it gives the total too.
_SKIPPED_NAMED = 20


def _describe_skipped(skipped: list[SkippedValueDTO]) -> str:
    """A strict import's refusal: how many values would be skipped, and the first PVs."""
    named = ", ".join(f"{value.pv_name} ({value.pv_id})" for value in skipped[:_SKIPPED_NAMED])
    more = f" and {len(skipped) - _SKIPPED_NAMED} more" if len(skipped) > _SKIPPED_NAMED else ""
    return (
        f"{len(skipped)} value(s) would be skipped, since their PVs can't be imported here: {named}{more}. "
        "Nothing was saved"
    )


@dataclass
class CapturedValues:
    """What a capture read, before it is saved here or bundled for the snapshot's owner.

    ``rows`` are ``bulk_insert_snapshot_values`` dicts without ``snapshot_id``, one per
    PV; ``total_pvs`` is how many PVs the capture looked at, and ``taken_at`` the time
    the read finished, which becomes the snapshot's creation time.
    """

    rows: list[dict]
    total_pvs: int
    taken_at: datetime


@dataclass
class RestorePhase:
    """One stage of a restore: the setpoint writes, or one restore hook's derived writes.

    Reported to the job poller as a ``JobPhaseDTO``, beside the restore-wide percent.

    Attributes
    ----------
    name : str
        ``"Setpoints"`` or the hook's class name, the same name the restore's failure
        rows carry as ``phase``.
    total : int
        Writes the phase will attempt. A hook phase starts at every write it derived and
        drops to the writes it actually attempts once it starts, leaving out PVs whose
        setpoint or an earlier hook failed.
    current : int
        Writes done so far.
    status : {"pending", "running", "completed", "skipped"}
        ``pending`` until the phase starts, then ``running``, then ``completed``, or
        ``skipped`` when it has nothing to write.
    failures : int
        PVs that failed in this phase, matching the failure rows with this ``phase``.
    """

    name: str
    total: int
    current: int = 0
    status: Literal["pending", "running", "completed", "skipped"] = "pending"
    failures: int = 0


def _phase_dicts(phases: list[RestorePhase]) -> list[dict]:
    """Every phase's current state, in the ``JobPhaseDTO`` shape the Jobs module stores."""
    return [asdict(phase) for phase in phases]


def _sub_batch_progress(
    progress_callback: Callable, base: int, total: int, phases: list[RestorePhase], phase: RestorePhase
) -> Callable:
    """Offset one write batch's progress onto the restore-wide write count and its phase.

    Parameters
    ----------
    progress_callback : Callable
        The restore's async progress sink, called as ``(current, total, message, phases=...)``.
    base : int
        Writes already accounted for when this batch starts.
    total : int
        Every write the restore will attempt.
    phases : list[RestorePhase]
        Every phase of the restore, all reported on each call.
    phase : RestorePhase
        The phase this batch's writes belong to; its ``current`` advances with them.

    Returns
    -------
    Callable
        An async callback in the batch's own scale that reports on the global one.
    """
    phase_base = phase.current

    async def _report(current: int, _batch_total: int, message: str) -> None:
        phase.current = phase_base + current
        await progress_callback(base + current, total, message, phases=_phase_dicts(phases))

    return _report


def _parse_jsonb(value: Any) -> dict | None:
    """Parse a JSONB column that may come back as a ``str`` or ``dict`` (driver-dependent)."""
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return None
    return None


def _sanitize_for_json(value: Any) -> Any:
    """Make a value JSON-safe, recursing into lists/dicts.

    ``NaN``/``Inf``/``-Inf`` (invalid JSON) become ``None``; a Pydantic model (e.g. a
    TABLE PV's ``TableValue``) becomes its ``model_dump``. Without the model branch,
    ``json.dumps`` on the COPY path raises ``TypeError`` on a non-scalar PV value and
    aborts the entire snapshot create.
    """
    if value is None:
        return None
    if isinstance(value, BaseModel):
        return _sanitize_for_json(value.model_dump(mode="json"))
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    if isinstance(value, list):
        return [_sanitize_for_json(v) for v in value]
    if isinstance(value, dict):
        return {k: _sanitize_for_json(v) for k, v in value.items()}
    return value


def _utc_bound(name: str, value: datetime | None) -> datetime | None:
    """Convert a listing bound with ``to_utc``, refusing one that leaves ``datetime``'s range.

    Parameters
    ----------
    name : str
        The query parameter the bound came from, for the error message.
    value : datetime | None
        The bound as the client sent it.

    Returns
    -------
    datetime | None
        The bound as an aware UTC datetime, or ``None`` when it was omitted.

    Raises
    ------
    BadRequest
        When the UTC instant falls outside years 1 to 9999, e.g.
        ``9999-12-31T23:00:00-05:00``.
    """
    if value is None:
        return None
    try:
        return to_utc(value)
    except OverflowError as exc:
        raise BadRequest(f"{name} ({value.isoformat()}) is out of range once converted to UTC") from exc


def _encode_cursor(cursor: tuple[datetime, str] | None) -> str | None:
    """Pack a ``(created_at, id)`` listing cursor into an opaque continuation token.

    Parameters
    ----------
    cursor : tuple[datetime, str] | None
        Sort position of the last row on a page, from ``SnapshotRepository.search``.

    Returns
    -------
    str | None
        URL-safe token for the next page, or ``None`` when there is no next page.
    """
    if cursor is None:
        return None
    created_at, snapshot_id = cursor
    raw = f"{to_utc(created_at).isoformat()}|{snapshot_id}"
    return base64.urlsafe_b64encode(raw.encode()).decode().rstrip("=")


def _decode_cursor(token: str | None) -> tuple[datetime, str] | None:
    """Unpack a continuation token from ``_encode_cursor``.

    Parameters
    ----------
    token : str | None
        The ``continuationToken`` the client sent back, or ``None`` for the first page.

    Returns
    -------
    tuple[datetime, str] | None
        The ``(created_at, id)`` sort position to continue after.

    Raises
    ------
    BadRequest
        When the token was not produced by ``_encode_cursor``. Checked here so a tampered
        token is a clean 400 rather than an asyncpg encode error (500).
    """
    if token is None:
        return None
    try:
        raw = base64.urlsafe_b64decode(token + "=" * (-len(token) % 4)).decode()
        created_at_text, snapshot_id = raw.split("|")
        created_at = datetime.fromisoformat(created_at_text)
        UUID(snapshot_id)
    except (binascii.Error, UnicodeDecodeError, ValueError) as exc:
        raise BadRequest("Invalid continuationToken") from exc
    if created_at.tzinfo is None:
        raise BadRequest("Invalid continuationToken")
    return created_at, snapshot_id


class SnapshotService:
    """Snapshot operations with EPICS integration. Owns no transaction — the caller
    (request dep or worker ``session_scope``) commits."""

    def __init__(
        self,
        session: AsyncSession,
        epics_io: EpicsIO | None = None,
        restore_hooks: list[RestoreHook] | None = None,
        facility_scope: set[str] | None = None,
    ):
        """Wire the repositories, EPICS adapter and restore hooks.

        Parameters
        ----------
        session : AsyncSession
            The caller's session; the service owns no transaction.
        epics_io : EpicsIO | None
            Required only for create/restore (run in the worker, which builds the
            connector). The read endpoints construct the service without it.
        restore_hooks : list[RestoreHook] | None
            Hooks to run during a restore. ``None`` falls back to the configured
            singletons; an empty list disables hooks outright, which is how tests
            isolate themselves from the environment.
        facility_scope : set[str] | None
            The calling key's granted facility ids, hierarchy-expanded, resolved per
            request by ``squirrel.dependencies``; ``None`` means unrestricted, which
            is what the worker gets.
        """
        self.session = session
        self.facility_scope = facility_scope
        self.snapshot_repo = SnapshotRepository(session)
        self.value_repo = SnapshotValueRepository(session)
        self.pv_repo = PvRepository(session)
        self.meta_pv_repo = MetaPvRepository(session)
        self.epics = epics_io
        self.restore_hooks = restore_hooks if restore_hooks is not None else get_restore_hooks()

    async def _get_meta_pv_values_for_snapshot(self, snapshot_id: str, pv_ids: list[str]) -> list[MetaPVValueDTO]:
        meta_pv_values: list[MetaPVValueDTO] = []
        if pv_ids:
            for value in await self.value_repo.get_by_snapshot_and_pvs(snapshot_id, pv_ids):
                data = _parse_jsonb(value.setpoint_value) or _parse_jsonb(value.readback_value)
                meta_pv_values.append(
                    MetaPVValueDTO(
                        pv_id=value.pv_id,
                        pv_name=value.pv_name,
                        **(data or {"value": None}),
                    )
                )
        return meta_pv_values

    async def _require_facility_write(self, facility_id: str | None, what: str) -> None:
        """Gate a write on the calling key's facility grant. Unrestricted keys pass
        unconditionally; a restricted key — including an empty-grant one — needs the
        row's facility in its grant, and an *unassigned* row (``facility_id IS NULL``)
        is refused rather than passed, since no grant can cover it. Mirrors
        ``PvService._require_facility_write`` — a snapshot has at most one facility, so
        "any overlap" degenerates to membership.

        An *assigned* row outside the grant is a 404, not a 403 naming its facility:
        ``GET`` already hides such a row behind ``_in_facility_scope``'s not-found
        masking, and a 403 naming its facility would leak which facility a hidden
        snapshot is filed under to a caller that merely guessed its id. An unassigned
        row has no hidden facility to leak, so it stays a 403 — it's known to exist,
        just not writable by any restricted key."""
        if self.facility_scope is None:
            return
        if facility_id is None:
            raise InsufficientFacilityAccess(
                f"API key is scoped to specific facilities; {what} is unassigned and only an unrestricted key may write it"
            )
        if facility_id not in self.facility_scope:
            raise SnapshotNotFound("Snapshot not found")

    def _require_facility_for_create(self, facility_id: str | None, slug: str | None) -> None:
        """Gate the facility of a *new* snapshot, as the capture route does.

        A restricted key must name a facility it holds: minting an unassigned snapshot
        would create data it couldn't reach again. Unlike ``_require_facility_write``,
        an out-of-scope facility is a 403 naming it, since the caller named it.
        """
        if self.facility_scope is None:
            return
        if facility_id is None:
            raise InsufficientFacilityAccess("API key is scoped to specific facilities; 'facility' is required")
        if facility_id not in self.facility_scope:
            raise InsufficientFacilityAccess(f"API key is not scoped to facility: {slug}")

    async def _summary(self, snapshot: Snapshot, *, pv_count: int) -> SnapshotSummaryDTO:
        """``snapshot`` as a summary holding ``pv_count`` values."""
        meta_pvs = [pv.pv_id for pv in await self.meta_pv_repo.get(active=True)]
        return SnapshotSummaryDTO(
            id=snapshot.id,
            owner_instance=snapshot.owner_instance,
            title=snapshot.title,
            description=snapshot.description,
            created_date=snapshot.created_at,
            created_by=snapshot.created_by,
            pv_count=pv_count,
            facility=await self._facility_slug(snapshot.facility_id),
            retain=snapshot.retain,
            imported_from=snapshot.imported_from,
            meta_pvs=await self._get_meta_pv_values_for_snapshot(snapshot.id, meta_pvs),
        )

    async def _facility_slug(self, facility_id: str | None) -> str | None:
        """A stored facility id as the slug reported on the wire."""
        if facility_id is None:
            return None
        return (await facilities_gateway.slugs_by_id([facility_id])).get(facility_id)

    def _in_facility_scope(self, facility_id: str | None) -> bool:
        """Same membership rule as ``_require_facility_write``, as a predicate rather
        than a raise. Read paths use this to report an out-of-scope snapshot as
        not-found rather than forbidden — existence itself must not leak to a key that
        cannot reach the snapshot's facility. An unassigned row is in scope only for an
        unrestricted key, matching ``list_snapshots``'s filter."""
        return self.facility_scope is None or (facility_id is not None and facility_id in self.facility_scope)

    def _pv_in_scope(self, pv_facility_ids: set[str]) -> bool:
        """Whether the key may restore, or import, a value of a PV assigned to ``pv_facility_ids``.

        An unrestricted key may use any PV. A restricted key needs one shared facility; an
        unassigned PV has none to share, and facility-filtered reads hide it from such a
        key too.
        """
        return self.facility_scope is None or bool(pv_facility_ids & self.facility_scope)

    async def list_snapshots(
        self,
        title: str | None = None,
        tag_ids: list[str] | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        page_size: int = 100,
        continuation_token: str | None = None,
        facility: str | None = None,
    ) -> PagedResult[SnapshotSummaryDTO]:
        """List snapshots one keyset page at a time, newest first, optionally filtered.

        Each page continues from the sort position the previous page ended on, so
        snapshots created or deleted between requests cannot make a later page repeat
        or skip a row the way an offset would.

        Parameters
        ----------
        title : str | None
            Case-insensitive substring match against the snapshot title.
        tag_ids : list[str] | None
            Tag IDs; a snapshot matches when it holds a PV carrying any of them.
        start : datetime | None
            Lower bound on creation time, inclusive. Converted with ``to_utc``, because
            ``created_at`` is a ``timestamptz`` and asyncpg refuses a naive bind parameter.
        end : datetime | None
            Upper bound on creation time, inclusive. Converted the same way as ``start``.
        page_size : int
            Maximum number of summaries on the page.
        continuation_token : str | None
            Token from the previous page's result; ``None`` for the first page.
        facility : str | None
            A facility slug to additionally narrow the search to, resolved against the
            calling key's own facility scope by ``facilities_gateway.resolve_effective_scope``.

        Returns
        -------
        PagedResult[SnapshotSummaryDTO]
            The page's summaries, the token for the next page (``None`` on the last page),
            and the number of snapshots matching the filters.

        Raises
        ------
        BadRequest
            When a bound falls outside ``datetime``'s range once converted to UTC, both
            bounds are given and ``start`` is after ``end``, or the continuation token is
            malformed.
        FacilityNotFound
            When ``facility`` names a slug that doesn't exist.
        """
        start = _utc_bound("start", start)
        end = _utc_bound("end", end)
        if start is not None and end is not None and start > end:
            raise BadRequest(f"start ({start.isoformat()}) must not be after end ({end.isoformat()})")
        facility_ids = await facilities_gateway.resolve_effective_scope(facility, self.facility_scope)
        snapshots, next_cursor, total_count = await self.snapshot_repo.search(
            title=title,
            tag_ids=tag_ids,
            start=start,
            end=end,
            limit=page_size,
            after=_decode_cursor(continuation_token),
            facility_ids=facility_ids,
        )

        # One grouped query for all value counts (avoids N+1).
        snapshot_ids = [snap.id for snap in snapshots]
        counts = await self.snapshot_repo.get_value_counts_batch(snapshot_ids)
        slug_by_id = await facilities_gateway.slugs_by_id({s.facility_id for s in snapshots if s.facility_id})

        meta_pvs = [pv.pv_id for pv in await self.meta_pv_repo.get(active=True)]
        result = []
        for snap in snapshots:
            result.append(
                SnapshotSummaryDTO(
                    id=snap.id,
                    owner_instance=snap.owner_instance,
                    title=snap.title,
                    description=snap.description,
                    created_date=snap.created_at,
                    created_by=snap.created_by,
                    pv_count=counts.get(snap.id, 0),
                    facility=slug_by_id.get(snap.facility_id),
                    retain=snap.retain,
                    imported_from=snap.imported_from,
                    meta_pvs=await self._get_meta_pv_values_for_snapshot(snap.id, meta_pvs),
                )
            )
        return PagedResult(
            results=result,
            continuation_token=_encode_cursor(next_cursor),
            total_count=total_count,
        )

    async def restorable_owner(self, snapshot_id: str) -> str | None:
        """The owner of a snapshot the key may restore, or ``None`` when it can't.

        A lightweight check (no value load) used before enqueueing a restore: the snapshot
        exists, isn't deleted, and is in the key's facility scope, the same rule as
        ``get_by_id``. A snapshot the key can't view doesn't exist for it, so it can't be
        restored either.
        """
        snapshot = await self.snapshot_repo.get_by_id(snapshot_id)
        if snapshot is None or not self._in_facility_scope(snapshot.facility_id):
            return None
        return snapshot.owner_instance

    async def get_by_id(self, snapshot_id: str, limit: int | None = None, offset: int = 0) -> SnapshotDTO | None:
        """Get a snapshot with its values (values optionally paginated)."""
        result = await self.snapshot_repo.get_with_values(snapshot_id, limit=limit, offset=offset)
        if not result:
            return None
        snapshot, values = result
        if not self._in_facility_scope(snapshot.facility_id):
            return None

        total_count = await self.value_repo.count_by_snapshot(snapshot_id)

        pv_ids = [v.pv_id for v in values]
        # A PV deleted since the capture still names and tags its value here.
        pvs_with_tags = await self.pv_repo.get_by_ids(pv_ids, include_deleted=True)
        pv_tag_map: dict[str, list[TagInfoDTO]] = {}
        pv_address_map: dict[str, tuple[str | None, str | None]] = {}  # pv_id -> (setpoint, readback)
        pv_device_map: dict[str, str | None] = {}
        for pv in pvs_with_tags:
            pv_tag_map[pv.id] = [TagInfoDTO(id=tag.id, name=tag.name, group_name=tag.group.name) for tag in pv.tags]
            pv_address_map[pv.id] = (pv.setpoint_address, pv.readback_address)
            pv_device_map[pv.id] = pv.device

        pv_values = []
        for v in values:
            setpoint_data = _parse_jsonb(v.setpoint_value)
            readback_data = _parse_jsonb(v.readback_value)
            setpoint_addr, readback_addr = pv_address_map.get(v.pv_id, (None, None))

            pv_values.append(
                PVValueDTO(
                    pv_id=v.pv_id,
                    pv_name=v.pv_name,
                    device=pv_device_map.get(v.pv_id),
                    setpoint_name=setpoint_addr,
                    readback_name=readback_addr,
                    setpoint_value=EpicsValueDTO(**setpoint_data) if setpoint_data else None,
                    readback_value=EpicsValueDTO(**readback_data) if readback_data else None,
                    status=v.status,
                    severity=v.severity,
                    timestamp=v.timestamp,
                    tags=pv_tag_map.get(v.pv_id, []),
                )
            )

        meta_pvs = [pv.pv_id for pv in await self.meta_pv_repo.get(active=True)]
        return SnapshotDTO(
            id=snapshot.id,
            owner_instance=snapshot.owner_instance,
            title=snapshot.title,
            description=snapshot.description,
            created_date=snapshot.created_at,
            created_by=snapshot.created_by,
            pv_count=total_count,
            facility=await self._facility_slug(snapshot.facility_id),
            retain=snapshot.retain,
            imported_from=snapshot.imported_from,
            pv_values=pv_values,
            meta_pvs=await self._get_meta_pv_values_for_snapshot(snapshot.id, meta_pvs),
        )

    async def update_snapshot_metadata(
        self,
        snapshot_id: str,
        title: str | None = None,
        description: str | None = None,
        retain: bool | None = None,
    ) -> SnapshotSummaryDTO | None:
        """Update a snapshot's title, description and/or ``retain`` flag; ``None`` keeps a field."""
        # Read before writing: the facility gate needs the row's `facility_id`, and a
        # refused write must not have already landed.
        existing = await self.snapshot_repo.get_by_id(snapshot_id)
        if not existing:
            return None
        # The facility gate first: a caller who can't reach the row isn't told its owner.
        await self._require_facility_write(existing.facility_id, "this snapshot")
        require_owned(existing.owner_instance, f"Snapshot {snapshot_id}")
        snapshot = await self.snapshot_repo.update_metadata(
            snapshot_id, title=title, description=description, retain=retain
        )
        if not snapshot:
            return None

        return await self._summary(snapshot, pv_count=await self.snapshot_repo.get_value_count(snapshot.id))

    async def create_snapshot(
        self,
        data: NewSnapshotDTO,
        created_by: str | None = None,
        progress_callback: Callable | None = None,
        facility_id: str | None = None,
        pv_cache: PVCacheReader | None = None,
    ) -> SnapshotSummaryDTO:
        """Create a snapshot by reading PVs from EPICS (the hot read path), and save it here.

        ``capture_values`` then ``save_captured``, with an id and time assigned here at
        capture. A facility snapshot captures only the PVs assigned to that facility or
        one of its descendants; unassigned PVs are left out. A snapshot with no facility
        captures every PV this instance owns.

        ``facility_id`` lets a caller that already resolved ``data.facility`` pass the id
        straight through. The route does that — it has to resolve the slug anyway to gate
        the write, and passing the id over the job queue avoids re-resolving it.
        """
        logger.info("Creating snapshot: %s", data.title)
        facility_id = await self._capture_facility(data.facility, facility_id)
        captured = await self.capture_values(
            facility_id, owner=local_instance(), progress_callback=progress_callback, pv_cache=pv_cache
        )
        return await self.save_captured(
            captured,
            snapshot_id=str(uuid4()),
            title=data.title,
            description=data.description,
            created_by=created_by,
            facility_id=facility_id,
            progress_callback=progress_callback,
        )

    async def _capture_facility(self, slug: str | None, facility_id: str | None) -> str | None:
        # The snapshot's own facility is a specific node, so this goes through
        # `id_by_slug`. An unknown slug here is the same hard `FacilityNotFound` a
        # read filter naming one now gets, too.
        if facility_id is None and slug is not None:
            facility_id = await facilities_gateway.id_by_slug(slug)
            if facility_id is None:
                raise FacilityNotFound(f"Facility '{slug}' not found")
        return facility_id

    async def capture_values(
        self,
        facility_id: str | None,
        *,
        owner: str,
        progress_callback: Callable | None = None,
        pv_cache: PVCacheReader | None = None,
    ) -> CapturedValues:
        """Read from EPICS the PVs of ``owner`` in the facility's subtree (every PV of ``owner`` without one).

        Writes nothing. ``owner`` is the snapshot's owner: a snapshot's values reference
        PVs of its own owner, and a mirror holds its arms' PVs beside its own. A
        facility's tree is one owner already.

        With ``pv_cache``, values the worker's PV cache can vouch for are taken from it
        and only the rest are read live. The stored snapshot has the same shape either
        way, but a cached value is the PV's last monitor update, which can trail a live
        read by up to the record's monitor deadband (MDEL).
        """
        start_time = datetime.now(UTC)
        capture_scope = await facilities_gateway.descendant_ids(facility_id) if facility_id is not None else None
        pv_addresses = await self.pv_repo.get_all_addresses(capture_scope, owner=owner)
        total_pv_count = len(pv_addresses)
        # Collect the unique addresses to read (config addresses are not snapshotted).
        # An address can belong to more than one PV (only setpoint_address is unique;
        # readback addresses may be shared), so map each address to a *list* of pv_ids —
        # keying by a single pv_id would silently drop the reading for every other PV on
        # that address.
        setpoint_map: dict[str, list[str]] = {}  # address -> pv_ids
        readback_map: dict[str, list[str]] = {}  # address -> pv_ids
        for pv_id, setpoint, readback, _config in pv_addresses:
            if setpoint:
                setpoint_map.setdefault(setpoint, []).append(pv_id)
            if readback:
                readback_map.setdefault(readback, []).append(pv_id)

        all_addresses = list(set(setpoint_map.keys()) | set(readback_map.keys()))
        logger.info("Reading %d unique EPICS addresses for %d PVs", len(all_addresses), total_pv_count)

        cached: dict[str, Any] = {}
        to_read = all_addresses
        if pv_cache is not None:
            lookup = await pv_cache.lookup(all_addresses)
            cached, to_read = lookup.hits, lookup.misses
            if lookup.skipped:
                logger.warning("PV cache not used, reading every PV live: %s", lookup.skipped)
            logger.info(
                "PV cache: %d of %d addresses from cache, %d read live", len(cached), len(all_addresses), len(to_read)
            )
        cache_note = f"{len(cached):,} from cache; " if cached else ""
        total_addresses = len(all_addresses)

        # Rescale address-level progress to PV-level so the user sees "N/total PVs";
        # cached addresses count as already read.
        async def pv_progress_callback(current_addr: int, _total_addr: int, _message: str) -> None:
            done = len(cached) + current_addr
            pv_progress_count = int((done / total_addresses) * total_pv_count) if total_addresses > 0 else 0
            await progress_callback(
                pv_progress_count, total_pv_count, f"{cache_note}Read {pv_progress_count:,}/{total_pv_count:,} PVs"
            )

        if progress_callback and to_read:
            live_values = await self.epics.get_many_with_progress(to_read, pv_progress_callback)
        else:
            live_values = await self.epics.get_many(to_read) if to_read else {}
        epics_values = {**cached, **live_values}

        logger.info("EPICS read completed in %.2fs", (datetime.now(UTC) - start_time).total_seconds())

        if progress_callback:
            await progress_callback(total_pv_count, total_pv_count, "Processing results...")

        # Group the readings by PV, keeping only connected reads.
        pv_data: dict[str, dict] = {}  # pv_id -> {setpoint, readback, address}

        def _value_blob(epics_val: Any) -> dict:
            return {
                "value": _sanitize_for_json(epics_val.value),
                "status": epics_val.status,
                "severity": epics_val.severity,
                "timestamp": epics_val.timestamp.isoformat() if epics_val.timestamp else None,
                "units": epics_val.units,
                "precision": epics_val.precision,
            }

        for address, pv_ids in setpoint_map.items():
            epics_val = epics_values.get(address)
            for pv_id in pv_ids:
                pv_data.setdefault(pv_id, {"setpoint": None, "readback": None, "address": address})
                if epics_val and epics_val.connected:
                    pv_data[pv_id]["setpoint"] = _value_blob(epics_val)

        for address, pv_ids in readback_map.items():
            epics_val = epics_values.get(address)
            for pv_id in pv_ids:
                pv_data.setdefault(pv_id, {"setpoint": None, "readback": None, "address": address})
                if epics_val and epics_val.connected:
                    pv_data[pv_id]["readback"] = _value_blob(epics_val)

        now = datetime.now(UTC)
        snapshot_values_data = []
        for pv_id, data_dict in pv_data.items():
            setpoint = data_dict.get("setpoint")
            readback = data_dict.get("readback")
            # The row's alarm is the setpoint's, or the readback's when there is no
            # setpoint reading (a readback-only PV), the same fallback the meta-PV
            # read path uses.
            alarm_source = setpoint or readback
            snapshot_values_data.append(
                {
                    "pv_id": pv_id,
                    "pv_name": data_dict.get("address", ""),
                    "setpoint_value": setpoint,
                    "readback_value": readback,
                    "status": alarm_source.get("status") if alarm_source else None,
                    "severity": alarm_source.get("severity") if alarm_source else None,
                    "timestamp": now,
                }
            )

        return CapturedValues(rows=snapshot_values_data, total_pvs=total_pv_count, taken_at=now)

    async def save_captured(
        self,
        captured: CapturedValues,
        *,
        snapshot_id: str,
        title: str,
        description: str | None,
        created_by: str | None,
        facility_id: str | None,
        progress_callback: Callable | None = None,
    ) -> SnapshotSummaryDTO:
        """Save what ``capture_values`` read as this instance's snapshot ``snapshot_id``, taken at its time.

        Raises
        ------
        NotOwner
            When the facility is another instance's: the backstop on the write itself,
            after the create route's check.
        """
        start_time = datetime.now(UTC)
        if facility_id is not None:
            facility_owner = (await facilities_gateway.owners_by_id({facility_id})).get(facility_id)
            if facility_owner is not None:
                require_owned(facility_owner, f"A snapshot of facility {facility_id}")
        if progress_callback:
            await progress_callback(
                captured.total_pvs, captured.total_pvs, f"Saving {len(captured.rows):,} PV values to database..."
            )
        count = await self._save_snapshot(
            snapshot_id,
            title=title,
            description=description,
            created_by=created_by,
            facility_id=facility_id,
            rows=captured.rows,
            created_at=captured.taken_at,
            progress_callback=progress_callback,
        )
        if count is None:
            raise SnapshotIdConflict(f"Snapshot {snapshot_id} is already here")
        logger.info("Snapshot saved in %.2fs (%d values)", (datetime.now(UTC) - start_time).total_seconds(), count)
        snapshot = await self.snapshot_repo.get_by_id(snapshot_id)
        return await self._summary(snapshot, pv_count=count)

    async def bundle_captured(
        self,
        captured: CapturedValues,
        *,
        snapshot_id: str,
        title: str,
        description: str | None,
        created_by: str | None,
        facility_id: str | None,
    ) -> SnapshotBundleDTO:
        """What ``capture_values`` read, as a bundle for the owner's captured import.

        The id and creation time are this instance's, assigned at capture; values
        carry their PV ids, which are the owner's, since a mirror's PV rows are its
        arms' copies.
        """
        return SnapshotBundleDTO(
            format=BUNDLE_FORMAT,
            version=1,
            source_instance=local_instance(),
            id=snapshot_id,
            title=title,
            description=description,
            created_date=captured.taken_at,
            created_by=created_by,
            facility=await self._facility_slug(facility_id),
            values=[
                BundleValueDTO(
                    pv_id=row["pv_id"],
                    pv_name=row["pv_name"],
                    setpoint_value=row["setpoint_value"],
                    readback_value=row["readback_value"],
                    status=row["status"],
                    severity=row["severity"],
                    timestamp=row["timestamp"],
                )
                for row in captured.rows
            ],
        )

    async def plan_restore(
        self, snapshot_id: str, pv_ids: list[str] | None = None
    ) -> tuple[list[str] | None, list[str]]:
        """Narrow a restore's write set to what the calling key may actually write.

        Returns ``(pv_ids_to_restore, skipped_pv_ids)``, where a ``None`` first element
        keeps the "restore everything in the snapshot" meaning ``pv_ids=None`` already has.

        This runs in the *route*, not the worker: no caller identity crosses the Arq
        boundary, so the write set has to be narrowed before the job is queued.

        The route has already gated on the snapshot's own facility, and capture only takes
        that facility's PVs, so this normally keeps everything. It catches PVs whose
        facility assignment changed after capture, and snapshots taken before capture was
        facility-scoped.
        """
        if self.facility_scope is None:
            return pv_ids, []

        # Only the id column: the full row (including the setpoint_value/readback_value
        # JSONB) is never used here, and a snapshot can hold tens of thousands of values.
        candidate_ids = await self.value_repo.get_pv_ids_by_snapshot(snapshot_id, pv_ids)
        by_pv = await self.pv_repo.facility_ids_for_many(candidate_ids)
        allowed, skipped = [], []
        for pv_id in candidate_ids:
            assigned = by_pv.get(pv_id, set())
            # An unassigned PV is skipped rather than let through: a restore is a real EPICS
            # setpoint write, not a filtered read.
            (allowed if self._pv_in_scope(assigned) else skipped).append(pv_id)
        return allowed, skipped

    async def export_bundle(self, snapshot_id: str) -> SnapshotBundleDTO | None:
        """A live snapshot with all its values, for another instance to import.

        Each value carries its PV's addresses, read from the PV row even when the PV
        has been deleted since the capture.

        Parameters
        ----------
        snapshot_id : str
            The snapshot to export.

        Returns
        -------
        SnapshotBundleDTO | None
            The bundle, or ``None`` when no live snapshot has ``snapshot_id`` or the key
            can't reach its facility (the same rule as ``get_by_id``).
        """
        snapshot = await self.snapshot_repo.get_by_id(snapshot_id)
        if snapshot is None or not self._in_facility_scope(snapshot.facility_id):
            return None
        values = [
            BundleValueDTO(
                pv_id=row.pv_id,
                pv_name=row.pv_name,
                setpoint_address=row.setpoint_address,
                readback_address=row.readback_address,
                config_address=row.config_address,
                setpoint_value=_parse_jsonb(row.setpoint_value),
                readback_value=_parse_jsonb(row.readback_value),
                status=row.status,
                severity=row.severity,
                timestamp=row.timestamp,
            )
            for row in await self.value_repo.get_export_rows(snapshot_id)
        ]
        return SnapshotBundleDTO(
            format=BUNDLE_FORMAT,
            version=1,
            source_instance=local_instance(),
            id=snapshot.id,
            title=snapshot.title,
            description=snapshot.description,
            created_date=snapshot.created_at,
            created_by=snapshot.created_by,
            facility=await self._facility_slug(snapshot.facility_id),
            values=values,
        )

    async def _importable_pv_ids(self, pv_ids: list[str], capture_scope: set[str] | None) -> set[str]:
        """The PV ids of a bundle that an import may keep values for.

        A PV must exist here and be this instance's (deleted PVs included: a mirror
        replicates its arms' PV rows, ids and all, but a snapshot's PVs share its owner,
        which is the importer), and the key must be able to restore it (``_pv_in_scope``).
        The new snapshot's export shows each value's PV addresses, so a value of a PV the
        key can't see would reveal it. For a bundle with a facility, the PV must also be
        assigned to that facility or one below it (``capture_scope``), as a capture into
        that facility would take it. Every rule is a condition in the loop below.
        """
        known = await self.pv_repo.get_existing_ids(pv_ids, include_deleted=True, owner=local_instance())
        needs_facilities = self.facility_scope is not None or capture_scope is not None
        assigned = await self.pv_repo.facility_ids_for_many(sorted(known)) if needs_facilities else {}
        importable = set()
        for pv_id in known:
            facility_ids = assigned.get(pv_id, set())
            if not self._pv_in_scope(facility_ids):
                continue
            if capture_scope is not None and not facility_ids & capture_scope:
                continue
            importable.add(pv_id)
        return importable

    async def import_bundle(
        self,
        bundle: SnapshotBundleDTO,
        *,
        captured: bool = False,
        created_by: str | None = None,
        strict: bool = False,
    ) -> SnapshotImportResultDTO:
        """Save a snapshot from a bundle exported or captured by another instance.

        Two modes:

        * **History** (the default): bring an old snapshot back, typically from a history
          mirror, so it can be restored here. The snapshot gets a new id and today's
          creation time, so pruning treats it as recent; its title is marked "Restored
          from history" and ``imported_from`` records the bundle's id.
        * **Captured**: save a snapshot another instance captured for this one, keeping
          the bundle's id, creation time, title and description, with ``created_by`` as
          the author; ``imported_from`` stays empty. It is idempotent on the id: if a
          snapshot with that id is already here (deleted or not), with the same facility
          and creation time, nothing is written and the answer has ``created`` false.
          That is checked first, so a resend is answered from what was saved even if
          the PVs or the caller's grant have changed since. ``strict`` refuses a bundle
          that would leave any value out.

        In both modes the snapshot belongs to this instance, and values are matched to
        PVs here by ``pv_id``, deleted PVs included: a history mirror replicates its
        arms' PV rows, ids and all. Each value keeps its capture ``timestamp``. Values
        whose PV isn't here or is another instance's, that the key couldn't restore, or
        that isn't assigned to the bundle's facility or one below it are skipped and
        listed alike, so the answer doesn't reveal another facility's PV. A PV moved out
        of that facility since the export is skipped too. Nothing is read from EPICS.
        The snapshot takes the bundle's facility, which must be this instance's and
        which the key must be able to write, as it would for a capture; a bundle without
        one is saved unassigned.

        Parameters
        ----------
        bundle : SnapshotBundleDTO
            The exported or captured snapshot.
        captured : bool
            Save it under its own id and time rather than as a new copy.
        created_by : str | None
            The author recorded for a captured snapshot; required with ``captured``.
        strict : bool
            Refuse, saving nothing, when any value would be skipped.

        Returns
        -------
        SnapshotImportResultDTO
            The snapshot's summary, the values left out, and whether this call saved it.

        Raises
        ------
        NothingToImport
            When the bundle has values and none of their PVs can be imported here.
        ImportWouldSkip
            With ``strict``, when any value would be skipped.
        SnapshotIdConflict
            When a captured bundle's id is taken by a snapshot with another owner, facility or time.
        FacilityNotFound
            When the bundle names a facility this instance doesn't have.
        NotOwner
            When the bundle's facility is another instance's.
        InsufficientFacilityAccess
            When the key can't write the bundle's facility.
        """
        if captured:
            if not created_by:
                raise ValueError("a captured import records its author; pass created_by")
            if (existing := await self.snapshot_repo.get_by_id(str(bundle.id), include_deleted=True)) is not None:
                return await self._already_saved(existing, bundle)

        facility_id = None
        if bundle.facility is not None:
            facility = await facilities_gateway.get_by_slug(bundle.facility)
            if facility is None:
                raise FacilityNotFound(f"Facility '{bundle.facility}' not found")
            require_create_owner(facility.owner_instance, None, f"Facility '{bundle.facility}'")
            facility_id = facility.id
        self._require_facility_for_create(facility_id, bundle.facility)

        capture_scope = await facilities_gateway.descendant_ids(facility_id) if facility_id is not None else None
        known = await self._importable_pv_ids(sorted({str(value.pv_id) for value in bundle.values}), capture_scope)
        rows: list[dict] = []
        skipped: list[SkippedValueDTO] = []
        for value in bundle.values:
            pv_id = str(value.pv_id)
            if pv_id not in known:
                skipped.append(SkippedValueDTO(pv_id=pv_id, pv_name=value.pv_name))
                continue
            rows.append(
                {
                    "pv_id": pv_id,
                    "pv_name": value.pv_name,
                    "setpoint_value": value.setpoint_value,
                    "readback_value": value.readback_value,
                    "status": value.status,
                    "severity": value.severity,
                    # JSON may carry a naive time; COPY needs an aware one for timestamptz.
                    "timestamp": to_utc(value.timestamp) if value.timestamp else None,
                }
            )
        if bundle.values and not rows:
            raise NothingToImport("None of the bundle's PVs can be imported on this instance; nothing to import")
        if strict and skipped:
            raise ImportWouldSkip(_describe_skipped(skipped))

        if captured:
            snapshot_id = str(bundle.id)
            fields = {
                "title": bundle.title,
                "description": bundle.description,
                "created_by": created_by,
                "created_at": to_utc(bundle.created_date),
            }
        else:
            snapshot_id = str(uuid4())
            fields = {
                "title": f"{IMPORTED_TITLE_PREFIX}{bundle.title}"[:255],
                "description": bundle.description,
                "created_by": bundle.created_by,
                "imported_from": str(bundle.id),
            }
        count = await self._save_snapshot(snapshot_id, facility_id=facility_id, rows=rows, **fields)
        if count is None:
            # Another delivery of the same captured snapshot saved it first.
            existing = await self.snapshot_repo.get_by_id(snapshot_id, include_deleted=True)
            return await self._already_saved(existing, bundle)
        logger.info(
            "Imported snapshot %s%s (%d values, %d skipped)",
            bundle.id,
            "" if captured else f" as {snapshot_id}",
            count,
            len(skipped),
        )
        snapshot = await self.snapshot_repo.get_by_id(snapshot_id)
        return SnapshotImportResultDTO(snapshot=await self._summary(snapshot, pv_count=count), skipped=skipped)

    async def _save_snapshot(
        self,
        snapshot_id: str,
        *,
        title: str,
        description: str | None,
        created_by: str | None,
        facility_id: str | None,
        rows: list[dict],
        created_at: datetime | None = None,
        imported_from: str | None = None,
        progress_callback: Callable | None = None,
    ) -> int | None:
        """Insert this instance's snapshot with a given id, and its values, unless that id is taken.

        ``created_at`` defaults to now. Returns the number of values written, or ``None``
        when a snapshot with ``snapshot_id`` already exists, in which case nothing is
        written. Two calls for one id can't both write: the second waits on the first's
        primary key, then finds it taken.
        """
        fields = {
            "id": snapshot_id,
            # Set here rather than by the column default, which this statement would fire
            # too: whatever saves a snapshot here saves it as this instance's.
            "owner_instance": local_instance(),
            "title": title,
            "description": description,
            "created_by": created_by,
            "facility_id": facility_id,
            "imported_from": imported_from,
        }
        if created_at is not None:
            fields["created_at"] = created_at
        if not await self.snapshot_repo.insert_unless_taken(fields):
            return None
        for row in rows:
            row["snapshot_id"] = snapshot_id
        return await bulk_insert_snapshot_values(self.session, rows, progress_callback=progress_callback)

    async def _already_saved(self, existing: Snapshot, bundle: SnapshotBundleDTO) -> SnapshotImportResultDTO:
        """The answer to a captured import whose id is already here: what was saved, unchanged.

        Raises
        ------
        SnapshotIdConflict
            When the saved snapshot is another instance's, or has another facility or
            creation time than the bundle.
        """
        if existing.owner_instance != local_instance():
            raise SnapshotIdConflict(f"Snapshot {existing.id} is already here, owned by '{existing.owner_instance}'")
        facility = await self._facility_slug(existing.facility_id)
        if facility != bundle.facility or existing.created_at != to_utc(bundle.created_date):
            raise SnapshotIdConflict(
                f"Snapshot {existing.id} is already saved here with facility {facility or 'none'} and creation time "
                f"{existing.created_at.isoformat()}, not this bundle's {bundle.facility or 'none'} and "
                f"{to_utc(bundle.created_date).isoformat()}"
            )
        saved = await self.snapshot_repo.get_value_pv_ids(existing.id)
        skipped = [
            SkippedValueDTO(pv_id=str(value.pv_id), pv_name=value.pv_name)
            for value in bundle.values
            if str(value.pv_id) not in saved
        ]
        logger.info("Snapshot %s is already saved; nothing imported", existing.id)
        return SnapshotImportResultDTO(
            snapshot=await self._summary(existing, pv_count=len(saved)), skipped=skipped, created=False
        )

    async def restore_snapshot(
        self,
        snapshot_id: str,
        request: RestoreRequestDTO | None = None,
        progress_callback: Callable | None = None,
    ) -> RestoreResultDTO:
        """Restore a snapshot's setpoints to EPICS (the hot write path).

        Setpoints only, never readbacks. Every hook derives its extra writes up front,
        before anything is written; a hook error drops that PV from the write set so the
        system is never left half-restored. The setpoints go out as one batch, then each
        hook's extras as a batch of its own, in configured order, restricted to the PVs
        whose setpoint and earlier hook writes all succeeded. A failed extra write marks
        its primary address failed, with ``phase`` naming that hook, and later hooks skip
        the PV.

        Parameters
        ----------
        snapshot_id : str
            The snapshot to restore.
        request : RestoreRequestDTO | None
            Restores only ``request.pv_ids`` when given, otherwise the whole snapshot.
        progress_callback : Callable | None
            Async sink called as ``(current, total, message, phases=...)``, where
            ``total`` is every EPICS write the restore will attempt: the setpoints plus
            all derived writes. Derived writes skipped because their setpoint or an
            earlier hook failed count as done, so the count always reaches ``total``.
            ``phases`` is sent on every call: each `RestorePhase` as a dict, Setpoints
            first, then one per hook in configured order.

        Returns
        -------
        RestoreResultDTO
            Totals plus a ``{pvId, pvName, error, phase}`` row per failed PV, where
            ``phase`` is ``"Setpoints"`` or the class name of the hook that failed
            (``MagnetTrimHook`` for the ``magnet_trim`` provider, and so on).
        """
        logger.info("Restoring snapshot: %s", snapshot_id)

        # A deleted snapshot keeps its values, so check first: one deleted after its restore
        # was queued restores nothing, the same as a snapshot that doesn't exist.
        if not await self.snapshot_repo.is_live(snapshot_id):
            return RestoreResultDTO(total_pvs=0, success_count=0, failure_count=0, failures=[])

        if request is not None and request.pv_ids is not None:
            values = await self.value_repo.get_by_snapshot_and_pvs(snapshot_id, request.pv_ids)
        else:
            values = await self.value_repo.get_by_snapshot(snapshot_id)
        return await self.restore_values(values, progress_callback=progress_callback)

    async def restore_values(
        self,
        values: list[Any],
        progress_callback: Callable | None = None,
    ) -> RestoreResultDTO:
        """Restore stored values' setpoints to EPICS: ``restore_snapshot`` without the load.

        ``values`` are rows with a ``pv_id`` and a ``setpoint_value`` (the stored JSONB
        blob, or its text): a snapshot's values here, or a bundle's adapted to that
        shape for one that isn't saved here yet. Everything else is as
        ``restore_snapshot`` describes.
        """
        start_time = datetime.now(UTC)
        if not values:
            return RestoreResultDTO(total_pvs=0, success_count=0, failure_count=0, failures=[])

        pv_ids = [v.pv_id for v in values]
        # Live PVs only: a PV deleted since the capture is never written, even when named.
        pvs = await self.pv_repo.get_by_ids(pv_ids)
        pv_map = {pv.id: pv for pv in pvs}

        # Build the write set — only setpoints, never readbacks.
        values_to_write: dict[str, Any] = {}
        pv_id_by_address: dict[str, str] = {}
        for val in values:
            pv = pv_map.get(val.pv_id)
            if not pv:
                continue
            if pv.setpoint_address and val.setpoint_value:
                setpoint_data = _parse_jsonb(val.setpoint_value)
                write_value = setpoint_data.get("value") if setpoint_data else None
                if write_value is not None:
                    values_to_write[pv.setpoint_address] = write_value
                    pv_id_by_address[pv.setpoint_address] = pv.id

        total_pvs = len(values_to_write)
        logger.info("Writing %d PV values", total_pvs)

        derived_per_hook: list[dict[str, list[tuple[str, Any]]]] = []
        hook_errors: dict[str, str] = {}
        for hook in self.restore_hooks:
            derived: dict[str, list[tuple[str, Any]]] = {}
            for addr, value in values_to_write.items():
                if addr in hook_errors:
                    continue
                try:
                    extras = hook.derive_extra_writes(addr, value)
                except RestoreHookError as e:
                    hook_errors[addr] = str(e)
                    continue
                if extras:
                    derived[addr] = extras
            derived_per_hook.append(derived)

        original_values = {addr: value for addr, value in values_to_write.items() if addr not in hook_errors}

        total_writes = len(original_values) + sum(
            len(extras) for derived in derived_per_hook for extras in derived.values()
        )

        setpoints = RestorePhase(PRIMARY_PHASE, total=total_pvs)
        phases = [setpoints] + [
            RestorePhase(type(hook).__name__, total=sum(len(extras) for extras in derived.values()))
            for hook, derived in zip(self.restore_hooks, derived_per_hook, strict=True)
        ]

        async def report(current: int, message: str) -> None:
            if progress_callback:
                await progress_callback(current, total_writes, message, phases=_phase_dicts(phases))

        await report(0, f"Starting restore of {total_pvs:,} PVs")

        # A PV a hook couldn't derive from is never written: it finishes the Setpoints
        # phase as a failure the moment that phase starts.
        setpoints.status = "running"
        setpoints.current = setpoints.failures = len(hook_errors)

        results: dict[str, tuple[bool, str | None]] = {}
        failure_phase: dict[str, str] = {}
        if original_values:
            if progress_callback:
                results.update(
                    await self.epics.put_many_with_progress(
                        original_values, _sub_batch_progress(progress_callback, 0, total_writes, phases, setpoints)
                    )
                )
            else:
                results.update(await self.epics.put_many(original_values))
        writes_done = len(original_values)

        for address, (success, _error) in results.items():
            if not success:
                failure_phase[address] = PRIMARY_PHASE
                setpoints.failures += 1
        setpoints.current = setpoints.total
        setpoints.status = "completed" if setpoints.total else "skipped"

        for derived, phase in zip(derived_per_hook, phases[1:], strict=True):
            extra_values: dict[str, Any] = {}
            primary_by_extra: dict[str, str] = {}
            derived_writes = 0
            for primary_addr, extras in derived.items():
                derived_writes += len(extras)
                # A later hook's derivation error kept this PV out of the write set, so
                # it has no result; its extras are skipped like a failed setpoint's.
                if primary_addr in hook_errors or not results[primary_addr][0]:
                    continue
                for extra_addr, extra_val in extras:
                    extra_values[extra_addr] = extra_val
                    primary_by_extra[extra_addr] = primary_addr

            writes_done += derived_writes - len(extra_values)
            phase.total = len(extra_values)
            if not extra_values:
                phase.status = "skipped"
                continue

            phase.status = "running"
            if progress_callback:
                await report(writes_done, f"Writing {len(extra_values):,} {phase.name} values...")
                hook_results = await self.epics.put_many_with_progress(
                    extra_values, _sub_batch_progress(progress_callback, writes_done, total_writes, phases, phase)
                )
            else:
                hook_results = await self.epics.put_many(extra_values)
            writes_done += len(extra_values)

            failed_primaries: set[str] = set()
            for extra_addr, (ok, err) in hook_results.items():
                if ok:
                    continue
                primary_addr = primary_by_extra[extra_addr]
                results[primary_addr] = (
                    False,
                    f"Primary write succeeded but {phase.name} write failed for {extra_addr}: {err or 'unknown error'}",
                )
                failure_phase[primary_addr] = phase.name
                failed_primaries.add(primary_addr)
            phase.current = phase.total
            phase.failures = len(failed_primaries)
            phase.status = "completed"

        for addr, err in hook_errors.items():
            results[addr] = (False, err)
            failure_phase[addr] = PRIMARY_PHASE

        failures = []
        success_count = 0
        for address, (success, error) in results.items():
            if success:
                success_count += 1
            else:
                pv_id = pv_id_by_address.get(address, "")
                failures.append(
                    {
                        "pvId": pv_id,
                        "pvName": address,
                        "error": error or "Unknown error",
                        "phase": failure_phase.get(address, PRIMARY_PHASE),
                    }
                )

        await report(total_writes, f"{success_count:,}/{total_pvs:,} PVs restored")
        logger.info(
            "Restore completed in %.2fs (%d success, %d failures)",
            (datetime.now(UTC) - start_time).total_seconds(),
            success_count,
            len(failures),
        )

        return RestoreResultDTO(
            total_pvs=total_pvs,
            success_count=success_count,
            failure_count=len(failures),
            failures=failures,
        )

    async def compare_snapshots(self, snapshot1_id: str, snapshot2_id: str) -> ComparisonResultDTO:
        """Compare two stored snapshots (setpoint + readback) under each PV's tolerances.

        Loads every value row for *both* snapshots into memory (no pagination) and runs on
        the request path, so comparing two very large (~100k-PV) snapshots is a sizeable
        synchronous allocation. Fine at current scale; a known v1 limit to revisit
        (stream/paginate the diff) if snapshot sizes grow.
        """
        result1 = await self.snapshot_repo.get_with_values(snapshot1_id)
        result2 = await self.snapshot_repo.get_with_values(snapshot2_id)
        if not result1 or not result2:
            raise SnapshotNotFound("One or both snapshots not found")
        snap1, values1_list = result1
        snap2, values2_list = result2
        if not self._in_facility_scope(snap1.facility_id) or not self._in_facility_scope(snap2.facility_id):
            # Same message/shape as a genuine not-found — an out-of-scope snapshot must
            # not be distinguishable from one that doesn't exist.
            raise SnapshotNotFound("One or both snapshots not found")

        values1 = {v.pv_id: v for v in values1_list}
        values2 = {v.pv_id: v for v in values2_list}

        all_pv_ids = set(values1.keys()) | set(values2.keys())
        pvs = await self.pv_repo.get_by_ids(list(all_pv_ids), include_deleted=True)
        pv_map = {pv.id: pv for pv in pvs}

        differences = []
        match_count = 0
        for pv_id in all_pv_ids:
            val1 = values1.get(pv_id)
            val2 = values2.get(pv_id)
            pv = pv_map.get(pv_id)

            setpoint1 = _parse_jsonb(val1.setpoint_value) if val1 else None
            setpoint2 = _parse_jsonb(val2.setpoint_value) if val2 else None
            v1 = setpoint1.get("value") if setpoint1 else None
            v2 = setpoint2.get("value") if setpoint2 else None
            readback1 = _parse_jsonb(val1.readback_value) if val1 else None
            readback2 = _parse_jsonb(val2.readback_value) if val2 else None
            r1 = readback1.get("value") if readback1 else None
            r2 = readback2.get("value") if readback2 else None

            abs_tol = pv.abs_tolerance if pv else 0
            rel_tol = pv.rel_tolerance if pv else 0

            has_setpoint = v1 is not None or v2 is not None
            has_readback = r1 is not None or r2 is not None

            setpoint_within = self._values_within_tolerance(v1, v2, abs_tol, rel_tol) if has_setpoint else True
            readback_within = self._values_within_tolerance(r1, r2, abs_tol, rel_tol) if has_readback else True

            if setpoint_within and readback_within:
                match_count += 1
            else:
                # Report the pair that is actually out of tolerance (prefer setpoint when
                # both differ) rather than always emitting the setpoint values — otherwise
                # a readback-only difference reports two equal setpoints.
                if not setpoint_within:
                    out_v1, out_v2 = v1, v2
                else:
                    out_v1, out_v2 = r1, r2
                differences.append(
                    {
                        "pvId": pv_id,
                        "pvName": val1.pv_name if val1 else (val2.pv_name if val2 else ""),
                        "value1": out_v1,
                        "value2": out_v2,
                        "withinTolerance": False,
                    }
                )

        return ComparisonResultDTO(
            snapshot1_id=snapshot1_id,
            snapshot2_id=snapshot2_id,
            differences=differences,
            match_count=match_count,
            difference_count=len(differences),
        )

    @staticmethod
    def _values_within_tolerance(v1: Any, v2: Any, abs_tol: float, rel_tol: float) -> bool:
        """Absolute tolerance first, then relative (vs. ``|v1|``); non-numeric → exact match.

        Waveform (list) values are compared element-wise under the same rule;
        mismatched lengths are always "different" rather than raising.
        """
        if v1 is None and v2 is None:
            return True
        if v1 is None or v2 is None:
            return False
        if isinstance(v1, list) or isinstance(v2, list):
            if not (isinstance(v1, list) and isinstance(v2, list)):
                return False
            if len(v1) != len(v2):
                return False
            return all(SnapshotService._scalar_within_tolerance(e1, e2, abs_tol, rel_tol) for e1, e2 in zip(v1, v2))
        return SnapshotService._scalar_within_tolerance(v1, v2, abs_tol, rel_tol)

    @staticmethod
    def _scalar_within_tolerance(v1: Any, v2: Any, abs_tol: float, rel_tol: float) -> bool:
        try:
            f1, f2 = float(v1), float(v2)
            diff = abs(f1 - f2)
            if diff <= abs_tol:
                return True
            if rel_tol > 0 and f1 != 0:
                if diff / abs(f1) <= rel_tol:
                    return True
            return False
        except (TypeError, ValueError):
            return v1 == v2

    async def delete_snapshot(self, snapshot_id: str) -> bool:
        """Delete a snapshot by setting ``deleted_at``; its values stay. ``False`` when there
        is no live snapshot to delete."""
        snapshot = await self.snapshot_repo.get_by_id(snapshot_id)
        if not snapshot:
            return False
        await self._require_facility_write(snapshot.facility_id, "this snapshot")
        require_owned(snapshot.owner_instance, f"Snapshot {snapshot_id}")
        return await self.snapshot_repo.soft_delete(snapshot_id)
