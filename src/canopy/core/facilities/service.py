"""Facility domain service: hierarchy CRUD + the one place descendant expansion happens.

Both request-level ``?facility=`` filtering and key-grant resolution go through
``resolve_facility_scope``, so "itself plus descendants" is defined once.
"""

from __future__ import annotations

from collections.abc import Callable, Awaitable

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from canopy.core.errors import (
    FacilityInUse,
    FacilityNotFound,
    FacilityHasChildren,
    FacilitySlugConflict,
)
from canopy.core.modules import WILDCARD
from canopy.core.facilities.models import Facility
from canopy.core.facilities.schemas import FacilityDTO, FacilityCreate, FacilityUpdate
from canopy.core.facilities.repositories import FacilityRepository

# Async `(facility_id, slug) -> bool` callables, each answering "is this facility still
# referenced by my data?", paired with a label naming what the operator has to go
# unassign. Registered at startup so `delete()` can enforce its guard without core
# importing any referencing module's models. Both the id and the slug are passed because
# references come in both forms: module data stores ids, key grants store slugs.
_in_use_checkers: list[tuple[Callable[[str, str], Awaitable[bool]], str]] = []


def register_in_use_checker(checker: Callable[[str, str], Awaitable[bool]], what: str) -> None:
    """``what`` completes "Cannot delete a facility that still has ``<what>`` assigned
    to it" — e.g. ``"PVs or snapshots"``."""
    _in_use_checkers.append((checker, what))


class FacilityService:
    """Facility hierarchy management. Owns no transaction — the caller commits."""

    def __init__(self, session: AsyncSession):
        self.session = session
        self.facility_repo = FacilityRepository(session)

    def _to_dto(self, facility: Facility, parent_slug: str | None) -> FacilityDTO:
        return FacilityDTO(
            id=facility.id,
            slug=facility.slug,
            name=facility.name,
            description=facility.description,
            parent=parent_slug,
        )

    async def _with_parent(self, facility: Facility) -> FacilityDTO:
        """``_to_dto`` for a single row, looking up its parent's slug."""
        parent = await self.facility_repo.get_by_id(facility.parent_id) if facility.parent_id else None
        return self._to_dto(facility, parent.slug if parent else None)

    async def get_all(self) -> list[FacilityDTO]:
        """Full hierarchy, flat — unfiltered by key. Facility existence isn't a
        secret, only the data is filtered."""
        facilities = await self.facility_repo.get_all()
        # Every parent is in the same list, so no extra query is needed.
        slug_by_id = {f.id: f.slug for f in facilities}
        return [self._to_dto(f, slug_by_id.get(f.parent_id)) for f in facilities]

    async def get_by_id(self, facility_id: str) -> FacilityDTO | None:
        facility = await self.facility_repo.get_by_id(facility_id)
        return await self._with_parent(facility) if facility else None

    async def get_by_slug(self, slug: str) -> FacilityDTO | None:
        facility = await self.facility_repo.get_by_slug(slug)
        return await self._with_parent(facility) if facility else None

    async def _resolve_parent_id(self, parent_slug: str) -> str:
        """Parent *slug* -> its id. Unknown slug is a hard ``FacilityNotFound``."""
        parent = await self.facility_repo.get_by_slug(parent_slug)
        if not parent:
            raise FacilityNotFound(f"Parent facility '{parent_slug}' not found")
        return parent.id

    async def known_slugs(self, slugs: list[str]) -> set[str]:
        """The subset of ``slugs`` that exist. One query, not one per slug — callers
        reach this through the gateway, which opens a session per call."""
        return await self.facility_repo.existing_slugs(slugs)

    async def ids_by_slug(self, slugs: list[str] | set[str]) -> dict[str, str]:
        """``{slug: id}`` for the subset of ``slugs`` that exist. One query, not one
        per slug — callers that also need to reject unknown slugs get every unknown
        one back at once (``slugs - result.keys()``) rather than discovering them one
        at a time."""
        return await self.facility_repo.ids_by_slug(slugs)

    async def slugs_by_id(self, ids: list[str] | set[str]) -> dict[str, str]:
        """``{id: slug}`` for the subset of ``ids`` that exist. The reverse of
        ``ids_by_slug``, for turning stored ids back into slugs on the way out."""
        return await self.facility_repo.slugs_by_id(ids)

    async def create(self, data: FacilityCreate) -> FacilityDTO:
        if await self.facility_repo.get_by_slug(data.slug):
            raise FacilitySlugConflict(f"Facility with slug '{data.slug}' already exists")
        parent_id = await self._resolve_parent_id(data.parent) if data.parent is not None else None
        facility = Facility(slug=data.slug, name=data.name, description=data.description, parent_id=parent_id)
        try:
            facility = await self.facility_repo.create(facility)
        except IntegrityError as e:
            raise FacilitySlugConflict(f"Facility with slug '{data.slug}' already exists") from e
        return self._to_dto(facility, data.parent)

    async def update(self, facility_id: str, data: FacilityUpdate) -> FacilityDTO | None:
        facility = await self.facility_repo.get_by_id(facility_id)
        if not facility:
            return None
        if data.parent is not None:
            parent_id = await self._resolve_parent_id(data.parent)
            if parent_id == facility_id:
                raise FacilityHasChildren("A facility cannot be its own parent")
            facility.parent_id = parent_id
        if data.name is not None:
            facility.name = data.name
        if data.description is not None:
            facility.description = data.description
        facility = await self.facility_repo.update(facility)
        return await self._with_parent(facility)

    async def delete(self, facility_id: str) -> bool:
        facility = await self.facility_repo.get_by_id(facility_id)
        if not facility:
            return False
        if await self.facility_repo.get_children(facility_id):
            raise FacilityHasChildren("Cannot delete a facility that has children; reparent or remove them first")
        for checker, what in _in_use_checkers:
            if await checker(facility_id, facility.slug):
                raise FacilityInUse(f"Cannot delete a facility that still has {what} assigned to it")
        await self.facility_repo.delete(facility)
        return True

    async def descendant_ids(self, facility_id: str) -> set[str]:
        """``{facility_id, all descendant facility_ids}``. Empty for an unknown id."""
        return await self.facility_repo.descendant_ids(facility_id)

    async def resolve_facility_scope(self, slug: str | None) -> set[str] | None:
        """Slug -> {facility_id, all descendant facility_ids}.

        ``slug=None`` means "no filter requested" and returns ``None``, so the caller
        applies no facility filter. A given-but-unknown slug raises ``FacilityNotFound``
        rather than degrading to "no filter", which would leak every facility's data for
        a typo'd slug. Facility existence isn't a secret (``GET /v1/facilities`` lists
        them all), so the 404 reveals nothing.
        """
        if slug is None:
            return None
        result = await self.facility_repo.resolve_facility_scope(slug)
        if result is None:
            raise FacilityNotFound(f"Facility '{slug}' not found")
        return result

    async def resolve_key_facility_scope(self, slugs: frozenset[str]) -> set[str] | None:
        """A credential's granted slugs -> the union of each one's descendant id set.

        Returns ``None`` — "unrestricted, apply no facility filter" — when the
        grant holds the wildcard, using the same sentinel convention as
        ``resolve_facility_scope`` so the two compose by plain intersection.

        An empty grant yields ``set()``: authenticated, but no facility's data — a
        meaningful provisioned-but-unassigned state, not an error.

        An unknown granted slug contributes nothing rather than raising. Grants are
        validated at mint time and a granted facility can't be deleted, so this is a
        backstop for rows predating either guard.
        """
        if WILDCARD in slugs:
            return None
        scope: set[str] = set()
        for slug in slugs:
            if (resolved := await self.facility_repo.resolve_facility_scope(slug)) is not None:
                scope |= resolved
        return scope
