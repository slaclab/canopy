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

# Async callables `(facility_id) -> bool`, each answering "is this facility still
# referenced by module X's data?" Modules that reference a facility by opaque id
# (today, only Squirrel's `pv_facility`/`Snapshot.facility_id`) register one at
# startup via `register_in_use_checker`, so `delete()` can enforce the same
# "can't delete a facility still in use" guard it did when Facility lived in
# Squirrel, without core taking on a dependency on Squirrel's models.
_in_use_checkers: list[Callable[[str], Awaitable[bool]]] = []


def register_in_use_checker(checker: Callable[[str], Awaitable[bool]]) -> None:
    _in_use_checkers.append(checker)


class FacilityService:
    """Facility hierarchy management. Owns no transaction — the caller commits."""

    def __init__(self, session: AsyncSession):
        self.session = session
        self.facility_repo = FacilityRepository(session)

    def _to_dto(self, facility: Facility) -> FacilityDTO:
        return FacilityDTO(
            id=facility.id,
            slug=facility.slug,
            name=facility.name,
            description=facility.description,
            parent_id=facility.parent_id,
        )

    async def get_all(self) -> list[FacilityDTO]:
        """Full hierarchy, flat — unfiltered by key. Facility existence isn't a
        secret, only the data is filtered."""
        facilities = await self.facility_repo.get_all()
        return [self._to_dto(f) for f in facilities]

    async def get_by_id(self, facility_id: str) -> FacilityDTO | None:
        facility = await self.facility_repo.get_by_id(facility_id)
        return self._to_dto(facility) if facility else None

    async def get_by_slug(self, slug: str) -> FacilityDTO | None:
        facility = await self.facility_repo.get_by_slug(slug)
        return self._to_dto(facility) if facility else None

    async def create(self, data: FacilityCreate) -> FacilityDTO:
        if await self.facility_repo.get_by_slug(data.slug):
            raise FacilitySlugConflict(f"Facility with slug '{data.slug}' already exists")
        if data.parent_id is not None and not await self.facility_repo.get_by_id(data.parent_id):
            raise FacilityNotFound(f"Parent facility {data.parent_id} not found")
        facility = Facility(slug=data.slug, name=data.name, description=data.description, parent_id=data.parent_id)
        try:
            facility = await self.facility_repo.create(facility)
        except IntegrityError as e:
            raise FacilitySlugConflict(f"Facility with slug '{data.slug}' already exists") from e
        return self._to_dto(facility)

    async def update(self, facility_id: str, data: FacilityUpdate) -> FacilityDTO | None:
        facility = await self.facility_repo.get_by_id(facility_id)
        if not facility:
            return None
        if data.parent_id is not None:
            if data.parent_id == facility_id:
                raise FacilityHasChildren("A facility cannot be its own parent")
            if not await self.facility_repo.get_by_id(data.parent_id):
                raise FacilityNotFound(f"Parent facility {data.parent_id} not found")
            facility.parent_id = data.parent_id
        if data.name is not None:
            facility.name = data.name
        if data.description is not None:
            facility.description = data.description
        facility = await self.facility_repo.update(facility)
        return self._to_dto(facility)

    async def delete(self, facility_id: str) -> bool:
        facility = await self.facility_repo.get_by_id(facility_id)
        if not facility:
            return False
        if await self.facility_repo.get_children(facility_id):
            raise FacilityHasChildren("Cannot delete a facility that has children; reparent or remove them first")
        for checker in _in_use_checkers:
            if await checker(facility_id):
                raise FacilityInUse("Cannot delete a facility that still has PVs or snapshots assigned to it")
        await self.facility_repo.delete(facility)
        return True

    async def resolve_facility_scope(self, slug: str | None) -> set[str] | None:
        """Slug -> {facility_id, all descendant facility_ids}.

        ``slug=None`` means "no filter requested" and returns ``None``, so the caller
        applies no facility filter. A given-but-unknown slug returns ``set()`` —
        "filter requested, nothing matches" — never ``None``, which would leak every
        facility's data for a typo'd slug. Callers must test ``is not None``, not
        truthiness.
        """
        if slug is None:
            return None
        result = await self.facility_repo.resolve_facility_scope(slug)
        return result if result is not None else set()

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
