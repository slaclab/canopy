"""Facility repository: hierarchy CRUD + descendant-set resolution.

Owns no notion of "is this facility in use" — references live in other modules'
schemas, which this repository can't reach without a cross-schema query. See
``service.py``'s ``_in_use_checkers`` hook for how that check is composed back in.
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from canopy.core.db import BaseRepository
from canopy.core.facilities.models import Facility


class FacilityRepository(BaseRepository[Facility]):
    """Repository for Facility operations, including hierarchy resolution."""

    def __init__(self, session: AsyncSession):
        super().__init__(Facility, session)

    async def get_by_slug(self, slug: str) -> Facility | None:
        result = await self.session.execute(select(Facility).where(Facility.slug == slug))
        return result.scalar_one_or_none()

    async def get_children(self, facility_id: str) -> list[Facility]:
        result = await self.session.execute(select(Facility).where(Facility.parent_id == facility_id))
        return list(result.scalars().all())

    async def existing_slugs(self, slugs: list[str]) -> set[str]:
        """The subset of ``slugs`` that exist, in one query."""
        if not slugs:
            return set()
        result = await self.session.execute(select(Facility.slug).where(Facility.slug.in_(slugs)))
        return {row[0] for row in result.all()}

    async def ids_by_slug(self, slugs: list[str] | set[str]) -> dict[str, str]:
        """``{slug: id}`` for the subset of ``slugs`` that exist, in one query. An
        unknown slug is simply absent from the result — the caller decides whether
        that's fatal."""
        if not slugs:
            return {}
        result = await self.session.execute(select(Facility.slug, Facility.id).where(Facility.slug.in_(slugs)))
        return {slug: id_ for slug, id_ in result.all()}

    async def slugs_by_id(self, ids: list[str] | set[str]) -> dict[str, str]:
        """``{id: slug}`` for the subset of ``ids`` that exist, in one query."""
        if not ids:
            return {}
        result = await self.session.execute(select(Facility.id, Facility.slug).where(Facility.id.in_(ids)))
        return {id_: slug for id_, slug in result.all()}

    async def resolve_facility_scope(self, slug: str) -> set[str] | None:
        """Slug -> {facility_id, all descendant facility_ids}. ``None`` if the slug
        is unknown — distinct from ``set()``, which means "known slug, no facilities
        currently match" (not reachable for a single root, but kept for symmetry with
        callers that intersect multiple scopes).

        A recursive CTE over ``parent_id`` starting at the named facility.
        """
        root = await self.get_by_slug(slug)
        if root is None:
            return None
        return await self.descendant_ids(root.id)

    async def descendant_ids(self, facility_id: str) -> set[str]:
        """``{facility_id, all descendant facility_ids}``, by id rather than slug."""
        hierarchy = select(Facility.id).where(Facility.id == facility_id).cte("facility_scope", recursive=True)
        children = select(Facility.id).join(hierarchy, Facility.parent_id == hierarchy.c.id)
        hierarchy = hierarchy.union_all(children)

        result = await self.session.execute(select(hierarchy.c.id))
        return {row[0] for row in result.all()}
