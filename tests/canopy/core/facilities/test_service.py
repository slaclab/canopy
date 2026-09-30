"""Tests for FacilityService — hierarchy resolution and CRUD guards."""

from canopy.core.errors import (
    FacilityInUse,
    FacilityNotFound,
    FacilityHasChildren,
    FacilitySlugConflict,
)
from canopy.core.facilities.models import Facility
from canopy.core.facilities.schemas import FacilityCreate
from canopy.core.facilities.service import FacilityService, register_in_use_checker


async def _seed_facility(session, slug: str, parent_id: str | None = None) -> Facility:
    facility = Facility(slug=slug, name=slug.upper(), parent_id=parent_id)
    session.add(facility)
    await session.flush()
    return facility


async def test_resolve_facility_scope_leaf_slug_returns_only_itself(session, session_maker):
    root = await _seed_facility(session, "root")
    leaf = await _seed_facility(session, "leaf", parent_id=root.id)
    await session.commit()

    service = FacilityService(session)
    scope = await service.resolve_facility_scope("leaf")
    assert scope == {leaf.id}


async def test_resolve_facility_scope_mid_tree_returns_descendants(session, session_maker):
    root = await _seed_facility(session, "lcls")
    sc = await _seed_facility(session, "lcls-sc", parent_id=root.id)
    nc = await _seed_facility(session, "lcls-nc", parent_id=root.id)
    leaf = await _seed_facility(session, "lcls-sc-leaf", parent_id=sc.id)
    await session.commit()

    service = FacilityService(session)
    scope = await service.resolve_facility_scope("lcls-sc")
    assert scope == {sc.id, leaf.id}
    assert nc.id not in scope


async def test_resolve_facility_scope_root_slug_returns_everything_beneath(session, session_maker):
    root = await _seed_facility(session, "top")
    sc = await _seed_facility(session, "top-sc", parent_id=root.id)
    nc = await _seed_facility(session, "top-nc", parent_id=root.id)
    await session.commit()

    service = FacilityService(session)
    scope = await service.resolve_facility_scope("top")
    assert scope == {root.id, sc.id, nc.id}


async def test_resolve_facility_scope_unknown_slug_returns_empty_set_not_none(session, session_maker):
    service = FacilityService(session)
    scope = await service.resolve_facility_scope("does-not-exist")
    assert scope == set()
    assert scope is not None


async def test_resolve_facility_scope_none_means_no_filter(session, session_maker):
    service = FacilityService(session)
    scope = await service.resolve_facility_scope(None)
    assert scope is None


async def test_resolve_facility_scope_no_parent_facility(session, session_maker):
    solo = await _seed_facility(session, "solo")
    await session.commit()

    service = FacilityService(session)
    scope = await service.resolve_facility_scope("solo")
    assert scope == {solo.id}


async def test_create_facility_rejects_duplicate_slug(session, session_maker):
    service = FacilityService(session)
    await service.create(FacilityCreate(slug="dupe", name="Dupe"))
    try:
        await service.create(FacilityCreate(slug="dupe", name="Dupe Again"))
        assert False, "expected FacilitySlugConflict"
    except FacilitySlugConflict:
        pass


async def test_create_facility_rejects_unknown_parent(session, session_maker):
    service = FacilityService(session)
    try:
        await service.create(
            FacilityCreate(slug="orphan", name="Orphan", parent_id="11111111-1111-1111-1111-111111111111")
        )
        assert False, "expected FacilityNotFound"
    except FacilityNotFound:
        pass


async def test_delete_blocked_by_children(session, session_maker):
    parent = await _seed_facility(session, "parent")
    await _seed_facility(session, "child", parent_id=parent.id)
    await session.commit()

    service = FacilityService(session)
    try:
        await service.delete(parent.id)
        assert False, "expected FacilityHasChildren"
    except FacilityHasChildren:
        pass


async def test_delete_blocked_by_in_use_checker(session, session_maker):
    facility = await _seed_facility(session, "in-use")
    await session.commit()

    async def always_in_use(facility_id: str) -> bool:
        return facility_id == facility.id

    register_in_use_checker(always_in_use)
    try:
        service = FacilityService(session)
        try:
            await service.delete(facility.id)
            assert False, "expected FacilityInUse"
        except FacilityInUse:
            pass
    finally:
        from canopy.core.facilities import service as service_module

        service_module._in_use_checkers.remove(always_in_use)


async def test_delete_succeeds_when_unused(session, session_maker):
    facility = await _seed_facility(session, "unused")
    await session.commit()

    service = FacilityService(session)
    assert await service.delete(facility.id) is True
