"""Facility owners: stamped on create, written only by their owner, one owner per tree.

Rows another instance owns are created inside ``as_instance("arm2")``, the way a
mirror receives an arm's rows through replication.
"""

import pytest

from canopy.core.errors import NotOwner, OwnerMismatch, FacilitySlugConflict
from canopy.core.facilities.models import Facility
from canopy.core.facilities.schemas import FacilityCreate, FacilityUpdate
from canopy.core.facilities.service import FacilityService


async def _seed(session, as_instance, slug: str, owner: str, parent_id: str | None = None) -> Facility:
    with as_instance(owner):
        facility = Facility(slug=slug, name=slug.upper(), parent_id=parent_id)
        session.add(facility)
        await session.flush()
    return facility


async def test_create_stamps_this_instance(session, session_maker):
    created = await FacilityService(session).create(FacilityCreate(slug="site", name="Site"))

    assert created.owner_instance == "arm1"


async def test_a_root_facility_takes_the_requested_owner(session, session_maker):
    service = FacilityService(session)

    assert (await service.create(FacilityCreate(slug="here", name="Here"), owner="arm1")).owner_instance == "arm1"
    with pytest.raises(NotOwner, match="'arm2'"):
        await service.create(FacilityCreate(slug="there", name="There"), owner="arm2")


async def test_a_child_belongs_to_its_parents_owner(session, session_maker, as_instance):
    await _seed(session, as_instance, "remote", "arm2")

    with pytest.raises(NotOwner, match="'arm2'"):
        await FacilityService(session).create(FacilityCreate(slug="remote-child", name="Child", parent="remote"))


async def test_a_requested_owner_must_match_the_parents(session, session_maker):
    service = FacilityService(session)
    await service.create(FacilityCreate(slug="local", name="Local"))

    with pytest.raises(OwnerMismatch):
        await service.create(FacilityCreate(slug="local-child", name="Child", parent="local"), owner="arm2")


async def test_another_owners_facility_cant_be_edited_or_deleted(session, session_maker, as_instance):
    remote = await _seed(session, as_instance, "remote", "arm2")
    service = FacilityService(session)

    with pytest.raises(NotOwner):
        await service.update(remote.id, FacilityUpdate(name="Renamed"))
    with pytest.raises(NotOwner):
        await service.delete(remote.id)


async def test_a_facility_cant_move_under_another_owners(session, session_maker, as_instance):
    await _seed(session, as_instance, "remote", "arm2")
    service = FacilityService(session)
    local = await service.create(FacilityCreate(slug="local", name="Local"))

    with pytest.raises(OwnerMismatch):
        await service.update(local.id, FacilityUpdate(parent="remote"))


async def test_slugs_stay_unique_across_owners(session, session_maker, as_instance):
    await _seed(session, as_instance, "shared", "arm2")

    with pytest.raises(FacilitySlugConflict):
        await FacilityService(session).create(FacilityCreate(slug="shared", name="Shared"))
