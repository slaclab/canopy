"""Finding a routed write's owner in the head's copy (Postgres, the ``database`` fixture).

Rows are made the way the head receives them: each under its owner's name
(``as_instance``), as replication brings an arm's rows with their ``owner_instance``.
"""

from uuid import uuid4

import pytest
import pytest_asyncio

from canopy.core import db
from canopy.core.errors import FacilitySlugConflict
from canopy.magpie.data.models import Config
from canopy.squirrel.data.models import Pv, Snapshot, TagGroup, pv_facility
from canopy.core.facilities.models import Facility
from canopy.core.forwarding.owners import owner_of, require_unused, snapshot_is_live
from canopy.core.forwarding.routes import ROUTES, match

ROUTED = {(route.method, route.path): route for route in ROUTES}


@pytest_asyncio.fixture
async def rows(database, head, as_instance):
    """For each of ``head``, ``arm1`` and ``arm2``: a facility, a PV in it, a snapshot of it, a tag group, a config."""
    made = {}
    for owner in ("head", "arm1", "arm2"):
        with as_instance(owner):
            async with db.get_session_maker()() as session:
                facility = Facility(slug=f"{owner}-site", name=owner)
                pv = Pv(setpoint_address=f"{owner.upper()}:PV", readback_address="SHARED:RBV")
                group = TagGroup(name="Area")
                config = Config(slug=f"{owner}cfg", app="trace", data={})
                session.add_all([facility, pv, group, config])
                await session.flush()
                snapshot = Snapshot(title="S", facility_id=facility.id)
                session.add(snapshot)
                await session.execute(pv_facility.insert().values(pv_id=pv.id, facility_id=facility.id))
                await session.commit()
                made[owner] = {
                    "facility": facility.slug,
                    "pv": pv.id,
                    "snapshot": snapshot.id,
                    "group": group.id,
                    "config": config.slug,
                }
    return made


async def _owner(method: str, path: str, body: object = None, *, requested: str | None = None) -> str | None:
    route, params = match(method, path)
    return await owner_of(route, params, body, requested=requested, local="head")


class TestRows:
    @pytest.mark.parametrize("owner", ["head", "arm1", "arm2"])
    async def test_each_row_routes_to_its_owner(self, rows, owner):
        mine = rows[owner]
        assert await _owner("DELETE", f"/squirrel/v1/snapshots/{mine['snapshot']}") == owner
        assert await _owner("POST", f"/squirrel/v1/snapshots/{mine['snapshot']}/restore") == owner
        assert await _owner("PUT", f"/squirrel/v1/pvs/{mine['pv']}") == owner
        assert await _owner("PATCH", f"/squirrel/v1/pvs/meta/{mine['pv']}") == owner
        assert await _owner("DELETE", f"/squirrel/v1/tags/{mine['group']}/tags/{uuid4()}") == owner
        assert await _owner("PUT", f"/magpie/v1/configs/trace/{mine['config']}") == owner
        assert await _owner("DELETE", f"/v1/facilities/{mine['facility']}") == owner

    @pytest.mark.parametrize("key", [str(uuid4()), "not-a-uuid"])
    async def test_a_row_not_here_is_the_apps(self, rows, key):
        assert await _owner("DELETE", f"/squirrel/v1/snapshots/{key}") is None
        assert await _owner("PUT", f"/v1/facilities/{key}") is None


class TestCreates:
    async def test_by_facility(self, rows):
        assert await _owner("POST", "/squirrel/v1/snapshots", {"title": "t", "facility": "arm1-site"}) == "arm1"
        assert await _owner("POST", "/squirrel/v1/snapshots/import", {"facility": "arm2-site"}) == "arm2"
        assert await _owner("POST", "/v1/facilities", {"slug": "child", "parent": "arm1-site"}) == "arm1"

    async def test_by_facilities(self, rows):
        assert await _owner("POST", "/squirrel/v1/pvs", {"facilities": ["arm1-site"]}) == "arm1"
        rows_body = [{"facilities": ["arm2-site"]}, {"facilities": []}, {"facilities": ["arm2-site"]}]
        assert await _owner("POST", "/squirrel/v1/pvs/multi", rows_body) == "arm2"

    async def test_facilities_of_two_owners_are_the_apps(self, rows):
        # The app answers owner_mismatch.
        assert await _owner("POST", "/squirrel/v1/pvs", {"facilities": ["arm1-site", "head-site"]}) is None
        multi = [{"facilities": ["arm1-site"]}, {"facilities": ["arm2-site"]}]
        assert await _owner("POST", "/squirrel/v1/pvs/multi", multi) is None

    async def test_an_unknown_facility_is_the_apps(self, rows):
        assert await _owner("POST", "/squirrel/v1/snapshots", {"facility": "nowhere"}) is None
        assert await _owner("POST", "/squirrel/v1/pvs", {"facilities": ["nowhere"]}) is None

    @pytest.mark.parametrize(
        ("method", "path", "body"),
        [
            ("POST", "/squirrel/v1/snapshots", {"title": "t"}),
            ("POST", "/squirrel/v1/pvs", {"setpointAddress": "NEW:PV"}),
            ("POST", "/squirrel/v1/pvs/multi", [{"setpointAddress": "NEW:PV"}]),
            ("POST", "/squirrel/v1/tags", {"name": "Area"}),
            ("POST", "/squirrel/v1/tags/bulk", {}),
            ("POST", "/magpie/v1/configs/trace", {"data": {}}),
            ("POST", "/v1/facilities", {"slug": "root"}),
        ],
    )
    async def test_with_no_target_the_owner_param_decides(self, rows, method, path, body):
        assert await _owner(method, path, body) is None  # this instance's
        assert await _owner(method, path, body, requested="arm1") == "arm1"
        assert await _owner(method, path, body, requested="nosuch") == "nosuch"

    async def test_an_owner_param_the_target_contradicts_is_the_apps(self, rows):
        assert await _owner("POST", "/squirrel/v1/pvs", {"facilities": ["arm1-site"]}, requested="arm1") == "arm1"
        assert await _owner("POST", "/squirrel/v1/pvs", {"facilities": ["head-site"]}, requested="arm1") is None
        body = {"title": "t", "facility": "arm2-site"}
        assert await _owner("POST", "/squirrel/v1/snapshots", body, requested="head") is None

    async def test_a_body_that_isnt_json_is_the_apps(self, rows):
        assert await _owner("POST", "/squirrel/v1/pvs", None) is None
        assert await _owner("POST", "/squirrel/v1/snapshots", {"facility": 3}) is None


class TestMetaPvByAddress:
    async def test_an_address_one_arm_holds(self, rows):
        assert await _owner("POST", "/squirrel/v1/pvs/meta", {"pvName": "ARM1:PV"}) == "arm1"
        assert await _owner("POST", "/squirrel/v1/pvs/meta", {"pv_name": "ARM2:PV"}) == "arm2"

    async def test_this_instances_pv_comes_first(self, rows):
        # Every owner holds a PV reading back SHARED:RBV.
        assert await _owner("POST", "/squirrel/v1/pvs/meta", {"pvName": "SHARED:RBV"}) == "head"

    async def test_several_other_owners_are_the_apps(self, rows, as_instance):
        async with db.get_session_maker()() as session:
            with as_instance("arm2"):
                session.add(Pv(setpoint_address="BOTH:PV"))
                await session.commit()
            with as_instance("arm1"):
                session.add(Pv(setpoint_address="BOTH:PV"))
                await session.commit()

        assert await _owner("POST", "/squirrel/v1/pvs/meta", {"pvName": "BOTH:PV"}) is None
        assert await _owner("POST", "/squirrel/v1/pvs/meta", {"pvName": "BOTH:PV"}, requested="arm1") == "arm1"

    async def test_an_owner_param_names_one(self, rows):
        assert await _owner("POST", "/squirrel/v1/pvs/meta", {"pvName": "SHARED:RBV"}, requested="arm2") == "arm2"
        assert await _owner("POST", "/squirrel/v1/pvs/meta", {"pvName": "ARM1:PV"}, requested="arm2") is None
        assert await _owner("POST", "/squirrel/v1/pvs/meta", {"pvName": "NOWHERE"}, requested="arm2") == "arm2"
        assert await _owner("POST", "/squirrel/v1/pvs/meta", {"pvName": "NOWHERE"}) is None


async def test_a_facility_slug_held_here_under_any_owner_is_taken(rows):
    route = ROUTED[("POST", "/v1/facilities")]

    for owner in ("head", "arm1", "arm2"):
        with pytest.raises(FacilitySlugConflict, match=f"owned by '{owner}'"):
            await require_unused(route, {"slug": f"{owner}-site"})
    await require_unused(route, {"slug": "fresh"})


async def test_a_snapshot_is_live_here(rows, as_instance):
    live = rows["arm1"]["snapshot"]
    async with db.get_session_maker()() as session:
        with as_instance("arm1"):
            gone = Snapshot(title="gone")
            session.add(gone)
            await session.flush()
            await session.execute(
                Snapshot.__table__.update().where(Snapshot.id == gone.id).values(deleted_at=Snapshot.created_at)
            )
            await session.commit()

    assert await snapshot_is_live(live)
    assert not await snapshot_is_live(gone.id)
    assert not await snapshot_is_live(str(uuid4()))
    assert not await snapshot_is_live("latest")
