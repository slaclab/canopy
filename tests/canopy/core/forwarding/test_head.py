"""Writes on a head, end to end: the router, the owner lookup and the app here, with arm1 mocked.

Postgres (the ``database`` fixture) holds the head's copy: rows of ``head``, ``arm1``
(forwardable) and ``arm2`` (read-only), made as replication brings them.
"""

import json
from uuid import uuid4

import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from sqlalchemy import text
from pytest_httpx import HTTPXMock

from canopy.core import db, security
from canopy.main import app
from canopy.core.modules import WILDCARD
from canopy.core.security import APIKey
from canopy.core.forwarding import middleware
from canopy.core.keys.service import ApiKeyService
from canopy.core.keys.registry import DbKeyRegistry
from canopy.squirrel.data.models import Pv, Snapshot, pv_facility
from canopy.core.facilities.models import Facility

from .conftest import ARM1, ARM1_SERVICE_KEY

EVERY = frozenset({WILDCARD})
WRITER = APIKey(token="writer", read=True, write=True, modules=EVERY, facility_ids=EVERY, name="console")
OPS = APIKey(token="ops", read=True, write=True, admin=True, modules=EVERY, facility_ids=EVERY, name="ops")
HEADERS = {"X-API-Key": "writer"}
# Each owner's facility ``<owner>-site`` has a fixed id, so a key can grant it.
SITE_IDS = {
    "head": "00000000-0000-4000-8000-000000000001",
    "arm1": "00000000-0000-4000-8000-000000000002",
    "arm2": "00000000-0000-4000-8000-000000000003",
}
# Keys limited to one facility: the head's own, and arm1's.
LAB = APIKey(token="lab", read=True, write=True, modules=EVERY, facility_ids=frozenset({SITE_IDS["head"]}), name="lab")
ARM1_LAB = APIKey(
    token="arm1-lab", read=True, write=True, modules=EVERY, facility_ids=frozenset({SITE_IDS["arm1"]}), name="a1"
)


@pytest_asyncio.fixture
async def copy(database, head, as_instance, bind_keys, monkeypatch):
    """The head's copy: a facility, a PV in it and a snapshot of it, for each owner."""
    bind_keys(WRITER, OPS)

    async def no_wait(commit_lsn, timeout_s, *, subscription):
        return False

    monkeypatch.setattr(middleware, "wait_until_applied", no_wait)
    made = {}
    for owner in ("head", "arm1", "arm2"):
        with as_instance(owner):
            async with db.get_session_maker()() as session:
                facility = Facility(id=SITE_IDS[owner], slug=f"{owner}-site", name=owner)
                pv = Pv(setpoint_address=f"{owner.upper()}:PV")
                session.add_all([facility, pv])
                await session.flush()
                snapshot = Snapshot(title="S", facility_id=facility.id)
                session.add(snapshot)
                await session.execute(pv_facility.insert().values(pv_id=pv.id, facility_id=facility.id))
                await session.commit()
                made[owner] = {"facility": facility.slug, "pv": pv.id, "snapshot": snapshot.id}
    return made


@pytest_asyncio.fixture
async def http(copy):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client


async def _owner_of_tag_group(name: str) -> str | None:
    async with db.get_session_maker()() as session:
        return await session.scalar(text("SELECT owner_instance FROM squirrel.tag_group WHERE name = :n"), {"n": name})


async def test_an_arm1_snapshot_change_goes_to_arm1(http, copy, httpx_mock: HTTPXMock):
    snapshot = copy["arm1"]["snapshot"]
    httpx_mock.add_response(
        method="PATCH",
        url=f"{ARM1}/squirrel/v1/snapshots/{snapshot}",
        json={"id": snapshot},
        headers={"X-Canopy-Commit-LSN": "0/1"},
    )

    resp = await http.patch(f"/squirrel/v1/snapshots/{snapshot}", json={"title": "new"}, headers=HEADERS)

    assert resp.status_code == 200, resp.text
    assert resp.headers["x-canopy-replica-stale"] == "true"
    sent = httpx_mock.get_request()
    assert sent.headers["X-API-Key"] == ARM1_SERVICE_KEY
    assert json.loads(sent.content) == {"title": "new"}


async def test_an_arm2_row_is_refused_here(http, copy):
    resp = await http.delete(f"/squirrel/v1/snapshots/{copy['arm2']['snapshot']}", headers=HEADERS)

    assert resp.status_code == 403
    assert resp.json()["code"] == "not_owner"
    assert "'arm2'" in resp.json()["detail"]


async def test_the_heads_own_row_is_written_here(http, copy):
    resp = await http.patch(f"/squirrel/v1/snapshots/{copy['head']['snapshot']}", json={"title": "x"}, headers=HEADERS)

    assert resp.status_code == 200, resp.text
    assert resp.json()["title"] == "x"


async def test_a_create_by_body_is_written_here_whole(http, copy):
    # The router reads the body to find the owner; the app must still get all of it.
    resp = await http.post(
        "/squirrel/v1/pvs", json={"setpointAddress": "NEW:PV", "facilities": ["head-site"]}, headers=HEADERS
    )

    assert resp.status_code == 200, resp.text
    assert (resp.json()["ownerInstance"], resp.json()["facilities"]) == ("head", ["head-site"])


async def test_a_create_in_an_arm1_facility_goes_to_arm1(http, copy, httpx_mock: HTTPXMock):
    httpx_mock.add_response(method="POST", url=f"{ARM1}/squirrel/v1/pvs", json={"id": "p"})

    body = {"setpointAddress": "NEW:PV", "facilities": ["arm1-site"]}
    resp = await http.post("/squirrel/v1/pvs", json=body, headers=HEADERS)

    assert resp.status_code == 200, resp.text
    assert json.loads(httpx_mock.get_request().content) == body


async def test_facilities_of_two_owners_are_refused_here(http, copy):
    body = {"setpointAddress": "NEW:PV", "facilities": ["arm1-site", "head-site"]}
    resp = await http.post("/squirrel/v1/pvs", json=body, headers=HEADERS)

    assert resp.status_code == 409
    assert resp.json()["code"] == "owner_mismatch"


async def _pvs_at(address: str) -> list[str]:
    async with db.get_session_maker()() as session:
        rows = await session.scalars(
            text("SELECT owner_instance FROM squirrel.pv WHERE setpoint_address = :a ORDER BY owner_instance"),
            {"a": address},
        )
        return list(rows)


class TestPvImport:
    """``POST /squirrel/v1/pvs/import``: one owner per import, decided before the head's import runs."""

    @pytest.mark.parametrize("mode", ["skip", "update"])
    async def test_rows_in_an_arm1_facility_go_to_arm1_and_nothing_is_written_here(
        self, http, copy, httpx_mock: HTTPXMock, mode
    ):
        rows = [
            {"setpointAddress": "ARM1:PV", "description": "changed", "facilities": ["arm1-site"]},
            {"setpointAddress": "ARM1:NEW", "facilities": ["arm1-site"]},
        ]
        httpx_mock.add_response(
            method="POST", url=f"{ARM1}/squirrel/v1/pvs/import?onConflict={mode}", json={"created": []}
        )

        resp = await http.post(f"/squirrel/v1/pvs/import?onConflict={mode}", json=rows, headers=HEADERS)

        assert resp.status_code == 200, resp.text
        assert json.loads(httpx_mock.get_request().content) == rows
        # Replication brings arm1's changes back; the head's copy isn't written here.
        assert await _pvs_at("ARM1:PV") == ["arm1"]
        assert await _pvs_at("ARM1:NEW") == []

    async def test_facilities_of_two_arms_are_refused_here(self, http, copy):
        rows = [
            {"setpointAddress": "ARM1:NEW", "facilities": ["arm1-site"]},
            {"setpointAddress": "ARM2:NEW", "facilities": ["arm2-site"]},
        ]

        resp = await http.post("/squirrel/v1/pvs/import?onConflict=skip", json=rows, headers=HEADERS)

        assert resp.status_code == 409, resp.text
        assert resp.json()["code"] == "owner_mismatch"
        assert "(arm1, arm2)" in resp.json()["detail"]
        assert await _pvs_at("ARM1:NEW") == await _pvs_at("ARM2:NEW") == []

    async def test_the_owner_param_sends_a_facility_less_import_to_that_arm(self, http, copy, httpx_mock: HTTPXMock):
        httpx_mock.add_response(
            method="POST", url=f"{ARM1}/squirrel/v1/pvs/import?onConflict=update&owner=arm1", json={"created": []}
        )

        resp = await http.post(
            "/squirrel/v1/pvs/import?onConflict=update&owner=arm1", json=[{"setpointAddress": "N:1"}], headers=HEADERS
        )

        assert resp.status_code == 200, resp.text

    @pytest.mark.parametrize("mode", ["skip", "update"])
    async def test_a_row_without_a_facility_is_the_heads(self, http, copy, mode):
        # Rows match only this instance's PVs, so a row at arm1's address naming no
        # facility makes the head's own PV there, in either mode; arm1's is left alone.
        rows = [{"setpointAddress": "ARM1:PV", "description": "mine"}]

        resp = await http.post(f"/squirrel/v1/pvs/import?onConflict={mode}", json=rows, headers=HEADERS)

        assert resp.status_code == 200, resp.text
        assert await _pvs_at("ARM1:PV") == ["arm1", "head"]


class TestTheOwnerParam:
    async def test_absent_or_this_instance_is_here(self, http, copy):
        assert (await http.post("/squirrel/v1/tags", json={"name": "Mine"}, headers=HEADERS)).status_code == 200
        resp = await http.post("/squirrel/v1/tags?owner=head", json={"name": "Also"}, headers=HEADERS)
        assert resp.status_code == 200, resp.text
        assert await _owner_of_tag_group("Mine") == await _owner_of_tag_group("Also") == "head"

    async def test_a_forwardable_arm_is_sent_on_with_the_param(self, http, copy, httpx_mock: HTTPXMock):
        httpx_mock.add_response(method="POST", url=f"{ARM1}/squirrel/v1/tags?owner=arm1", json={"id": "g"})

        resp = await http.post("/squirrel/v1/tags?owner=arm1", json={"name": "Theirs"}, headers=HEADERS)

        assert resp.status_code == 200, resp.text
        assert await _owner_of_tag_group("Theirs") is None

    @pytest.mark.parametrize("owner", ["arm2", "nosuch"])
    async def test_anything_else_is_refused_here(self, http, copy, owner):
        resp = await http.post(f"/squirrel/v1/tags?owner={owner}", json={"name": "No"}, headers=HEADERS)

        assert resp.status_code == 403
        assert resp.json()["code"] == "not_owner"

    async def test_a_pv_import_naming_arm1(self, http, copy, httpx_mock: HTTPXMock):
        httpx_mock.add_response(method="POST", url=f"{ARM1}/squirrel/v1/pvs/multi?owner=arm1", json=[])

        resp = await http.post("/squirrel/v1/pvs/multi?owner=arm1", json=[{"setpointAddress": "N:1"}], headers=HEADERS)

        assert resp.status_code == 200, resp.text


class TestFacilities:
    async def test_a_child_of_an_arm1_facility_goes_to_arm1(self, http, copy, httpx_mock: HTTPXMock):
        httpx_mock.add_response(method="POST", url=f"{ARM1}/v1/facilities", status_code=201, json={})

        body = {"slug": "arm1-child", "name": "Child", "parent": "arm1-site"}
        resp = await http.post("/v1/facilities", json=body, headers={"X-API-Key": "ops"})

        assert resp.status_code == 201, resp.text

    @pytest.mark.parametrize("taken", ["head-site", "arm2-site"])
    async def test_a_slug_held_here_is_refused_before_sending(self, http, copy, taken):
        body = {"slug": taken, "name": "Child", "parent": "arm1-site"}
        resp = await http.post("/v1/facilities", json=body, headers={"X-API-Key": "ops"})

        assert resp.status_code == 409
        assert resp.json()["code"] == "facility_slug_conflict"


class TestArchivedImmutable:
    async def test_a_snapshot_arm1_pruned_is_history_here(self, http, copy, httpx_mock: HTTPXMock):
        snapshot = copy["arm1"]["snapshot"]
        httpx_mock.add_response(method="DELETE", status_code=404, json={"code": "snapshot_not_found"})

        resp = await http.delete(f"/squirrel/v1/snapshots/{snapshot}", headers=HEADERS)

        assert resp.status_code == 409
        assert resp.json()["code"] == "archived_immutable"

    async def test_a_snapshot_not_here_goes_to_the_app(self, http, copy):
        resp = await http.delete(f"/squirrel/v1/snapshots/{uuid4()}", headers=HEADERS)

        assert resp.status_code == 404
        assert resp.json()["code"] == "snapshot_not_found"


class TestOutsideTheCallersFacilities:
    """A caller whose grant doesn't reach a row is answered here, as on any instance, and nothing is sent."""

    @pytest.fixture(autouse=True)
    def keys(self, copy, bind_keys):
        bind_keys(WRITER, OPS, LAB, ARM1_LAB)

    async def _unassigned_snapshot(self, as_instance, owner: str) -> str:
        with as_instance(owner):
            async with db.get_session_maker()() as session:
                snapshot = Snapshot(title="unassigned")
                session.add(snapshot)
                await session.commit()
                return snapshot.id

    @pytest.mark.parametrize(("method", "suffix"), [("PATCH", ""), ("PUT", ""), ("DELETE", ""), ("POST", "/restore")])
    @pytest.mark.parametrize("owner", ["arm1", "arm2"])
    async def test_a_snapshot_is_a_404_as_on_get(self, http, copy, method, suffix, owner):
        # arm1 forwards and arm2 doesn't: either way, not archived_immutable or not_owner.
        path = f"/squirrel/v1/snapshots/{copy[owner]['snapshot']}"
        get = await http.get(path, headers={"X-API-Key": "lab"})

        resp = await http.request(method, path + suffix, json={}, headers={"X-API-Key": "lab"})

        assert get.json()["code"] == "snapshot_not_found"
        assert (resp.status_code, resp.json()["code"]) == (404, "snapshot_not_found"), resp.text

    @pytest.mark.parametrize("owner", ["arm1", "arm2"])
    async def test_an_unassigned_snapshot_is_for_unrestricted_keys(self, http, as_instance, owner):
        snapshot = await self._unassigned_snapshot(as_instance, owner)

        resp = await http.patch(f"/squirrel/v1/snapshots/{snapshot}", json={"title": "x"}, headers={"X-API-Key": "lab"})

        assert (resp.status_code, resp.json()["code"]) == (403, "insufficient_facility_access")

    @pytest.mark.parametrize("method", ["PUT", "DELETE"])
    @pytest.mark.parametrize("owner", ["arm1", "arm2"])
    async def test_a_pv_names_its_facilities_as_on_a_primary(self, http, copy, method, owner):
        resp = await http.request(
            method, f"/squirrel/v1/pvs/{copy[owner]['pv']}", json={"description": "x"}, headers={"X-API-Key": "lab"}
        )

        assert (resp.status_code, resp.json()["code"]) == (403, "insufficient_facility_access")
        assert f"{owner}-site" in resp.json()["detail"]

    async def test_a_caller_in_the_rows_facility_is_forwarded(self, http, copy, httpx_mock: HTTPXMock):
        snapshot = copy["arm1"]["snapshot"]
        httpx_mock.add_response(method="PATCH", url=f"{ARM1}/squirrel/v1/snapshots/{snapshot}", json={"id": snapshot})

        resp = await http.patch(
            f"/squirrel/v1/snapshots/{snapshot}", json={"title": "x"}, headers={"X-API-Key": "arm1-lab"}
        )

        assert resp.status_code == 200, resp.text
        assert json.loads(httpx_mock.get_request().headers["X-Canopy-On-Behalf-Of-Facilities"]) == [SITE_IDS["arm1"]]

    async def test_an_arm2_row_in_the_callers_facility_is_still_not_owned(self, http, copy, bind_keys):
        arm2_lab = APIKey(
            token="arm2-lab", read=True, write=True, modules=EVERY, facility_ids=frozenset({SITE_IDS["arm2"]})
        )
        bind_keys(arm2_lab)

        resp = await http.delete(
            f"/squirrel/v1/snapshots/{copy['arm2']['snapshot']}", headers={"X-API-Key": "arm2-lab"}
        )

        assert (resp.status_code, resp.json()["code"]) == (403, "not_owner")


class TestARecreatedSlug:
    """A key the head minted granting ``north``, after the arm owning it deleted it and arm1 created ``north`` again.

    Replication brings the delete and the new facility to the head as plain row changes,
    with no check of the head's keys. Parametrized on who deleted the old ``north``: arm1
    itself, or arm2, the new one being arm1's either way, so writes to it are forwarded.
    """

    OLD = "00000000-0000-4000-8000-0000000000f1"

    @pytest_asyncio.fixture(params=["arm1", "arm2"])
    async def north(self, request, copy, as_instance) -> dict:
        with as_instance(request.param):
            async with db.get_session_maker()() as session:
                session.add(Facility(id=self.OLD, slug="north", name="North"))
                await session.commit()
        async with db.get_session_maker()() as session:
            minted = await ApiKeyService(session).create_key(
                "lab", read=True, write=True, modules=[WILDCARD], facilities=["north"]
            )
            await session.commit()
        security.bind_key_registry(DbKeyRegistry())  # the copy fixture's bind_keys restores the registry
        headers = {"X-API-Key": minted.token}
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            before = await client.get("/v1/facilities/access", headers=headers)
        assert before.json() == {"unrestricted": False, "facilities": ["north"]}

        async with db.get_session_maker()() as session:
            await session.execute(Facility.__table__.delete().where(Facility.id == self.OLD))
            await session.commit()
        with as_instance("arm1"):
            async with db.get_session_maker()() as session:
                facility = Facility(slug="north", name="North again")
                pv = Pv(setpoint_address="NORTH:PV")
                session.add_all([facility, pv])
                await session.flush()
                snapshot = Snapshot(title="S", facility_id=facility.id)
                session.add(snapshot)
                await session.execute(pv_facility.insert().values(pv_id=pv.id, facility_id=facility.id))
                await session.commit()
        return {"headers": headers, "id": facility.id, "pv": pv.id, "snapshot": snapshot.id}

    async def test_the_key_doesnt_read_the_new_facility(self, http, north):
        access = await http.get("/v1/facilities/access", headers=north["headers"])
        listed = await http.get("/squirrel/v1/snapshots", headers=north["headers"])
        one = await http.get(f"/squirrel/v1/snapshots/{north['snapshot']}", headers=north["headers"])
        pvs = await http.get("/squirrel/v1/pvs", headers=north["headers"])

        assert access.json() == {"unrestricted": False, "facilities": []}
        assert listed.status_code == 200, listed.text
        assert north["snapshot"] not in {snapshot["id"] for snapshot in listed.json()["results"]}
        assert (one.status_code, one.json()["code"]) == (404, "snapshot_not_found")
        assert north["pv"] not in {pv["id"] for pv in pvs.json()}

    async def test_a_change_to_the_new_facilitys_row_is_answered_here(self, http, north, httpx_mock: HTTPXMock):
        resp = await http.patch(
            f"/squirrel/v1/snapshots/{north['snapshot']}", json={"title": "x"}, headers=north["headers"]
        )

        assert (resp.status_code, resp.json()["code"]) == (404, "snapshot_not_found")
        assert httpx_mock.get_requests() == []

    async def test_a_create_in_the_new_facility_goes_with_the_minted_grant(self, http, north, httpx_mock: HTTPXMock):
        # The arm decides a create; the head sends the grant as minted, which names the old id.
        httpx_mock.add_response(
            method="POST",
            url=f"{ARM1}/squirrel/v1/pvs",
            status_code=403,
            json={"code": "insufficient_facility_access", "detail": "API key is not scoped to the PV's facility(s)"},
        )

        body = {"setpointAddress": "NORTH:NEW", "facilities": ["north"]}
        resp = await http.post("/squirrel/v1/pvs", json=body, headers=north["headers"])

        assert resp.status_code == 403
        assert json.loads(httpx_mock.get_request().headers["X-Canopy-On-Behalf-Of-Facilities"]) == [self.OLD]
