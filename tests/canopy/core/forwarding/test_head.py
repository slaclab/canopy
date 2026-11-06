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

from canopy.core import db
from canopy.main import app
from canopy.core.modules import WILDCARD
from canopy.core.security import APIKey
from canopy.core.forwarding import middleware
from canopy.squirrel.data.models import Pv, Snapshot, pv_facility
from canopy.core.facilities.models import Facility

from .conftest import ARM1, ARM1_SERVICE_KEY

EVERY = frozenset({WILDCARD})
WRITER = APIKey(token="writer", read=True, write=True, modules=EVERY, facilities=EVERY, name="console")
OPS = APIKey(token="ops", read=True, write=True, admin=True, modules=EVERY, facilities=EVERY, name="ops")
HEADERS = {"X-API-Key": "writer"}


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
                facility = Facility(slug=f"{owner}-site", name=owner)
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
