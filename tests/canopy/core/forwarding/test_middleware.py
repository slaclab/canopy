"""A forward instance sending writes to its primary (mocked with ``httpx_mock``).

The primary is ``https://primary.example.org`` (``forward_role``); keys here are
fakes bound to this instance's registry, and the service key is ``service-token``.
"""

import json
from uuid import uuid4

import httpx
import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from sqlalchemy import text
from pytest_httpx import HTTPXMock
from fastapi.testclient import TestClient

from canopy.core import db, security
from canopy.main import app
from canopy.core.modules import WILDCARD
from canopy.core.security import APIKey
from canopy.core.forwarding import middleware
from canopy.core.forwarding.routes import FACILITY, FORWARDABLE

from ..test_read_only_instance import FakeRegistry, _concrete

PRIMARY = "https://primary.example.org"
WRITER = APIKey(token="writer", read=True, write=True, modules=frozenset({WILDCARD}), name="console")
READER = APIKey(token="reader", read=True, write=False, modules=frozenset({WILDCARD}), name="viewer")
MAGPIE_ONLY = APIKey(token="magpie", read=True, write=True, modules=frozenset({"magpie"}), name="configs")
SNAPSHOT_ID = "6f1c2c1e-0d4b-4c43-9a55-2f0d7a3c9b10"


@pytest.fixture
def client(forward_role):
    previous = security._registry
    security.bind_key_registry(FakeRegistry(WRITER, READER, MAGPIE_ONLY))
    yield TestClient(app, raise_server_exceptions=False)
    security._registry = previous


def _as(key: APIKey, **extra: str) -> dict[str, str]:
    return {"X-API-Key": key.token, **extra}


FACILITY_ROUTES = [route for route in FORWARDABLE if route.route_class is FACILITY]


@pytest.mark.parametrize("route", FACILITY_ROUTES, ids=lambda r: f"{r.method} {r.path}")
def test_facility_writes_go_to_the_primary_as_the_service_key(client, httpx_mock: HTTPXMock, route):
    path = _concrete(route.path)
    httpx_mock.add_response(method=route.method, url=PRIMARY + path, json={"ok": True})

    resp = client.request(route.method, path, headers=_as(WRITER))

    assert resp.status_code == 200, resp.text
    assert resp.json() == {"ok": True}
    sent = httpx_mock.get_request()
    assert sent.headers["X-API-Key"] == "service-token"
    assert sent.headers["X-Canopy-On-Behalf-Of"] == "console@site_a"
    assert json.loads(sent.headers["X-Canopy-On-Behalf-Of-Facilities"]) == []


def test_only_the_body_query_and_content_headers_go_on(client, httpx_mock: HTTPXMock):
    httpx_mock.add_response(
        method="PUT",
        url=f"{PRIMARY}/squirrel/v1/pvs/pv-1?dryRun=true",
        status_code=201,
        json={"id": "pv-1"},
        headers={"Set-Cookie": "primary=1", "X-Canopy-Something": "kept"},
    )

    resp = client.put(
        "/squirrel/v1/pvs/pv-1?dryRun=true",
        json={"description": "new"},
        headers=_as(
            WRITER,
            **{
                "X-Canopy-On-Behalf-Of": "someone-else",
                "X-Canopy-On-Behalf-Of-Facilities": '["*"]',
                "Cookie": "sso=secret",
            },
        ),
    )

    assert resp.status_code == 201
    assert resp.json() == {"id": "pv-1"}
    assert resp.headers["x-canopy-something"] == "kept"
    assert "set-cookie" not in resp.headers
    sent = httpx_mock.get_request()
    assert json.loads(sent.content) == {"description": "new"}
    assert sent.headers["content-type"] == "application/json"
    assert sent.headers["X-Canopy-On-Behalf-Of"] == "console@site_a"
    # The caller's own grant (none), not the one it claimed.
    assert json.loads(sent.headers["X-Canopy-On-Behalf-Of-Facilities"]) == []
    assert "cookie" not in sent.headers
    assert sent.headers["X-API-Key"] == "service-token"


def test_the_primarys_errors_come_back_as_they_are(client, httpx_mock: HTTPXMock):
    httpx_mock.add_response(
        method="DELETE", url=f"{PRIMARY}/squirrel/v1/pvs/nope", status_code=404, json={"code": "pv_not_found"}
    )

    resp = client.delete("/squirrel/v1/pvs/nope", headers=_as(WRITER))

    assert resp.status_code == 404
    assert resp.json() == {"code": "pv_not_found"}


def test_the_root_path_is_not_sent_to_the_primary(forward_role, httpx_mock: HTTPXMock):
    previous = security._registry
    security.bind_key_registry(FakeRegistry(WRITER))
    try:
        httpx_mock.add_response(method="POST", url=f"{PRIMARY}/squirrel/v1/pvs", json={})
        client = TestClient(app, root_path="/canopy", raise_server_exceptions=False)

        # As uvicorn --root-path sends it: the prefix is part of the path.
        assert client.post("/canopy/squirrel/v1/pvs", json={}, headers=_as(WRITER)).status_code == 200
    finally:
        security._registry = previous


class TestRefusedHere:
    """Answered by this instance without a request to the primary (``httpx_mock`` fails on any)."""

    def test_no_key(self, client):
        assert client.post("/squirrel/v1/pvs", json={}).json()["code"] == "invalid_api_key"
        assert client.post("/squirrel/v1/pvs", json={}, headers={"X-API-Key": "nope"}).status_code == 401

    def test_read_key(self, client):
        resp = client.post("/squirrel/v1/pvs", json={}, headers=_as(READER))

        assert resp.status_code == 403
        assert resp.json()["code"] == "insufficient_write_access"

    def test_key_for_another_module(self, client):
        resp = client.post("/squirrel/v1/pvs", json={}, headers=_as(MAGPIE_ONLY))

        assert resp.status_code == 403
        assert resp.json()["code"] == "insufficient_module_access"

    def test_shared_tables_without_the_policy(self, client):
        resp = client.post("/squirrel/v1/tags", json={"name": "Area"}, headers=_as(WRITER))

        assert resp.status_code == 403
        assert resp.json() == {"code": "read_only_instance", "detail": security.NOT_FORWARDED_DETAIL}

    def test_restore(self, client):
        resp = client.post(f"/squirrel/v1/snapshots/{SNAPSHOT_ID}/restore", headers=_as(WRITER))

        assert resp.json()["code"] == "read_only_instance"


def test_shared_tables_with_the_policy(client, httpx_mock: HTTPXMock, monkeypatch):
    monkeypatch.setenv("CANOPY_FORWARD__ALLOW_SHARED", "true")
    httpx_mock.add_response(method="POST", url=f"{PRIMARY}/magpie/v1/configs/trace", json={"slug": "abc"})

    resp = client.post("/magpie/v1/configs/trace", json={"data": {}}, headers=_as(MAGPIE_ONLY))

    assert resp.status_code == 200
    assert httpx_mock.get_request().headers["X-Canopy-On-Behalf-Of"] == "configs@site_a"


class TestPrimaryTrouble:
    def test_unreachable_means_nothing_was_sent(self, client, httpx_mock: HTTPXMock):
        httpx_mock.add_exception(httpx.ConnectError("connection refused"))

        resp = client.post("/squirrel/v1/pvs", json={}, headers=_as(WRITER))

        assert resp.status_code == 502
        assert resp.json()["code"] == "primary_unavailable"
        assert "nothing was sent" in resp.json()["detail"]

    def test_no_answer_may_have_been_applied(self, client, httpx_mock: HTTPXMock):
        httpx_mock.add_exception(httpx.ReadTimeout("timed out"))

        resp = client.post("/squirrel/v1/pvs", json={}, headers=_as(WRITER))

        assert resp.status_code == 504
        assert resp.json()["code"] == "primary_unavailable"
        assert "may or may not have been applied" in resp.json()["detail"]

    def test_missing_configuration(self, client, monkeypatch):
        monkeypatch.delenv("CANOPY_FORWARD__SERVICE_KEY")

        resp = client.post("/squirrel/v1/pvs", json={}, headers=_as(WRITER))

        assert resp.status_code == 502
        assert "CANOPY_FORWARD__SERVICE_KEY" in resp.json()["detail"]

    def test_plain_http_needs_the_override(self, client, httpx_mock: HTTPXMock, monkeypatch):
        monkeypatch.setenv("CANOPY_FORWARD__PRIMARY_URL", "http://primary.example.org/")

        assert "https" in client.post("/squirrel/v1/pvs", json={}, headers=_as(WRITER)).json()["detail"]

        monkeypatch.setenv("CANOPY_FORWARD__ALLOW_INSECURE_HTTP", "true")
        httpx_mock.add_response(method="POST", url="http://primary.example.org/squirrel/v1/pvs", json={})
        assert client.post("/squirrel/v1/pvs", json={}, headers=_as(WRITER)).status_code == 200


class TestReadYourWrites:
    @pytest.fixture
    def waited(self, monkeypatch):
        calls = []

        def fake_wait(applied: bool):
            async def wait_until_applied(commit_lsn: str, timeout_s: float) -> bool:
                calls.append((commit_lsn, timeout_s))
                return applied

            monkeypatch.setattr(middleware, "wait_until_applied", wait_until_applied)
            return calls

        return fake_wait

    def test_applied_in_time(self, client, httpx_mock: HTTPXMock, waited):
        calls = waited(True)
        httpx_mock.add_response(json={}, headers={"X-Canopy-Commit-LSN": "0/16B3748"})

        resp = client.post("/squirrel/v1/pvs", json={}, headers=_as(WRITER))

        assert calls == [("0/16B3748", 2.0)]
        assert resp.headers["x-canopy-commit-lsn"] == "0/16B3748"
        assert "x-canopy-replica-stale" not in resp.headers

    def test_not_applied_in_time(self, client, httpx_mock: HTTPXMock, waited):
        waited(False)
        httpx_mock.add_response(json={}, headers={"X-Canopy-Commit-LSN": "0/16B3748"})

        resp = client.post("/squirrel/v1/pvs", json={}, headers=_as(WRITER))

        assert resp.status_code == 200
        assert resp.headers["x-canopy-replica-stale"] == "true"

    def test_no_commit_no_wait(self, client, httpx_mock: HTTPXMock, waited):
        calls = waited(False)
        httpx_mock.add_response(json={"jobId": "j1", "statusUrl": "/jobs/v1/j1"})

        resp = client.post("/squirrel/v1/snapshots", json={"title": "t"}, headers=_as(WRITER))

        assert calls == []
        assert "x-canopy-replica-stale" not in resp.headers


class TestArchivedImmutable:
    """The age check reads this instance's copy of ``squirrel.snapshot`` (Postgres, ``database``)."""

    @pytest.fixture
    def windowed(self, monkeypatch):
        monkeypatch.setenv("CANOPY_FORWARD__PRIMARY_WINDOW_DAYS", "30")

    @pytest_asyncio.fixture
    async def http(self, database, forward_role):
        previous = security._registry
        security.bind_key_registry(FakeRegistry(WRITER))
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            yield client
        security._registry = previous

    @staticmethod
    async def _snapshot(age_days: int, *, retain: bool = False, deleted: bool = False) -> str:
        snapshot_id = str(uuid4())
        async with db.get_session_maker()() as session:
            await session.execute(
                text(
                    "INSERT INTO squirrel.snapshot (id, title, created_at, retain, deleted_at) "
                    "VALUES (CAST(:id AS uuid), 'S', now() - make_interval(days => :age), :retain, "
                    "CASE WHEN :deleted THEN now() END)"
                ),
                {"id": snapshot_id, "age": age_days, "retain": retain, "deleted": deleted},
            )
            await session.commit()
        return snapshot_id

    async def test_older_than_the_window_is_refused_here(self, http, windowed):
        snapshot_id = await self._snapshot(31)

        resp = await http.put(f"/squirrel/v1/snapshots/{snapshot_id}", json={"title": "x"}, headers=_as(WRITER))

        assert resp.status_code == 409
        assert resp.json()["code"] == "archived_immutable"

    @pytest.mark.parametrize("method", ["PATCH", "DELETE"])
    async def test_every_snapshot_change_is_checked(self, http, windowed, method):
        snapshot_id = await self._snapshot(400)

        resp = await http.request(
            method, f"/squirrel/v1/snapshots/{snapshot_id}", json={"retain": True}, headers=_as(WRITER)
        )

        assert resp.json()["code"] == "archived_immutable"

    async def test_inside_the_window_is_forwarded(self, http, httpx_mock: HTTPXMock, windowed):
        snapshot_id = await self._snapshot(29)
        httpx_mock.add_response(method="DELETE", url=f"{PRIMARY}/squirrel/v1/snapshots/{snapshot_id}", json=True)

        assert (await http.delete(f"/squirrel/v1/snapshots/{snapshot_id}", headers=_as(WRITER))).status_code == 200

    async def test_a_retained_snapshot_is_still_on_the_primary(self, http, httpx_mock: HTTPXMock, windowed):
        snapshot_id = await self._snapshot(400, retain=True)
        httpx_mock.add_response(method="PATCH", url=f"{PRIMARY}/squirrel/v1/snapshots/{snapshot_id}", json={})

        resp = await http.patch(f"/squirrel/v1/snapshots/{snapshot_id}", json={"title": "x"}, headers=_as(WRITER))

        assert resp.status_code == 200

    @pytest.mark.parametrize("state", ["deleted", "missing", "not-a-uuid"])
    async def test_left_for_the_primary_to_answer(self, http, httpx_mock: HTTPXMock, windowed, state):
        snapshot_id = {
            "deleted": await self._snapshot(400, deleted=True),
            "missing": str(uuid4()),
            "not-a-uuid": "latest",
        }[state]
        httpx_mock.add_response(method="DELETE", status_code=404, json={"code": "snapshot_not_found"})

        resp = await http.delete(f"/squirrel/v1/snapshots/{snapshot_id}", headers=_as(WRITER))

        assert resp.json() == {"code": "snapshot_not_found"}

    async def test_no_window_no_check(self, http, httpx_mock: HTTPXMock):
        snapshot_id = await self._snapshot(400)
        httpx_mock.add_response(method="DELETE", json=True)

        assert (await http.delete(f"/squirrel/v1/snapshots/{snapshot_id}", headers=_as(WRITER))).status_code == 200

    async def test_an_import_is_never_checked(self, http, httpx_mock: HTTPXMock, windowed):
        httpx_mock.add_response(method="POST", url=f"{PRIMARY}/squirrel/v1/snapshots/import", json={})

        resp = await http.post("/squirrel/v1/snapshots/import", json={"id": str(uuid4())}, headers=_as(WRITER))

        assert resp.status_code == 200


class TestJobStatus:
    """A forwarded snapshot capture's job runs on the primary; polling it here asks the primary."""

    @pytest.fixture
    def no_local_job(self, monkeypatch):
        async def get_status(job_id):
            return None

        monkeypatch.setattr("canopy.jobs.gateway.get_status", get_status)

    def test_a_job_this_instance_lacks_is_asked_of_the_primary(self, client, httpx_mock: HTTPXMock, no_local_job):
        httpx_mock.add_response(
            method="GET", url=f"{PRIMARY}/jobs/v1/j1", json={"id": "j1", "status": "running", "progress": 40}
        )

        resp = client.get("/jobs/v1/j1", headers=_as(READER))

        assert resp.status_code == 200, resp.text
        assert resp.json()["status"] == "running"
        sent = httpx_mock.get_request()
        assert sent.headers["X-API-Key"] == "service-token"
        assert sent.headers["X-Canopy-On-Behalf-Of"] == "viewer@site_a"

    def test_unknown_on_the_primary_too(self, client, httpx_mock: HTTPXMock, no_local_job):
        httpx_mock.add_response(method="GET", url=f"{PRIMARY}/jobs/v1/j2", status_code=404, json={})

        resp = client.get("/jobs/v1/j2", headers=_as(READER))

        assert resp.status_code == 404
        assert resp.json()["code"] == "job_not_found"

    def test_a_mirror_never_asks(self, mirror_role, no_local_job):
        previous = security._registry
        security.bind_key_registry(FakeRegistry(READER))
        try:
            resp = TestClient(app).get("/jobs/v1/j3", headers=_as(READER))
        finally:
            security._registry = previous

        assert resp.status_code == 404
