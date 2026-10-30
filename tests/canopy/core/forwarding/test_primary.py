"""The primary's half: trusting ``X-Canopy-On-Behalf-Of``, recording it, the caller's
facility grant, and ``X-Canopy-Commit-LSN``.

The header-trust tests use fake keys. The commit-position tests run the real app
against Postgres (the ``database`` fixture in this directory's conftest).
"""

import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from fastapi import FastAPI
from sqlalchemy import text
from fastapi.testclient import TestClient

from canopy.core import db, security
from canopy.main import app
from canopy.core.config import get_settings
from canopy.core.errors import register_canopy_error_handler
from canopy.core.modules import WILDCARD, tag_module
from canopy.core.security import APIKey, ActorDep
from canopy.core.facilities.models import Facility
from canopy.core.forwarding.client import lsn_value
from canopy.core.forwarding.context import forwarded_write

from ..test_read_only_instance import FakeRegistry

EVERY = frozenset({WILDCARD})
SERVICE = APIKey(
    token="svc",
    read=True,
    write=True,
    is_service=True,
    modules=EVERY,
    facilities=EVERY,
    name="history-site-a",
)
# A service key granted only site-a, for the intersection tests.
SERVICE_A = APIKey(
    token="svc-a",
    read=True,
    write=True,
    is_service=True,
    modules=EVERY,
    facilities=frozenset({"site-a"}),
    name="history-site-a-only",
)
CLIENT = APIKey(token="cli", read=True, write=True, modules=EVERY, facilities=EVERY, name="console")


def _for(facilities: str) -> dict[str, str]:
    return {"X-Canopy-On-Behalf-Of": "operator@site_a", "X-Canopy-On-Behalf-Of-Facilities": facilities}


ON_BEHALF = _for('["*"]')


@pytest.fixture
def keys():
    previous = security._registry
    security.bind_key_registry(FakeRegistry(SERVICE, SERVICE_A, CLIENT))
    yield
    security._registry = previous


@pytest.fixture
def accepting(monkeypatch):
    monkeypatch.setenv("CANOPY_ACCEPT_FORWARDED_WRITES", "true")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
def actor_client(keys) -> TestClient:
    """A throwaway app whose one route answers with the request's recorded actor."""
    gated = FastAPI()
    register_canopy_error_handler(gated)
    tag_module(gated, "core")

    @gated.post("/actor")
    async def actor_route(actor: ActorDep) -> dict[str, str]:
        return {"actor": actor}

    return TestClient(gated, raise_server_exceptions=False)


class TestTrust:
    def test_without_the_header_the_actor_is_the_key(self, actor_client, accepting):
        assert actor_client.post("/actor", headers={"X-API-Key": "svc"}).json() == {"actor": "history-site-a"}

    def test_a_service_key_names_the_caller(self, actor_client, accepting):
        resp = actor_client.post("/actor", headers={"X-API-Key": "svc", **ON_BEHALF})

        assert resp.json() == {"actor": "operator@site_a"}

    def test_refused_unless_the_primary_accepts_forwarded_writes(self, actor_client):
        resp = actor_client.post("/actor", headers={"X-API-Key": "svc", **ON_BEHALF})

        assert resp.status_code == 403
        assert resp.json()["code"] == "forwarded_write_refused"

    def test_refused_from_a_client_key(self, actor_client, accepting):
        resp = actor_client.post("/actor", headers={"X-API-Key": "cli", **ON_BEHALF})

        assert resp.status_code == 403
        assert "service key" in resp.json()["detail"]

    def test_refused_on_a_mirror(self, actor_client, accepting, mirror_role, monkeypatch):
        monkeypatch.setenv("CANOPY_ACCEPT_FORWARDED_WRITES", "true")
        get_settings.cache_clear()

        assert actor_client.post("/actor", headers={"X-API-Key": "svc", **ON_BEHALF}).status_code == 403

    @pytest.mark.parametrize("value", ["   ", "x" * 256, "tab\there"])
    def test_malformed_names(self, actor_client, accepting, value):
        resp = actor_client.post("/actor", headers={"X-API-Key": "svc", "X-Canopy-On-Behalf-Of": value})

        assert resp.status_code == 400
        assert resp.json()["code"] == "bad_request"

    def test_the_callers_facilities_are_required(self, actor_client, accepting):
        resp = actor_client.post("/actor", headers={"X-API-Key": "svc", "X-Canopy-On-Behalf-Of": "operator@site_a"})

        assert resp.status_code == 400
        assert "X-Canopy-On-Behalf-Of-Facilities" in resp.json()["detail"]

    @pytest.mark.parametrize("value", ["site-a", '{"site-a": true}', "[1]", '"*"'])
    def test_malformed_facilities(self, actor_client, accepting, value):
        resp = actor_client.post("/actor", headers={"X-API-Key": "svc", **_for(value)})

        assert resp.status_code == 400
        assert resp.json()["code"] == "bad_request"

    def test_nothing_leaks_into_the_next_request(self, actor_client, accepting):
        actor_client.post("/actor", headers={"X-API-Key": "svc", **ON_BEHALF})

        assert forwarded_write.get() is None
        assert actor_client.post("/actor", headers={"X-API-Key": "svc"}).json() == {"actor": "history-site-a"}


def test_a_forwarded_capture_records_the_caller(keys, accepting, monkeypatch):
    enqueued = []

    async def fake_enqueue(function, *args, **kwargs):
        enqueued.append(kwargs)
        return "job-1"

    monkeypatch.setattr("canopy.jobs.gateway.enqueue", fake_enqueue)
    client = TestClient(app, raise_server_exceptions=False)

    resp = client.post("/squirrel/v1/snapshots", json={"title": "t"}, headers={"X-API-Key": "svc", **ON_BEHALF})

    assert resp.status_code == 200, resp.text
    assert enqueued[0]["created_by"] == "operator@site_a"
    # Only enqueued: nothing committed, so nothing to wait for.
    assert "x-canopy-commit-lsn" not in resp.headers


# --------------------------------------------------------------------------- #
# Against Postgres
# --------------------------------------------------------------------------- #


@pytest_asyncio.fixture
async def http(database, keys, accepting):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client


async def test_a_forwarded_write_returns_its_commit_position(http):
    first = await http.post("/squirrel/v1/tags", json={"name": "Area"}, headers={"X-API-Key": "svc", **ON_BEHALF})
    second = await http.post("/squirrel/v1/tags", json={"name": "Beam"}, headers={"X-API-Key": "svc", **ON_BEHALF})

    assert first.status_code == second.status_code == 200, first.text
    assert lsn_value(second.headers["x-canopy-commit-lsn"]) > lsn_value(first.headers["x-canopy-commit-lsn"])


async def test_reads_and_unforwarded_writes_carry_no_position(http):
    read = await http.get("/squirrel/v1/tags", headers={"X-API-Key": "svc", **ON_BEHALF})
    direct = await http.post("/squirrel/v1/tags", json={"name": "Area"}, headers={"X-API-Key": "cli"})

    assert read.status_code == direct.status_code == 200
    assert "x-canopy-commit-lsn" not in read.headers
    assert "x-canopy-commit-lsn" not in direct.headers


async def test_a_failed_forwarded_write_carries_no_position(http):
    await http.post("/squirrel/v1/tags", json={"name": "Area"}, headers={"X-API-Key": "cli"})

    clash = await http.post("/squirrel/v1/tags", json={"name": "Area"}, headers={"X-API-Key": "svc", **ON_BEHALF})

    assert clash.status_code == 409
    assert "x-canopy-commit-lsn" not in clash.headers


async def test_a_forwarded_import_keeps_the_author_and_logs_the_caller(http, caplog):
    bundle = {
        "format": "canopy.squirrel.snapshot-bundle",
        "version": 1,
        "id": "6f1c2c1e-0d4b-4c43-9a55-2f0d7a3c9b10",
        "title": "Old",
        "createdDate": "2025-01-01T00:00:00Z",
        "createdBy": "original-author",
    }

    with caplog.at_level("INFO", logger="canopy.squirrel.api.v1.snapshots"):
        resp = await http.post("/squirrel/v1/snapshots/import", json=bundle, headers={"X-API-Key": "svc", **ON_BEHALF})

    assert resp.status_code == 200, resp.text
    assert resp.json()["snapshot"]["createdBy"] == "original-author"
    new_id = resp.json()["snapshot"]["id"]
    assert f"Snapshot {bundle['id']} imported as {new_id} for operator@site_a" in caplog.text
    assert "x-canopy-commit-lsn" in resp.headers


@pytest_asyncio.fixture
async def sites(http) -> dict[str, str]:
    """Facilities site-a and site-b, by slug."""
    async with db.get_session_maker()() as session:
        rows = [Facility(slug=slug, name=slug) for slug in ("site-a", "site-b")]
        session.add_all(rows)
        await session.commit()
        return {row.slug: row.id for row in rows}


@pytest.fixture
def captures(monkeypatch) -> list[dict]:
    calls: list[dict] = []

    async def fake_enqueue(function, *args, **kwargs):
        calls.append(kwargs)
        return "job-1"

    monkeypatch.setattr("canopy.jobs.gateway.enqueue", fake_enqueue)
    return calls


async def test_a_forwarded_write_reaches_only_the_callers_facilities(http, sites, captures):
    headers = {"X-API-Key": "svc", **_for('["site-a"]')}

    own = await http.post("/squirrel/v1/snapshots", json={"title": "t", "facility": "site-a"}, headers=headers)
    other = await http.post("/squirrel/v1/snapshots", json={"title": "t", "facility": "site-b"}, headers=headers)

    assert own.status_code == 200, own.text
    assert other.status_code == 403
    assert other.json()["code"] == "insufficient_facility_access"
    assert [call["facility_id"] for call in captures] == [sites["site-a"]]


async def test_the_service_keys_grant_bounds_a_wildcard_caller(http, sites, captures):
    headers = {"X-API-Key": "svc-a", **_for('["*"]')}

    other = await http.post("/squirrel/v1/snapshots", json={"title": "t", "facility": "site-b"}, headers=headers)

    assert other.status_code == 403
    assert captures == []


async def test_a_caller_with_no_facilities_reaches_none(http, sites, captures):
    headers = {"X-API-Key": "svc", **_for("[]")}

    resp = await http.post("/squirrel/v1/snapshots", json={"title": "t", "facility": "site-a"}, headers=headers)

    assert resp.status_code == 403
    assert captures == []


async def test_a_forwarded_restore_logs_the_caller(http, monkeypatch, caplog):
    async def fake_enqueue(function, *args, **kwargs):
        return "job-r"

    monkeypatch.setattr("canopy.jobs.gateway.enqueue", fake_enqueue)
    snapshot_id = "6f1c2c1e-0d4b-4c43-9a55-2f0d7a3c9b10"
    async with db.get_session_maker()() as session:
        await session.execute(
            text("INSERT INTO squirrel.snapshot (id, title) VALUES (:id, 'Before')"), {"id": snapshot_id}
        )
        await session.commit()

    with caplog.at_level("INFO", logger="canopy.squirrel.api.v1.snapshots"):
        resp = await http.post(
            f"/squirrel/v1/snapshots/{snapshot_id}/restore", headers={"X-API-Key": "svc", **ON_BEHALF}
        )

    assert resp.status_code == 200, resp.text
    assert f"Restore of snapshot {snapshot_id} queued as job job-r for operator@site_a" in caplog.text


async def test_a_forwarded_config_records_the_caller_not_the_body(http):
    forwarded = await http.post(
        "/magpie/v1/configs/trace",
        json={"data": {}, "createdBy": "claimed"},
        headers={"X-API-Key": "svc", **ON_BEHALF},
    )
    direct = await http.post(
        "/magpie/v1/configs/trace", json={"data": {}, "createdBy": "claimed"}, headers={"X-API-Key": "cli"}
    )

    assert forwarded.json()["createdBy"] == "operator@site_a"
    assert direct.json()["createdBy"] == "claimed"
