"""The primary's half: trusting ``X-Canopy-On-Behalf-Of``, recording it, the caller's
facility grant, and ``X-Canopy-Commit-LSN``.

The header-trust tests use fake keys. The commit-position tests run the real app
against Postgres (the ``database`` fixture in this directory's conftest).
"""

from uuid import uuid4

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
from canopy.squirrel.data.models import Pv, pv_facility
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

    def test_the_name_arrives_percent_encoded(self, actor_client, accepting):
        resp = actor_client.post(
            "/actor",
            headers={"X-API-Key": "svc", **ON_BEHALF, "X-Canopy-On-Behalf-Of": "op%C3%A9rateur@site_a"},
        )

        assert resp.json() == {"actor": "opérateur@site_a"}

    def test_the_length_limit_is_on_the_decoded_name(self, actor_client, accepting):
        resp = actor_client.post(
            "/actor", headers={"X-API-Key": "svc", **ON_BEHALF, "X-Canopy-On-Behalf-Of": "%C3%A9" * 255}
        )

        assert resp.json() == {"actor": "é" * 255}

    def test_a_name_that_isnt_utf8_is_a_bad_request(self, actor_client, accepting):
        resp = actor_client.post("/actor", headers={"X-API-Key": "svc", **ON_BEHALF, "X-Canopy-On-Behalf-Of": "%C3"})

        assert resp.status_code == 400
        assert "percent-encoded UTF-8" in resp.json()["detail"]

    def test_a_client_key_is_refused_before_the_name_is_read(self, actor_client, accepting):
        resp = actor_client.post("/actor", headers={"X-API-Key": "cli", **ON_BEHALF, "X-Canopy-On-Behalf-Of": "%C3"})

        assert resp.json()["code"] == "forwarded_write_refused"

    @pytest.mark.parametrize("value", ["   ", "x" * 256, "tab\there", "tab%09here", "%25" * 256])
    def test_malformed_names(self, actor_client, accepting, value):
        resp = actor_client.post("/actor", headers={"X-API-Key": "svc", **ON_BEHALF, "X-Canopy-On-Behalf-Of": value})

        assert resp.status_code == 400
        assert "printable characters" in resp.json()["detail"]

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


class TestForwardedImport:
    """#83's import rules hold for a caller a forward instance names, not just the service key.

    The service key reaches every facility, so what's left out comes from the caller's
    grant and the bundle's facility, never from the service key.
    """

    @pytest_asyncio.fixture
    async def site_pvs(self, sites) -> dict[str, str]:
        """One PV in each site, by slug."""
        async with db.get_session_maker()() as session:
            pvs = {slug: Pv(setpoint_address=f"SIM:{slug.upper()}:SP", device="SIM:DEV") for slug in sites}
            session.add_all(pvs.values())
            await session.flush()
            await session.execute(
                pv_facility.insert(), [{"pv_id": pv.id, "facility_id": sites[slug]} for slug, pv in pvs.items()]
            )
            await session.commit()
            return {slug: pv.id for slug, pv in pvs.items()}

    @staticmethod
    def _bundle(facility: str, *pv_ids: str) -> dict:
        return {
            "format": "canopy.squirrel.snapshot-bundle",
            "version": 1,
            "id": str(uuid4()),
            "title": "Old",
            "createdDate": "2025-01-01T00:00:00Z",
            "facility": facility,
            "values": [{"pvId": pv_id, "pvName": f"PV {pv_id}", "setpointValue": {"value": 1.0}} for pv_id in pv_ids],
        }

    async def _import(self, http, bundle: dict, grant: str):
        return await http.post(
            "/squirrel/v1/snapshots/import", json=bundle, headers={"X-API-Key": "svc", **_for(grant)}
        )

    async def test_a_pv_outside_the_callers_grant_is_left_out(self, http, site_pvs):
        resp = await self._import(http, self._bundle("site-a", site_pvs["site-a"], site_pvs["site-b"]), '["site-a"]')

        assert resp.status_code == 200, resp.text
        assert resp.json()["snapshot"]["pvCount"] == 1
        assert [value["pvId"] for value in resp.json()["skipped"]] == [site_pvs["site-b"]]

    async def test_only_pvs_outside_the_callers_grant_is_refused(self, http, site_pvs):
        resp = await self._import(http, self._bundle("site-a", site_pvs["site-b"]), '["site-a"]')

        assert resp.status_code == 400
        assert resp.json()["code"] == "bad_request"
        async with db.get_session_maker()() as session:
            assert await session.scalar(text("SELECT count(*) FROM squirrel.snapshot")) == 0

    async def test_pvs_in_the_callers_grant_import_whole(self, http, site_pvs):
        resp = await self._import(http, self._bundle("site-a", site_pvs["site-a"]), '["site-a"]')

        assert resp.status_code == 200, resp.text
        assert resp.json()["skipped"] == []

    async def test_a_pv_outside_the_bundles_facility_is_left_out_even_when_the_caller_holds_it(self, http, site_pvs):
        bundle = self._bundle("site-a", site_pvs["site-a"], site_pvs["site-b"])

        resp = await self._import(http, bundle, '["site-a", "site-b"]')

        assert resp.status_code == 200, resp.text
        assert [value["pvId"] for value in resp.json()["skipped"]] == [site_pvs["site-b"]]


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


async def test_a_non_ascii_caller_is_recorded_as_named(http):
    resp = await http.post(
        "/magpie/v1/configs/trace",
        json={"data": {}},
        headers={"X-API-Key": "svc", **ON_BEHALF, "X-Canopy-On-Behalf-Of": "op%C3%A9rateur@site_a"},
    )

    assert resp.status_code == 200, resp.text
    assert resp.json()["createdBy"] == "opérateur@site_a"
