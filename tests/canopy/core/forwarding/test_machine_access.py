"""``CANOPY_MACHINE_ACCESS`` on any instance: Hare PV writes and restores only with ``write``.

Hare runs on the simulation backend (``with TestClient(app)`` starts the lifespans and
the ``CANOPY_API_KEYS`` test keys). Restores run against Postgres (the ``database``
fixture) with the jobs queue stubbed; arm1 is mocked with ``httpx_mock``.
"""

from uuid import uuid4

import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from pytest_httpx import HTTPXMock
from fastapi.testclient import TestClient

from canopy.core import db, instance
from canopy.main import app
from tests.conftest import READ_HEADERS, WRITE_HEADERS
from canopy.core.config import Settings, get_settings
from canopy.squirrel.data.models import Snapshot

from .conftest import ARM1, ARM1_SERVICE_KEY


@pytest.fixture
def access(monkeypatch):
    def _set(value: str) -> None:
        monkeypatch.setenv("CANOPY_MACHINE_ACCESS", value)
        get_settings.cache_clear()

    yield _set
    get_settings.cache_clear()


@pytest.mark.parametrize(("role", "default"), [("primary", "write"), ("mirror", "read")])
def test_the_default_follows_the_role(role, default):
    assert Settings(role=role).machine_access == default
    assert Settings(role=role, machine_access=" ").machine_access == default
    assert Settings(role=role, machine_access="read" if default == "write" else "write").machine_writes is (
        default == "read"
    )


class TestHare:
    def test_a_primary_writes_by_default(self):
        with TestClient(app) as client:
            put = client.put("/hare/v1/pvs/SIM:MA:RT", json={"value": 2.0}, headers=WRITE_HEADERS)
            instance = client.get("/v1/instance", headers=READ_HEADERS).json()

        assert put.status_code == 200, put.text
        assert instance["machineAccess"] == "write"

    def test_a_primary_set_to_read_refuses(self, access):
        access("read")
        with TestClient(app) as client:
            resp = client.put("/hare/v1/pvs/SIM:TEMP", json={"value": 1.0}, headers=WRITE_HEADERS)

        assert resp.status_code == 403
        assert resp.json()["code"] == "machine_read_only"

    def test_a_head_refuses_by_default(self, head, monkeypatch):
        async def states(session, arms):
            return dict.fromkeys(arms, "streaming")

        # /v1/instance reads a head's subscriptions from Postgres; this test needs none.
        monkeypatch.setattr(instance, "arm_states", states)
        with TestClient(app) as client:
            resp = client.put("/hare/v1/pvs/SIM:TEMP", json={"value": 1.0}, headers=WRITE_HEADERS)
            reported = client.get("/v1/instance", headers=READ_HEADERS)

        assert resp.json()["code"] == "machine_read_only"
        assert reported.json()["machineAccess"] == "read"

    def test_a_head_with_write_writes(self, head, access):
        access("write")
        with TestClient(app) as client:
            put = client.put("/hare/v1/pvs/SIM:HEAD:RT", json={"value": 7.5}, headers=WRITE_HEADERS)
            get = client.get("/hare/v1/pvs/SIM:HEAD:RT", headers=READ_HEADERS)

        assert put.status_code == 200, put.text
        assert get.json()["value"] == 7.5

    def test_machine_access_is_reported_before_the_keys_scope(self, access):
        # A read key isn't told it needs a write key where no key would do.
        access("read")
        with TestClient(app) as client:
            resp = client.put("/hare/v1/pvs/SIM:TEMP", json={"value": 1.0}, headers=READ_HEADERS)

        assert resp.json()["code"] == "machine_read_only"

    def test_write_still_needs_write_scope(self):
        with TestClient(app) as client:
            resp = client.put("/hare/v1/pvs/SIM:TEMP", json={"value": 1.0}, headers=READ_HEADERS)

        assert resp.json()["code"] == "insufficient_write_access"

    def test_still_a_401_without_a_key(self, access):
        access("read")
        with TestClient(app) as client:
            assert client.put("/hare/v1/pvs/SIM:TEMP", json={"value": 1.0}).status_code == 401


class TestRestore:
    """Where a restore runs, by machine access and by the snapshot's owner."""

    @pytest_asyncio.fixture
    async def snapshots(self, database, as_instance, monkeypatch) -> dict[str, str]:
        jobs = []

        async def enqueue(function, *args, **kwargs):
            jobs.append(function)
            return "job-here"

        monkeypatch.setattr("canopy.jobs.gateway.enqueue", enqueue)
        made = {"jobs": jobs}
        for owner in ("head", "arm1", "arm2"):
            with as_instance(owner):
                async with db.get_session_maker()() as session:
                    snapshot = Snapshot(title=owner)
                    session.add(snapshot)
                    await session.commit()
                    made[owner] = snapshot.id
        return made

    @pytest_asyncio.fixture
    async def http(self, head, snapshots):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            yield client

    async def _restore(self, http, snapshot_id: str):
        return await http.post(f"/squirrel/v1/snapshots/{snapshot_id}/restore", json={}, headers=WRITE_HEADERS)

    @pytest.mark.parametrize("owner", ["head", "arm1", "arm2"])
    async def test_with_write_a_head_restores_any_snapshot_here(self, http, snapshots, access, owner):
        access("write")

        resp = await self._restore(http, snapshots[owner])

        assert resp.status_code == 200, resp.text
        assert resp.json()["jobId"] == "job-here"
        assert snapshots["jobs"] == ["restore_snapshot_task"]

    async def test_with_read_an_arm1_snapshot_goes_to_arm1(self, http, snapshots, httpx_mock: HTTPXMock):
        snapshot = snapshots["arm1"]
        httpx_mock.add_response(
            method="POST",
            url=f"{ARM1}/squirrel/v1/snapshots/{snapshot}/restore",
            json={"jobId": "job-on-arm1", "statusUrl": "/jobs/v1/job-on-arm1"},
        )

        resp = await self._restore(http, snapshot)

        assert resp.json()["jobId"] == "job-on-arm1"
        assert httpx_mock.get_request().headers["X-API-Key"] == ARM1_SERVICE_KEY
        assert snapshots["jobs"] == []

    async def test_with_read_a_read_only_arms_snapshot_is_not_ours(self, http, snapshots):
        resp = await self._restore(http, snapshots["arm2"])

        assert resp.status_code == 403
        assert resp.json()["code"] == "not_owner"

    async def test_with_read_the_heads_own_snapshot_is_machine_read_only(self, http, snapshots):
        resp = await self._restore(http, snapshots["head"])

        assert resp.status_code == 403
        assert resp.json()["code"] == "machine_read_only"
        assert snapshots["jobs"] == []

    async def test_an_unknown_snapshot_is_a_404_either_way(self, http, snapshots):
        assert (await self._restore(http, str(uuid4()))).json()["code"] == "snapshot_not_found"
