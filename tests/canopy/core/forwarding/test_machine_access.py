"""``CANOPY_MACHINE_ACCESS`` on a forward instance: Hare PV writes here, restores on the primary.

Hare runs on the simulation backend (``with TestClient(app)`` starts the lifespans
and the ``CANOPY_API_KEYS`` test keys); the primary is mocked with ``httpx_mock``.
"""

import logging

import pytest
from pytest_httpx import HTTPXMock
from fastapi.testclient import TestClient

from canopy.main import app
from tests.conftest import READ_HEADERS, WRITE_HEADERS
from canopy.core.config import Settings, get_settings

SNAPSHOT_ID = "6f1c2c1e-0d4b-4c43-9a55-2f0d7a3c9b10"
RESTORE = f"/squirrel/v1/snapshots/{SNAPSHOT_ID}/restore"
PRIMARY = "https://primary.example.org"


@pytest.fixture
def machine_write(monkeypatch):
    monkeypatch.setenv("CANOPY_MACHINE_ACCESS", "write")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


class TestForwardWithoutMachineAccess:
    def test_a_pv_write_is_refused(self, forward_role):
        with TestClient(app) as client:
            resp = client.put("/hare/v1/pvs/SIM:TEMP", json={"value": 1.0}, headers=WRITE_HEADERS)

        assert resp.status_code == 403
        assert resp.json()["code"] == "machine_read_only"

    def test_a_restore_is_refused_without_reaching_the_primary(self, forward_role):
        with TestClient(app) as client:
            resp = client.post(RESTORE, headers=WRITE_HEADERS)

        assert resp.status_code == 403
        assert resp.json()["code"] == "machine_read_only"

    def test_machine_access_is_reported_before_the_keys_scope(self, forward_role):
        # As with read_only_instance: a read key isn't told it needs a write key.
        with TestClient(app) as client:
            hare = client.put("/hare/v1/pvs/SIM:TEMP", json={"value": 1.0}, headers=READ_HEADERS)
            restore = client.post(RESTORE, headers=READ_HEADERS)

        assert hare.json()["code"] == restore.json()["code"] == "machine_read_only"

    def test_still_a_401_without_a_key(self, forward_role):
        with TestClient(app) as client:
            assert client.post(RESTORE).status_code == 401
            assert client.put("/hare/v1/pvs/SIM:TEMP", json={"value": 1.0}).status_code == 401


class TestForwardWithMachineAccess:
    def test_a_pv_write_runs_here(self, forward_role, machine_write):
        with TestClient(app) as client:
            put = client.put("/hare/v1/pvs/SIM:FWD:RT", json={"value": 7.5}, headers=WRITE_HEADERS)
            get = client.get("/hare/v1/pvs/SIM:FWD:RT", headers=READ_HEADERS)

        assert put.status_code == 200, put.text
        assert get.json()["value"] == 7.5

    def test_a_pv_write_still_needs_write_scope(self, forward_role, machine_write):
        with TestClient(app) as client:
            resp = client.put("/hare/v1/pvs/SIM:TEMP", json={"value": 1.0}, headers=READ_HEADERS)

        assert resp.json()["code"] == "insufficient_write_access"

    def test_a_restore_goes_to_the_primary(self, forward_role, machine_write, httpx_mock: HTTPXMock):
        httpx_mock.add_response(method="POST", url=PRIMARY + RESTORE, json={"jobId": "r1", "statusUrl": "/jobs/v1/r1"})

        with TestClient(app) as client:
            resp = client.post(RESTORE, json={"pvIds": ["p1"]}, headers=WRITE_HEADERS)

        assert resp.status_code == 200, resp.text
        assert resp.json()["jobId"] == "r1"
        sent = httpx_mock.get_request()
        assert sent.headers["X-API-Key"] == "service-token"
        assert sent.headers["X-Canopy-On-Behalf-Of"] == "env:2@site_a"

    def test_the_instance_reports_it(self, forward_role, machine_write):
        with TestClient(app) as client:
            assert client.get("/v1/instance", headers=READ_HEADERS).json()["machineAccess"] == "write"


class TestOtherRoles:
    def test_a_mirror_never_writes_the_machine(self, mirror_role, machine_write, caplog):
        with TestClient(app) as client:
            hare = client.put("/hare/v1/pvs/SIM:TEMP", json={"value": 1.0}, headers=WRITE_HEADERS)
            restore = client.post(RESTORE, headers=WRITE_HEADERS)
            instance = client.get("/v1/instance", headers=READ_HEADERS).json()

        assert hare.json()["code"] == restore.json()["code"] == "read_only_instance"
        assert instance["machineAccess"] == "read"

    def test_a_primary_always_does(self, monkeypatch):
        monkeypatch.setenv("CANOPY_MACHINE_ACCESS", "read")
        get_settings.cache_clear()
        try:
            with TestClient(app) as client:
                put = client.put("/hare/v1/pvs/SIM:TEMP", json={"value": 2.0}, headers=WRITE_HEADERS)
                instance = client.get("/v1/instance", headers=READ_HEADERS).json()
        finally:
            get_settings.cache_clear()

        assert put.status_code == 200
        assert instance["machineAccess"] == "write"


@pytest.mark.parametrize("role", ["primary", "mirror"])
def test_write_access_outside_forward_mode_is_flagged(role, caplog):
    with caplog.at_level(logging.WARNING, logger="canopy.core.config"):
        Settings(role=role, machine_access="write")

    assert "has no effect" in caplog.text


def test_forward_mode_takes_it_quietly(caplog):
    with caplog.at_level(logging.WARNING, logger="canopy.core.config"):
        Settings(role="forward", machine_access="write")

    assert "has no effect" not in caplog.text
