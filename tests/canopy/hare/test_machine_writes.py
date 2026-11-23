"""Hare PV writes change the machine, so only a primary makes them."""

from fastapi.testclient import TestClient

from canopy.main import app
from tests.conftest import WRITE_HEADERS


def test_a_primary_writes_a_pv():
    with TestClient(app) as client:
        resp = client.put("/hare/v1/pvs/SIM:MACHINE:RT", json={"value": 2.0}, headers=WRITE_HEADERS)

    assert resp.status_code == 200, resp.text


def test_a_mirror_refuses_a_pv_write(mirror_role):
    with TestClient(app) as client:
        resp = client.put("/hare/v1/pvs/SIM:MACHINE:RT", json={"value": 2.0}, headers=WRITE_HEADERS)

    assert resp.status_code == 403
    assert resp.json()["code"] == "machine_read_only"
