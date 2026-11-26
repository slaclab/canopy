"""``POST /v1/pvs/import`` and soft-deleted PVs.

A deleted PV is never "registered": ``skip`` doesn't skip it and ``update`` doesn't
update it. A row with its identifying address brings it back, with its id, as ``/multi``
and single create do, or, when the key couldn't write it, refuses the whole import.
"""

import pytest
from httpx import AsyncClient

from tests.canopy.squirrel.test_pv_import_conflicts import (  # noqa: F401 (fixtures)
    ALL,
    ALPHA,
    keys,
    _create,
    _import,
    _all_pvs,
    facilities,
)

MODES = ["skip", "update"]
# One row per identity kind: a setpoint, a setpoint-less readback, a config-only address.
KINDS = {
    "setpoint": {"setpointAddress": "SIM:DEL:01:SET", "readbackAddress": "SIM:DEL:01:RBV"},
    "readback": {"readbackAddress": "SIM:DEL:02:RBV"},
    "config": {"configAddress": "SIM:DEL:03:CFG"},
}


async def _deleted(client: AsyncClient, fields: dict, facility: str) -> dict:
    """A PV registered in ``facility`` with the unrestricted key, then deleted."""
    pv = await _create(client, **fields, description="before", facilities=[facility])
    r = await client.delete(f"/v1/pvs/{pv['id']}", headers=ALL)
    assert r.status_code == 200, r.text
    return pv


@pytest.mark.usefixtures("facilities")
@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("kind", list(KINDS))
async def test_a_deleted_pv_is_brought_back_not_skipped_or_updated(client: AsyncClient, kind, mode):
    old = await _deleted(client, KINDS[kind], "alpha")

    r = await _import(client, [{**KINDS[kind], "description": "after", "facilities": ["beta"]}], mode)

    assert r.status_code == 200, r.text
    result = r.json()
    assert (result["skipped"], result["updated"]) == ([], [])
    [back] = result["created"]
    # The row's whole registration, as a create: its facilities too, which an update keeps.
    assert (back["id"], back["description"], back["facilities"]) == (old["id"], "after", ["beta"])
    assert [pv["id"] for pv in (await _all_pvs(client)).values()] == [old["id"]]


@pytest.mark.usefixtures("facilities")
@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("kind", list(KINDS))
async def test_a_deleted_pv_the_key_cant_bring_back_refuses_the_import(client: AsyncClient, kind, mode):
    # The key holds alpha; the deleted PV is beta's, so bringing it back would move its
    # past values into alpha. The 409 names the row, not the PV's facility.
    await _deleted(client, KINDS[kind], "beta")
    address = next(iter(KINDS[kind].values()))
    rows = [
        {"setpointAddress": "SIM:DEL:09:SET", "facilities": ["alpha"]},
        {**KINDS[kind], "facilities": ["alpha"]},
    ]

    r = await _import(client, rows, mode, headers=ALPHA)

    assert r.status_code == 409, r.text
    body = r.json()
    assert body["code"] == "pv_already_exists"
    assert body["detail"] == f"Addresses already exist: row 2 ({address}): already exists"
    assert await _all_pvs(client) == {}
