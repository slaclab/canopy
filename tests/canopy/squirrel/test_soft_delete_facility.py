"""Soft deletes under facility scoping: who may bring a deleted PV back, and with what.

A deleted PV keeps its row, so registering its address again brings it back with its
past values. Under facility scoping that is a write to the deleted row, so only a key
that could write the row may do it. The keys and facilities are
``test_facility_enforcement``'s.
"""

from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from tests.canopy.squirrel.test_facility_enforcement import (  # noqa: F401 - scoped_keys is an autouse fixture
    NC_HEADERS,
    SC_HEADERS,
    WILD_HEADERS,
    _create_pv,
    scoped_keys,
    _seed_hierarchy,
)


async def _delete(client: AsyncClient, headers: dict, pv_id: str) -> None:
    r = await client.delete(f"/v1/pvs/{pv_id}", headers=headers)
    assert r.status_code == 200, r.text


async def _register(client: AsyncClient, headers: dict, address: str, facilities: list[str] | None = None):
    body = {"setpointAddress": address} | ({"facilities": facilities} if facilities is not None else {})
    return await client.post("/v1/pvs", headers=headers, json=body)


async def test_a_deleted_pv_comes_back_only_for_a_key_that_could_write_it(
    client: AsyncClient, db_session: AsyncSession
):
    await _seed_hierarchy(db_session)
    pv_id = await _create_pv(client, SC_HEADERS, "SC:BACK:SP", ["lcls-sc"])
    await _delete(client, SC_HEADERS, pv_id)

    taken = await _register(client, NC_HEADERS, "SC:BACK:SP", ["lcls-nc"])
    back = await _register(client, SC_HEADERS, "SC:BACK:SP", ["lcls-sc"])

    assert taken.status_code == 409
    assert taken.json()["code"] == "pv_already_exists"
    assert back.status_code == 200, back.text
    assert back.json()["id"] == pv_id


async def test_a_pv_brought_back_takes_the_new_registrations_facilities(client: AsyncClient, db_session: AsyncSession):
    await _seed_hierarchy(db_session)
    moved = await _create_pv(client, WILD_HEADERS, "SC:MOVE:SP", ["lcls-sc"])
    cleared = await _create_pv(client, WILD_HEADERS, "SC:CLEAR:SP", ["lcls-sc"])
    await _delete(client, WILD_HEADERS, moved)
    await _delete(client, WILD_HEADERS, cleared)

    back = await _register(client, WILD_HEADERS, "SC:MOVE:SP", ["facet"])
    unassigned = await _register(client, WILD_HEADERS, "SC:CLEAR:SP")

    assert (back.json()["id"], back.json()["facilities"]) == (moved, ["facet"])
    assert (unassigned.json()["id"], unassigned.json()["facilities"]) == (cleared, [])


async def test_a_bulk_import_brings_back_only_deleted_pvs_the_key_could_write(
    client: AsyncClient, db_session: AsyncSession
):
    await _seed_hierarchy(db_session)
    pv_id = await _create_pv(client, SC_HEADERS, "SC:BULK:SP", ["lcls-sc"])
    await _delete(client, SC_HEADERS, pv_id)
    rows = [{"setpointAddress": "SC:BULK:SP", "facilities": ["lcls-nc"]}]

    taken = await client.post("/v1/pvs/multi", headers=NC_HEADERS, json=rows)
    back = await client.post("/v1/pvs/multi", headers=WILD_HEADERS, json=rows)

    assert taken.status_code == 409
    assert back.status_code == 200, back.text
    assert [(pv["id"], pv["facilities"]) for pv in back.json()] == [(pv_id, ["lcls-nc"])]
