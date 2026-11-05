"""Snapshot export and import under facility scoping.

A bundle names its snapshot's facility, so a snapshot brought back from history lands
in the facility it left. Export is read-masked like ``GET /snapshots/{id}``, and import
is gated like a capture. The keys and facilities are ``test_facility_enforcement``'s.
"""

from httpx import AsyncClient
from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession

from canopy.squirrel.data.models import Pv, Snapshot, SnapshotValue, pv_facility
from tests.canopy.squirrel.test_facility_enforcement import (  # noqa: F401 - scoped_keys is an autouse fixture
    NC_HEADERS,
    SC_HEADERS,
    WILD_HEADERS,
    scoped_keys,
    _seed_hierarchy,
)


async def _seed_snapshot(
    session: AsyncSession, facility_id: str | None, pvs: dict[str, str | None] | None = None
) -> str:
    """A snapshot in ``facility_id`` with one value per PV in ``pvs``: ``{address: facility id}``.

    By default it holds one PV in the snapshot's own facility.
    """
    snapshot = Snapshot(title="sc save", facility_id=facility_id)
    session.add(snapshot)
    for address, pv_facility_id in (pvs or {"SC:BUNDLE:SP": facility_id}).items():
        pv = Pv(setpoint_address=address)
        session.add(pv)
        await session.flush()
        if pv_facility_id is not None:
            await session.execute(insert(pv_facility).values(pv_id=pv.id, facility_id=pv_facility_id))
        session.add(SnapshotValue(snapshot_id=snapshot.id, pv_id=pv.id, pv_name=address, setpoint_value={"value": 1}))
    await session.commit()
    return snapshot.id


async def _export(client: AsyncClient, headers: dict, snapshot_id: str) -> dict:
    r = await client.get(f"/v1/snapshots/{snapshot_id}/export", headers=headers)
    assert r.status_code == 200, r.text
    return r.json()


async def test_a_round_trip_keeps_the_facility(client: AsyncClient, db_session: AsyncSession):
    facilities = await _seed_hierarchy(db_session)
    bundle = await _export(client, SC_HEADERS, await _seed_snapshot(db_session, facilities["lcls-sc"].id))

    r = await client.post("/v1/snapshots/import", headers=SC_HEADERS, json=bundle)

    assert bundle["facility"] == "lcls-sc"
    assert r.status_code == 200, r.text
    assert r.json()["snapshot"]["facility"] == "lcls-sc"
    # The importing key can see what it brought back.
    new = await client.get(f"/v1/snapshots/{r.json()['snapshot']['id']}", headers=SC_HEADERS)
    assert new.status_code == 200
    assert new.json()["facility"] == "lcls-sc"


async def test_export_hides_another_facilitys_snapshot(client: AsyncClient, db_session: AsyncSession):
    facilities = await _seed_hierarchy(db_session)
    sid = await _seed_snapshot(db_session, facilities["lcls-sc"].id)

    r = await client.get(f"/v1/snapshots/{sid}/export", headers=NC_HEADERS)

    assert r.status_code == 404


async def test_import_into_a_facility_the_key_lacks_is_refused(client: AsyncClient, db_session: AsyncSession):
    facilities = await _seed_hierarchy(db_session)
    bundle = await _export(client, WILD_HEADERS, await _seed_snapshot(db_session, facilities["lcls-sc"].id))

    r = await client.post("/v1/snapshots/import", headers=NC_HEADERS, json=bundle)

    assert r.status_code == 403
    assert r.json()["code"] == "insufficient_facility_access"


async def test_import_naming_an_unknown_facility_is_404(client: AsyncClient, db_session: AsyncSession):
    facilities = await _seed_hierarchy(db_session)
    bundle = await _export(client, WILD_HEADERS, await _seed_snapshot(db_session, facilities["lcls-sc"].id))

    r = await client.post("/v1/snapshots/import", headers=WILD_HEADERS, json=bundle | {"facility": "elsewhere"})

    assert r.status_code == 404
    assert r.json()["code"] == "facility_not_found"


async def test_a_bundle_without_a_facility_needs_an_unrestricted_key(client: AsyncClient, db_session: AsyncSession):
    await _seed_hierarchy(db_session)
    bundle = await _export(client, WILD_HEADERS, await _seed_snapshot(db_session, None))

    restricted = await client.post("/v1/snapshots/import", headers=SC_HEADERS, json=bundle)
    unrestricted = await client.post("/v1/snapshots/import", headers=WILD_HEADERS, json=bundle)

    assert bundle["facility"] is None
    assert restricted.status_code == 403
    assert unrestricted.status_code == 200, unrestricted.text
    assert unrestricted.json()["snapshot"]["facility"] is None


async def test_import_keeps_only_values_of_pvs_the_key_can_restore(client: AsyncClient, db_session: AsyncSession):
    facilities = await _seed_hierarchy(db_session)
    sc, nc = facilities["lcls-sc"].id, facilities["lcls-nc"].id
    pvs = {"SC:OWN:SP": sc, "NC:OTHER:SP": nc, "UNASSIGNED:SP": None}
    bundle = await _export(client, WILD_HEADERS, await _seed_snapshot(db_session, sc, pvs))
    names = {value["pvName"]: value["pvId"] for value in bundle["values"]}

    r = await client.post("/v1/snapshots/import", headers=SC_HEADERS, json=bundle)

    assert r.status_code == 200, r.text
    # Left out like an unknown id, so the answer doesn't say the PV exists elsewhere.
    assert sorted(v["pvName"] for v in r.json()["skipped"]) == ["NC:OTHER:SP", "UNASSIGNED:SP"]
    assert r.json()["snapshot"]["pvCount"] == 1
    exported = await _export(client, SC_HEADERS, r.json()["snapshot"]["id"])
    assert [(v["pvId"], v["setpointAddress"]) for v in exported["values"]] == [(names["SC:OWN:SP"], "SC:OWN:SP")]


async def test_import_of_only_another_facilitys_pvs_creates_nothing(client: AsyncClient, db_session: AsyncSession):
    facilities = await _seed_hierarchy(db_session)
    sc, nc = facilities["lcls-sc"].id, facilities["lcls-nc"].id
    bundle = await _export(client, WILD_HEADERS, await _seed_snapshot(db_session, sc, {"NC:ONLY:SP": nc}))

    r = await client.post("/v1/snapshots/import", headers=SC_HEADERS, json=bundle)

    assert r.status_code == 400 and r.json()["code"] == "bad_request"
    listing = await client.get("/v1/snapshots", headers=WILD_HEADERS)
    assert listing.json()["totalCount"] == 1


async def test_an_unrestricted_import_keeps_every_known_pv(client: AsyncClient, db_session: AsyncSession):
    facilities = await _seed_hierarchy(db_session)
    sc, nc = facilities["lcls-sc"].id, facilities["lcls-nc"].id
    pvs = {"SC:OWN:SP": sc, "NC:OTHER:SP": nc, "UNASSIGNED:SP": None}
    bundle = await _export(client, WILD_HEADERS, await _seed_snapshot(db_session, sc, pvs))

    r = await client.post("/v1/snapshots/import", headers=WILD_HEADERS, json=bundle)

    assert r.status_code == 200 and r.json()["skipped"] == []
    assert r.json()["snapshot"]["pvCount"] == 3
