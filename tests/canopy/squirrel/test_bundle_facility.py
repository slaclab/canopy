"""Snapshot export and import under facility scoping.

A bundle names its snapshot's facility, so a snapshot brought back from history lands
in the facility it left. Export is read-masked like ``GET /snapshots/{id}``, and import
is gated like a capture. The keys and facilities are ``test_facility_enforcement``'s.
"""

from httpx import AsyncClient
from sqlalchemy import delete, insert
from sqlalchemy.ext.asyncio import AsyncSession

from canopy.squirrel.data.models import Pv, Snapshot, SnapshotValue, pv_facility
from tests.canopy.squirrel.test_facility_enforcement import (  # noqa: F401 - scoped_keys is an autouse fixture
    NC_HEADERS,
    SC_HEADERS,
    LCLS_HEADERS,
    WILD_HEADERS,
    SC_FACET_HEADERS,
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

    assert r.status_code == 400 and r.json()["code"] == "nothing_to_import"
    listing = await client.get("/v1/snapshots", headers=WILD_HEADERS)
    assert listing.json()["totalCount"] == 1


async def test_an_unrestricted_import_keeps_the_pvs_a_capture_would(client: AsyncClient, db_session: AsyncSession):
    facilities = await _seed_hierarchy(db_session)
    sc, nc = facilities["lcls-sc"].id, facilities["lcls-nc"].id
    pvs = {"SC:OWN:SP": sc, "NC:OTHER:SP": nc, "UNASSIGNED:SP": None}
    in_sc = await _export(client, WILD_HEADERS, await _seed_snapshot(db_session, sc, pvs))
    unassigned = in_sc | {"facility": None}

    scoped = await client.post("/v1/snapshots/import", headers=WILD_HEADERS, json=in_sc)
    everything = await client.post("/v1/snapshots/import", headers=WILD_HEADERS, json=unassigned)

    # A bundle in a facility keeps that facility's PVs, as a capture there would; one
    # with no facility keeps every PV, as an unassigned capture does.
    assert sorted(v["pvName"] for v in scoped.json()["skipped"]) == ["NC:OTHER:SP", "UNASSIGNED:SP"]
    assert everything.json()["skipped"] == [] and everything.json()["snapshot"]["pvCount"] == 3


# --- the capture rule: a bundle's PVs come from its facility's subtree -------- #


async def test_a_key_holding_two_facilities_cant_mix_them(client: AsyncClient, db_session: AsyncSession):
    facilities = await _seed_hierarchy(db_session)
    sc, facet = facilities["lcls-sc"].id, facilities["facet"].id
    bundle = await _export(
        client, WILD_HEADERS, await _seed_snapshot(db_session, sc, {"SC:OWN:SP": sc, "FACET:OTHER:SP": facet})
    )

    # The key could restore both PVs, but a capture into lcls-sc takes only lcls-sc's.
    r = await client.post("/v1/snapshots/import", headers=SC_FACET_HEADERS, json=bundle)

    assert r.status_code == 200, r.text
    assert [v["pvName"] for v in r.json()["skipped"]] == ["FACET:OTHER:SP"]
    assert r.json()["snapshot"]["pvCount"] == 1


async def test_a_pv_below_the_bundles_facility_is_kept(client: AsyncClient, db_session: AsyncSession):
    facilities = await _seed_hierarchy(db_session)
    lcls, sc = facilities["lcls"].id, facilities["lcls-sc"].id
    bundle = await _export(client, WILD_HEADERS, await _seed_snapshot(db_session, lcls, {"SC:CHILD:SP": sc}))

    r = await client.post("/v1/snapshots/import", headers=LCLS_HEADERS, json=bundle)

    assert r.status_code == 200, r.text
    assert r.json()["skipped"] == [] and r.json()["snapshot"]["pvCount"] == 1


async def test_a_pv_moved_out_of_the_facility_since_export_is_skipped(client: AsyncClient, db_session: AsyncSession):
    facilities = await _seed_hierarchy(db_session)
    sc, nc = facilities["lcls-sc"].id, facilities["lcls-nc"].id
    sid = await _seed_snapshot(db_session, sc, {"SC:STAYS:SP": sc, "SC:MOVES:SP": sc})
    bundle = await _export(client, WILD_HEADERS, sid)
    moved = next(v["pvId"] for v in bundle["values"] if v["pvName"] == "SC:MOVES:SP")
    await db_session.execute(delete(pv_facility).where(pv_facility.c.pv_id == moved))
    await db_session.execute(insert(pv_facility).values(pv_id=moved, facility_id=nc))
    await db_session.commit()

    # The lcls key could restore it in lcls-nc, but it no longer belongs in an lcls-sc snapshot.
    r = await client.post("/v1/snapshots/import", headers=LCLS_HEADERS, json=bundle)

    assert r.status_code == 200, r.text
    assert r.json()["skipped"] == [{"pvId": moved, "pvName": "SC:MOVES:SP"}]
    assert r.json()["snapshot"]["pvCount"] == 1
