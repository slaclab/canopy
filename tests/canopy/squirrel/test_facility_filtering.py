"""Facility-filtered PV/snapshot search — squirrel's own query filtering, exercised
against ``core.facility`` rows (facility CRUD itself now lives in
``tests/canopy/core/facilities/``)."""

from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from canopy.squirrel.data.models import Snapshot, pv_facility
from canopy.core.facilities.models import Facility
from tests.canopy.squirrel.conftest import READ_HEADERS, WRITE_HEADERS


async def _seed_facility(session: AsyncSession, slug: str, parent_id: str | None = None) -> Facility:
    facility = Facility(slug=slug, name=slug.upper(), parent_id=parent_id)
    session.add(facility)
    await session.commit()
    return facility


async def test_pv_search_filtered_by_facility_includes_descendants(client: AsyncClient, db_session: AsyncSession):
    lcls = await _seed_facility(db_session, "lcls")
    sc = await _seed_facility(db_session, "lcls-sc", parent_id=lcls.id)
    facet = await _seed_facility(db_session, "facet")

    r = await client.post("/v1/pvs", headers=WRITE_HEADERS, json={"setpointAddress": "SC:PV:SP"})
    sc_pv_id = r.json()["id"]
    r = await client.post("/v1/pvs", headers=WRITE_HEADERS, json={"setpointAddress": "FACET:PV:SP"})
    facet_pv_id = r.json()["id"]

    await db_session.execute(pv_facility.insert().values(pv_id=sc_pv_id, facility_id=sc.id))
    await db_session.execute(pv_facility.insert().values(pv_id=facet_pv_id, facility_id=facet.id))
    await db_session.commit()

    r = await client.get("/v1/pvs/paged?facility=lcls", headers=READ_HEADERS)
    assert r.status_code == 200, r.text
    addrs = {p["setpointAddress"] for p in r.json()["results"]}
    assert addrs == {"SC:PV:SP"}

    r = await client.get("/v1/pvs/paged?facility=lcls-sc", headers=READ_HEADERS)
    assert {p["setpointAddress"] for p in r.json()["results"]} == {"SC:PV:SP"}

    r = await client.get("/v1/pvs/paged", headers=READ_HEADERS)
    addrs = {p["setpointAddress"] for p in r.json()["results"]}
    assert addrs == {"SC:PV:SP", "FACET:PV:SP"}


async def test_filtering_by_an_unknown_facility_is_404(client: AsyncClient, db_session: AsyncSession):
    await _seed_facility(db_session, "lcls")

    for path in ("/v1/pvs", "/v1/pvs/paged", "/v1/pvs/search", "/v1/snapshots"):
        r = await client.get(f"{path}?facility=does-not-exist", headers=READ_HEADERS)
        assert r.status_code == 404, f"{path}: {r.text}"
        assert r.json()["code"] == "facility_not_found"


async def test_pv_in_two_facilities_visible_under_both(client: AsyncClient, db_session: AsyncSession):
    facility_a = await _seed_facility(db_session, "fac-a")
    facility_b = await _seed_facility(db_session, "fac-b")
    r = await client.post("/v1/pvs", headers=WRITE_HEADERS, json={"setpointAddress": "SHARED:SP"})
    pv_id = r.json()["id"]

    await db_session.execute(pv_facility.insert().values(pv_id=pv_id, facility_id=facility_a.id))
    await db_session.execute(pv_facility.insert().values(pv_id=pv_id, facility_id=facility_b.id))
    await db_session.commit()

    for slug in ("fac-a", "fac-b"):
        r = await client.get(f"/v1/pvs/paged?facility={slug}", headers=READ_HEADERS)
        assert {p["setpointAddress"] for p in r.json()["results"]} == {"SHARED:SP"}


async def test_snapshot_list_filtered_by_facility(client: AsyncClient, db_session: AsyncSession):
    lcls = await _seed_facility(db_session, "lcls2")
    sc = await _seed_facility(db_session, "lcls2-sc", parent_id=lcls.id)
    facet = await _seed_facility(db_session, "facet2")

    db_session.add(Snapshot(title="sc-snap", facility_id=sc.id))
    db_session.add(Snapshot(title="facet-snap", facility_id=facet.id))
    db_session.add(Snapshot(title="no-facility-snap"))
    await db_session.commit()

    r = await client.get("/v1/snapshots?facility=lcls2", headers=READ_HEADERS)
    assert {s["title"] for s in r.json()["results"]} == {"sc-snap"}

    r = await client.get("/v1/snapshots?facility=lcls2-sc", headers=READ_HEADERS)
    assert {s["title"] for s in r.json()["results"]} == {"sc-snap"}

    r = await client.get("/v1/snapshots", headers=READ_HEADERS)
    titles = {s["title"] for s in r.json()["results"]}
    assert {"sc-snap", "facet-snap", "no-facility-snap"} <= titles


async def test_create_snapshot_enqueues_with_facility(client: AsyncClient, enqueued: list, db_session: AsyncSession):
    facility = await _seed_facility(db_session, "createfac")
    r = await client.post("/v1/snapshots", headers=WRITE_HEADERS, json={"title": "T", "facility": "createfac"})
    assert r.status_code == 200
    # The slug is resolved in the route (it has to be, to gate the write) and the *id*
    # crosses the queue, so a rename between enqueue and execution can't redirect it.
    assert enqueued[-1][2] == {"title": "T", "description": None, "facility_id": facility.id}
