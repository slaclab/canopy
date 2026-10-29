"""Deleting a PV or a snapshot sets ``deleted_at``; the rows and their snapshot values stay.

API tests go through the squirrel sub-app; the restore and capture tests drive
``SnapshotService`` with the fake EPICS adapter from ``test_services_snapshot``.
"""

from datetime import UTC, datetime

import pytest
from httpx import AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from canopy.squirrel.data.models import Pv, Snapshot, SnapshotValue
from tests.canopy.squirrel.conftest import READ_HEADERS, WRITE_HEADERS
from canopy.squirrel.schemas.snapshot import NewSnapshotDTO, RestoreRequestDTO
from canopy.squirrel.services.pv_service import PvService
from canopy.squirrel.services.snapshot_service import SnapshotService
from tests.canopy.squirrel.test_services_snapshot import FakeEpicsIO, _ev, _NoHook


async def _snapshot_with_values(session: AsyncSession, title: str, values: dict[str, float]) -> str:
    """Commit a snapshot holding one setpoint value per PV id in ``values``."""
    snap = Snapshot(title=title)
    session.add(snap)
    await session.flush()
    for pv_id, value in values.items():
        pv = await session.get(Pv, pv_id)
        session.add(
            SnapshotValue(
                snapshot_id=snap.id,
                pv_id=pv_id,
                pv_name=pv.setpoint_address or pv.readback_address or pv.config_address,
                setpoint_value={"value": value, "severity": 0, "status": 0},
                timestamp=datetime.now(UTC),
            )
        )
    await session.commit()
    return snap.id


async def _value_rows(session: AsyncSession, snapshot_id: str) -> int:
    return await session.scalar(
        select(func.count()).select_from(SnapshotValue).where(SnapshotValue.snapshot_id == snapshot_id)
    )


async def _new_tag(client: AsyncClient, group_id: str, name: str) -> str:
    r = await client.post(f"/v1/tags/{group_id}/tags", headers=WRITE_HEADERS, json={"name": name})
    assert r.status_code == 200, r.text
    return next(t["id"] for t in r.json()["group"]["tags"] if t["name"] == name)


# --- PVs ---------------------------------------------------------------------- #


async def test_deleted_pv_keeps_its_values_in_past_snapshots(
    client: AsyncClient, db_session: AsyncSession, sample_tag_group: dict
):
    tag = await _new_tag(client, sample_tag_group["id"], "kept")
    r = await client.post("/v1/pvs", headers=WRITE_HEADERS, json={"setpointAddress": "GONE:SP", "tags": [tag]})
    pid = r.json()["id"]
    sid = await _snapshot_with_values(db_session, "before", {pid: 1.5})

    assert (await client.delete(f"/v1/pvs/{pid}", headers=WRITE_HEADERS)).status_code == 200

    assert await _value_rows(db_session, sid) == 1
    r = await client.get(f"/v1/snapshots/{sid}", headers=READ_HEADERS)
    assert r.status_code == 200
    [value] = r.json()["pvValues"]
    assert value["pvId"] == pid and value["setpointName"] == "GONE:SP"
    assert value["setpointValue"]["value"] == 1.5
    assert [t["name"] for t in value["tags"]] == ["kept"]
    db_session.expire_all()
    assert (await db_session.get(Pv, pid)).deleted_at is not None


async def test_deleted_pv_leaves_every_pv_listing(client: AsyncClient, sample_pv: dict):
    pid = sample_pv["id"]
    assert (await client.delete(f"/v1/pvs/{pid}", headers=WRITE_HEADERS)).status_code == 200

    assert (await client.get("/v1/pvs", headers=READ_HEADERS)).json() == []
    paged = (await client.get("/v1/pvs/paged", headers=READ_HEADERS)).json()
    assert paged["results"] == [] and paged["totalCount"] == 0
    filtered = (await client.get("/v1/pvs/search", headers=READ_HEADERS)).json()
    assert filtered["results"] == [] and filtered["totalCount"] == 0
    assert (await client.get("/v1/pvs/devices", headers=READ_HEADERS)).json() == []
    r = await client.put(f"/v1/pvs/{pid}", headers=WRITE_HEADERS, json={"description": "x"})
    assert r.status_code == 404
    r = await client.post("/v1/pvs/meta", headers=WRITE_HEADERS, json={"pvName": "TEST:PV:001:SP"})
    assert r.status_code == 404


async def test_meta_pv_still_blocks_delete(client: AsyncClient, sample_pv: dict):
    pid = sample_pv["id"]
    await client.post("/v1/pvs/meta", headers=WRITE_HEADERS, json={"pvName": "TEST:PV:001:SP"})

    r = await client.delete(f"/v1/pvs/{pid}", headers=WRITE_HEADERS)
    assert r.status_code == 400 and "meta-PV" in r.json()["detail"]

    assert (await client.delete(f"/v1/pvs/meta/{pid}", headers=WRITE_HEADERS)).status_code == 204
    assert (await client.delete(f"/v1/pvs/{pid}", headers=WRITE_HEADERS)).status_code == 200


@pytest.mark.parametrize(
    "addresses",
    [
        {"setpointAddress": "BACK:SP", "readbackAddress": "BACK:RB"},
        {"readbackAddress": "BACK:RB"},
        {"configAddress": "BACK:CFG"},
    ],
    ids=["setpoint", "readback", "config"],
)
async def test_registering_a_deleted_pv_again_restores_its_row(
    client: AsyncClient, db_session: AsyncSession, sample_tag_group: dict, addresses: dict
):
    old_tag = await _new_tag(client, sample_tag_group["id"], "old")
    new_tag = await _new_tag(client, sample_tag_group["id"], "new")
    first = (
        await client.post(
            "/v1/pvs", headers=WRITE_HEADERS, json={**addresses, "device": "D1", "absTolerance": 0.5, "tags": [old_tag]}
        )
    ).json()
    sid = await _snapshot_with_values(db_session, "before", {first["id"]: 2.0})
    assert (await client.delete(f"/v1/pvs/{first['id']}", headers=WRITE_HEADERS)).status_code == 200

    r = await client.post("/v1/pvs", headers=WRITE_HEADERS, json={**addresses, "device": "D2", "tags": [new_tag]})

    assert r.status_code == 200, r.text
    again = r.json()
    assert again["id"] == first["id"]
    assert again["createdDate"] == first["createdDate"]
    assert (again["device"], again["absTolerance"]) == ("D2", 0.0)
    assert [t["name"] for t in again["tags"]] == ["new"]
    assert [p["id"] for p in (await client.get("/v1/pvs", headers=READ_HEADERS)).json()] == [first["id"]]
    [value] = (await client.get(f"/v1/snapshots/{sid}", headers=READ_HEADERS)).json()["pvValues"]
    assert value["pvId"] == first["id"]

    # Once restored it is a live PV again, so registering it a third time conflicts.
    assert (await client.post("/v1/pvs", headers=WRITE_HEADERS, json=addresses)).status_code == 409


async def test_bulk_registration_restores_deleted_rows_in_input_order(client: AsyncClient):
    gone = (await client.post("/v1/pvs", headers=WRITE_HEADERS, json={"readbackAddress": "BULK:GONE:RB"})).json()
    kept = (await client.post("/v1/pvs", headers=WRITE_HEADERS, json={"setpointAddress": "BULK:KEPT:SP"})).json()
    assert (await client.delete(f"/v1/pvs/{gone['id']}", headers=WRITE_HEADERS)).status_code == 200

    r = await client.post(
        "/v1/pvs/multi",
        headers=WRITE_HEADERS,
        json=[
            {"setpointAddress": "BULK:NEW1:SP"},
            {"readbackAddress": "BULK:GONE:RB", "description": "back"},
            {"setpointAddress": "BULK:NEW2:SP"},
        ],
    )

    assert r.status_code == 200, r.text
    rows = r.json()
    assert [p["setpointAddress"] or p["readbackAddress"] for p in rows] == [
        "BULK:NEW1:SP",
        "BULK:GONE:RB",
        "BULK:NEW2:SP",
    ]
    assert rows[1]["id"] == gone["id"] and rows[1]["description"] == "back"
    assert len({p["id"] for p in rows} | {kept["id"]}) == 4
    # A live PV in the import is still refused.
    r = await client.post("/v1/pvs/multi", headers=WRITE_HEADERS, json=[{"setpointAddress": "BULK:KEPT:SP"}])
    assert r.status_code == 409


# --- snapshots ---------------------------------------------------------------- #


async def test_deleted_snapshot_keeps_its_values_but_leaves_the_api(
    client: AsyncClient, db_session: AsyncSession, sample_pv: dict, enqueued: list
):
    pid = sample_pv["id"]
    other = await _snapshot_with_values(db_session, "other", {pid: 1.0})
    sid = await _snapshot_with_values(db_session, "doomed", {pid: 2.0})

    assert (await client.delete(f"/v1/snapshots/{sid}", headers=WRITE_HEADERS)).status_code == 200

    assert await _value_rows(db_session, sid) == 1
    listing = (await client.get("/v1/snapshots", headers=READ_HEADERS)).json()
    assert [s["id"] for s in listing["results"]] == [other] and listing["totalCount"] == 1
    assert (await client.get(f"/v1/snapshots/{sid}", headers=READ_HEADERS)).status_code == 404
    r = await client.put(f"/v1/snapshots/{sid}", headers=WRITE_HEADERS, json={"title": "x"})
    assert r.status_code == 404
    assert (await client.post(f"/v1/snapshots/{sid}/restore", headers=WRITE_HEADERS)).status_code == 404
    assert enqueued == []
    r = await client.get(f"/v1/snapshots/{other}/compare/{sid}", headers=READ_HEADERS)
    assert r.status_code == 404
    assert (await client.delete(f"/v1/snapshots/{sid}", headers=WRITE_HEADERS)).status_code == 404


async def test_compare_keeps_a_deleted_pvs_tolerance(db_session: AsyncSession):
    pv = Pv(setpoint_address="TOL:SP", abs_tolerance=0.5)
    db_session.add(pv)
    await db_session.flush()
    first = await _snapshot_with_values(db_session, "a", {pv.id: 1.0})
    second = await _snapshot_with_values(db_session, "b", {pv.id: 1.2})
    assert await PvService(db_session).delete(pv.id)

    result = await SnapshotService(db_session).compare_snapshots(first, second)

    assert (result.match_count, result.difference_count) == (1, 0)


async def test_restore_of_a_deleted_snapshot_writes_nothing(db_session: AsyncSession):
    pv = Pv(setpoint_address="R:SP")
    db_session.add(pv)
    await db_session.flush()
    sid = await _snapshot_with_values(db_session, "queued", {pv.id: 3.0})
    io = FakeEpicsIO()
    svc = SnapshotService(db_session, io, restore_hooks=[_NoHook()])
    assert await svc.delete_snapshot(sid)

    result = await svc.restore_snapshot(sid)

    assert result.total_pvs == 0 and io.writes == {}


async def test_restore_never_writes_a_deleted_pv(db_session: AsyncSession):
    live, gone = Pv(setpoint_address="LIVE:SP"), Pv(setpoint_address="GONE:SP")
    db_session.add_all([live, gone])
    await db_session.flush()
    sid = await _snapshot_with_values(db_session, "both", {live.id: 1.0, gone.id: 2.0})
    assert await PvService(db_session).delete(gone.id)
    io = FakeEpicsIO()
    svc = SnapshotService(db_session, io, restore_hooks=[_NoHook()])

    result = await svc.restore_snapshot(sid)
    named = await svc.restore_snapshot(sid, RestoreRequestDTO(pv_ids=[gone.id]))

    assert io.writes == {"LIVE:SP": 1.0}
    assert result.total_pvs == 1 and named.total_pvs == 0


async def test_capture_skips_deleted_pvs(db_session: AsyncSession):
    live, gone = Pv(setpoint_address="CAP:LIVE"), Pv(setpoint_address="CAP:GONE")
    db_session.add_all([live, gone])
    await db_session.flush()
    assert await PvService(db_session).delete(gone.id)
    io = FakeEpicsIO({"CAP:LIVE": _ev(1.0), "CAP:GONE": _ev(2.0)})

    summary = await SnapshotService(db_session, io, restore_hooks=[_NoHook()]).create_snapshot(
        NewSnapshotDTO(title="after delete")
    )

    assert summary.pv_count == 1
