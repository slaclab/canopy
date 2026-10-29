"""The snapshot ``retain`` flag, and moving a snapshot between instances as a bundle.

Export and import run on one test database here: the export stands for a history
mirror's copy, and importing it back creates a second, new snapshot.
"""

import uuid
from datetime import UTC, datetime

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from canopy.core.config import get_settings
from canopy.squirrel.data.models import Pv, Snapshot, SnapshotValue
from tests.canopy.squirrel.conftest import READ_HEADERS, WRITE_HEADERS
from canopy.squirrel.services.snapshot_service import SnapshotService
from tests.canopy.squirrel.test_services_snapshot import FakeEpicsIO, _NoHook

CAPTURED = datetime(2026, 3, 1, 8, 30, tzinfo=UTC)


@pytest.fixture
def instance_name(monkeypatch):
    monkeypatch.setenv("CANOPY_INSTANCE", "site-a")
    get_settings.cache_clear()
    yield "site-a"
    get_settings.cache_clear()


async def _seed(session: AsyncSession) -> tuple[str, Pv, Pv]:
    """A snapshot with a setpoint PV and a readback-only PV; returns its id and the PVs."""
    sp = Pv(setpoint_address="EXP:SP", readback_address="EXP:RB", device="D")
    rb = Pv(readback_address="EXP:ONLY:RB")
    snap = Snapshot(title="weekly save", description="before shutdown", created_by="operator")
    session.add_all([sp, rb, snap])
    await session.flush()
    session.add_all(
        [
            SnapshotValue(
                snapshot_id=snap.id,
                pv_id=sp.id,
                pv_name="EXP:SP",
                setpoint_value={"value": 1.25, "units": "A", "precision": 3, "extra": "kept"},
                readback_value={"value": 1.2},
                status=0,
                severity=0,
                timestamp=CAPTURED,
            ),
            SnapshotValue(
                snapshot_id=snap.id,
                pv_id=rb.id,
                pv_name="EXP:ONLY:RB",
                readback_value={"value": [1, 2, 3]},
                status=3,
                severity=1,
                timestamp=CAPTURED,
            ),
        ]
    )
    await session.commit()
    return snap.id, sp, rb


async def _export(client: AsyncClient, snapshot_id: str) -> dict:
    r = await client.get(f"/v1/snapshots/{snapshot_id}/export", headers=READ_HEADERS)
    assert r.status_code == 200, r.text
    return r.json()


# --- retain ------------------------------------------------------------------- #


async def test_patch_sets_retain_and_reads_report_it(client: AsyncClient, db_session: AsyncSession):
    sid, _, _ = await _seed(db_session)

    r = await client.patch(f"/v1/snapshots/{sid}", headers=WRITE_HEADERS, json={"retain": True})

    assert r.status_code == 200, r.text
    assert r.json()["retain"] is True and r.json()["title"] == "weekly save"
    [row] = (await client.get("/v1/snapshots", headers=READ_HEADERS)).json()["results"]
    assert row["retain"] is True and row["importedFrom"] is None
    assert (await client.get(f"/v1/snapshots/{sid}", headers=READ_HEADERS)).json()["retain"] is True
    # Fields left out keep their value, through PATCH and PUT alike.
    r = await client.patch(f"/v1/snapshots/{sid}", headers=WRITE_HEADERS, json={"title": "renamed"})
    assert (r.json()["title"], r.json()["retain"]) == ("renamed", True)
    r = await client.put(f"/v1/snapshots/{sid}", headers=WRITE_HEADERS, json={"retain": False})
    assert (r.json()["title"], r.json()["retain"]) == ("renamed", False)


async def test_patch_of_a_deleted_snapshot_is_404(client: AsyncClient, db_session: AsyncSession):
    sid, _, _ = await _seed(db_session)
    assert (await client.delete(f"/v1/snapshots/{sid}", headers=WRITE_HEADERS)).status_code == 200

    r = await client.patch(f"/v1/snapshots/{sid}", headers=WRITE_HEADERS, json={"retain": True})

    assert r.status_code == 404


# --- export ------------------------------------------------------------------- #


async def test_export_carries_every_value_with_its_pv_addresses(
    client: AsyncClient, db_session: AsyncSession, instance_name: str
):
    sid, sp, rb = await _seed(db_session)
    # A PV deleted after the capture still exports its addresses.
    assert (await client.delete(f"/v1/pvs/{rb.id}", headers=WRITE_HEADERS)).status_code == 200

    bundle = await _export(client, sid)

    assert bundle["format"] == "canopy.squirrel.snapshot-bundle" and bundle["version"] == 1
    assert bundle["sourceInstance"] == instance_name
    assert (bundle["id"], bundle["title"], bundle["description"], bundle["createdBy"]) == (
        sid,
        "weekly save",
        "before shutdown",
        "operator",
    )
    assert [v["pvName"] for v in bundle["values"]] == ["EXP:ONLY:RB", "EXP:SP"]
    readback_only, setpoint = bundle["values"]
    assert (readback_only["pvId"], readback_only["readbackAddress"], readback_only["setpointAddress"]) == (
        rb.id,
        "EXP:ONLY:RB",
        None,
    )
    assert readback_only["readbackValue"] == {"value": [1, 2, 3]}
    assert (readback_only["status"], readback_only["severity"]) == (3, 1)
    assert (setpoint["setpointAddress"], setpoint["readbackAddress"]) == ("EXP:SP", "EXP:RB")
    # The stored blob comes out unchanged, keys the DTOs don't model included.
    assert setpoint["setpointValue"] == {"value": 1.25, "units": "A", "precision": 3, "extra": "kept"}
    assert datetime.fromisoformat(setpoint["timestamp"]) == CAPTURED


async def test_export_of_a_missing_or_deleted_snapshot_is_404(client: AsyncClient, db_session: AsyncSession):
    sid, _, _ = await _seed(db_session)
    assert (await client.delete(f"/v1/snapshots/{sid}", headers=WRITE_HEADERS)).status_code == 200

    assert (await client.get(f"/v1/snapshots/{sid}/export", headers=READ_HEADERS)).status_code == 404
    assert (await client.get(f"/v1/snapshots/{uuid.uuid4()}/export", headers=READ_HEADERS)).status_code == 404
    assert (await client.get("/v1/snapshots/not-a-uuid/export", headers=READ_HEADERS)).status_code == 404


# --- import ------------------------------------------------------------------- #


async def test_import_creates_a_new_snapshot_from_an_export(client: AsyncClient, db_session: AsyncSession):
    sid, _, _ = await _seed(db_session)
    bundle = await _export(client, sid)

    r = await client.post("/v1/snapshots/import", headers=WRITE_HEADERS, json=bundle)

    assert r.status_code == 200, r.text
    result = r.json()
    new = result["snapshot"]
    assert result["skipped"] == []
    assert new["id"] != sid and new["importedFrom"] == sid
    assert new["title"] == "Restored from history: weekly save"
    assert (new["description"], new["createdBy"], new["pvCount"], new["retain"]) == (
        "before shutdown",
        "operator",
        2,
        False,
    )
    assert datetime.fromisoformat(new["createdDate"]) > CAPTURED
    listing = (await client.get("/v1/snapshots", headers=READ_HEADERS)).json()["results"]
    assert [row["id"] for row in listing] == [new["id"], sid]

    original = (await client.get(f"/v1/snapshots/{sid}", headers=READ_HEADERS)).json()["pvValues"]
    copied = (await client.get(f"/v1/snapshots/{new['id']}", headers=READ_HEADERS)).json()["pvValues"]
    fields = ("pvId", "pvName", "setpointValue", "readbackValue", "status", "severity", "timestamp")
    assert [{k: v[k] for k in fields} for v in copied] == [{k: v[k] for k in fields} for v in original]
    # The new snapshot exports the same values again.
    assert (await _export(client, new["id"]))["values"] == bundle["values"]


async def test_import_keeps_a_deleted_pvs_value_but_restore_still_skips_it(
    client: AsyncClient, db_session: AsyncSession
):
    sid, sp, _ = await _seed(db_session)
    bundle = await _export(client, sid)
    assert (await client.delete(f"/v1/pvs/{sp.id}", headers=WRITE_HEADERS)).status_code == 200

    new = (await client.post("/v1/snapshots/import", headers=WRITE_HEADERS, json=bundle)).json()["snapshot"]

    assert new["pvCount"] == 2
    values = (await client.get(f"/v1/snapshots/{new['id']}", headers=READ_HEADERS)).json()["pvValues"]
    assert {v["pvName"]: v["setpointName"] for v in values}["EXP:SP"] == "EXP:SP"
    io = FakeEpicsIO()
    result = await SnapshotService(db_session, io, restore_hooks=[_NoHook()]).restore_snapshot(new["id"])
    assert result.total_pvs == 0 and io.writes == {}


async def test_import_lists_values_whose_pv_is_not_here(client: AsyncClient, db_session: AsyncSession):
    sid, _, _ = await _seed(db_session)
    bundle = await _export(client, sid)
    stranger = str(uuid.uuid4())
    bundle["values"][0]["pvId"] = stranger

    r = await client.post("/v1/snapshots/import", headers=WRITE_HEADERS, json=bundle)

    assert r.status_code == 200, r.text
    assert r.json()["skipped"] == [{"pvId": stranger, "pvName": "EXP:ONLY:RB"}]
    assert r.json()["snapshot"]["pvCount"] == 1


async def test_import_with_no_known_pv_creates_nothing(client: AsyncClient, db_session: AsyncSession):
    sid, _, _ = await _seed(db_session)
    bundle = await _export(client, sid)
    for value in bundle["values"]:
        value["pvId"] = str(uuid.uuid4())

    r = await client.post("/v1/snapshots/import", headers=WRITE_HEADERS, json=bundle)

    assert r.status_code == 400
    assert (await client.get("/v1/snapshots", headers=READ_HEADERS)).json()["totalCount"] == 1


async def test_import_reads_a_timestamp_without_offset_as_utc(client: AsyncClient, db_session: AsyncSession):
    sid, _, _ = await _seed(db_session)
    bundle = await _export(client, sid)
    for value in bundle["values"]:
        value["timestamp"] = "2026-03-01T08:30:00"

    new = (await client.post("/v1/snapshots/import", headers=WRITE_HEADERS, json=bundle)).json()["snapshot"]

    values = (await client.get(f"/v1/snapshots/{new['id']}", headers=READ_HEADERS)).json()["pvValues"]
    assert {datetime.fromisoformat(v["timestamp"]) for v in values} == {CAPTURED}


@pytest.mark.parametrize(
    "change",
    [
        {"format": "something-else"},
        {"version": 2},
        {"id": "not-a-uuid"},
        {"title": "x" * 256},
        {"values": [{"pvId": "not-a-uuid", "pvName": "EXP:SP"}]},
        {"values": [{"pvId": str(uuid.uuid4()), "pvName": "x" * 256}]},
    ],
    ids=["format", "version", "id", "title", "pv-id", "pv-name"],
)
async def test_import_rejects_a_malformed_bundle(client: AsyncClient, db_session: AsyncSession, change: dict):
    sid, _, _ = await _seed(db_session)
    bundle = {**(await _export(client, sid)), **change}

    r = await client.post("/v1/snapshots/import", headers=WRITE_HEADERS, json=bundle)

    assert r.status_code == 422, r.text
