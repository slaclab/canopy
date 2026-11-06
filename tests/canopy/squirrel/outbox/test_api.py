"""The snapshot API on a history head serves snapshots it holds only in its outbox."""

from datetime import timedelta

from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from canopy.core.config import Settings
from canopy.squirrel.data.models import Snapshot, SnapshotValue
from tests.canopy.squirrel.conftest import READ_HEADERS, WRITE_HEADERS
from canopy.squirrel.services.snapshot_service import SnapshotService
from tests.canopy.squirrel.test_services_snapshot import FakeEpicsIO

from .conftest import CAPTURED, Seeded, queue


async def _stored(session: AsyncSession, s: Seeded, *, minutes: int, title: str = "stored save") -> str:
    snap = Snapshot(title=title, facility_id=s.facility_id, created_at=CAPTURED + timedelta(minutes=minutes))
    session.add(snap)
    await session.flush()
    session.add(SnapshotValue(snapshot_id=snap.id, pv_id=s.sp_id, pv_name="LAB:B:SP", setpoint_value={"value": 2.0}))
    await session.commit()
    return snap.id


async def _list(client: AsyncClient, query: str = "") -> dict:
    r = await client.get(f"/v1/snapshots{query}", headers=READ_HEADERS)
    assert r.status_code == 200, r.text
    return r.json()


async def test_the_list_interleaves_pending_and_failed_snapshots(client, db_session, seeded: Seeded):
    old = await _stored(db_session, seeded, minutes=-10)
    pending = await queue(db_session, seeded, minutes=0)
    new = await _stored(db_session, seeded, minutes=10)
    failed = await queue(
        db_session,
        seeded,
        minutes=20,
        title="refused save",
        state="failed",
        attempts=2,
        last_error="facility_not_found: Facility 'lab-b' not found",
    )
    delivered = await queue(db_session, seeded, minutes=30, state="delivered", attempts=1)

    page = await _list(client)

    assert [row["id"] for row in page["results"]] == [delivered, failed, new, pending, old]
    assert page["totalCount"] == 5
    by_id = {row["id"]: row for row in page["results"]}
    assert by_id[old]["delivery"] is None
    assert by_id[pending]["delivery"] == {
        "state": "pending",
        "arm": "arm2",
        "queuedAt": by_id[pending]["delivery"]["queuedAt"],
        "attempts": 0,
        "reason": None,
    }
    assert by_id[failed]["delivery"]["state"] == "failed"
    assert by_id[failed]["delivery"]["reason"] == "facility_not_found: Facility 'lab-b' not found"
    assert by_id[delivered]["delivery"]["state"] == "pending"
    assert by_id[delivered]["delivery"]["reason"] == "Saved on arm2; waiting for replication"
    row = by_id[pending]
    assert (row["pvCount"], row["createdBy"], row["facility"], row["retain"], row["importedFrom"]) == (
        2,
        "operator@head",
        "lab-b",
        False,
        None,
    )
    assert row["createdDate"].startswith("2026-11-06T14:02")
    # The meta-PV's value comes from the bundle, so the list has one to show.
    assert [(m["pvName"], m["value"]) for m in row["metaPvs"]] == [("LAB:B:SP", 2.5)]


async def test_the_list_pages_across_both_with_one_cursor(client, db_session, seeded: Seeded):
    ids = []
    for minutes in range(6):
        if minutes % 2:
            ids.append(await queue(db_session, seeded, minutes=minutes))
        else:
            ids.append(await _stored(db_session, seeded, minutes=minutes))

    seen, token = [], None
    while True:
        page = await _list(client, f"?pageSize=2{'&continuationToken=' + token if token else ''}")
        seen += [row["id"] for row in page["results"]]
        assert page["totalCount"] == 6
        if not (token := page["continuationToken"]):
            break
    assert seen == ids[::-1]


async def test_every_filter_applies_to_queued_snapshots(client, db_session, seeded: Seeded):
    sid = await queue(db_session, seeded, title="Morning Save")

    async def found(query: str) -> bool:
        return sid in [row["id"] for row in (await _list(client, query))["results"]]

    assert await found("?title=morning")
    assert not await found("?title=evening")
    assert await found(f"?tags={seeded.tag_id}")
    assert not await found("?tags=00000000-0000-0000-0000-000000000000")
    assert await found("?facility=lab-b")
    assert await found("?start=2026-11-06T14:00:00Z&end=2026-11-06T14:05:00Z")
    assert not await found("?start=2026-11-06T14:03:00Z")


async def test_a_snapshot_that_came_back_is_listed_once(client, db_session, seeded: Seeded):
    sid = await queue(db_session, seeded, state="delivered")
    db_session.add(Snapshot(id=sid, title="queued save", facility_id=seeded.facility_id, created_at=CAPTURED))
    await db_session.commit()

    page = await _list(client)

    assert [(row["id"], row["delivery"]) for row in page["results"]] == [(sid, None)]
    assert page["totalCount"] == 1


async def test_detail_serves_the_bundle_with_names_and_tags_from_the_pv_rows(client, db_session, seeded: Seeded):
    sid = await queue(db_session, seeded)

    r = await client.get(f"/v1/snapshots/{sid}", headers=READ_HEADERS)

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["delivery"]["state"] == "pending" and body["pvCount"] == 2
    values = {v["pvName"]: v for v in body["pvValues"]}
    sp = values["LAB:B:SP"]
    assert (sp["setpointName"], sp["readbackName"], sp["device"]) == ("LAB:B:SP", "LAB:B:RB", "MAG-1")
    assert sp["setpointValue"]["value"] == 2.5 and sp["setpointValue"]["units"] == "A"
    assert [(t["name"], t["groupName"]) for t in sp["tags"]] == [("North", "Area")]
    assert values["LAB:B:ONLY:RB"]["readbackValue"]["value"] == [1, 2, 3]

    r = await client.get(f"/v1/snapshots/{sid}?limit=1&offset=1", headers=READ_HEADERS)
    assert [v["pvName"] for v in r.json()["pvValues"]] == ["LAB:B:SP"]


async def test_export_returns_the_bundle_it_waits_with(client, db_session, seeded: Seeded):
    sid = await queue(db_session, seeded, state="failed", last_error="x: y")

    r = await client.get(f"/v1/snapshots/{sid}/export", headers=READ_HEADERS)

    assert r.status_code == 200, r.text
    assert r.json()["id"] == sid and len(r.json()["values"]) == 2


async def test_compare_works_from_the_bundle(client, db_session, seeded: Seeded):
    stored = await _stored(db_session, seeded, minutes=-5)
    first = await queue(db_session, seeded)
    second = await queue(db_session, seeded, minutes=1)

    r = await client.get(f"/v1/snapshots/{stored}/compare/{first}", headers=READ_HEADERS)
    assert r.status_code == 200, r.text
    assert {d["pvName"]: (d["value1"], d["value2"]) for d in r.json()["differences"]} == {
        "LAB:B:SP": (2.0, 2.5),
        "LAB:B:ONLY:RB": (None, [1, 2, 3]),
    }
    r = await client.get(f"/v1/snapshots/{first}/compare/{second}", headers=READ_HEADERS)
    assert r.json()["differenceCount"] == 0 and r.json()["matchCount"] == 2


async def test_edit_and_delete_are_refused_while_pending_or_failed(client, db_session, seeded: Seeded):
    pending = await queue(db_session, seeded)
    failed = await queue(db_session, seeded, minutes=1, state="failed", last_error="x: y")

    for request in (
        client.patch(f"/v1/snapshots/{pending}", headers=WRITE_HEADERS, json={"title": "renamed"}),
        client.put(f"/v1/snapshots/{pending}", headers=WRITE_HEADERS, json={"retain": True}),
        client.delete(f"/v1/snapshots/{pending}", headers=WRITE_HEADERS),
    ):
        r = await request
        assert r.status_code == 409, r.text
        assert r.json()["code"] == "snapshot_pending"
        assert r.json()["detail"].startswith(f"Snapshot {pending} is waiting for delivery to arm2;")

    r = await client.delete(f"/v1/snapshots/{failed}", headers=WRITE_HEADERS)
    assert r.status_code == 409
    assert r.json()["detail"] == f"Snapshot {failed} was not saved: its delivery to arm2 failed: x: y"


async def test_restore_runs_from_the_bundle_only_with_machine_access(
    client, db_session, seeded: Seeded, enqueued, monkeypatch
):
    pending = await queue(db_session, seeded)
    failed = await queue(db_session, seeded, minutes=1, state="failed", last_error="x: y")

    for sid in (pending, failed):
        r = await client.post(f"/v1/snapshots/{sid}/restore", headers=WRITE_HEADERS)
        assert r.status_code == 200, r.text
    assert [(fn, kw["snapshot_id"]) for fn, _, kw in enqueued] == [
        ("restore_snapshot_task", pending),
        ("restore_snapshot_task", failed),
    ]

    monkeypatch.setattr(Settings, "machine_writes", property(lambda self: False))
    r = await client.post(f"/v1/snapshots/{pending}/restore", headers=WRITE_HEADERS)
    assert r.status_code == 409
    assert r.json() == {
        "code": "snapshot_pending",
        "detail": f"Snapshot {pending} is waiting for delivery to arm2; it can be restored once it's saved there",
    }
    assert len(enqueued) == 2


async def test_the_restore_task_writes_the_bundles_setpoints(db_session, seeded: Seeded):
    sid = await queue(db_session, seeded)
    epics = FakeEpicsIO()

    result = await SnapshotService(db_session, epics, restore_hooks=[]).restore_snapshot(sid)

    assert epics.writes == {"LAB:B:SP": 2.5}
    assert (result.total_pvs, result.success_count) == (1, 1)


async def test_a_queued_snapshot_outside_the_keys_facilities_does_not_exist_for_it(db_session, seeded: Seeded):
    sid = await queue(db_session, seeded)
    service = SnapshotService(db_session, restore_hooks=[], facility_scope={"00000000-0000-0000-0000-000000000000"})

    assert await service.get_by_id(sid) is None
    assert await service.export_bundle(sid) is None
    assert await service.queued(sid) is None
    assert (await service.list_snapshots()).total_count == 0
