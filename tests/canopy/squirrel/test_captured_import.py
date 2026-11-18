"""A captured import: a snapshot another instance captured, saved under its own id and time.

A history head captures a facility's PVs, assigns the snapshot's id and time, and sends
the bundle to the arm that owns the facility, which saves it with ``captured``. The
route that accepts it, for service keys only, is in the forwarding layer; these tests
call the service, on one test database. The bundle comes from an export, with a fresh
id standing for the head's.
"""

import uuid
from datetime import UTC, datetime

import pytest
from httpx import AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from canopy.core.errors import NotOwner, InsufficientFacilityAccess
from canopy.squirrel.errors import ImportWouldSkip, SnapshotIdConflict
from canopy.squirrel.data.models import Pv, Snapshot, SnapshotValue
from canopy.core.facilities.models import Facility
from canopy.squirrel.schemas.snapshot import SnapshotBundleDTO
from tests.canopy.squirrel.test_soft_deletes import _race
from canopy.squirrel.services.snapshot_service import SnapshotService
from tests.canopy.squirrel.test_snapshot_retain_import import _seed, _export

TAKEN = datetime(2026, 4, 2, 14, 2, 7, 123456, tzinfo=UTC)


async def _captured_bundle(client: AsyncClient, db_session: AsyncSession) -> dict:
    """An exported bundle with a fresh id and capture time, as a head would send it."""
    sid, _, _ = await _seed(db_session)
    bundle = await _export(client, sid)
    bundle["id"] = str(uuid.uuid4())
    bundle["createdDate"] = TAKEN.isoformat()
    bundle["title"] = "before the energy change"
    return bundle


async def _save(session: AsyncSession, bundle: dict, **options):
    options.setdefault("created_by", "alice")
    return await SnapshotService(session).import_bundle(
        SnapshotBundleDTO.model_validate(bundle), captured=True, **options
    )


async def _value_count(session: AsyncSession, snapshot_id: str) -> int:
    return await session.scalar(select(func.count()).where(SnapshotValue.snapshot_id == snapshot_id))


async def test_a_captured_import_keeps_the_bundles_id_time_and_title(client, db_session):
    bundle = await _captured_bundle(client, db_session)

    result = await _save(db_session, bundle)
    await db_session.commit()

    assert result.created and result.skipped == []
    summary = result.snapshot
    assert (summary.id, summary.created_date, summary.title) == (bundle["id"], TAKEN, "before the energy change")
    assert (summary.description, summary.created_by) == ("before shutdown", "alice")
    assert (summary.imported_from, summary.retain, summary.pv_count) == (None, False, 2)
    assert summary.owner_instance == "arm1"
    exported = await _export(client, bundle["id"])
    assert exported["values"] == bundle["values"]
    assert exported["createdBy"] == "alice"


async def test_a_captured_import_needs_its_author(client, db_session):
    bundle = await _captured_bundle(client, db_session)

    with pytest.raises(ValueError, match="created_by"):
        await _save(db_session, bundle, created_by=None)


async def test_a_repeat_writes_nothing_and_answers_with_what_was_saved(client, db_session):
    bundle = await _captured_bundle(client, db_session)
    stranger = str(uuid.uuid4())
    bundle["values"][0]["pvId"] = stranger
    first = await _save(db_session, bundle)
    await db_session.commit()

    again = await _save(db_session, bundle, created_by="someone else")

    assert first.created and not again.created
    assert again.snapshot.id == bundle["id"] and again.snapshot.created_by == "alice"
    assert again.snapshot.pv_count == 1
    assert [s.pv_id for s in again.skipped] == [s.pv_id for s in first.skipped] == [stranger]
    assert await _value_count(db_session, bundle["id"]) == 1


async def test_a_repeat_is_answered_before_the_rules_run(client, db_session):
    # Delivered once, then the caller's grant narrowed and a PV moved out of reach. A
    # resend still reports the snapshot as saved, strict or not, instead of refusing it.
    bundle = await _captured_bundle(client, db_session)
    await _save(db_session, bundle)
    await db_session.commit()
    bundle["values"].append({**bundle["values"][0], "pvId": str(uuid.uuid4()), "pvName": "GONE:SP"})

    narrowed = SnapshotService(db_session, facility_scope=set())
    again = await narrowed.import_bundle(
        SnapshotBundleDTO.model_validate(bundle), captured=True, created_by="alice", strict=True
    )

    assert not again.created
    assert [s.pv_name for s in again.skipped] == ["GONE:SP"]
    with pytest.raises(InsufficientFacilityAccess):
        await narrowed.import_bundle(SnapshotBundleDTO.model_validate({**bundle, "id": str(uuid.uuid4())}))


async def test_a_snapshot_deleted_since_still_counts_as_saved(client, db_session):
    bundle = await _captured_bundle(client, db_session)
    await _save(db_session, bundle)
    await db_session.commit()
    assert await SnapshotService(db_session).delete_snapshot(bundle["id"])
    await db_session.commit()

    again = await _save(db_session, bundle)

    assert not again.created and again.snapshot.id == bundle["id"]


@pytest.mark.parametrize(
    "change",
    [
        {"createdDate": "2026-04-02T14:02:08+00:00"},
        {"facility": "elsewhere"},
    ],
    ids=["time", "facility"],
)
async def test_an_id_already_used_for_another_snapshot_is_a_conflict(client, db_session, change):
    bundle = await _captured_bundle(client, db_session)
    await _save(db_session, bundle)
    await db_session.commit()

    with pytest.raises(SnapshotIdConflict, match=bundle["id"]):
        await _save(db_session, {**bundle, **change})


async def test_the_same_time_written_another_way_is_not_a_conflict(client, db_session):
    bundle = await _captured_bundle(client, db_session)
    await _save(db_session, bundle)
    await db_session.commit()

    again = await _save(db_session, {**bundle, "createdDate": "2026-04-02T07:02:07.123456-07:00"})

    assert not again.created


async def test_strict_refuses_a_bundle_that_would_skip_and_saves_nothing(client, db_session):
    bundle = await _captured_bundle(client, db_session)
    moved = bundle["values"][0]
    moved["pvId"] = str(uuid.uuid4())

    with pytest.raises(ImportWouldSkip, match=rf"1 value\(s\) would be skipped.*{moved['pvName']} \({moved['pvId']}\)"):
        await _save(db_session, bundle, strict=True)
    await db_session.rollback()

    assert await db_session.get(Snapshot, bundle["id"]) is None
    accepted = await _save(db_session, bundle, strict=False)
    assert accepted.created and [s.pv_name for s in accepted.skipped] == [moved["pvName"]]


async def test_strict_names_the_first_twenty_skipped_pvs_and_the_total(client, db_session):
    bundle = await _captured_bundle(client, db_session)
    bundle["values"] += [
        {**bundle["values"][0], "pvId": str(uuid.uuid4()), "pvName": f"GONE:{n:02}"} for n in range(25)
    ]

    with pytest.raises(ImportWouldSkip) as refused:
        await _save(db_session, bundle, strict=True)

    assert refused.value.detail.startswith("25 value(s) would be skipped")
    assert "GONE:19" in refused.value.detail and "GONE:20" not in refused.value.detail
    assert "and 5 more" in refused.value.detail


async def test_two_deliveries_at_once_save_one_snapshot(test_engine, client, db_session):
    bundle = await _captured_bundle(client, db_session)

    async with _race(test_engine, lambda s: _save(s, bundle), lambda s: _save(s, bundle)) as commit:
        [second] = await commit()

    assert not second.created and second.snapshot.id == bundle["id"]
    assert await _value_count(db_session, bundle["id"]) == 2


async def test_a_history_import_still_makes_a_new_copy(client, db_session):
    bundle = await _captured_bundle(client, db_session)

    result = await SnapshotService(db_session).import_bundle(SnapshotBundleDTO.model_validate(bundle))

    assert result.created and result.snapshot.id != bundle["id"]
    assert result.snapshot.imported_from == bundle["id"]
    assert result.snapshot.title == "Restored from history: before the energy change"


# --- owners ----------------------------------------------------------------------- #
# The suite runs as arm1; rows made inside ``as_instance("arm2")`` stand for an arm's
# rows replicated to a head.


@pytest.mark.parametrize("captured", [False, True], ids=["history", "captured"])
async def test_a_bundle_for_another_instances_facility_is_refused(client, db_session, as_instance, captured):
    bundle = await _captured_bundle(client, db_session)
    with as_instance("arm2"):
        db_session.add(Facility(slug="theirs", name="Theirs"))
        await db_session.commit()

    with pytest.raises(NotOwner, match="'arm2'"):
        await SnapshotService(db_session).import_bundle(
            SnapshotBundleDTO.model_validate({**bundle, "facility": "theirs"}),
            captured=captured,
            created_by="alice" if captured else None,
        )


@pytest.mark.parametrize("captured", [False, True], ids=["history", "captured"])
async def test_values_of_another_instances_pvs_are_skipped(client, db_session, as_instance, captured):
    bundle = await _captured_bundle(client, db_session)
    with as_instance("arm2"):
        theirs = Pv(setpoint_address="THEIRS:SP")
        db_session.add(theirs)
        await db_session.commit()
    bundle["values"].append({**bundle["values"][0], "pvId": theirs.id, "pvName": "THEIRS:SP"})

    result = await SnapshotService(db_session).import_bundle(
        SnapshotBundleDTO.model_validate(bundle), captured=captured, created_by="alice" if captured else None
    )

    assert [s.pv_name for s in result.skipped] == ["THEIRS:SP"]
    assert result.snapshot.pv_count == 2
    if captured:
        with pytest.raises(ImportWouldSkip, match="THEIRS:SP"):
            await _save(db_session, {**bundle, "id": str(uuid.uuid4())}, strict=True)


async def test_an_id_another_instance_holds_is_a_conflict(client, db_session, as_instance):
    bundle = await _captured_bundle(client, db_session)
    with as_instance("arm2"):
        db_session.add(Snapshot(id=bundle["id"], title=bundle["title"], created_at=TAKEN))
        await db_session.commit()

    with pytest.raises(SnapshotIdConflict, match="owned by 'arm2'"):
        await _save(db_session, bundle)
