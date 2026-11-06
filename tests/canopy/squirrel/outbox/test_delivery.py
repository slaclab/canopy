"""Queueing snapshots for an arm within its caps, and delivering them in order."""

import asyncio
import logging
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from canopy.core.errors import PrimaryTimeout, PrimaryUnavailable
from canopy.squirrel.config import OutboxConfig
from canopy.squirrel.errors import OutboxFull
from canopy.squirrel.data.models import Snapshot, SnapshotOutbox
from canopy.core.forwarding.client import ArmRefused
from canopy.squirrel.outbox.delivery import (
    enqueue,
    bundle_json,
    deliver_arm,
    outbox_full,
    retry_delay,
)
from canopy.squirrel.data.repositories import OutboxRepository

from .conftest import CALLER, CAPTURED, Seeded, queue, make_bundle

CONFIG = OutboxConfig(retry_initial_s=30, retry_max_s=900)


async def _rows(session: AsyncSession) -> dict[str, SnapshotOutbox]:
    session.expire_all()
    result = await session.execute(select(SnapshotOutbox).order_by(SnapshotOutbox.seq))
    return {row.id: row for row in result.scalars().all()}


class FakeArm:
    """Records each bundle sent, and answers with the next queued outcome (``None`` = saved)."""

    def __init__(self, *outcomes):
        self.outcomes = list(outcomes)
        self.sent: list[tuple[str, str, dict, bool]] = []

    async def __call__(self, arm, document, caller, strict):
        self.sent.append((arm, document, caller, strict))
        outcome = self.outcomes.pop(0) if self.outcomes else None
        if isinstance(outcome, Exception):
            raise outcome


def _ids(arm: FakeArm) -> list[str]:
    import json

    return [json.loads(document)["id"] for _, document, _, _ in arm.sent]


# --- enqueue and caps ---------------------------------------------------------- #


async def test_enqueue_stores_the_bundle_as_sent_and_its_copies(db_session, seeded: Seeded):
    bundle = make_bundle(seeded)
    repo = OutboxRepository(db_session)
    await enqueue(repo, arm="arm2", bundle=bundle, facility_id=seeded.facility_id, caller=CALLER, config=CONFIG)
    await db_session.commit()

    row = (await _rows(db_session))[str(bundle.id)]
    assert (row.arm, row.state, row.attempts, row.strict) == ("arm2", "pending", 0, True)
    assert (row.title, row.created_at, row.created_by, row.value_count) == (
        "queued save",
        CAPTURED,
        "operator@head",
        2,
    )
    assert row.size_bytes == len(bundle_json(bundle).encode())
    assert row.caller == CALLER
    assert await repo.bundle(str(bundle.id)) == bundle.model_dump(mode="json", by_alias=True)


async def test_the_count_cap_counts_every_state(db_session, seeded: Seeded):
    config = OutboxConfig(max_snapshots=2)
    await queue(db_session, seeded, state="failed", config=config)
    await queue(db_session, seeded, minutes=1, state="delivered", config=config)

    with pytest.raises(OutboxFull, match="arm2 can't be reached and its outbox is full"):
        await queue(db_session, seeded, minutes=2, config=config)
    await db_session.rollback()
    # Another arm's queue is its own.
    await queue(db_session, seeded, arm="arm3", minutes=3, config=config)
    assert len(await _rows(db_session)) == 3


async def test_the_size_cap(db_session, seeded: Seeded):
    one = len(bundle_json(make_bundle(seeded)).encode())
    config = OutboxConfig(max_bytes=one * 2 - 1)
    await queue(db_session, seeded, config=config)
    with pytest.raises(OutboxFull):
        await queue(db_session, seeded, minutes=1, config=config)
    await db_session.rollback()

    repo = OutboxRepository(db_session)
    assert await outbox_full(repo, "arm2", OutboxConfig(max_bytes=one)) is not None
    assert await outbox_full(repo, "arm2", OutboxConfig(max_bytes=one + 1)) is None
    assert "1 snapshots" in await outbox_full(repo, "arm2", OutboxConfig(max_snapshots=1))


async def test_two_captures_at_once_cannot_both_pass_the_cap(test_engine, seeded: Seeded):
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    maker = async_sessionmaker(test_engine, class_=AsyncSession, expire_on_commit=False)
    config = OutboxConfig(max_snapshots=1)

    async def capture(minutes: int):
        async with maker() as session:
            await queue(session, seeded, minutes=minutes, config=config)

    results = await asyncio.gather(capture(0), capture(1), return_exceptions=True)
    assert sorted(type(r).__name__ for r in results) == ["NoneType", "OutboxFull"]


def test_retry_delay_doubles_up_to_the_cap():
    class Middle:
        def uniform(self, low, high):
            return (low + high) / 2

    assert [retry_delay(n, CONFIG, Middle()) for n in (1, 2, 3, 6, 20)] == [30, 60, 120, 900, 900]


# --- delivery ------------------------------------------------------------------ #


async def test_delivers_one_arms_queue_in_capture_order(db_session, seeded: Seeded):
    later = await queue(db_session, seeded, minutes=5)
    first = await queue(db_session, seeded, minutes=-5)
    other = await queue(db_session, seeded, arm="arm3")
    arm = FakeArm()

    counts = await deliver_arm("arm2", arm, config=CONFIG)

    # Queue order is the order rows were queued in (seq), not capture time.
    assert _ids(arm) == [later, first]
    assert counts == {"delivered": 2, "retried": 0, "failed": 0}
    rows = await _rows(db_session)
    assert {rows[later].state, rows[first].state} == {"delivered"}
    assert rows[later].attempts == 1 and rows[later].delivered_at is not None
    assert rows[other].state == "pending"
    # What went out: the stored caller and grant, and the strict flag.
    assert {(a, caller["on_behalf_of"], strict) for a, _, caller, strict in arm.sent} == {
        ("arm2", "operator@head", True)
    }


@pytest.mark.parametrize(
    "error",
    [
        PrimaryUnavailable("arm2 could not be reached"),
        PrimaryTimeout("arm2 did not answer"),
        ArmRefused("arm2", 503, "unhandled_exception", "down for maintenance"),
        ArmRefused("arm2", 401, "invalid_api_key", "Invalid API key"),
        ArmRefused("arm2", 403, "insufficient_write_access", "API key does not have write access"),
        ArmRefused("arm2", 429, "too_many_requests", "slow down"),
    ],
)
async def test_a_transient_failure_backs_off_and_holds_the_arms_queue(db_session, seeded: Seeded, error):
    first = await queue(db_session, seeded)
    second = await queue(db_session, seeded, minutes=1)
    now = datetime.now(UTC) + timedelta(seconds=1)
    arm = FakeArm(error)

    counts = await deliver_arm("arm2", arm, config=CONFIG, clock=lambda: now)

    assert _ids(arm) == [first]
    assert counts == {"delivered": 0, "retried": 1, "failed": 0}
    rows = await _rows(db_session)
    assert rows[first].state == "pending" and rows[first].attempts == 1
    assert rows[first].last_error
    assert now + timedelta(seconds=24) <= rows[first].next_attempt_at <= now + timedelta(seconds=36)
    assert rows[second].attempts == 0

    # Not due yet: nothing is sent, the later row included.
    arm.sent.clear()
    await deliver_arm("arm2", arm, config=CONFIG, clock=lambda: now + timedelta(seconds=10))
    assert arm.sent == []
    # Due: the head row first, then the rest.
    await deliver_arm("arm2", arm, config=CONFIG, clock=lambda: now + timedelta(minutes=1))
    assert _ids(arm) == [first, second]


@pytest.mark.parametrize(
    "error",
    [
        ArmRefused("arm2", 404, "facility_not_found", "Facility 'lab-b' not found"),
        ArmRefused("arm2", 409, "import_would_skip", "would skip LAB:B:SP (moved out of lab-b)"),
        ArmRefused("arm2", 409, "snapshot_id_conflict", "id exists with another createdDate"),
        ArmRefused("arm2", 403, "insufficient_facility_access", "API key is not scoped to facility: lab-b"),
        ArmRefused("arm2", 400, "nothing_to_import", "None of the bundle's PVs can be imported here"),
        ArmRefused("arm2", 422, "unprocessable_entity", "bad bundle"),
    ],
)
async def test_a_refusal_fails_the_row_with_the_arms_reason_and_the_queue_goes_on(
    db_session, seeded: Seeded, error, caplog
):
    refused = await queue(db_session, seeded)
    after = await queue(db_session, seeded, minutes=1)
    arm = FakeArm(error)

    with caplog.at_level(logging.ERROR, logger="canopy.squirrel.outbox.delivery"):
        counts = await deliver_arm("arm2", arm, config=CONFIG)

    assert _ids(arm) == [refused, after]
    assert counts == {"delivered": 1, "retried": 0, "failed": 1}
    rows = await _rows(db_session)
    assert rows[refused].state == "failed" and rows[refused].failed_at is not None
    assert rows[refused].last_error == f"{error.code}: {error.detail}"
    assert rows[after].state == "delivered"
    assert refused in caplog.text


async def test_a_snapshot_already_back_is_removed_at_once(db_session, seeded: Seeded):
    sid = await queue(db_session, seeded)

    async def arm_saves_and_it_replicates(arm, document, caller, strict):
        db_session.add(Snapshot(id=sid, title="queued save", facility_id=seeded.facility_id, created_at=CAPTURED))
        await db_session.commit()

    assert (await deliver_arm("arm2", arm_saves_and_it_replicates, config=CONFIG))["delivered"] == 1
    assert await _rows(db_session) == {}


async def test_the_sweep_removes_rows_whose_snapshot_came_back(db_session, seeded: Seeded, caplog):
    delivered = await queue(db_session, seeded, state="delivered")
    pending = await queue(db_session, seeded, minutes=1)  # a 504 that was applied after all
    failed = await queue(db_session, seeded, minutes=2, state="failed", last_error="x: y")
    waiting = await queue(db_session, seeded, minutes=3, state="delivered")
    for sid in (delivered, pending, failed):
        db_session.add(Snapshot(id=sid, title="back", created_at=CAPTURED))
    await db_session.commit()

    with caplog.at_level(logging.INFO, logger="canopy.squirrel.outbox.delivery"):
        await deliver_arm("arm2", FakeArm(), config=CONFIG)

    assert set(await _rows(db_session)) == {waiting}
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1 and failed in warnings[0] and "x: y" in warnings[0]


async def test_a_run_stops_starting_sends_after_its_budget(db_session, seeded: Seeded):
    for minutes in range(3):
        await queue(db_session, seeded, minutes=minutes)
    arm = FakeArm()

    assert (await deliver_arm("arm2", arm, config=CONFIG, budget_s=0))["delivered"] == 0
    assert arm.sent == []


async def test_two_runs_for_one_arm_send_each_row_once_and_in_order(db_session, seeded: Seeded):
    ids = [await queue(db_session, seeded, minutes=m) for m in range(4)]
    sent: list[str] = []

    async def slow_arm(arm, document, caller, strict):
        import json

        sent.append(json.loads(document)["id"])
        await asyncio.sleep(0.05)

    first, second = await asyncio.gather(
        deliver_arm("arm2", slow_arm, config=CONFIG), deliver_arm("arm2", slow_arm, config=CONFIG)
    )

    assert sent == ids
    assert sorted([first, second], key=len) == [{"locked": 1}, {"delivered": 4, "retried": 0, "failed": 0}]


async def test_an_allow_skips_row_is_sent_without_strict(db_session, seeded: Seeded):
    sid = await queue(db_session, seeded)
    await db_session.execute(update(SnapshotOutbox).where(SnapshotOutbox.id == sid).values(strict=False))
    await db_session.commit()
    arm = FakeArm()

    await deliver_arm("arm2", arm, config=CONFIG)

    assert [strict for *_, strict in arm.sent] == [False]


def test_settings_from_the_environment(monkeypatch):
    from pydantic import ValidationError

    from canopy.squirrel.config import get_squirrel_config

    monkeypatch.setenv("CANOPY_SQUIRREL__OUTBOX__MAX_SNAPSHOTS", "50")
    monkeypatch.setenv("CANOPY_SQUIRREL__OUTBOX__MAX_BYTES", "1048576")
    outbox = get_squirrel_config().outbox
    assert (outbox.max_snapshots, outbox.max_bytes, outbox.retry_initial_s, outbox.interval_s) == (50, 1048576, 30, 30)

    with pytest.raises(ValidationError, match="retry_initial_s"):
        OutboxConfig(retry_initial_s=1000, retry_max_s=10)


async def test_a_capture_time_without_an_offset_is_read_as_utc(db_session, seeded: Seeded):
    bundle = make_bundle(seeded, created=CAPTURED.replace(tzinfo=None))
    await enqueue(
        OutboxRepository(db_session),
        arm="arm2",
        bundle=bundle,
        facility_id=seeded.facility_id,
        caller=CALLER,
        config=CONFIG,
    )
    await db_session.commit()

    assert (await _rows(db_session))[str(bundle.id)].created_at == CAPTURED
