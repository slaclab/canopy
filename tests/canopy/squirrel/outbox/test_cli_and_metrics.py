"""``python -m canopy.squirrel.outbox`` and the outbox gauges, which share ``outbox_status``."""

import json

import pytest
import asyncpg
from sqlalchemy import select, update
from prometheus_client import CollectorRegistry, generate_latest

from canopy.squirrel.config import OutboxConfig
from canopy.squirrel.data.models import SnapshotOutbox
from canopy.squirrel.outbox.status import outbox_status
from canopy.squirrel.outbox.metrics import OutboxCollector
from tests.canopy.squirrel.conftest import _ADMIN_DSN
from canopy.squirrel.outbox.__main__ import run, build_parser

from .conftest import Seeded, queue

TEST_DSN = _ADMIN_DSN.rsplit("/", 1)[0] + "/canopy_test"


async def _cli(capsys, *argv: str) -> tuple[int, dict | None]:
    code = await run(build_parser().parse_args(list(argv)))
    out = capsys.readouterr().out
    return code, json.loads(out) if out.strip() else None


async def _state(db_session, sid: str) -> SnapshotOutbox | None:
    db_session.expire_all()
    return (await db_session.execute(select(SnapshotOutbox).where(SnapshotOutbox.id == sid))).scalar_one_or_none()


async def test_status_lists_each_arm_and_every_failed_row(db_session, seeded: Seeded):
    await queue(db_session, seeded)
    await queue(db_session, seeded, minutes=1, state="delivered")
    failed = await queue(db_session, seeded, minutes=2, state="failed", last_error="x: y", attempts=3)
    await queue(db_session, seeded, arm="arm3", minutes=3)

    conn = await asyncpg.connect(TEST_DSN)
    try:
        status = await outbox_status(conn, arms=["arm1", "arm2"], config=OutboxConfig(max_snapshots=7))
    finally:
        await conn.close()

    assert list(status["arms"]) == ["arm1", "arm2", "arm3"]
    assert status["arms"]["arm1"] == {
        "pending": 0,
        "delivered": 0,
        "failed": 0,
        "bytes": 0,
        "oldest_pending_age_s": None,
        "oldest_delivered_age_s": None,
    }
    arm2 = status["arms"]["arm2"]
    assert (arm2["pending"], arm2["delivered"], arm2["failed"]) == (1, 1, 1)
    assert arm2["bytes"] > 0 and arm2["oldest_pending_age_s"] >= 0
    assert [(f["id"], f["arm"], f["reason"], f["attempts"]) for f in status["failed"]] == [(failed, "arm2", "x: y", 3)]
    assert status["max_snapshots"] == 7


async def test_cli_status_retry_and_discard(db_session, seeded: Seeded, app_database, capsys):
    pending = await queue(db_session, seeded)
    refused = await queue(db_session, seeded, minutes=1, state="failed", last_error="import_would_skip: …", attempts=2)
    other = await queue(db_session, seeded, arm="arm3", minutes=2, state="failed", last_error="x: y")

    code, status = await _cli(capsys, "status")
    assert code == 0 and status["arms"]["arm2"]["failed"] == 1

    # A pending row can't be discarded, and only a failed row can be retried.
    assert (await _cli(capsys, "discard", pending))[0] == 1
    assert await _state(db_session, pending) is not None
    assert (await _cli(capsys, "retry", pending))[0] == 1

    code, out = await _cli(capsys, "retry", refused, "--allow-skips")
    assert (code, out) == (0, {"retried": [refused]})
    row = await _state(db_session, refused)
    assert (row.state, row.attempts, row.last_error, row.failed_at, row.strict) == ("pending", 0, None, None, False)

    code, out = await _cli(capsys, "retry", "--arm", "arm3")
    assert (code, out) == (0, {"retried": [other]})
    assert (await _state(db_session, other)).strict is True

    await db_session.execute(update(SnapshotOutbox).where(SnapshotOutbox.id == other).values(state="failed"))
    await db_session.commit()
    code, out = await _cli(capsys, "discard", other)
    assert code == 0 and (out["discarded"], out["arm"], out["state"]) == (other, "arm3", "failed")
    assert await _state(db_session, other) is None
    assert (await _cli(capsys, "discard", other))[0] == 1


async def test_cli_discards_a_delivered_rows_copy_and_frees_its_cap_share(
    db_session, seeded: Seeded, app_database, capsys, caplog
):
    from canopy.squirrel.data.repositories import OutboxRepository

    delivered = await queue(db_session, seeded, state="delivered", attempts=1)
    repo = OutboxRepository(db_session)
    rows, size = await repo.usage("arm2")
    assert rows == 1 and size > 0

    code, out = await _cli(capsys, "discard", delivered)

    assert code == 0 and (out["discarded"], out["arm"], out["state"], out["bytes"]) == (
        delivered,
        "arm2",
        "delivered",
        size,
    )
    assert await _state(db_session, delivered) is None
    assert await repo.usage("arm2") == (0, 0)
    # It says what it did: the head's copy only; the arm has the snapshot.
    [message] = [r.getMessage() for r in caplog.records if r.name == "canopy.squirrel.outbox"]
    assert "waiting copy" in message and "arm2 still holds the snapshot" in message
    assert "reseed --arm arm2" in message and "gone" not in message


async def test_cli_exits_1_when_the_database_cant_be_reached(monkeypatch, capsys):
    from canopy.core.config import get_settings

    monkeypatch.setenv("CANOPY_DATABASE_URL", "postgresql+asyncpg://nobody:x@127.0.0.1:1/none")
    get_settings.cache_clear()
    try:
        assert (await _cli(capsys, "status"))[0] == 1
    finally:
        get_settings.cache_clear()


def _scrape(collector: OutboxCollector) -> str:
    registry = CollectorRegistry()
    registry.register(collector)
    return generate_latest(registry).decode()


async def test_gauges_report_each_delivery_arm(db_session, seeded: Seeded, app_database):
    await queue(db_session, seeded)
    await queue(db_session, seeded, minutes=1, state="failed", last_error="x: y")
    collector = OutboxCollector(arms=lambda: ["arm1", "arm2"])

    await collector.refresh()
    text = _scrape(collector)

    assert "canopy_squirrel_outbox_status_up 1.0" in text
    assert 'canopy_squirrel_outbox_snapshots{arm="arm2",state="pending"} 1.0' in text
    assert 'canopy_squirrel_outbox_snapshots{arm="arm2",state="failed"} 1.0' in text
    assert 'canopy_squirrel_outbox_snapshots{arm="arm1",state="pending"} 0.0' in text
    assert 'canopy_squirrel_outbox_oldest_pending_age_seconds{arm="arm2"}' in text
    assert 'canopy_squirrel_outbox_oldest_pending_age_seconds{arm="arm1"}' not in text
    assert 'canopy_squirrel_outbox_bytes{arm="arm1"} 0.0' in text
    assert "canopy_squirrel_outbox_max_snapshots 200.0" in text
    assert "canopy_squirrel_outbox_max_bytes" in text


async def test_no_gauges_where_nothing_is_delivered_to(app_database):
    collector = OutboxCollector(arms=lambda: [])
    await collector.refresh()
    assert "canopy_squirrel_outbox" not in _scrape(collector)


@pytest.mark.parametrize("failure", ["database", "query"])
async def test_a_failed_read_leaves_only_status_up_0(monkeypatch, failure, app_database):
    from canopy.core.config import get_settings

    if failure == "database":
        monkeypatch.setenv("CANOPY_DATABASE_URL", "postgresql+asyncpg://nobody:x@127.0.0.1:1/none")
        get_settings.cache_clear()
    else:

        async def broken(*args, **kwargs):
            raise RuntimeError("boom")

        monkeypatch.setattr("canopy.squirrel.outbox.metrics.outbox_status", broken)
    collector = OutboxCollector(arms=lambda: ["arm2"])

    await collector.refresh()
    text = _scrape(collector)

    assert "canopy_squirrel_outbox_status_up 0.0" in text
    assert "canopy_squirrel_outbox_snapshots" not in text


async def test_cli_exits_1_when_a_command_fails_in_the_database(app_database, monkeypatch, capsys):
    async def broken(*args, **kwargs):
        raise asyncpg.UndefinedTableError("relation squirrel.snapshot_outbox does not exist")

    monkeypatch.setattr("canopy.squirrel.outbox.__main__.outbox_status", broken)

    assert (await _cli(capsys, "status"))[0] == 1


def test_main_logs_to_stderr_and_returns_the_exit_status(monkeypatch, capsys):
    import canopy.squirrel.outbox.__main__ as cli

    seen = []

    async def run(options):
        seen.append(options.command)
        cli.logger.info("ran")
        return 0

    monkeypatch.setattr(cli, "run", run)

    assert cli.main(["status"]) == 0
    assert cli.main(["status"]) == 0
    assert seen == ["status", "status"]
    # One handler however often main runs, and only on stderr.
    assert sum(getattr(h, "_canopy_outbox", False) for h in cli.logger.handlers) == 1
    captured = capsys.readouterr()
    assert captured.out == "" and "ran" in captured.err


async def test_gauges_come_back_after_a_failed_read(db_session, seeded: Seeded, app_database, monkeypatch, caplog):
    import logging

    real = outbox_status

    async def broken(*args, **kwargs):
        raise RuntimeError("boom")

    collector = OutboxCollector(arms=lambda: ["arm2"])
    monkeypatch.setattr("canopy.squirrel.outbox.metrics.outbox_status", broken)
    await collector.refresh()
    monkeypatch.setattr("canopy.squirrel.outbox.metrics.outbox_status", real)
    with caplog.at_level(logging.INFO, logger="canopy.squirrel.outbox.metrics"):
        await collector.refresh()

    assert "canopy_squirrel_outbox_status_up 1.0" in _scrape(collector)
    assert "available again" in caplog.text
