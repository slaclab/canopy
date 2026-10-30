"""The prune job's choices against Postgres, with the replication gate stubbed.

The gate itself is tested in ``tests/canopy/core/replication``; here it is a list of
results handed out one call at a time.
"""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from canopy.squirrel import prune as prune_module
from canopy.squirrel.prune import prune
from canopy.squirrel.data.models import Pv, Snapshot, SnapshotValue
from canopy.core.replication.gate import GateResult
from canopy.core.replication.config import PruneConfig

NOW = datetime(2026, 10, 1, tzinfo=UTC)
PASS = GateResult()
STALE = GateResult({"heartbeat_fresh": "the last heartbeat is 9.0 h old (limit 3 h)"})


@pytest.fixture
def maker(test_engine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(test_engine, class_=AsyncSession, expire_on_commit=False)


def _gate(*results: GateResult):
    """A gate that returns ``results`` in turn, then the last one forever; counts its calls."""
    calls = []

    async def check() -> GateResult:
        calls.append(None)
        return results[min(len(calls), len(results)) - 1]

    check.calls = calls
    return check


async def _seed(maker, **snapshots: dict) -> dict[str, str]:
    """Commit snapshots named by keyword, each ``{"age_days", "retain"?, "deleted"?, "values"?}``."""
    async with maker() as session:
        pv = Pv(setpoint_address="PRUNE:SP")
        session.add(pv)
        await session.flush()
        ids = {}
        for name, spec in snapshots.items():
            snap = Snapshot(
                title=name,
                created_at=NOW - timedelta(days=spec["age_days"]),
                retain=spec.get("retain", False),
                deleted_at=NOW if spec.get("deleted") else None,
            )
            session.add(snap)
            await session.flush()
            session.add_all(
                SnapshotValue(snapshot_id=snap.id, pv_id=pv.id, pv_name=f"PRUNE:{i}")
                for i in range(spec.get("values", 2))
            )
            ids[name] = snap.id
        await session.commit()
    return ids


async def _left(maker) -> tuple[set[str], int]:
    async with maker() as session:
        titles = set((await session.scalars(select(Snapshot.title))).all())
        values = await session.scalar(select(func.count()).select_from(SnapshotValue))
    return titles, values


async def test_a_run_removes_old_unretained_snapshots_and_their_values(maker):
    await _seed(
        maker,
        old={"age_days": 40, "values": 3},
        old_deleted={"age_days": 35, "deleted": True},
        old_retained={"age_days": 50, "retain": True},
        recent={"age_days": 5},
    )

    result = await prune(maker, window_days=30, config=PruneConfig(dry_run=False), check_gate=_gate(PASS), now=NOW)

    assert result == {"dry_run": False, "snapshots": 2, "values": 5, "gate": {}, "complete": True}
    assert await _left(maker) == ({"old_retained", "recent"}, 4)


async def test_a_dry_run_counts_and_deletes_nothing(maker, caplog):
    await _seed(maker, old={"age_days": 40, "values": 3}, old_retained={"age_days": 50, "retain": True})
    caplog.set_level("INFO", logger="canopy.squirrel.prune")

    result = await prune(maker, window_days=30, config=PruneConfig(), check_gate=_gate(STALE), now=NOW)

    assert result == {"dry_run": True, "snapshots": 1, "values": 3, "gate": STALE.failures, "complete": True}
    assert await _left(maker) == ({"old", "old_retained"}, 5)
    assert "would remove 1 snapshots (3 values); gate fails: heartbeat_fresh" in caplog.text


async def test_a_failed_gate_deletes_nothing(maker, caplog):
    await _seed(maker, old={"age_days": 40})

    result = await prune(maker, window_days=30, config=PruneConfig(dry_run=False), check_gate=_gate(STALE), now=NOW)

    assert (result["snapshots"], result["gate"], result["complete"]) == (0, STALE.failures, False)
    assert await _left(maker) == ({"old"}, 2)
    assert "gate failed (heartbeat_fresh: the last heartbeat is 9.0 h old" in caplog.text


async def test_the_gate_is_checked_before_each_snapshot(maker):
    await _seed(maker, oldest={"age_days": 60}, older={"age_days": 50}, old={"age_days": 40})
    gate = _gate(PASS, STALE)

    result = await prune(maker, window_days=30, config=PruneConfig(dry_run=False), check_gate=gate, now=NOW)

    # Oldest first; the gate failed after the first deletion, so the run stopped.
    assert (result["snapshots"], result["gate"]) == (1, STALE.failures)
    assert len(gate.calls) == 2
    assert (await _left(maker))[0] == {"older", "old"}


async def test_a_run_stops_at_its_limit_and_the_next_one_continues(maker):
    await _seed(maker, a={"age_days": 60}, b={"age_days": 50}, c={"age_days": 40})
    config = PruneConfig(dry_run=False, max_snapshots_per_run=2)

    first = await prune(maker, window_days=30, config=config, check_gate=_gate(PASS), now=NOW)
    second = await prune(maker, window_days=30, config=config, check_gate=_gate(PASS), now=NOW)

    assert (first["snapshots"], first["complete"]) == (2, False)
    assert (second["snapshots"], second["complete"]) == (1, True)
    assert await _left(maker) == (set(), 0)


async def test_without_a_window_nothing_is_pruned(maker):
    await _seed(maker, old={"age_days": 400})
    unset = GateResult({"window_set": "set CANOPY_WINDOW_DAYS to the days of snapshots this primary keeps"})

    result = await prune(maker, window_days=None, config=PruneConfig(dry_run=False), check_gate=_gate(unset), now=NOW)

    assert (result["snapshots"], list(result["gate"])) == (0, ["window_set"])
    assert await _left(maker) == ({"old"}, 2)


async def test_the_cron_task_does_nothing_while_disabled(monkeypatch):
    async def no_connect(*args, **kwargs):
        raise AssertionError("connected while disabled")

    monkeypatch.setattr(prune_module.asyncpg, "connect", no_connect)

    assert await prune_module.prune_snapshots({}) == {"enabled": False}
