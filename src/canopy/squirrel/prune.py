"""Remove old snapshots from a primary whose history mirror keeps them.

Runs on a primary's worker as an Arq cron job (``prune_snapshots``), off unless
``CANOPY_PRUNE__ENABLED`` and a dry run until ``CANOPY_PRUNE__DRY_RUN`` is off.
A snapshot is pruned once it is older than ``CANOPY_WINDOW_DAYS`` and not retained,
deleted or not: its values first, then its row, one snapshot per transaction. The
replication gate (``canopy.core.replication.gate``) is checked before each snapshot,
and the run stops at the first failed check.

The deletes are plain SQL, never ``SnapshotService.delete_snapshot`` (which only
marks a snapshot deleted, and would reach the mirror as an update). They reach the
mirror not at all: both tables are ``time_series`` in the replication registry,
whose split-layout publication carries no deletes, and the gate refuses to run
while any publication carrying them does.

Prune can't see references held outside this database: a service that refers to
snapshots it may need later, such as an optimization service's runs, sets ``retain``
on them.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from collections.abc import Callable, Awaitable

import asyncpg
from sqlalchemy import func, delete, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from canopy.core.db import get_session_maker
from canopy.core.config import get_settings
from canopy.core.replication.sql import asyncpg_dsn
from canopy.squirrel.data.models import Snapshot, SnapshotValue
from canopy.core.replication.gate import GateResult, check_prune_gate
from canopy.core.replication.config import (
    PruneConfig,
    get_prune_config,
    get_replication_config,
)

logger = logging.getLogger(__name__)

# Every table this job deletes from, for the gate. Both are time_series in the registry.
PRUNED_TABLES = ("squirrel.snapshot", "squirrel.snapshot_value")


def _candidates(cutoff: datetime):
    """Snapshots old enough to prune, oldest first (``snapshot (created_at, id)`` serves it)."""
    return (
        select(Snapshot.id)
        .where(Snapshot.created_at < cutoff, Snapshot.retain.is_(False))
        .order_by(Snapshot.created_at, Snapshot.id)
    )


async def prune(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    window_days: int | None,
    config: PruneConfig,
    check_gate: Callable[[], Awaitable[GateResult]],
    now: datetime | None = None,
) -> dict:
    """Prune up to ``config.max_snapshots_per_run`` snapshots older than the window.

    Parameters
    ----------
    session_maker : async_sessionmaker[AsyncSession]
        Sessions on the primary; each snapshot is deleted in a transaction of its own.
    window_days : int | None
        Days of snapshots to keep (``CANOPY_WINDOW_DAYS``). The gate fails without it.
    config : PruneConfig
        ``dry_run`` and ``max_snapshots_per_run``.
    check_gate : Callable[[], Awaitable[GateResult]]
        Judges the replication gate; called before the run and before each snapshot.
    now : datetime | None
        The current time; tests pin it.

    Returns
    -------
    dict
        ``dry_run``; ``snapshots`` and ``values`` pruned (or, in a dry run, that would be);
        ``gate``: the failed checks, empty when every check passed; and ``complete``,
        whether no prunable snapshot was left.
    """
    gate = await check_gate()
    result: dict = {"dry_run": config.dry_run, "snapshots": 0, "values": 0, "gate": gate.failures, "complete": False}
    if not window_days:
        _report(result)
        return result
    cutoff = (now or datetime.now(UTC)) - timedelta(days=window_days)

    if config.dry_run:
        async with session_maker() as session:
            ids = _candidates(cutoff).subquery()
            result["snapshots"] = await session.scalar(select(func.count()).select_from(ids))
            result["values"] = await session.scalar(
                select(func.count()).select_from(SnapshotValue).where(SnapshotValue.snapshot_id.in_(select(ids.c.id)))
            )
        result["complete"] = True
        _report(result)
        return result

    for _ in range(config.max_snapshots_per_run):
        if not gate.ok:
            break
        async with session_maker() as session, session.begin():
            # FOR UPDATE re-checks the row: one retained meanwhile is passed over.
            snapshot_id = await session.scalar(_candidates(cutoff).limit(1).with_for_update(of=Snapshot))
            if snapshot_id is None:
                result["complete"] = True
                break
            values = await session.execute(delete(SnapshotValue).where(SnapshotValue.snapshot_id == snapshot_id))
            await session.execute(delete(Snapshot).where(Snapshot.id == snapshot_id))
        result["snapshots"] += 1
        result["values"] += values.rowcount
        gate = await check_gate()
        result["gate"] = gate.failures
    _report(result)
    return result


def _report(result: dict) -> None:
    """Log the run's outcome as one structured record."""
    extra = {f"prune_{key}": value for key, value in result.items()}
    if result["gate"] and not result["dry_run"]:
        logger.warning(
            "Prune stopped after %d snapshots: gate failed (%s)",
            result["snapshots"],
            "; ".join(f"{name}: {why}" for name, why in result["gate"].items()),
            extra=extra,
        )
    elif result["dry_run"]:
        logger.info(
            "Prune dry run: would remove %d snapshots (%d values); gate %s",
            result["snapshots"],
            result["values"],
            "passes" if not result["gate"] else "fails: " + ", ".join(result["gate"]),
            extra=extra,
        )
    else:
        logger.info("Pruned %d snapshots (%d values)", result["snapshots"], result["values"], extra=extra)


async def prune_snapshots(ctx: dict) -> dict:
    """Arq cron task: prune old snapshots when enabled, checking the gate over its own connection.

    Returns the run's outcome (see ``prune``), which Arq keeps as the job result.
    """
    config = get_prune_config()
    if not config.enabled:
        logger.info("Prune is disabled (CANOPY_PRUNE__ENABLED); nothing to do")
        return {"enabled": False}
    settings = get_settings()
    replication = get_replication_config()
    conn = await asyncpg.connect(
        asyncpg_dsn(settings.database_url), server_settings={"application_name": "canopy-prune"}
    )
    try:

        async def gate() -> GateResult:
            return await check_prune_gate(
                conn,
                instance=settings.instance,
                window_days=settings.window_days,
                tables=PRUNED_TABLES,
                config=config,
                replication=replication,
            )

        return await prune(get_session_maker(), window_days=settings.window_days, config=config, check_gate=gate)
    finally:
        await conn.close()
