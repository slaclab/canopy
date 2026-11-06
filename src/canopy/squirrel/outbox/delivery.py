"""Queueing snapshots for an arm, and delivering them in order.

A capture that can't reach its arm is queued with ``enqueue``, which holds the arm's
caps. ``deliver_arm`` then sends that arm's rows oldest first, one at a time, each
through ``send``. A row stays pending through any failure that says nothing about
the snapshot itself (the arm can't be reached, answered 5xx, or turned down the
head's own key) and its retry backs off, doubling up to ``retry_max_s``; it never
gives up. A refusal of the snapshot itself (its facility went, its PVs moved, its id
clashes) marks it failed with the arm's reason and the next row goes on. Once the
arm has saved a row, the row stays until the snapshot is on the head, by replication.

Every state change is committed in Postgres before the next send; Redis only ever
triggers a run. A session advisory lock per arm keeps two runs (two workers, or a
cron tick and a trigger) from sending one arm's rows out of order.
"""

from __future__ import annotations

import time
import random
import logging
from typing import Any
from datetime import UTC, datetime, timedelta
from contextlib import asynccontextmanager
from collections.abc import Callable, Awaitable, AsyncIterator

from sqlalchemy import text

from canopy.core.db import session_scope
from canopy.core.errors import PrimaryTimeout, PrimaryUnavailable
from canopy.core.time_parse import to_utc
from canopy.squirrel.config import OutboxConfig
from canopy.squirrel.errors import OutboxFull
from canopy.squirrel.data.models import SnapshotOutbox
from canopy.squirrel.schemas.snapshot import SnapshotBundleDTO
from canopy.squirrel.data.repositories import OutboxRepository

logger = logging.getLogger(__name__)

RUN_BUDGET_S = 300.0
"""No new send starts after this long in one run; the next run goes on. The worker's
``job_timeout`` is 600 s and a triggered run inherits it, and one send is bounded by
the arm client's own timeout."""

# Answers that turn down the head's own key or settings rather than the snapshot: every
# row for the arm would get the same, so the row waits for the fix instead of failing.
_CONFIGURATION_CODES = frozenset(
    {"invalid_api_key", "insufficient_module_access", "insufficient_write_access", "forwarded_write_refused"}
)
_RETRIED_STATUSES = frozenset({408, 429})


class ArmRefused(Exception):
    """The arm answered a delivery with an error: its status, ``code`` and ``detail``."""

    def __init__(self, status: int, code: str, detail: str) -> None:
        super().__init__(f"{status} {code}: {detail}")
        self.status = status
        self.code = code
        self.detail = detail


def transient(refusal: ArmRefused) -> bool:
    """Whether a later attempt can succeed with nothing changed about the snapshot."""
    return refusal.status >= 500 or refusal.status in _RETRIED_STATUSES or refusal.code in _CONFIGURATION_CODES


def refusal_reason(refusal: ArmRefused) -> str:
    """How a failed row records the arm's answer: ``<code>: <detail>``."""
    return f"{refusal.code}: {refusal.detail}"


Send = Callable[[str, str, dict, bool], Awaitable[Any]]
"""``send(arm, bundle_json, caller, strict)``: post one bundle to the arm's captured
import and wait for it to replicate. ``caller`` is the stored ``Caller``
(``{on_behalf_of, facilities}``). Raises ``PrimaryUnavailable`` (nothing sent),
``PrimaryTimeout`` (sent, no answer) or ``ArmRefused``."""


def retry_delay(attempts: int, config: OutboxConfig, rng: random.Random | None = None) -> float:
    """Seconds before the next attempt after ``attempts`` failed ones: doubling, capped, ±20%."""
    delay = min(config.retry_initial_s * 2 ** max(attempts - 1, 0), config.retry_max_s)
    return delay * (rng or random).uniform(0.8, 1.2)


def bundle_json(bundle: SnapshotBundleDTO) -> str:
    """The bundle as it is stored and sent: the export's wire JSON."""
    return bundle.model_dump_json(by_alias=True)


async def outbox_full(repo: OutboxRepository, arm: str, config: OutboxConfig) -> str | None:
    """Why the arm's outbox can take no more snapshots, or ``None`` while it can."""
    rows, size = await repo.usage(arm)
    if rows >= config.max_snapshots:
        return f"{arm} can't be reached and its outbox is full ({rows} snapshots); the snapshot would not be saved"
    if size >= config.max_bytes:
        return f"{arm} can't be reached and its outbox is full ({size} bytes); the snapshot would not be saved"
    return None


async def enqueue(
    repo: OutboxRepository,
    *,
    arm: str,
    bundle: SnapshotBundleDTO,
    facility_id: str,
    caller: dict,
    config: OutboxConfig,
) -> None:
    """Queue a captured snapshot for ``arm`` in the caller's transaction, within the arm's caps.

    Raises
    ------
    OutboxFull
        When the snapshot would take the arm past either cap; nothing is queued.
    """
    document = bundle_json(bundle)
    size = len(document.encode())
    await repo.lock_arm(arm)
    rows, used = await repo.usage(arm)
    if rows + 1 > config.max_snapshots or used + size > config.max_bytes:
        raise OutboxFull(
            f"{arm} can't be reached and its outbox is full ({rows} snapshots, {used} of {config.max_bytes} bytes); "
            "the snapshot was not saved"
        )
    await repo.add(
        bundle_json=document,
        id=str(bundle.id),
        arm=arm,
        facility_id=facility_id,
        title=bundle.title,
        description=bundle.description,
        created_by=bundle.created_by,
        # JSON may carry a naive time; asyncpg needs an aware one for timestamptz.
        created_at=to_utc(bundle.created_date),
        value_count=len(bundle.values),
        size_bytes=size,
        caller=caller,
    )
    logger.info("Snapshot %s queued for %s (%d values, %d bytes)", bundle.id, arm, len(bundle.values), size)


def log_swept(rows: list) -> None:
    """Log the rows a sweep removed; a failed one loudly, since the arm has it after all."""
    for row in rows:
        if row.state == "failed":
            logger.warning(
                "Snapshot %s, which %s had refused (%s), is on this head now; removed from the outbox",
                row.id,
                row.arm,
                row.last_error,
            )
        else:
            logger.info("Snapshot %s is back from %s; removed from the outbox", row.id, row.arm)


@asynccontextmanager
async def arm_lock(arm: str) -> AsyncIterator[bool]:
    """Hold the arm's delivery lock for the block, outside any transaction; yields whether it was free."""
    async with session_scope() as session:
        await session.connection(execution_options={"isolation_level": "AUTOCOMMIT"})
        key = f"canopy.squirrel.outbox.deliver:{arm}"
        got = bool(await session.scalar(text("SELECT pg_try_advisory_lock(hashtext(:key))"), {"key": key}))
        try:
            yield got
        finally:
            if got:
                await session.execute(text("SELECT pg_advisory_unlock(hashtext(:key))"), {"key": key})


async def _next_due(arm: str, now: datetime) -> tuple[SnapshotOutbox, str] | None:
    """Sweep, then the arm's oldest pending row and its bundle, if its attempt is due."""
    async with session_scope() as session:
        repo = OutboxRepository(session)
        log_swept(await repo.sweep())
        entry = await repo.next_pending(arm)
        due = entry is not None and entry.next_attempt_at <= now
        document = await repo.bundle_text(entry.id) if due else None
        await session.commit()
    if not due or document is None:
        return None
    return entry, document


async def _record(entry: SnapshotOutbox, **values: Any) -> None:
    async with session_scope() as session:
        await OutboxRepository(session).update_state(entry.id, **values)
        await session.commit()


async def _delivered(entry: SnapshotOutbox, attempts: int, now: datetime) -> None:
    """The arm saved it: the row goes now if the snapshot is already here, else it waits for the sweep."""
    async with session_scope() as session:
        repo = OutboxRepository(session)
        if swept := await repo.sweep(entry.id):
            log_swept(swept)
        else:
            await repo.update_state(
                entry.id, state="delivered", attempts=attempts, last_attempt_at=now, delivered_at=now, last_error=None
            )
            logger.info("Snapshot %s saved on %s; waiting for it to replicate back", entry.id, entry.arm)
        await session.commit()


async def deliver_arm(
    arm: str,
    send: Send,
    *,
    config: OutboxConfig,
    budget_s: float = RUN_BUDGET_S,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> dict[str, int]:
    """Send the arm's due rows in order until one has to wait, the queue is empty, or the budget runs out.

    Returns counts by outcome (``delivered``, ``retried``, ``failed``), or
    ``{"locked": 1}`` when another run holds the arm.
    """
    counts = {"delivered": 0, "retried": 0, "failed": 0}
    async with arm_lock(arm) as got:
        if not got:
            return {"locked": 1}
        deadline = time.monotonic() + budget_s
        while time.monotonic() < deadline:
            due = await _next_due(arm, clock())
            if due is None:
                break
            entry, document = due
            attempts = entry.attempts + 1
            try:
                await send(arm, document, entry.caller, entry.strict)
            except (PrimaryUnavailable, PrimaryTimeout, ArmRefused) as exc:
                now = clock()
                if isinstance(exc, ArmRefused) and not transient(exc):
                    await _record(
                        entry,
                        state="failed",
                        attempts=attempts,
                        last_attempt_at=now,
                        failed_at=now,
                        last_error=refusal_reason(exc),
                    )
                    logger.error("Snapshot %s refused by %s: %s", entry.id, arm, refusal_reason(exc))
                    counts["failed"] += 1
                    continue
                reason = refusal_reason(exc) if isinstance(exc, ArmRefused) else exc.detail
                wait = retry_delay(attempts, config)
                await _record(
                    entry,
                    attempts=attempts,
                    last_attempt_at=now,
                    last_error=reason,
                    next_attempt_at=now + timedelta(seconds=wait),
                )
                logger.warning(
                    "Snapshot %s not delivered to %s (attempt %d), retrying in %.0f s: %s",
                    entry.id,
                    arm,
                    attempts,
                    wait,
                    reason,
                )
                counts["retried"] += 1
                break
            await _delivered(entry, attempts, clock())
            counts["delivered"] += 1
    return counts


def delivery_arms() -> list[str]:
    """The arms this instance delivers captures to: those with a forward URL and a service key."""
    return []
