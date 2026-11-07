"""Queueing snapshots for an arm, and delivering them in order.

A capture that can't reach its arm is queued with ``enqueue``, which holds the arm's
caps. ``deliver_arm`` then sends that arm's rows oldest first, one at a time, each
through ``send``. A row stays pending through any failure that says nothing about
the snapshot itself (the arm can't be reached, answered 5xx, or turned down the
head's own key) and its retry backs off, doubling up to ``retry_max_s``; it never
gives up. A refusal of the snapshot itself (its facility went, its PVs moved, its id
clashes) marks it failed with the arm's reason and the next row goes on. Once the
arm has saved a row, the row stays until the snapshot is on the head, by replication.
A capture the arm saved at once that hasn't replicated back when ``deliver_capture``
stops waiting is kept the same way, as a row already delivered (``submit``).

Every state change is committed in Postgres before the next send; Redis only ever
triggers a run. A session advisory lock per arm keeps two runs (two workers, or a
cron tick and a trigger) from sending one arm's rows out of order.
"""

from __future__ import annotations

import time
import random
import asyncio
import logging
from typing import Any
from datetime import UTC, datetime, timedelta
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass
from collections.abc import Callable, Awaitable, AsyncIterator

from sqlalchemy import text

from canopy.core.db import session_scope
from canopy.core.errors import PrimaryTimeout, PrimaryUnavailable
from canopy.core.time_parse import to_utc
from canopy.squirrel.config import OutboxConfig, get_squirrel_config
from canopy.squirrel.errors import OutboxFull
from canopy.squirrel.capture import deliver_capture
from canopy.squirrel.data.models import SnapshotOutbox
from canopy.core.forwarding.client import Caller, ArmRefused, forwardable_arms
from canopy.squirrel.schemas.snapshot import SnapshotBundleDTO
from canopy.squirrel.data.repositories import OutboxRepository
from canopy.squirrel.services.snapshot_service import delivery_of

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


def transient(refusal: ArmRefused) -> bool:
    """Whether a later attempt can succeed with nothing changed about the snapshot."""
    return refusal.status >= 500 or refusal.status in _RETRIED_STATUSES or refusal.code in _CONFIGURATION_CODES


def refusal_reason(refusal: ArmRefused) -> str:
    """How a row records the arm's answer: ``<code>: <detail>``, or the status when the arm sent no code."""
    return f"{refusal.code or refusal.status}: {refusal.detail}"


Send = Callable[[str, str, dict, bool], Awaitable[dict]]
"""``send(arm, bundle_json, caller, strict)``: post one bundle to the arm's captured
import and wait for it to replicate; ``send_queued`` in production. ``caller`` is the
stored ``Caller`` as a dict. Raises ``PrimaryUnavailable`` (nothing sent),
``PrimaryTimeout`` (sent, no answer) or ``ArmRefused``."""


async def send_queued(arm: str, document: str, caller: dict, strict: bool) -> dict:
    """Deliver one queued bundle through ``deliver_capture``, as its capture would have."""
    return await deliver_capture(arm, SnapshotBundleDTO.model_validate_json(document), Caller(**caller), strict=strict)


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


async def refuse_if_full(repo: OutboxRepository, arm: str, config: OutboxConfig) -> None:
    """Refuse a capture for ``arm`` up front when it would only queue, and its outbox is full.

    Raises
    ------
    OutboxFull
        When the arm has snapshots waiting and is at a cap. Without a backlog the capture
        may reach the arm directly, so it isn't refused here.
    """
    if await repo.has_backlog(arm) and (reason := await outbox_full(repo, arm, config)):
        raise OutboxFull(reason)


async def _keep(
    repo: OutboxRepository,
    *,
    arm: str,
    bundle: SnapshotBundleDTO,
    document: str,
    facility_id: str | None,
    caller: dict,
    config: OutboxConfig,
    **state: Any,
) -> str | None:
    """Add ``bundle`` to the arm's outbox in the caller's transaction, if it fits the arm's caps.

    Holds the arm's queue for the rest of the transaction, so two captures can't both
    pass a cap. ``state`` sets columns beyond the bundle's own (a row delivered already).
    Returns why it doesn't fit, with nothing added, or ``None`` once it's added.
    """
    size = len(document.encode())
    await repo.lock_arm(arm)
    rows, used = await repo.usage(arm)
    if rows + 1 > config.max_snapshots or used + size > config.max_bytes:
        return f"its outbox is full ({rows} snapshots, {used} of {config.max_bytes} bytes)"
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
        **state,
    )
    return None


async def enqueue(
    repo: OutboxRepository,
    *,
    arm: str,
    bundle: SnapshotBundleDTO,
    facility_id: str | None,
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
    full = await _keep(
        repo, arm=arm, bundle=bundle, document=document, facility_id=facility_id, caller=caller, config=config
    )
    if full is not None:
        raise OutboxFull(f"{arm} can't be reached and {full}; the snapshot was not saved")
    logger.info(
        "Snapshot %s queued for %s (%d values, %d bytes)", bundle.id, arm, len(bundle.values), len(document.encode())
    )


@dataclass(frozen=True)
class Submitted:
    """What became of a capture for an arm (``submit``).

    ``result`` is the capture job's result, ``message`` its last progress message, and
    ``queued`` whether it waits in the arm's queue, for a delivery run to send.
    """

    result: dict
    message: str
    queued: bool = False


async def _saved(
    arm: str, bundle: SnapshotBundleDTO, caller: Caller, result: dict, *, facility_id: str | None, config: OutboxConfig
) -> Submitted:
    """The arm saved the capture: done if it's here, else kept as a delivered row until it replicates.

    The head then serves it as it serves a queued snapshot, pending, rather than not at
    all; the sweep removes the row once the snapshot is here. A row that would pass a
    cap isn't kept: the arm has the snapshot, so the capture still succeeds.
    """
    snapshot_id = str(bundle.id)
    async with session_scope() as session:
        repo = OutboxRepository(session)
        if await repo.is_stored(snapshot_id):
            return Submitted(result, f"Saved on {arm}")
        now = datetime.now(UTC)
        full = await _keep(
            repo,
            arm=arm,
            bundle=bundle,
            document=bundle_json(bundle),
            facility_id=facility_id,
            caller=asdict(caller),
            config=config,
            state="delivered",
            attempts=1,
            last_attempt_at=now,
            delivered_at=now,
        )
        if full is not None:
            logger.warning(
                "Snapshot %s is saved on %s but hasn't replicated here, and isn't kept here meanwhile: %s",
                snapshot_id,
                arm,
                full,
            )
            return Submitted(result, f"Saved on {arm}; it hasn't replicated here yet")
        await session.commit()
        entry = await repo.get(snapshot_id)
    logger.info("Snapshot %s saved on %s; kept here until it replicates back", snapshot_id, arm)
    delivery = delivery_of(entry).model_dump(mode="json", by_alias=True)
    return Submitted({**result, "delivery": delivery}, f"Saved on {arm}; waiting for it to replicate here")


async def submit(
    arm: str, bundle: SnapshotBundleDTO, caller: Caller, *, facility_id: str | None, config: OutboxConfig | None = None
) -> Submitted:
    """Save a capture on its arm now, or queue it.

    A capture for an arm with snapshots already waiting queues behind them, so the arm
    gets its snapshots in the order they were taken. Otherwise it goes straight to the
    arm (``deliver_capture``). If that can't reach the arm, gets no answer, or meets a
    failure the outbox would retry, the snapshot is queued with the attempt recorded;
    a refusal of the snapshot itself is raised, so the capture job fails with it. A
    snapshot the arm saved that hasn't replicated here yet is kept until it has.

    Returns
    -------
    Submitted
        Its ``result`` is ``deliver_capture``'s when the arm saved it, with
        ``delivery`` added while the snapshot isn't back here; for a queued one,
        ``{"type": "snapshot", "id", "arm", "delivery"}``. ``delivery`` is the object
        the snapshot API shows.

    Raises
    ------
    OutboxFull
        When the snapshot had to queue and the arm's outbox is at a cap.
    ArmRefused
        When the arm refused the snapshot.
    """
    config = config or get_squirrel_config().outbox
    async with session_scope() as session:
        behind = await OutboxRepository(session).has_backlog(arm)
    attempted: str | None = None
    if not behind:
        try:
            result = await deliver_capture(arm, bundle, caller)
        except PrimaryUnavailable as exc:  # and PrimaryTimeout, a 504 that may have been applied
            attempted = exc.detail
        except ArmRefused as exc:
            if not transient(exc):
                raise
            attempted = refusal_reason(exc)
        else:
            return await _saved(arm, bundle, caller, result, facility_id=facility_id, config=config)
    snapshot_id = str(bundle.id)
    async with session_scope() as session:
        repo = OutboxRepository(session)
        await enqueue(repo, arm=arm, bundle=bundle, facility_id=facility_id, caller=asdict(caller), config=config)
        if attempted is not None:
            now = datetime.now(UTC)
            await repo.update_state(
                snapshot_id,
                attempts=1,
                last_attempt_at=now,
                last_error=attempted,
                next_attempt_at=now + timedelta(seconds=retry_delay(1, config)),
            )
            logger.warning("Snapshot %s queued for %s, which it couldn't be saved on: %s", snapshot_id, arm, attempted)
        await session.commit()
        entry = await repo.get(snapshot_id)
    delivery = delivery_of(entry).model_dump(mode="json", by_alias=True)
    return Submitted(
        {"type": "snapshot", "id": snapshot_id, "arm": arm, "delivery": delivery},
        f"Captured; waiting for {arm}",
        queued=True,
    )


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
                outcome = await send(arm, document, entry.caller, entry.strict)
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
            if skipped := (outcome or {}).get("skipped"):
                # Only a row retried with --allow-skips is sent without strict.
                logger.warning(
                    "Snapshot %s saved on %s without %d value(s), as allowed: %s",
                    entry.id,
                    arm,
                    len(skipped),
                    ", ".join(value["pvName"] for value in skipped),
                )
            await _delivered(entry, attempts, clock())
            counts["delivered"] += 1
    return counts


async def deliver_all(send: Send = send_queued, *, config: OutboxConfig | None = None) -> dict[str, dict[str, int]]:
    """One delivery run for every arm this head forwards to, the arms side by side."""
    config = config or get_squirrel_config().outbox
    arms = sorted(forwardable_arms())
    runs = await asyncio.gather(*(deliver_arm(arm, send, config=config) for arm in arms))
    return dict(zip(arms, runs, strict=True))
