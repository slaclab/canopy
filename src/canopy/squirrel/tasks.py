"""Arq worker tasks for snapshot create / restore.

Run in the ``canopy-worker`` process off the request path. Progress here rides a
per-job Redis key via ``jobs.gateway.report_progress`` (Arq passes the pool as
``ctx['redis']`` and the id as ``ctx['job_id']``), a restore's per-phase progress
included, and the whole DB write is one ``session_scope`` transaction committed once
at the end — so the snapshot row and its COPY'd values are all-or-nothing and an Arq
retry never resumes against partial state.

The EPICS connector is built once at worker startup and shared via ``ctx['epics']``.
Task results are opaque refs the Jobs module returns verbatim to pollers.
"""

from __future__ import annotations

import logging
from uuid import uuid4

from arq import Retry

from canopy.core.db import session_scope
from canopy.core.errors import PrimaryUnavailable
from canopy.jobs.gateway import report_progress
from canopy.squirrel.config import get_squirrel_config
from canopy.squirrel.outbox import delivery as outbox
from canopy.core.forwarding.client import Caller, ArmRefused
from canopy.squirrel.pv_cache.reader import PVCacheReader
from canopy.squirrel.schemas.snapshot import NewSnapshotDTO, RestoreRequestDTO
from canopy.squirrel.adapters.epics_io import EpicsIO
from canopy.squirrel.services.snapshot_service import SnapshotService

logger = logging.getLogger(__name__)

# Substring markers in an error message that make a failure worth retrying.
_TRANSIENT_MARKERS = ("connection", "timeout", "database")


def _build_epics_io(ctx: dict) -> EpicsIO:
    cfg = get_squirrel_config()
    return EpicsIO(
        ctx["epics"],
        concurrency=cfg.epics_concurrency,
        chunk_size=cfg.epics_chunk_size,
        timeout=cfg.epics_timeout_s,
    )


def _progress_sink(ctx: dict):
    redis = ctx["redis"]
    job_id = ctx["job_id"]

    async def on_progress(current: int, total: int, message: str, phases: list[dict] | None = None) -> None:
        percent = int((current / total) * 100) if total else 0
        await report_progress(redis, job_id, percent, message, phases=phases)

    return on_progress


def _maybe_retry(exc: Exception) -> None:
    if any(marker in str(exc).lower() for marker in _TRANSIENT_MARKERS):
        logger.info("Retrying task after transient error: %s", exc)
        raise Retry(defer=10) from exc


async def create_snapshot_task(
    ctx: dict,
    title: str,
    description: str | None = None,
    created_by: str | None = None,
    facility_id: str | None = None,
    use_cache: bool = False,
    arm: str | None = None,
    caller: dict | None = None,
) -> dict:
    """Read the facility's PVs (or every PV of the snapshot's owner) from EPICS and save a
    snapshot. Returns ``{"type":"snapshot","id":...}``.

    The facility arrives as a resolved **id**, not a slug: the route had to resolve it to
    check the caller's facility grant before enqueueing, and re-resolving here would
    re-read a slug that may have changed in the interim.

    With ``arm`` (a history head's capture for that arm's facility), the arm's PVs are
    read here and the snapshot is saved on the arm, as ``caller`` (a ``Caller`` as a
    dict), under the id and time it got here (``canopy.squirrel.capture``). The result
    then names the arm too.

    With ``use_cache``, values the PV cache can vouch for come from it and the rest
    are read live from EPICS.
    """
    logger.info(
        "create_snapshot_task: job_id=%s title=%r use_cache=%s arm=%s", ctx.get("job_id"), title, use_cache, arm
    )
    try:
        epics_io = _build_epics_io(ctx)
        pv_cache = None
        if use_cache:
            cache_cfg = get_squirrel_config().pv_cache
            pv_cache = PVCacheReader(ctx["redis"], heartbeat_max_age_s=cache_cfg.heartbeat_max_age_s)
        if arm is not None:
            return await _capture_for_arm(ctx, epics_io, pv_cache, arm, caller or {}, title, description, facility_id)
        async with session_scope() as session:
            service = SnapshotService(session, epics_io)
            summary = await service.create_snapshot(
                NewSnapshotDTO(title=title, description=description),
                created_by=created_by,
                progress_callback=_progress_sink(ctx),
                facility_id=facility_id,
                pv_cache=pv_cache,
            )
            await session.commit()
        return {"type": "snapshot", "id": summary.id}
    except (PrimaryUnavailable, ArmRefused):
        # Not retried here: the arm refused the bundle, or nothing reached it. The job
        # fails with the reason, and the head's worker never saves the arm's snapshot.
        logger.exception("create_snapshot_task failed: job_id=%s", ctx.get("job_id"))
        raise
    except Exception as exc:
        logger.exception("create_snapshot_task failed: job_id=%s", ctx.get("job_id"))
        _maybe_retry(exc)
        raise


async def _capture_for_arm(
    ctx: dict,
    epics_io: EpicsIO,
    pv_cache: PVCacheReader | None,
    arm: str,
    caller: dict,
    title: str,
    description: str | None,
    facility_id: str | None,
) -> dict:
    """Capture the arm's PVs here, then save the snapshot on the arm, or queue it for the arm (``outbox.submit``)."""
    progress = _progress_sink(ctx)
    async with session_scope() as session:  # read-only: the snapshot is the arm's to save
        service = SnapshotService(session, epics_io)
        captured = await service.capture_values(facility_id, owner=arm, progress_callback=progress, pv_cache=pv_cache)
        bundle = await service.bundle_captured(
            captured,
            snapshot_id=str(uuid4()),
            title=title,
            description=description,
            created_by=caller.get("on_behalf_of"),
            facility_id=facility_id,
        )
    await progress(captured.total_pvs, captured.total_pvs, f"Saving {len(bundle.values):,} PV values on {arm}...")
    result = await outbox.submit(arm, bundle, Caller(**caller), facility_id=facility_id)
    if "delivery" in result:
        # Queued: the arm couldn't take it, or has older snapshots waiting. Redis only
        # starts a delivery run sooner; the queue itself is in Postgres.
        await progress(captured.total_pvs, captured.total_pvs, f"Captured; waiting for {arm}")
        await ctx["redis"].enqueue_job("deliver_outbox_task")
    return result


async def deliver_outbox_task(ctx: dict) -> dict:
    """Send every arm's due queued snapshots, in order (``canopy.squirrel.outbox.delivery``).

    Runs on the head's cron every ``CANOPY_SQUIRREL__OUTBOX__INTERVAL_S`` and whenever a
    capture is queued. Returns each arm's counts by outcome.
    """
    return await outbox.deliver_all()


async def restore_snapshot_task(
    ctx: dict, snapshot_id: str, pv_ids: list[str] | None = None, skipped_pv_ids: list[str] | None = None
) -> dict:
    """Restore a snapshot's setpoints to EPICS. Returns the restore summary (opaque to Jobs).

    ``skipped_pv_ids`` are PVs the route already dropped from ``pv_ids`` because the
    caller's key isn't granted their facility; they're carried through only so the result
    can report them.
    """
    logger.info("restore_snapshot_task: job_id=%s snapshot_id=%s", ctx.get("job_id"), snapshot_id)
    try:
        epics_io = _build_epics_io(ctx)
        request = RestoreRequestDTO(pv_ids=pv_ids) if pv_ids is not None else None
        async with session_scope() as session:  # read-only (restore writes to EPICS, not the DB)
            service = SnapshotService(session, epics_io)
            result = await service.restore_snapshot(snapshot_id, request, progress_callback=_progress_sink(ctx))
        return {
            "type": "restore",
            "snapshotId": snapshot_id,
            "totalPvs": result.total_pvs,
            "successCount": result.success_count,
            "failureCount": result.failure_count,
            "failures": result.failures[:50],
            "skippedPvIds": skipped_pv_ids or [],
        }
    except Exception as exc:
        logger.exception("restore_snapshot_task failed: job_id=%s", ctx.get("job_id"))
        _maybe_retry(exc)
        raise
