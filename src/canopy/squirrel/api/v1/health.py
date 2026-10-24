"""Squirrel health endpoints: liveness and restore-hook configuration diagnostics."""

import time
import asyncio
from datetime import UTC, datetime
from dataclasses import dataclass

from fastapi import Security
from redis.exceptions import RedisError

from canopy.jobs import gateway as jobs_gateway
from canopy.core.health import make_health_router
from canopy.core.security import require_read_access
from canopy.squirrel.config import get_squirrel_config
from canopy.squirrel.runtime import uptime_s
from canopy.squirrel.pv_cache import VALUES_KEY, read_status
from canopy.squirrel.restore_hooks import read_worker_status, describe_restore_hooks
from canopy.squirrel.schemas.health import (
    PVCacheStatusDTO,
    RestoreHooksViewDTO,
    RestoreHooksStatusDTO,
    WorkerRestoreHooksViewDTO,
)

router = make_health_router("squirrel", uptime_s)

WORKER_STATUS_TIMEOUT_S = 2.0


@dataclass(frozen=True)
class WorkerReport:
    """The outcome of reading the worker's published restore-hook report.

    Attributes
    ----------
    view : WorkerRestoreHooksViewDTO | None
        The report, or ``None`` when no worker has published one or Redis could not
        be read.
    error : str | None
        Why Redis could not be read within ``WORKER_STATUS_TIMEOUT_S``, else ``None``.
    """

    view: WorkerRestoreHooksViewDTO | None = None
    error: str | None = None


async def _worker_report() -> WorkerReport:
    """Fetch the worker's published restore-hook report from Redis.

    Returns
    -------
    WorkerReport
        The worker's view, or why it could not be read.
    """
    try:
        async with asyncio.timeout(WORKER_STATUS_TIMEOUT_S):
            report = await read_worker_status(await jobs_gateway.get_pool())
    except (TimeoutError, OSError, RedisError) as exc:
        return WorkerReport(error=f"Could not read the worker's report from Redis: {str(exc) or type(exc).__name__}")
    return WorkerReport(view=None if report is None else WorkerRestoreHooksViewDTO(**report))


@router.get("/restore-hooks", dependencies=[Security(require_read_access)])
async def restore_hooks_status() -> RestoreHooksStatusDTO:
    """Report which restore hooks the worker and this API process resolved.

    Restore hooks send extra per-PV writes during a snapshot restore (magnet trims,
    for instance) and are selected with ``CANOPY_SQUIRREL__RESTORE_HOOKS``. A variable
    that never reaches the worker, or a mistyped provider key, leaves the hook silently
    doing nothing — this endpoint makes that visible without running a restore.

    Restores run in ``canopy-worker``, so its view comes from the report it publishes
    to Redis at startup. The API's own view is computed here from configuration
    only: no hook is built or cached.

    Returns
    -------
    RestoreHooksStatusDTO
        Both views, whether they load the same hooks, and every available provider.
    """
    api = describe_restore_hooks()
    worker = await _worker_report()
    return RestoreHooksStatusDTO(
        available=api["available"],
        api=RestoreHooksViewDTO(**api),
        worker=worker.view,
        worker_error=worker.error,
        matches_api=None if worker.view is None else worker.view.loaded == api["loaded"],
    )


PV_CACHE_STATUS_TIMEOUT_S = 2.0


def _from_unix(value: float | None) -> datetime | None:
    return None if value is None else datetime.fromtimestamp(float(value), tz=UTC)


@router.get("/pv-cache", dependencies=[Security(require_read_access)])
async def pv_cache_status() -> PVCacheStatusDTO:
    """Report the worker-side PV cache that cached snapshots (``useCache=true``) read.

    The cache is written by whichever ``canopy-worker`` holds the leader lease, which
    publishes a report to Redis on every flush. A worker that never got
    ``CANOPY_SQUIRREL__PV_CACHE__ENABLED``, or one that stopped, leaves cached
    snapshots silently reading live; this endpoint shows which it is.

    Returns
    -------
    PVCacheStatusDTO
        The leader's last report, its heartbeat age, and the overall status.
    """
    cfg = get_squirrel_config().pv_cache
    try:
        async with asyncio.timeout(PV_CACHE_STATUS_TIMEOUT_S):
            pool = await jobs_gateway.get_pool()
            report = await read_status(pool)
            cached = await pool.hlen(VALUES_KEY)
    except (TimeoutError, OSError, RedisError) as exc:
        return PVCacheStatusDTO(
            status="down",
            enabled=cfg.enabled,
            error=f"Could not read the PV cache report from Redis: {str(exc) or type(exc).__name__}",
        )
    if report is None:
        if not cfg.enabled:
            return PVCacheStatusDTO(status="disabled", enabled=False, cached=cached)
        return PVCacheStatusDTO(
            status="down",
            enabled=True,
            cached=cached,
            error="No worker has reported a PV cache; is CANOPY_SQUIRREL__PV_CACHE__ENABLED set on canopy-worker?",
        )

    age = time.time() - float(report.get("heartbeatAt") or 0)
    monitored = report.get("monitored")
    connected = report.get("connected")
    error = None
    if age > cfg.heartbeat_max_age_s:
        status = "down"
        error = (
            f"The PV cache heartbeat is {age:.0f}s old (limit {cfg.heartbeat_max_age_s:.0f}s); no worker is leading it"
        )
    elif not report.get("warm") or (monitored is not None and connected is not None and connected < monitored):
        status = "degraded"
    else:
        status = "ok"
    return PVCacheStatusDTO(
        status=status,
        enabled=cfg.enabled,
        host=report.get("host"),
        instance=report.get("instance"),
        leader_since=_from_unix(report.get("leaderSince")),
        heartbeat_at=_from_unix(report.get("heartbeatAt")),
        heartbeat_age_s=round(age, 3),
        configured=report.get("configured"),
        monitored=monitored,
        connected=connected,
        seen=report.get("seen"),
        warm=report.get("warm"),
        cached=cached,
        error=error,
    )
