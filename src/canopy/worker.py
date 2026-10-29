"""Arq worker entrypoint - the ``canopy-worker`` process.

Same image as ``canopy-api``, different command::

    arq canopy.worker.WorkerSettings

Runs long operations (snapshot create/restore) off the request path. Each
persistent module contributes its task functions — on a read-only mirror
(``CANOPY_ROLE=mirror``) only the stateless ones; ``on_startup`` builds the
process-wide resources a task needs - the EPICS connector - and stashes them in
the Arq context so every job reuses one connection set rather than rebuilding it.
"""

import logging

from arq import cron
from arq.connections import RedisSettings

from canopy.core.db import dispose_engine
from canopy.core.config import get_settings
from canopy.core.logging import configure_logging
from canopy.epics.config import get_epics_config
from canopy.epics.gateway import build_connector
from canopy.squirrel.prune import prune_snapshots
from canopy.squirrel.tasks import create_snapshot_task, restore_snapshot_task
from canopy.converter.tasks import convert_batch
from canopy.squirrel.restore_hooks import get_restore_hooks, publish_worker_status
from canopy.core.replication.config import get_prune_config

configure_logging()
logger = logging.getLogger(__name__)


async def ping(ctx: dict) -> str:
    """Liveness task — enqueue and poll it to confirm the worker is processing jobs."""
    return "pong"


async def on_startup(ctx: dict) -> None:
    """Build process-wide resources once when the worker starts.

    Also builds the restore hooks, so configuration warnings are logged at startup
    rather than at the first restore, and publishes which ones this process resolved,
    since restores run here and not in the API; ``GET /squirrel/v1/health/restore-hooks``
    reads it back.

    Parameters
    ----------
    ctx : dict
        Arq's worker context; ``ctx["redis"]`` is its Redis connection.

    Returns
    -------
    None
    """
    connector = build_connector(get_epics_config())
    await connector.start()
    ctx["epics"] = connector
    get_restore_hooks()
    await publish_worker_status(ctx["redis"])
    logger.info("canopy-worker startup complete: EPICS connector ready")


async def on_shutdown(ctx: dict) -> None:
    connector = ctx.get("epics")
    if connector is not None:
        await connector.close()
    await dispose_engine()
    logger.info("canopy-worker shutdown complete")


# Task functions registered with Arq. Each module appends its tasks here.
FUNCTIONS = [ping, convert_batch, create_snapshot_task, restore_snapshot_task]

# What a mirror's (or forward instance's) worker runs: tasks that write no replicated table and
# touch no machine state. Snapshot create/restore are simply not registered there,
# so a job that reached a mirror's queue anyway fails instead of running.
MIRROR_FUNCTIONS = [ping, convert_batch]


def worker_functions(role: str) -> list:
    """The task functions a worker registers for an instance ``role`` (``Settings.role``)."""
    return FUNCTIONS if role == "primary" else MIRROR_FUNCTIONS


def worker_cron_jobs(role: str) -> list:
    """Scheduled tasks for an instance ``role``: pruning, on a primary only.

    ``unique`` makes one worker run each tick when several share the queue. The job
    itself does nothing unless ``CANOPY_PRUNE__ENABLED``; a mirror never registers it,
    since it holds the history the primary prunes.
    """
    if role != "primary":
        return []
    # A run may delete hundreds of full-machine snapshots, well past job_timeout; its
    # outcome is kept a day for anyone checking on it.
    return [
        cron(
            prune_snapshots,
            hour={get_prune_config().hour},
            minute={7},
            unique=True,
            run_at_startup=False,
            timeout=3600,
            keep_result=86400,
        )
    ]


class WorkerSettings:
    functions = worker_functions(get_settings().role)
    cron_jobs = worker_cron_jobs(get_settings().role)
    on_startup = on_startup
    on_shutdown = on_shutdown
    redis_settings = RedisSettings.from_dsn(get_settings().redis_url)
    max_jobs = 10
    job_timeout = 600  # 10 min — large snapshot create/restore
    keep_result = 3600  # keep results 1h so clients can poll after completion
    max_tries = 3
