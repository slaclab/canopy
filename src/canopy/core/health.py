"""Shared ``/health/watchdog`` router factory, and the database check behind ``/health/ready``.

Every request-scoped Canopy sub-module exposes an identical liveness heartbeat
route; this factory builds that route once instead of each module hand-rolling
the same handler. Modules with additional health endpoints (e.g.
``/health/summary``) add them to the returned router afterward — it's a plain
``APIRouter``, so further ``@router.get(...)`` registrations work normally.
"""

import time
import asyncio
import logging
from collections.abc import Callable

from fastapi import APIRouter
from sqlalchemy import text

from canopy import __version__
from canopy.core.db import get_engine
from canopy.contracts.health import HeartbeatDTO

logger = logging.getLogger(__name__)


def make_health_router(module: str, uptime_s: Callable[[], float]) -> APIRouter:
    """Build a ``/health`` router pre-populated with the ``/watchdog`` route.

    ``module`` names the sub-module (used in the DTO and the router's tag) and
    ``uptime_s`` is that module's ``ModuleRuntime.uptime_s`` (or the thin
    module-level wrapper around it).
    """
    router = APIRouter(prefix="/health", tags=[f"{module.capitalize()} / Health"])

    @router.get("/watchdog")
    async def watchdog() -> HeartbeatDTO:
        """Liveness heartbeat. Does not touch the database or any adapter."""
        return HeartbeatDTO(module=module, version=__version__, uptime_s=uptime_s(), now_ts_ns=time.time_ns())

    return router


# Set while the readiness check fails, so a probe every few seconds logs an outage once.
_database_down = False


async def database_ready(timeout_s: float = 2.0) -> bool:
    """Whether the app's database answers ``SELECT 1`` through the app's own pool within ``timeout_s``."""
    global _database_down
    try:
        async with asyncio.timeout(timeout_s):
            async with get_engine().connect() as conn:
                await conn.execute(text("SELECT 1"))
    except Exception as exc:
        if not _database_down:
            logger.warning("Readiness check: the database didn't answer: %r", exc)
        _database_down = True
        return False
    if _database_down:
        logger.info("Readiness check: the database answers again")
    _database_down = False
    return True
