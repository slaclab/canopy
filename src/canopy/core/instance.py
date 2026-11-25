"""``GET /v1/instance`` — what this deployment is, for clients and deploy checks.

Clients read ``instance`` to tell which rows they can write here (a row's
``ownerInstance`` must match it) and ``historyUrl`` to link to the full history.
The deploy pipeline compares the head's ``alembicHead`` with the new image's to
stop a release that would migrate the primary ahead of the copy subscribed to it.
On a history head, ``arms`` lists the arms it subscribes to and the state of each
subscription.
"""

import os
import sys
import logging
from typing import TYPE_CHECKING, Literal
from functools import lru_cache

from fastapi import Security, APIRouter

from canopy.core.arms import ArmsConfigError, get_arms_config, subscription_name
from canopy.core.config import get_settings
from canopy.core.security import require_read_access
from canopy.contracts.base import CanopyModel

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1/instance", tags=["Instance"])


SubscriptionState = Literal["missing", "disabled", "stopped", "copying", "streaming"]


class ArmDTO(CanopyModel):
    name: str
    subscription: SubscriptionState


class InstanceDTO(CanopyModel):
    role: Literal["primary", "mirror"]
    instance: str
    history_url: str | None
    window_days: int | None
    alembic_head: str | None
    arms: list[ArmDTO]


async def arm_states(session: "AsyncSession", arms: list[str]) -> dict[str, SubscriptionState]:
    """The state of this head's subscription to each of ``arms``.

    ``missing`` before ``subscribe``; ``disabled``; ``stopped`` when no apply worker
    runs (as while it retries a failing change); ``copying`` until every table has
    had its initial copy; otherwise ``streaming``. Read from the subscription
    catalogs, whose columns other than the connection string any role may read.
    """
    from sqlalchemy import text

    rows = await session.execute(
        text(
            """
            SELECT s.subname,
                   s.subenabled AS enabled,
                   EXISTS (SELECT 1 FROM pg_stat_subscription w
                           WHERE w.subid = s.oid AND w.relid IS NULL AND w.leader_pid IS NULL
                             AND w.pid IS NOT NULL) AS working,
                   EXISTS (SELECT 1 FROM pg_subscription_rel r
                           WHERE r.srsubid = s.oid AND r.srsubstate <> 'r') AS copying
            FROM pg_subscription s
            WHERE s.subname = ANY(:names)
              AND s.subdbid = (SELECT oid FROM pg_database WHERE datname = current_database())
            """
        ),
        {"names": [subscription_name(arm) for arm in arms]},
    )
    found = {row.subname: row for row in rows}
    states: dict[str, SubscriptionState] = {}
    for arm in arms:
        row = found.get(subscription_name(arm))
        if row is None:
            states[arm] = "missing"
        elif not row.enabled:
            states[arm] = "disabled"
        elif not row.working:
            states[arm] = "stopped"
        else:
            states[arm] = "copying" if row.copying else "streaming"
    return states


async def _head_arm_states(arms: list[str]) -> dict[str, SubscriptionState]:
    """``arm_states`` in a session of its own.

    The database is imported here, not at the top: ``/v1/instance`` is served on
    every install, including one without the ``data`` extra, and only a head,
    which has the database, has arms to read.
    """
    from canopy.core.db import session_scope

    async with session_scope() as session:
        return await arm_states(session, arms)


@lru_cache
def shipped_alembic_head() -> str | None:
    """The head revision of the migrations tree shipped with this process, not the database's.

    Found the way the ``alembic`` CLI finds it — ``$ALEMBIC_CONFIG``, else
    ``alembic.ini`` in the working directory (``/app`` in the prod image) — so
    it names exactly what ``alembic upgrade head`` would migrate to from here.
    ``None`` when the tree isn't there (the dev image has none), Alembic isn't
    installed (an install without the ``data`` extra), the tree can't be read, or
    it has more than one head; this endpoint must never fail over it.

    Reading the tree imports every version file, so the API calls this once at
    startup and the cache answers every request: the tree can't change under a
    running process. ``sys.path`` is restored afterwards, since Alembic prepends
    the ini's ``prepend_sys_path`` to it.
    """
    config_path = os.environ.get("ALEMBIC_CONFIG", "alembic.ini")
    try:
        # Imported here: Alembic comes with the ``data`` extra, which a lean install lacks.
        from alembic.config import Config
        from alembic.script import ScriptDirectory
    except ImportError:
        logger.info("Alembic is not installed; reporting no Alembic head")
        return None
    saved_path = sys.path[:]
    try:
        heads = ScriptDirectory.from_config(Config(config_path)).get_heads()
    except Exception:
        logger.warning("Cannot read the Alembic head from %s", config_path, exc_info=True)
        return None
    finally:
        sys.path[:] = saved_path
    if len(heads) != 1:
        logger.warning("Alembic tree at %s has %d heads; reporting none", config_path, len(heads))
        return None
    return heads[0]


@router.get("", dependencies=[Security(require_read_access)])
async def get_instance() -> InstanceDTO:
    settings = get_settings()
    try:
        arms = sorted(get_arms_config().arms) if settings.role == "mirror" else []
    except ArmsConfigError as exc:
        # The error handler answers with this message but doesn't log it; the operator needs it here.
        logger.error("GET /v1/instance: %s", exc)
        raise
    states = await _head_arm_states(arms) if arms else {}
    return InstanceDTO(
        role=settings.role,
        instance=settings.instance,
        history_url=settings.history_url,
        window_days=settings.window_days,
        alembic_head=shipped_alembic_head(),
        arms=[ArmDTO(name=arm, subscription=states[arm]) for arm in arms],
    )
