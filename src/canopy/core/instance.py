"""``GET /v1/instance`` — what this deployment is, for clients and deploy checks.

Clients read ``role`` to hide write controls on a read-only mirror and
``historyUrl`` to link to the full history; the deploy pipeline compares
``alembicHead`` between a mirror and its primary to stop a release that would
migrate the primary ahead of the copy subscribed to it. On a history head,
``arms`` lists the arms it subscribes to and the state of each subscription.
"""

import os
import sys
import logging
from typing import Literal
from functools import lru_cache

from fastapi import Security, APIRouter
from sqlalchemy import text
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy.ext.asyncio import AsyncSession

from canopy.core.db import SessionDep
from canopy.core.arms import get_arms_config, subscription_name
from canopy.core.config import get_settings
from canopy.core.security import require_read_access
from canopy.contracts.base import CanopyModel

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1/instance", tags=["Instance"])


SubscriptionState = Literal["missing", "disabled", "stopped", "copying", "streaming"]


class ArmDTO(CanopyModel):
    name: str
    subscription: SubscriptionState


class InstanceDTO(CanopyModel):
    role: Literal["primary", "mirror"]
    instance: str | None
    history_url: str | None
    window_days: int | None
    alembic_head: str | None
    arms: list[ArmDTO]


async def arm_states(session: AsyncSession, arms: list[str]) -> dict[str, SubscriptionState]:
    """The state of this head's subscription to each of ``arms``.

    ``missing`` before ``subscribe``; ``disabled``; ``stopped`` when no apply worker
    runs (as while it retries a failing change); ``copying`` until every table has
    had its initial copy; otherwise ``streaming``. Read from the subscription
    catalogs, whose columns other than the connection string any role may read.
    """
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


@lru_cache
def shipped_alembic_head() -> str | None:
    """The head revision of the migrations tree shipped with this process, not the database's.

    Found the way the ``alembic`` CLI finds it — ``$ALEMBIC_CONFIG``, else
    ``alembic.ini`` in the working directory (``/app`` in the prod image) — so
    it names exactly what ``alembic upgrade head`` would migrate to from here.
    ``None`` when the tree isn't there (the dev image has none), can't be read,
    or has more than one head; this endpoint must never fail over it.

    Reading the tree imports every version file, so the API calls this once at
    startup and the cache answers every request: the tree can't change under a
    running process. ``sys.path`` is restored afterwards, since Alembic prepends
    the ini's ``prepend_sys_path`` to it.
    """
    config_path = os.environ.get("ALEMBIC_CONFIG", "alembic.ini")
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
async def get_instance(session: SessionDep) -> InstanceDTO:
    settings = get_settings()
    arms = sorted(get_arms_config().arms) if settings.role == "mirror" else []
    states = await arm_states(session, arms) if arms else {}
    return InstanceDTO(
        role=settings.role,
        instance=settings.instance,
        history_url=settings.history_url,
        window_days=settings.window_days,
        alembic_head=shipped_alembic_head(),
        arms=[ArmDTO(name=arm, subscription=states[arm]) for arm in arms],
    )
