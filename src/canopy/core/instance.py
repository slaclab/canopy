"""``GET /v1/instance`` — what this deployment is, for clients and deploy checks.

Clients read ``role`` to hide write controls on a read-only mirror and
``historyUrl`` to link to the full history; the deploy pipeline compares
``alembicHead`` between a mirror and its primary to stop a release that would
migrate the primary ahead of the copy subscribed to it.
"""

import os
import logging
from typing import Literal
from functools import lru_cache

from fastapi import Security, APIRouter
from alembic.util import CommandError
from alembic.config import Config
from alembic.script import ScriptDirectory

from canopy.core.config import get_settings
from canopy.core.security import require_read_access
from canopy.contracts.base import CanopyModel

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1/instance", tags=["Instance"])


class InstanceDTO(CanopyModel):
    role: Literal["primary", "mirror"]
    instance: str | None
    history_url: str | None
    window_days: int | None
    alembic_head: str | None


@lru_cache
def shipped_alembic_head() -> str | None:
    """The head revision of the migrations tree shipped with this process, not the database's.

    Found the way the ``alembic`` CLI finds it — ``$ALEMBIC_CONFIG``, else
    ``alembic.ini`` in the working directory (``/app`` in the prod image) — so
    it names exactly what ``alembic upgrade head`` would migrate to from here.
    ``None`` when the tree isn't there or has more than one head. Cached: the
    tree can't change under a running process.
    """
    config_path = os.environ.get("ALEMBIC_CONFIG", "alembic.ini")
    try:
        heads = ScriptDirectory.from_config(Config(config_path)).get_heads()
    except CommandError as exc:
        logger.warning("Cannot read the Alembic head from %s: %s", config_path, exc)
        return None
    if len(heads) != 1:
        logger.warning("Alembic tree at %s has %d heads; reporting none", config_path, len(heads))
        return None
    return heads[0]


@router.get("", dependencies=[Security(require_read_access)])
async def get_instance() -> InstanceDTO:
    settings = get_settings()
    return InstanceDTO(
        role=settings.role,
        instance=settings.instance,
        history_url=settings.history_url,
        window_days=settings.window_days,
        alembic_head=shipped_alembic_head(),
    )
