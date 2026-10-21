import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from canopy.elog import gateway
from canopy.core.errors import register_canopy_error_handler
from canopy.elog.config import get_elog_config
from canopy.elog.runtime import mark_started
from canopy.elog.api.v1.router import router as v1_router
from canopy.elog.adapters.registry import build_registry
from canopy.elog.services.elog_service import ElogService

logger = logging.getLogger(__name__)


@asynccontextmanager
async def elog_lifespan():
    """Composed by the parent app's lifespan in ``canopy.main``.

    Mounted sub-apps don't run their own lifespan behind ``app.mount()``.
    Startup builds one adapter per configured app (each with its own token)
    and binds the ElogService to the gateway; shutdown closes the adapters.
    """
    config = get_elog_config()
    registry = build_registry(config)
    service = ElogService(config, registry)
    gateway.bind(service)
    mark_started()
    if registry.enabled:
        logger.info(
            "E-log started: provider=%s, apps=%s", registry.provider, ",".join(registry.apps) or "(none with a token)"
        )
    else:
        logger.info("E-log started DISABLED: %s", registry.disabled_reason)
    try:
        yield
    finally:
        gateway.bind(None)
        await service.close()
        logger.info("E-log shutdown complete")


app = FastAPI(
    title="Canopy / E-log",
    description="Server-side e-log posting: one e-log application token per client app, so clients never hold one.",
)

register_canopy_error_handler(app)
app.include_router(v1_router)
