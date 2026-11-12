import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from canopy.core.errors import register_canopy_error_handler
from canopy.core.modules import tag_module
from canopy.magpie.runtime import mark_started
from canopy.magpie.api.v1.router import router as v1_router

logger = logging.getLogger(__name__)


@asynccontextmanager
async def magpie_lifespan():
    """Record boot time and nothing more.

    Magpie's service is request-scoped (built per request from a DB session) and the
    shared engine connects lazily, so there is no singleton to wire here. Composed by
    the parent app's lifespan in ``canopy.main``. Mounted sub-apps don't run their own
    lifespan behind ``app.mount()``.
    """
    mark_started()
    logger.info("Magpie started")
    try:
        yield
    finally:
        logger.info("Magpie shutdown complete")


app = FastAPI(
    title="Canopy / Magpie",
    description="Generic config store: save and share app configs by short slug.",
)

register_canopy_error_handler(app)
tag_module(app, "magpie")
app.include_router(v1_router)
