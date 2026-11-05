import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from canopy.core.db import session_scope
from canopy.core.errors import register_canopy_error_handler
from canopy.core.facilities import gateway as facilities_gateway
from canopy.squirrel.runtime import mark_started
from canopy.squirrel.api.v1.router import router as v1_router
from canopy.squirrel.data.repositories import facility_in_use

logger = logging.getLogger(__name__)


async def _facility_in_use(facility_id: str, slug: str) -> bool:
    # `slug` is unused: Squirrel's PV and snapshot rows reference a facility by id.
    # The hook passes both because a key grant references one by slug instead.
    async with session_scope() as session:
        return await facility_in_use(session, facility_id)


@asynccontextmanager
async def squirrel_lifespan():
    """Record boot time and register Squirrel's facility in-use checker.

    Squirrel's services are request-scoped (built per request from a DB session) and the
    shared engine + Jobs Redis pool both connect lazily, so there is no singleton to wire
    here beyond the in-use checker below. Composed by the parent app's lifespan in
    ``canopy.main``. Mounted sub-apps don't run their own behind ``app.mount()``.
    """
    facilities_gateway.register_in_use_checker(_facility_in_use, "PVs or snapshots")
    mark_started()
    logger.info("Squirrel started")
    try:
        yield
    finally:
        logger.info("Squirrel shutdown complete")


app = FastAPI(
    title="Canopy / Squirrel",
    description="EPICS machine-state snapshots: save, restore, and compare.",
)

register_canopy_error_handler(app)
app.include_router(v1_router)
