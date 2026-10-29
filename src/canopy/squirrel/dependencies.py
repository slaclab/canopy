"""Typed ``Annotated[..., Depends(...)]`` aliases for Squirrel's API layer.

Each service is built per request from the shared ``canopy.core.db.SessionDep``.
``SnapshotService`` is constructed without an ``EpicsIO``: the read endpoints never touch
EPICS (snapshot create/restore run in the worker, which injects the connector).

The PV and snapshot factories also take the calling key's facility grant, resolved to an
id set by ``get_facility_scope`` below — the single chokepoint, so no route can forget it.
When a forward instance sent the request, the grant is narrowed to its caller's. A route
that gates before queueing a job (snapshot create) takes ``FacilityScopeDep`` directly.
``None`` means unrestricted, which is what a service built anywhere else (the Arq worker)
gets by default: the worker acts on a request the route already authorized.
"""

from typing import Annotated

from fastapi import Depends

from canopy.core.db import SessionDep
from canopy.core.security import APIKeyDep, forwarded_facilities
from canopy.core.facilities import gateway as facilities_gateway
from canopy.squirrel.services.pv_service import PvService
from canopy.squirrel.services.tag_service import TagService
from canopy.squirrel.services.snapshot_service import SnapshotService


async def get_facility_scope(key: APIKeyDep) -> set[str] | None:
    scope = await facilities_gateway.resolve_key_facility_scope(key.facilities)
    # A forward instance's service key acts for a caller whose grant comes with the
    # request; the request reaches only what both keys reach.
    if (caller := forwarded_facilities()) is not None:
        caller_scope = await facilities_gateway.resolve_key_facility_scope(caller)
        if caller_scope is not None:
            scope = caller_scope if scope is None else scope & caller_scope
    return scope


# FastAPI caches a dependency's result per request by default, so a route pulling in
# both PvServiceDep and SnapshotServiceDep still resolves the grant once, not twice.
FacilityScopeDep = Annotated[set[str] | None, Depends(get_facility_scope)]


async def get_snapshot_service(session: SessionDep, scope: FacilityScopeDep) -> SnapshotService:
    return SnapshotService(session, facility_scope=scope)


async def get_pv_service(session: SessionDep, scope: FacilityScopeDep) -> PvService:
    return PvService(session, facility_scope=scope)


def get_tag_service(session: SessionDep) -> TagService:
    # Tags carry no facility linkage in the schema, so there is nothing to scope.
    return TagService(session)


SnapshotServiceDep = Annotated[SnapshotService, Depends(get_snapshot_service)]
PvServiceDep = Annotated[PvService, Depends(get_pv_service)]
TagServiceDep = Annotated[TagService, Depends(get_tag_service)]
