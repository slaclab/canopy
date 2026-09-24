"""Typed ``Annotated[..., Depends(...)]`` aliases for Squirrel's API layer.

Each service is built per request from the shared ``canopy.core.db.SessionDep``.
``SnapshotService`` is constructed without an ``EpicsIO``: the read endpoints never touch
EPICS (snapshot create/restore run in the worker, which injects the connector).

The PV and snapshot factories also resolve the calling key's facility grant to an id set
here, once per request — the single chokepoint, so no route can forget it. ``None`` means
unrestricted, which is what a service built anywhere else (the Arq worker) gets by
default: the worker acts on a request the route already authorized.
"""

from typing import Annotated

from fastapi import Depends

from canopy.core.db import SessionDep
from canopy.core.security import APIKeyDep
from canopy.core.facilities import gateway as facilities_gateway
from canopy.squirrel.services.pv_service import PvService
from canopy.squirrel.services.tag_service import TagService
from canopy.squirrel.services.snapshot_service import SnapshotService


async def get_snapshot_service(session: SessionDep, key: APIKeyDep) -> SnapshotService:
    scope = await facilities_gateway.resolve_key_facility_scope(key.facilities)
    return SnapshotService(session, facility_scope=scope)


async def get_pv_service(session: SessionDep, key: APIKeyDep) -> PvService:
    scope = await facilities_gateway.resolve_key_facility_scope(key.facilities)
    return PvService(session, facility_scope=scope)


def get_tag_service(session: SessionDep) -> TagService:
    # Tags carry no facility linkage in the schema, so there is nothing to scope.
    return TagService(session)


SnapshotServiceDep = Annotated[SnapshotService, Depends(get_snapshot_service)]
PvServiceDep = Annotated[PvService, Depends(get_pv_service)]
TagServiceDep = Annotated[TagService, Depends(get_tag_service)]
