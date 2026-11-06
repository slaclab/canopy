"""PV registry + meta-PV endpoints.

Hare/Vole own live values and the EPICS module owns connectivity probing. So all we need here is
the PV registry: search (keyset + offset), CRUD, bulk import, device list, and meta-PVs.
"""

import json
from uuid import UUID

from fastapi import Query, Security, APIRouter
from pydantic import TypeAdapter, ValidationError

from canopy.core.errors import BadRequest
from canopy.core.security import require_read_access, require_write_access
from canopy.core.validation import require_uuid, parse_continuation_token
from canopy.squirrel.errors import PvNotFound
from canopy.squirrel.schemas.pv import (
    PVElementDTO,
    NewPVElementDTO,
    PVSearchResponse,
    UpdatePVElementDTO,
)
from canopy.contracts.pagination import PagedResult
from canopy.squirrel.dependencies import PvServiceDep
from canopy.squirrel.schemas.meta_pv import (
    MetaPVDTO,
    NewMetaPVDTO,
    UpdateMetaPVDTO,
    DisplayMetaPVDTO,
)

router = APIRouter(prefix="/pvs", tags=["PVs"])

# ``tagFilters`` is JSON inside a query string, so FastAPI can't validate it for us. One adapter
# checks the shape and the UUID form of keys and values: an unparseable id must not reach asyncpg.
_TAG_FILTERS = TypeAdapter(dict[UUID, list[UUID]])


def _first_problem(error: ValidationError) -> str:
    """The first pydantic error with its location, e.g. ``g.0: Input should be a valid UUID``."""
    first = error.errors()[0]
    loc = ".".join(str(part) for part in first["loc"])
    return f"{loc}: {first['msg']}" if loc else first["msg"]


@router.get("", dependencies=[Security(require_read_access)])
async def search_pvs(
    service: PvServiceDep,
    pvName: str | None = Query(None),
    facility: str | None = Query(None, description="Facility slug — includes descendants; 404 if unknown"),
) -> list[PVElementDTO]:
    """Search PVs by name (non-paginated, capped — kept for backward compatibility)."""
    result = await service.search_paged(search=pvName, page_size=1000, facility=facility)
    return result.results


@router.get("/paged", dependencies=[Security(require_read_access)])
async def search_pvs_paged(
    service: PvServiceDep,
    pvName: str | None = Query(None),
    pageSize: int = Query(100, ge=1, le=1000),
    continuationToken: str | None = Query(None),
    tagFilters: str | None = Query(None, description='JSON object: {"groupId": ["tagId", ...], ...}'),
    facility: str | None = Query(None, description="Facility slug — includes descendants; 404 if unknown"),
) -> PagedResult[PVElementDTO]:
    """Keyset-paginated search with optional tag filtering.

    ``tagFilters`` is ``{group: [tag, ...]}`` — a PV matches when it has at least one tag
    from every listed group (OR within a group, AND across groups). A ``continuationToken``
    that is not a PV id, or ``tagFilters`` that is not an object of tag-id lists keyed by
    group id, is a 400. A JSON ``null``, ``[]`` or ``{}`` for ``tagFilters`` means no filter.
    """
    continuation_token = parse_continuation_token(continuationToken)

    tag_filters = None
    if tagFilters:
        try:
            raw = json.loads(tagFilters)
        except json.JSONDecodeError as e:
            raise BadRequest(f"Invalid tagFilters: not JSON ({e.msg} at position {e.pos})") from e
        if raw:  # null, [], false and 0 mean "no filter", as an absent parameter does
            try:
                parsed = _TAG_FILTERS.validate_python(raw)
            except ValidationError as e:
                raise BadRequest(f"Invalid tagFilters: {_first_problem(e)}") from e
            tag_filters = {str(group): [str(tag) for tag in tags] for group, tags in parsed.items()}

    return await service.search_paged(
        search=pvName,
        page_size=pageSize,
        continuation_token=continuation_token,
        tag_filters=tag_filters,
        facility=facility,
    )


@router.get("/search", dependencies=[Security(require_read_access)])
async def search_pvs_filtered(
    service: PvServiceDep,
    q: str | None = Query(None, description="Text search"),
    devices: list[str] = Query(default=[], description="Filter by device"),
    tags: list[str] = Query(default=[], description="Filter by tag IDs"),
    limit: int = Query(100, ge=1, le=1000, description="Max results"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    facility: str | None = Query(None, description="Facility slug — includes descendants; 404 if unknown"),
) -> PVSearchResponse:
    """Offset-paginated, server-side filtered search (more efficient than client-side filtering)."""
    return await service.search_filtered(
        q=q, devices=devices or None, tag_ids=tags or None, limit=limit, offset=offset, facility=facility
    )


@router.get("/devices", dependencies=[Security(require_read_access)])
async def get_all_devices(service: PvServiceDep) -> list[str]:
    """All unique device names, for the frontend's device filter."""
    return await service.get_all_devices()


@router.post("", dependencies=[Security(require_write_access)])
async def create_pv(data: NewPVElementDTO, service: PvServiceDep) -> PVElementDTO:
    """Create a new PV."""
    return await service.create(data)


@router.post("/multi", dependencies=[Security(require_write_access)])
async def create_multiple_pvs(data: list[NewPVElementDTO], service: PvServiceDep) -> list[PVElementDTO]:
    """Bulk-create PVs (CSV import)."""
    return await service.create_many(data)


@router.put("/{pv_id}", dependencies=[Security(require_write_access)])
async def update_pv(pv_id: str, data: UpdatePVElementDTO, service: PvServiceDep) -> PVElementDTO:
    """Update a PV's metadata (addresses are immutable here)."""
    pv_id = require_uuid(pv_id, PvNotFound, f"PV {pv_id} not found")
    pv = await service.update(pv_id, data)
    if not pv:
        raise PvNotFound(f"PV {pv_id} not found")
    return pv


@router.delete("/{pv_id}", dependencies=[Security(require_write_access)])
async def delete_pv(pv_id: str, service: PvServiceDep) -> bool:
    """Delete a PV."""
    pv_id = require_uuid(pv_id, PvNotFound, f"PV {pv_id} not found")
    if not await service.delete(pv_id):
        raise PvNotFound(f"PV {pv_id} not found")
    return True


# --- meta PVs (PVs shown specially on snapshots) ---------------------------- #


@router.post("/meta", dependencies=[Security(require_write_access)])
async def add_meta_pv(pvInfo: NewMetaPVDTO, service: PvServiceDep) -> MetaPVDTO:
    """Promote an existing PV (by address) to a meta-PV."""
    return await service.add_meta_pv(pvInfo.pv_name, pvInfo.display_name)


@router.get("/meta", dependencies=[Security(require_read_access)])
async def get_meta_pvs(service: PvServiceDep, isActive: bool | None = None) -> list[DisplayMetaPVDTO]:
    """List meta-PVs, optionally filtered by active state."""
    return await service.get_meta_pvs(active=isActive)


@router.get("/meta/{id}", dependencies=[Security(require_read_access)])
async def get_meta_pv(id: str, service: PvServiceDep) -> MetaPVDTO:
    """Get one meta-PV by its PV id."""
    id = require_uuid(id, PvNotFound, f"Meta PV {id} not found")
    return await service.get_meta_pv(id)


@router.patch("/meta/{id}", dependencies=[Security(require_write_access)])
async def update_meta_pv(id: str, update: UpdateMetaPVDTO, service: PvServiceDep) -> MetaPVDTO:
    """Update a meta-PV's display name and/or active state."""
    id = require_uuid(id, PvNotFound, f"Meta PV {id} not found")
    result = None
    if update.display_name is not None:
        result = await service.set_meta_display_name(id=id, display_name=update.display_name)
    if update.active is not None:
        result = await (service.enable_meta_pv(id) if update.active else service.disable_meta_pv(id))
    if result is None:
        result = await service.get_meta_pv(id)
    return result


@router.delete("/meta/{id}", status_code=204, dependencies=[Security(require_write_access)])
async def delete_meta_pv(id: str, service: PvServiceDep) -> None:
    """Demote a meta-PV back to an ordinary PV."""
    id = require_uuid(id, PvNotFound, f"Meta PV {id} not found")
    await service.delete_meta_pv(id)
