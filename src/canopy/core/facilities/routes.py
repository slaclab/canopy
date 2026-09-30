"""Facility hierarchy endpoints."""

from fastapi import Security, APIRouter

from canopy.core.security import require_read_access
from canopy.core.facilities.schemas import FacilityDTO
from canopy.core.facilities.dependencies import FacilityServiceDep

router = APIRouter(prefix="/v1/facilities", tags=["Facilities"])


@router.get("", dependencies=[Security(require_read_access)])
async def list_facilities(service: FacilityServiceDep) -> list[FacilityDTO]:
    """Full facility hierarchy, flat with ``parent_id`` — the client assembles the tree."""
    return await service.get_all()
