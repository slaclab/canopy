from urllib.parse import quote

from fastapi import Security, APIRouter

from canopy.jobs import gateway
from canopy.core.config import get_settings
from canopy.jobs.errors import JobNotFound
from canopy.core.security import APIKey, require_read_access
from canopy.jobs.schemas.job import JobStatusDTO
from canopy.core.forwarding.client import send_to_primary
from canopy.core.forwarding.config import get_forward_config

router = APIRouter(tags=["Jobs"])


@router.get("/{job_id}")
async def get_job(job_id: str, key: APIKey = Security(require_read_access)) -> JobStatusDTO:
    """Poll the status / result of a background job by its id.

    On a forward instance, a job this instance doesn't have is asked of the primary:
    a forwarded snapshot capture runs there, and its job id is the primary's.
    """
    dto = await gateway.get_status(job_id)
    if dto is None and get_settings().role == "forward":
        dto = await _primary_job_status(job_id, key)
    if dto is None:
        raise JobNotFound(f"Job {job_id!r} not found")
    return dto


async def _primary_job_status(job_id: str, caller: APIKey) -> JobStatusDTO | None:
    answer = await send_to_primary(
        get_forward_config(), "GET", f"/jobs/v1/{quote(job_id, safe='')}", caller=caller, headers={}
    )
    if answer.status_code != 200:
        return None
    return JobStatusDTO.model_validate(answer.json())
