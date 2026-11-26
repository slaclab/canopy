from urllib.parse import quote

from fastapi import Security, APIRouter

from canopy.jobs import gateway
from canopy.core.config import get_settings
from canopy.core.errors import PrimaryUnavailable
from canopy.jobs.errors import JobNotFound
from canopy.core.security import APIKey, require_read_access
from canopy.jobs.schemas.job import JobStatusDTO
from canopy.core.forwarding.client import (
    Caller,
    arm_error,
    send_to_arm,
    forward_target,
    service_key_refusal,
)

router = APIRouter(tags=["Jobs"])


@router.get("/{job_id}")
async def get_job(job_id: str, key: APIKey = Security(require_read_access)) -> JobStatusDTO:
    """Poll the status / result of a background job by its id.

    On a history head, a job this instance doesn't have may be one a forwarded write
    started on an arm, such as a restore there: the head remembers which arm, and
    asks it. The arm's 404 is this one's; a refusal of the head's service key is
    502 ``service_key_refused``, and any other error 502 ``primary_unavailable``
    naming the arm's answer.
    """
    dto = await gateway.get_status(job_id)
    if dto is None and get_settings().role == "mirror":
        dto = await _forwarded_job_status(job_id, key)
    if dto is None:
        raise JobNotFound(f"Job {job_id!r} not found")
    return dto


async def _forwarded_job_status(job_id: str, caller: APIKey) -> JobStatusDTO | None:
    if (arm_name := await gateway.get_forwarded_job_arm(job_id)) is None:
        return None
    if (arm := forward_target(arm_name)) is None:
        return None
    answer = await send_to_arm(arm, "GET", f"/jobs/v1/{quote(job_id, safe='')}", caller=Caller.of(caller))
    if answer.status_code == 404:
        return None
    if refusal := service_key_refusal(arm.name, answer, manages_facilities=False):
        raise refusal
    if answer.status_code != 200:
        code, detail = arm_error(answer)
        answered = f"{answer.status_code} {code}" if code else str(answer.status_code)
        raise PrimaryUnavailable(f"Arm '{arm.name}' answered {answered} for job {job_id!r}: {detail}")
    try:
        return JobStatusDTO.model_validate(answer.json())
    except ValueError as exc:
        # Malformed JSON, or a job status this version doesn't read (a rolling deploy).
        raise PrimaryUnavailable(
            f"Arm '{arm.name}' answered job {job_id!r} with a status this head can't read: {exc}"
        ) from None
