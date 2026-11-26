from fastapi import Security

from canopy.core.health import make_health_router
from canopy.core.security import require_read_access
from canopy.contracts.health import ConverterHealthSummaryDTO
from canopy.converter.runtime import uptime_s
from canopy.converter.dependencies import ConversionServiceDep

router = make_health_router("converter", uptime_s)


@router.get("/summary", dependencies=[Security(require_read_access)])
def summary(service: ConversionServiceDep) -> ConverterHealthSummaryDTO:
    """The converter's fallbacks: ``color-scheme`` while its scheme file is rejected and ``canopy-default`` is served.

    Reported for the scheme the lifespan bound into the service, the one conversions use, so editing the
    environment without a restart changes neither.
    """
    causes = service.degraded
    return ConverterHealthSummaryDTO(status="degraded" if causes else "ok", degraded=causes)
