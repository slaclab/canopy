"""Typed ``Annotated[..., Depends(...)]`` aliases for the e-log API layer.

Routes pull these in by alias rather than re-spelling ``Depends()``.
"""

from typing import Annotated

from fastapi import Depends

from canopy.elog import gateway
from canopy.elog.config import ElogConfig, get_elog_config
from canopy.elog.services.elog_service import ElogService


def get_elog_service() -> ElogService:
    return gateway.get_service()


def get_config() -> ElogConfig:
    return get_elog_config()


ElogServiceDep = Annotated[ElogService, Depends(get_elog_service)]
ElogConfigDep = Annotated[ElogConfig, Depends(get_config)]
