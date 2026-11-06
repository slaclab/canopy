"""Meta-PV DTOs."""

from canopy.contracts.base import CanopyModel


class NewMetaPVDTO(CanopyModel):
    pv_name: str
    display_name: str = ""


class MetaPVDTO(CanopyModel):
    pv_id: str
    display_name: str = ""
    is_active: bool = True


class DisplayMetaPVDTO(MetaPVDTO):
    pv_name: str
    # The owner of the PV the marker follows (``canopy.core.ownership``). On a mirror two
    # owners' meta-PVs can share an address.
    owner_instance: str


class UpdateMetaPVDTO(CanopyModel):
    display_name: str | None = None
    active: bool | None = None
