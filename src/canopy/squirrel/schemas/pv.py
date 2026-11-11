"""PV registry DTOs."""

from typing import Literal
from datetime import datetime

from pydantic import Field, model_validator

from canopy.contracts.base import CanopyModel
from canopy.squirrel.schemas.tag import TagDTO


class PVBase(CanopyModel):
    setpoint_address: str | None = Field(None, max_length=255)
    readback_address: str | None = Field(None, max_length=255)
    config_address: str | None = Field(None, max_length=255)
    device: str | None = Field(None, max_length=255)
    description: str | None = None
    abs_tolerance: float = 0.0
    rel_tolerance: float = 0.0


class NewPVElementDTO(PVBase):
    """DTO for creating a PV. At least one address is required."""

    tags: list[str] = []  # Tag IDs
    # Facility *slugs*, not ids — consistent with `NewSnapshotDTO.facility` and with
    # the slugs a key's facility grant carries. Empty means unassigned, which is a
    # valid state: an unassigned PV is writable only by an unrestricted key.
    facilities: list[str] = []

    @model_validator(mode="after")
    def validate_at_least_one_address(self):
        if not any([self.setpoint_address, self.readback_address, self.config_address]):
            raise ValueError("At least one address (setpoint, readback, or config) is required")
        return self


class UpdatePVElementDTO(CanopyModel):
    """Partial update of a PV. Address fields are intentionally immutable here."""

    device: str | None = None
    description: str | None = None
    abs_tolerance: float | None = None
    rel_tolerance: float | None = None
    tags: list[str] | None = None  # Tag IDs
    # Facility slugs. ``None`` leaves assignments alone; ``[]`` clears them.
    facilities: list[str] | None = None


class PVElementDTO(PVBase):
    """Full PV response DTO."""

    id: str
    tags: list[TagDTO] = []
    facilities: list[str] = []  # slugs
    created_date: datetime
    last_modified_date: datetime
    created_by: str | None = None
    last_modified_by: str | None = None


class PVSearchResponse(CanopyModel):
    """Response from ``GET /v1/pvs/search`` (offset paginated). The source's
    ``liveValues`` fields are dropped with the live-PV path."""

    results: list[PVElementDTO]
    total_count: int
    limit: int
    offset: int


# How ``POST /v1/pvs/multi`` treats a row whose identifying address is already registered:
# refuse the whole import (``error``), leave the stored PV alone (``skip``), or apply the
# row's provided fields to it (``update``).
OnConflict = Literal["error", "skip", "update"]

AddressKind = Literal["setpoint", "readback", "config"]


class PVImportSkippedRowDTO(CanopyModel):
    """A row of a bulk import left out because its identifying address is already registered."""

    row: int  # 1-based position in the request
    kind: AddressKind  # which address identifies the PV
    address: str


class PVAddressMismatchDTO(CanopyModel):
    """A non-identifying address on an update row that differs from the stored PV's.

    Addresses are never changed by an import, so the stored value is kept.
    """

    row: int  # 1-based position in the request
    kind: AddressKind
    requested: str
    stored: str | None


class PVImportResultDTO(CanopyModel):
    """Response of ``POST /v1/pvs/multi`` with ``onConflict=skip`` or ``onConflict=update``.

    Each list is in request order. ``updated`` and ``address_mismatches`` are always
    empty for ``skip``.
    """

    created: list[PVElementDTO]
    updated: list[PVElementDTO]
    skipped: list[PVImportSkippedRowDTO]
    address_mismatches: list[PVAddressMismatchDTO]
