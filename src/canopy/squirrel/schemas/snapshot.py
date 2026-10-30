"""Snapshot DTOs."""

from uuid import UUID
from typing import Any, Literal
from datetime import datetime

from pydantic import Field, ConfigDict

from canopy.contracts.base import CanopyModel


class EpicsValueDTO(CanopyModel):
    """A stored EPICS value, read back out of the ``snapshot_value`` JSONB blob.

    ``extra="ignore"`` tolerates blob keys the DTO doesn't model (forward-compat as
    snapshot metadata fidelity grows).
    """

    model_config = ConfigDict(extra="ignore")

    value: Any = None
    status: int | None = None
    severity: int | None = None
    timestamp: datetime | None = None
    units: str | None = None
    precision: int | None = None
    upper_ctrl_limit: float | None = None
    lower_ctrl_limit: float | None = None


class TagInfoDTO(CanopyModel):
    """Lightweight tag info attached to a snapshot value."""

    id: str
    name: str
    group_name: str


class PVValueDTO(CanopyModel):
    """One PV's value within a snapshot (setpoint + readback)."""

    pv_id: str
    pv_name: str  # Primary name (setpoint or readback)
    device: str | None = None
    setpoint_name: str | None = None
    readback_name: str | None = None
    setpoint_value: EpicsValueDTO | None = None
    readback_value: EpicsValueDTO | None = None
    status: int | None = None
    severity: int | None = None
    timestamp: datetime | None = None
    tags: list[TagInfoDTO] = []


class MetaPVValueDTO(EpicsValueDTO):
    """A meta-PV's value within a snapshot."""

    pv_id: str
    pv_name: str  # Setpoint address if present, else readback address


class SnapshotBase(CanopyModel):
    title: str = Field(..., min_length=1, max_length=255)
    description: str | None = None


class NewSnapshotDTO(SnapshotBase):
    """Create a snapshot. With ``facility``, captures the PVs assigned to that facility
    or its descendants; without, every registered PV."""

    facility: str | None = None


class SnapshotSummaryDTO(SnapshotBase):
    """Summary DTO for listing snapshots.

    ``retain`` exempts the snapshot from pruning of old snapshots. ``imported_from`` is
    the id of the snapshot it was imported from (``POST /snapshots/import``), else ``None``.
    """

    id: str
    created_date: datetime
    created_by: str | None = None
    pv_count: int = 0
    facility: str | None = None  # slug
    retain: bool = False
    imported_from: str | None = None
    meta_pvs: list[MetaPVValueDTO] = Field(default_factory=list)


class SnapshotDTO(SnapshotSummaryDTO):
    """Full snapshot with all PV values."""

    pv_values: list[PVValueDTO] = []


class UpdateSnapshotDTO(CanopyModel):
    """Partial update: fields left out (or ``null``) keep their value."""

    title: str | None = Field(None, min_length=1, max_length=255)
    description: str | None = None
    retain: bool | None = None


BUNDLE_FORMAT = "canopy.squirrel.snapshot-bundle"


class BundleValueDTO(CanopyModel):
    """One value in a snapshot bundle, with the addresses its PV had when exported.

    ``setpoint_value`` and ``readback_value`` are the stored JSONB blobs, unchanged.
    """

    pv_id: UUID
    pv_name: str = Field(..., max_length=255)
    setpoint_address: str | None = Field(None, max_length=255)
    readback_address: str | None = Field(None, max_length=255)
    config_address: str | None = Field(None, max_length=255)
    setpoint_value: dict[str, Any] | None = None
    readback_value: dict[str, Any] | None = None
    status: int | None = None
    severity: int | None = None
    timestamp: datetime | None = None


class SnapshotBundleDTO(CanopyModel):
    """A snapshot with all its values, as one instance exports it for another to import.

    ``GET /snapshots/{id}/export`` writes it, typically on a history mirror;
    ``POST /snapshots/import`` reads it on the primary. ``source_instance`` is the
    exporter's ``CANOPY_INSTANCE``. ``facility`` is the snapshot's facility slug: slugs
    don't change and ``core.facility`` replicates, so it names the same facility on the
    mirror and its primary.
    """

    format: Literal["canopy.squirrel.snapshot-bundle"]
    version: Literal[1]
    source_instance: str | None = None
    id: UUID
    title: str = Field(..., min_length=1, max_length=255)
    description: str | None = None
    created_date: datetime
    created_by: str | None = Field(None, max_length=255)
    facility: str | None = Field(None, max_length=64)
    values: list[BundleValueDTO] = []


class SkippedValueDTO(CanopyModel):
    """A bundle value that wasn't imported, because no PV here has its ``pv_id``."""

    pv_id: str
    pv_name: str


class SnapshotImportResultDTO(CanopyModel):
    """The snapshot an import created, and the bundle values it left out."""

    snapshot: SnapshotSummaryDTO
    skipped: list[SkippedValueDTO] = []


class RestoreRequestDTO(CanopyModel):
    """Request to restore PV values from a snapshot (``pv_ids=None`` restores all)."""

    pv_ids: list[str] | None = None


class RestoreResultDTO(CanopyModel):
    """Result of a restore operation.

    ``failures`` rows are ``{pvId, pvName, error, phase}``, where ``phase`` names the
    stage that failed: ``"Setpoints"`` for a primary write or a hook-derivation error,
    otherwise the class name of the restore hook whose write failed; later hooks skip
    that PV. Class names map to the ``CANOPY_SQUIRREL__RESTORE_HOOKS`` provider keys as
    ``MagnetTrimHook`` → ``magnet_trim`` and ``WriteToConHook`` → ``write_to_con``.
    """

    total_pvs: int
    success_count: int
    failure_count: int
    failures: list[dict] = []


class ComparisonResultDTO(CanopyModel):
    """Comparison between two stored snapshots.

    ``differences`` rows are ``{pvId, pvName, value1, value2, withinTolerance}``.
    """

    snapshot1_id: str
    snapshot2_id: str
    differences: list[dict] = []
    match_count: int
    difference_count: int
