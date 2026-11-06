"""Snapshot endpoints.

Create and restore are long-running EPICS I/O, so they enqueue an Arq job and return a
``JobCreatedDTO`` for the client to poll at ``/jobs/v1/{id}`` — the worker holds the EPICS
connector. Everything else (list / get / update / delete / compare, and export / import of a
snapshot bundle) runs inline against the request session.
"""

import logging
from typing import Literal, Annotated

from fastapi import Query, Security, APIRouter

from canopy.jobs import gateway as jobs_gateway
from canopy.core.errors import (
    BadRequest,
    FacilityNotFound,
    ForwardedWriteRefused,
    InsufficientFacilityAccess,
)
from canopy.core.security import (
    ActorDep,
    WriteKeyDep,
    forwarded_for,
    require_read_access,
    require_write_access,
    report_commit_position,
)
from canopy.contracts.jobs import JobCreatedDTO
from canopy.core.ownership import OwnerQuery, require_create_owner
from canopy.core.facilities import gateway as facilities_gateway
from canopy.core.time_parse import DateTimeBound
from canopy.core.validation import require_uuid
from canopy.squirrel.errors import SnapshotNotFound
from canopy.contracts.pagination import PagedResult
from canopy.squirrel.dependencies import FacilityScopeDep, SnapshotServiceDep
from canopy.squirrel.schemas.snapshot import (
    SnapshotDTO,
    NewSnapshotDTO,
    RestoreRequestDTO,
    SnapshotBundleDTO,
    UpdateSnapshotDTO,
    SnapshotSummaryDTO,
    ComparisonResultDTO,
    SnapshotImportResultDTO,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/snapshots", tags=["Snapshots"])


@router.get("", dependencies=[Security(require_read_access)])
async def list_snapshots(
    service: SnapshotServiceDep,
    title: str | None = Query(None),
    tags: list[str] = Query(default=[], description="Filter by tag IDs"),
    start: Annotated[
        DateTimeBound,
        Query(
            description=(
                "Only snapshots created at or after this ISO-8601 date and time. Send a UTC offset "
                "(Z or -07:00); a value without one is read as UTC. A bare date is rejected."
            )
        ),
    ] = None,
    end: Annotated[
        DateTimeBound,
        Query(
            description=(
                "Only snapshots created at or before this ISO-8601 date and time. Send a UTC offset "
                "(Z or -07:00); a value without one is read as UTC. A bare date is rejected."
            )
        ),
    ] = None,
    pageSize: int = Query(100, ge=1, le=1000),
    continuationToken: str | None = Query(None, description="Token from the previous page; omit for the first"),
    facility: str | None = Query(None, description="Facility slug — includes descendants; 404 if unknown"),
) -> PagedResult[SnapshotSummaryDTO]:
    """List snapshots newest first, optionally filtered by title, tag IDs, creation time, and/or facility.

    Paged by keyset: each page continues from the sort position the previous one ended on
    rather than skipping a count of rows, so snapshots created or deleted while a client
    pages cannot make it see a row twice or miss one. Send the same filters with every
    page's request.

    Parameters
    ----------
    service : SnapshotServiceDep
        Snapshot domain service resolved from the request.
    title : str | None
        Case-insensitive substring match against the snapshot title.
    tags : list[str]
        Tag IDs; a snapshot matches when it holds a PV carrying any of them.
    start : datetime | None
        Lower bound on creation time, inclusive. A value without an offset is read as UTC,
        and a bare ``YYYY-MM-DD`` is refused with a 422.
    end : datetime | None
        Upper bound on creation time, inclusive. A value without an offset is read as UTC,
        and a bare ``YYYY-MM-DD`` is refused with a 422.
    pageSize : int
        Page size, 1–1000 (default 100).
    continuationToken : str | None
        The previous page's ``continuationToken``; omit it for the first page.
    facility : str | None
        A facility slug to filter by — includes descendants; 404 if unknown.

    Returns
    -------
    PagedResult[SnapshotSummaryDTO]
        The page's summaries, newest first; ``continuationToken`` for the next page, which
        is ``null`` on the last page; and ``totalCount``, the number of matching snapshots.

    Raises
    ------
    BadRequest
        When both bounds are given and ``start`` is after ``end``, or the continuation
        token is malformed.
    FacilityNotFound
        When ``facility`` names a slug that doesn't exist.
    """
    return await service.list_snapshots(
        title=title,
        tag_ids=tags or None,
        start=start,
        end=end,
        page_size=pageSize,
        continuation_token=continuationToken,
        facility=facility,
    )


@router.post("", dependencies=[Security(require_write_access)])
async def create_snapshot(
    data: NewSnapshotDTO,
    actor: ActorDep,
    key_scope: FacilityScopeDep,
    use_cache: Annotated[
        bool,
        Query(
            alias="useCache",
            description=(
                "Take values from the worker's PV cache where it can vouch for them and read only the rest "
                "live from EPICS. The cache must be enabled on the worker (CANOPY_SQUIRREL__PV_CACHE__ENABLED); "
                "without a live cache every PV is read live, as with useCache=false. Cached values are what "
                "each PV's monitor last posted: alarm changes are posted at once, but a value change smaller "
                "than the record's monitor deadband (MDEL) isn't, so a cached value can trail a live read by "
                "up to that deadband, with the timestamp of the value it holds."
            ),
        ),
    ] = False,
    owner: OwnerQuery = None,
) -> JobCreatedDTO:
    """Enqueue a snapshot of the facility's PVs (every PV this instance owns if no
    facility is given). The worker reads EPICS and persists it. The snapshot is owned by
    its facility's owner, else by this instance; ``owner`` accepts only this instance's name.

    ``createdBy`` records the key's name, or the user a history head forwarded the
    request for.
    """
    # The facility gate has to run *here*, not in the service: nothing of the caller's
    # identity crosses the Arq boundary, and a refused write must not become a queued job.
    # The owner check too, so a snapshot this instance wouldn't own is refused at once.
    facility_id = None
    facility_owner = None
    if data.facility is not None:
        facility_id = await facilities_gateway.id_by_slug(data.facility)
        if facility_id is None:
            raise FacilityNotFound(f"Facility '{data.facility}' not found")
        facility_owner = (await facilities_gateway.owners_by_id({facility_id})).get(facility_id)
    require_create_owner(facility_owner, owner, f"A snapshot of {data.facility or 'every PV'}")

    if key_scope is not None:
        # A facility-restricted key must name a facility it holds. Unlike mutating an
        # existing unassigned row, minting a *new* unscoped snapshot is refused — it
        # would create data the key could not otherwise have reached.
        if facility_id is None:
            raise InsufficientFacilityAccess("API key is scoped to specific facilities; 'facility' is required")
        if facility_id not in key_scope:
            raise InsufficientFacilityAccess(f"API key is not scoped to facility: {data.facility}")

    # use_cache goes to the job only when set, so a plain snapshot asks nothing new of the worker.
    extra = {"use_cache": True} if use_cache else {}
    job_id = await jobs_gateway.enqueue(
        "create_snapshot_task",
        title=data.title,
        description=data.description,
        facility_id=facility_id,
        created_by=actor,
        **extra,
    )
    return JobCreatedDTO(job_id=job_id, status_url=f"/jobs/v1/{job_id}")


@router.post("/import", dependencies=[Security(require_write_access)])
async def import_snapshot(
    bundle: SnapshotBundleDTO,
    service: SnapshotServiceDep,
    actor: ActorDep,
    key: WriteKeyDep,
    mode: Annotated[
        Literal["history", "captured"],
        Query(
            description=(
                "history: bring back a snapshot exported from a history copy, as a new snapshot. captured: save "
                "a snapshot a history head took for this instance, under its own id and time; for the head's "
                "service key only."
            ),
        ),
    ] = "history",
    strict: Annotated[
        bool, Query(description="Refuse, saving nothing, when any of the bundle's values would be left out.")
    ] = False,
) -> SnapshotImportResultDTO:
    """Save a snapshot from a bundle.

    **History** (the default) brings back a bundle exported by
    ``GET /snapshots/{id}/export``, typically from a history head, so it can be
    restored here. The new snapshot has a new id, today's creation time, a "Restored
    from history" title and ``importedFrom`` set to the bundle's id. ``createdBy`` stays
    the original snapshot's; who imported it (the key's name, or the user a head
    forwarded it for) is logged.

    **Captured** saves a snapshot a history head captured for one of this instance's
    facilities. Only a service key may send it, naming the user it was taken for in
    ``X-Canopy-On-Behalf-Of``, who becomes its author. It keeps the bundle's id, time,
    title and description, and is idempotent on the id: a resend answers with
    ``created`` false and saves nothing. The answer always carries
    ``X-Canopy-Commit-LSN``, so the head can wait for the snapshot to come back.

    Either way a value is kept only when its PV exists here, the key could restore it,
    and, for a bundle with a facility, the PV is assigned to that facility or one below
    it, as for a capture. A PV moved out of the facility since is therefore left out.
    Values left out are listed in ``skipped``; nothing is read from or written to EPICS.
    """
    if mode == "captured":
        if not key.is_service:
            raise ForwardedWriteRefused("Only a service key may import a captured snapshot")
        if forwarded_for() is None:
            raise ForwardedWriteRefused("A captured import names the user it was taken for in X-Canopy-On-Behalf-Of")
        report_commit_position()
        result = await service.import_bundle(bundle, captured=True, created_by=actor, strict=strict)
        logger.info(
            "Captured snapshot %s %s for %s", bundle.id, "saved" if result.created else "was already saved", actor
        )
        return result
    result = await service.import_bundle(bundle, strict=strict)
    logger.info("Snapshot %s imported as %s for %s", bundle.id, result.snapshot.id, actor)
    return result


@router.get("/{snapshot_id}", dependencies=[Security(require_read_access)])
async def get_snapshot(
    snapshot_id: str,
    service: SnapshotServiceDep,
    limit: int | None = Query(None, ge=1, description="Max PV values to return"),
    offset: int = Query(0, ge=0),
) -> SnapshotDTO:
    """Get one snapshot with its PV values (values optionally paginated)."""
    snapshot_id = require_uuid(snapshot_id, SnapshotNotFound, f"Snapshot {snapshot_id} not found")
    snapshot = await service.get_by_id(snapshot_id, limit=limit, offset=offset)
    if not snapshot:
        raise SnapshotNotFound(f"Snapshot {snapshot_id} not found")
    return snapshot


@router.get("/{snapshot_id}/export", dependencies=[Security(require_read_access)])
async def export_snapshot(snapshot_id: str, service: SnapshotServiceDep) -> SnapshotBundleDTO:
    """Export a snapshot with all its values, for ``POST /snapshots/import`` on another instance."""
    snapshot_id = require_uuid(snapshot_id, SnapshotNotFound, f"Snapshot {snapshot_id} not found")
    bundle = await service.export_bundle(snapshot_id)
    if bundle is None:
        raise SnapshotNotFound(f"Snapshot {snapshot_id} not found")
    return bundle


# PUT is kept for existing clients; both apply only the fields sent.
@router.patch("/{snapshot_id}", dependencies=[Security(require_write_access)])
@router.put("/{snapshot_id}", dependencies=[Security(require_write_access)])
async def update_snapshot(
    snapshot_id: str,
    data: UpdateSnapshotDTO,
    service: SnapshotServiceDep,
) -> SnapshotSummaryDTO:
    """Update a snapshot's title, description and/or ``retain`` flag (kept by pruning)."""
    snapshot_id = require_uuid(snapshot_id, SnapshotNotFound, f"Snapshot {snapshot_id} not found")
    snapshot = await service.update_snapshot_metadata(
        snapshot_id, title=data.title, description=data.description, retain=data.retain
    )
    if not snapshot:
        raise SnapshotNotFound(f"Snapshot {snapshot_id} not found")
    return snapshot


@router.delete("/{snapshot_id}", dependencies=[Security(require_write_access)])
async def delete_snapshot(snapshot_id: str, service: SnapshotServiceDep) -> bool:
    """Delete a snapshot: it is no longer listed or served, but its row and values are kept."""
    snapshot_id = require_uuid(snapshot_id, SnapshotNotFound, f"Snapshot {snapshot_id} not found")
    if not await service.delete_snapshot(snapshot_id):
        raise SnapshotNotFound(f"Snapshot {snapshot_id} not found")
    return True


@router.post("/{snapshot_id}/restore", dependencies=[Security(require_write_access)])
async def restore_snapshot(
    snapshot_id: str,
    service: SnapshotServiceDep,
    actor: ActorDep,
    request: RestoreRequestDTO | None = None,
) -> JobCreatedDTO:
    """Enqueue a restore of a snapshot's setpoints to EPICS (``pv_ids=None`` restores all)."""
    snapshot_id = require_uuid(snapshot_id, SnapshotNotFound, f"Snapshot {snapshot_id} not found")
    if not await service.snapshot_exists(snapshot_id):
        raise SnapshotNotFound(f"Snapshot {snapshot_id} not found")
    # Narrow the write set to the key's facilities here, pre-enqueue — the worker has no
    # caller identity to check against.
    pv_ids, skipped = await service.plan_restore(snapshot_id, request.pv_ids if request else None)
    # `None` restores everything; `[]` restores nothing and is refused, never enqueued.
    if pv_ids is not None and not pv_ids:
        if skipped:
            raise InsufficientFacilityAccess("API key is not scoped to any facility in this snapshot")
        raise BadRequest("Nothing to restore: none of the requested PVs are in this snapshot")
    job_id = await jobs_gateway.enqueue(
        "restore_snapshot_task", snapshot_id=snapshot_id, pv_ids=pv_ids, skipped_pv_ids=skipped
    )
    logger.info(
        "Restore of snapshot %s queued as job %s for %s (%s PVs)",
        snapshot_id,
        job_id,
        actor,
        "all" if pv_ids is None else len(pv_ids),
    )
    return JobCreatedDTO(job_id=job_id, status_url=f"/jobs/v1/{job_id}")


@router.get("/{snapshot_id1}/compare/{snapshot_id2}", dependencies=[Security(require_read_access)])
async def compare_snapshots(
    snapshot_id1: str,
    snapshot_id2: str,
    service: SnapshotServiceDep,
) -> ComparisonResultDTO:
    """Compare two stored snapshots value-by-value (within per-PV tolerance)."""
    snapshot_id1 = require_uuid(snapshot_id1, SnapshotNotFound, f"Snapshot {snapshot_id1} not found")
    snapshot_id2 = require_uuid(snapshot_id2, SnapshotNotFound, f"Snapshot {snapshot_id2} not found")
    return await service.compare_snapshots(snapshot_id1, snapshot_id2)
