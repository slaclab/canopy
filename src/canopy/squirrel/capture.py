"""Capture on a history head, saved on the arm that owns the facility.

A head reads EPICS for every facility through its read-only gateway, but writes
only its own rows. A snapshot of an arm's facility is therefore taken here, with an
id and time the head assigns, and sent to that arm's captured import
(``POST /squirrel/v1/snapshots/import?mode=captured``) as one bundle. The arm saves
it under that id, and replication brings it back to the head with the same id.

``capture_target`` decides where a capture is saved, at the route, before anything
is queued; ``deliver_capture`` sends a bundle and waits for it to come back.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import asyncpg

from canopy.core.config import get_settings
from canopy.core.errors import NotOwner, PrimaryUnavailable
from canopy.core.ownership import local_instance
from canopy.core.forwarding.client import (
    Caller,
    ArmRefused,
    send_to_arm,
    forward_target,
    wait_until_applied,
)
from canopy.core.forwarding.config import get_forward_config
from canopy.core.forwarding.context import COMMIT_LSN_HEADER
from canopy.squirrel.schemas.snapshot import SnapshotBundleDTO, SnapshotImportResultDTO

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CaptureTarget:
    """Where a capture is saved: here (``arm`` is ``None``), or on the forwardable arm ``arm``."""

    arm: str | None = None


def capture_target(facility_owner: str | None, requested: str | None, what: str) -> CaptureTarget:
    """Where a snapshot of a facility owned by ``facility_owner`` is saved.

    ``requested`` is the ``?owner=`` the caller named. The owner is the facility's,
    else the requested one, else this instance's. A head saves an arm's snapshot on
    that arm when the head forwards there.

    Raises
    ------
    NotOwner
        When ``requested`` contradicts the facility's owner, or the owner is another
        instance this one can't save for: any other instance on an arm, and on a head
        an arm without forward settings or a name that isn't an arm.
    """
    owner = facility_owner if facility_owner is not None else (requested or local_instance())
    if requested is not None and requested != owner:
        raise NotOwner(f"{what} is owned by '{owner}', not '{requested}'")
    if owner == local_instance():
        return CaptureTarget()
    if get_settings().role == "mirror" and forward_target(owner) is not None:
        return CaptureTarget(arm=owner)
    raise NotOwner(f"{what} is owned by '{owner}'; write it there")


async def deliver_capture(arm: str, bundle: SnapshotBundleDTO, caller: Caller, *, strict: bool = False) -> dict:
    """Save ``bundle`` through ``arm``'s captured import, then wait for it to come back here.

    The arm keeps the bundle's id and time and records ``caller`` as the author. A
    resend of a saved bundle saves nothing and answers the same, so this is safe to
    call again after a timeout. ``strict`` makes the arm refuse a bundle it would
    save only in part.

    The wait ends when this instance's subscription to the arm reaches the position
    the arm answered with (``X-Canopy-Commit-LSN``), or as soon as the snapshot is
    here: on a resend the arm's position is its current one, which unpublished WAL
    can keep ahead of anything that replicates. It is bounded by
    ``CANOPY_FORWARD__TIMEOUT_S``; a snapshot that hasn't come back by then is still
    saved on the arm, and the result is the same.

    Returns
    -------
    dict
        The job result: ``{"type": "snapshot", "id", "arm"}``, plus ``skipped`` (the
        ``{pvId, pvName}`` values the arm left out) when there are any.

    Raises
    ------
    PrimaryUnavailable
        502: the arm couldn't be reached, or has no forward settings here; nothing was sent.
    PrimaryTimeout
        504: the request went out and no answer came back; it may have been saved.
    ArmRefused
        The arm answered with any other status than 2xx: 4xx for a bundle it won't
        save, 5xx when it failed.
    """
    target = forward_target(arm)
    if target is None:
        raise PrimaryUnavailable(f"Arm '{arm}' has no forward settings here; nothing was sent to it")
    config = get_forward_config()
    answer = await send_to_arm(
        target,
        "POST",
        "/squirrel/v1/snapshots/import",
        caller=caller,
        query="mode=captured" + ("&strict=true" if strict else ""),
        body=bundle.model_dump_json(by_alias=True).encode(),
        headers={"content-type": "application/json"},
        config=config,
    )
    if not 200 <= answer.status_code < 300:
        raise ArmRefused.from_answer(arm, answer)
    result = SnapshotImportResultDTO.model_validate(answer.json())
    snapshot_id = str(bundle.id)

    async def is_here(conn: asyncpg.Connection) -> bool:
        return bool(await conn.fetchval("SELECT 1 FROM squirrel.snapshot WHERE id = $1::uuid", snapshot_id))

    commit_lsn = answer.headers.get(COMMIT_LSN_HEADER, "")
    if not await wait_until_applied(commit_lsn, config.timeout_s, subscription=target.subscription, until=is_here):
        logger.warning(
            "Snapshot %s is saved on arm %s but hadn't replicated here within %.0fs",
            snapshot_id,
            arm,
            config.timeout_s,
        )
    logger.info(
        "Snapshot %s captured here for %s, %s on arm %s",
        snapshot_id,
        caller.on_behalf_of,
        "saved" if result.created else "already saved",
        arm,
    )
    outcome: dict = {"type": "snapshot", "id": snapshot_id, "arm": arm}
    if result.skipped:
        outcome["skipped"] = [value.model_dump(mode="json", by_alias=True) for value in result.skipped]
    return outcome
