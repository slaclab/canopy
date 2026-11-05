from typing import Literal
from datetime import datetime

from canopy.contracts.base import CanopyModel

RestoreHooksState = Literal["disabled", "active", "unknown", "partial"]
PVCacheState = Literal["disabled", "down", "degraded", "ok"]


class RestoreHooksViewDTO(CanopyModel):
    """What one process's ``CANOPY_SQUIRREL__RESTORE_HOOKS`` resolves to.

    ``status`` is ``"disabled"`` when nothing is configured, ``"active"`` when every
    configured provider key is registered, ``"unknown"`` when none is, and
    ``"partial"`` when only some are. ``loaded`` holds the keys that will actually run,
    ``unknown`` the ones that match no provider, and ``warnings`` configuration
    problems that change nothing but deserve attention.
    """

    status: RestoreHooksState
    configured: list[str] = []
    loaded: list[str] = []
    unknown: list[str] = []
    warnings: list[str] = []


class WorkerRestoreHooksViewDTO(RestoreHooksViewDTO):
    """The worker's view, as it published it when it last started.

    ``reported_at`` and ``host`` identify that start, so a report left behind by a
    worker that is no longer running can be recognised.
    """

    reported_at: datetime
    host: str


class RestoreHooksStatusDTO(CanopyModel):
    """Restore hooks as the API and the worker each resolved them.

    Restores run in the worker, so ``worker`` is the view that decides what a restore
    does; ``api`` is this process's. ``worker`` is ``None`` until a worker has started
    and published its report, or when Redis could not be read (``worker_error`` says
    which). ``matches_api`` is ``None`` without a worker report, and otherwise says
    whether both processes load the same hooks. ``available`` lists every registered
    provider key.
    """

    available: list[str] = []
    api: RestoreHooksViewDTO
    worker: WorkerRestoreHooksViewDTO | None = None
    worker_error: str | None = None
    matches_api: bool | None = None


class PVCacheStatusDTO(CanopyModel):
    """The worker-side PV cache that cached snapshots (``useCache=true``) read from.

    ``status`` is ``"disabled"`` when this API process has the cache disabled and no
    worker is leading it: no report, or the one a leader leaves on stepping down.
    ``"down"`` when snapshot create won't use the cache otherwise (an unreadable
    report, a heartbeat older than the configured maximum, Redis unreachable, or with
    the API enabled no report or a leader that stepped down: ``error`` says which).
    A crashed leader's stale report reads ``"down"`` whatever this process's flag
    until it expires, an hour after its last write. ``"degraded"`` while the leader
    is still hearing from its PVs after taking over (``warm`` false) or when some
    monitored PVs are disconnected; ``"ok"`` otherwise. Snapshots still work in every
    state: what the cache can't vouch for is read live.

    ``configured`` counts the registered setpoint/readback addresses, ``monitored``
    the ones with an open monitor, ``connected`` those currently connected, ``seen``
    those that delivered a value since ``leaderSince``, and ``cached`` the entries in
    Redis. ``enabled`` is this API process's ``CANOPY_SQUIRREL__PV_CACHE__ENABLED``.
    """

    status: PVCacheState
    enabled: bool
    host: str | None = None
    instance: str | None = None
    leader_since: datetime | None = None
    heartbeat_at: datetime | None = None
    heartbeat_age_s: float | None = None
    configured: int | None = None
    monitored: int | None = None
    connected: int | None = None
    seen: int | None = None
    warm: bool | None = None
    cached: int | None = None
    error: str | None = None
