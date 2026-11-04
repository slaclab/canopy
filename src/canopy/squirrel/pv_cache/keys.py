"""Redis keys and the entry format shared by the PV cache's writer and readers.

The elected ``canopy-worker`` writes; snapshot create and the health endpoint read.
Each cached value is stored as ``{"gen": <lease generation>, "cachedAt": <unix seconds>,
"pv": <PVValue>}`` so a reader gets back exactly the ``PVValue`` a live
``connector.get()`` would return, and can tell which leader's lease wrote it. The
leader's status report is a `PVCacheReport`, and `untrusted_reason` is the one check
of it that snapshot create and the health endpoint share.
"""

from __future__ import annotations

import json
from typing import Any

from pydantic import ConfigDict, ValidationError

from canopy.contracts.pv import PVValue
from canopy.contracts.base import CanopyModel

# Hash: PV address (as registered, protocol prefix included) -> JSON entry.
VALUES_KEY = "canopy:squirrel:pv_cache:values"
# String: the leader's instance id, with a lease TTL.
LEADER_KEY = "canopy:squirrel:pv_cache:leader"
# String: the leader's JSON report, rewritten on every flush (its heartbeat).
STATUS_KEY = "canopy:squirrel:pv_cache:status"
# How long a report outlives its last write: long enough for /health/pv-cache to show
# who led last and when, and short of keeping it forever once the cache is turned off.
STATUS_TTL_MS = 60 * 60 * 1000

# Entries per HSET / HMGET call, so a full 40k-PV warm-up isn't one multi-MB command.
BATCH_SIZE = 2000


def encode_entry(value: PVValue, cached_at: float, generation: str | None) -> str:
    return json.dumps({"gen": generation, "cachedAt": cached_at, "pv": value.model_dump(mode="json")})


def decode_entry(raw: bytes | str) -> tuple[str | None, PVValue]:
    """The generation that wrote the entry, and its value."""
    entry = json.loads(raw)
    return entry.get("gen"), PVValue.model_validate(entry["pv"])


class PVCacheReport(CanopyModel):
    """The leader's status report, rewritten on every flush (camelCase on the wire).

    ``heartbeat_at`` is the flush time, or 0 once the leader has stepped down. The
    counts are described on `PVCacheStatusDTO`.
    """

    model_config = ConfigDict(allow_inf_nan=False)

    instance: str | None = None
    host: str | None = None
    generation: str | None = None
    leader_since: float | None = None
    heartbeat_at: float
    configured: int | None = None
    monitored: int | None = None
    connected: int | None = None
    seen: int | None = None
    warm: bool | None = None


class UnreadableStatus(ValueError):
    """``STATUS_KEY`` holds something that isn't a leader's report."""


async def read_status(redis: Any) -> PVCacheReport | None:
    """The leader's last report, or ``None`` when there is none.

    Raises `UnreadableStatus` when the key holds anything else, so a caller can fall
    back as it would with no report instead of failing on it.
    """
    raw = await redis.get(STATUS_KEY)
    if raw is None:
        return None
    try:
        return PVCacheReport.model_validate_json(raw)
    except ValidationError as exc:
        raise UnreadableStatus(f"the PV cache report is unreadable ({exc.error_count()} invalid field(s))") from exc


def untrusted_reason(report: PVCacheReport, now: float, max_age_s: float) -> str | None:
    """Why the cache this report describes mustn't be read, or ``None`` when it may.

    Snapshot create skips the whole cache for any of these; the health endpoint
    reports the same reason as ``down``.
    """
    if report.heartbeat_at == 0:
        return "the last PV cache leader stepped down and no worker has taken over"
    age = now - report.heartbeat_at
    if age > max_age_s:
        return f"the PV cache heartbeat is {age:.0f}s old (limit {max_age_s:g}s)"
    if not report.generation:
        return "the PV cache report names no leader generation"
    return None
