"""The PV cache reader used by snapshot create.

The cache is an optimisation, never a source of truth on its own: a value is served
from it only when the monitor that wrote it is demonstrably alive and was watching
that address. Everything else comes back as a miss, and snapshot create reads it
live exactly as it would without the cache.
"""

from __future__ import annotations

import time
import logging
from typing import Any
from dataclasses import field, dataclass
from collections.abc import Callable

from redis.exceptions import RedisError

from canopy.squirrel.pv_cache.keys import (
    BATCH_SIZE,
    VALUES_KEY,
    read_status,
    decode_entry,
)
from canopy.squirrel.adapters.epics_io import EpicsValue, _to_epics_value

logger = logging.getLogger(__name__)


@dataclass
class CacheLookup:
    """``hits`` are trusted cached values; ``misses`` must be read live.

    ``skipped`` says why the whole cache was passed over (no monitor report, a stale
    heartbeat, Redis unreachable), and is ``None`` when it was consulted.
    """

    hits: dict[str, EpicsValue] = field(default_factory=dict)
    misses: list[str] = field(default_factory=list)
    skipped: str | None = None


class PVCacheReader:
    def __init__(self, redis: Any, *, heartbeat_max_age_s: float, clock: Callable[[], float] = time.time) -> None:
        self._redis = redis
        self._heartbeat_max_age_s = heartbeat_max_age_s
        self._clock = clock

    async def lookup(self, addresses: list[str]) -> CacheLookup:
        """Split ``addresses`` into cache hits and the misses to read live.

        An entry is a hit only when it is connected and was written by the current
        leader (``cachedAt >= leaderSince``): an entry left by an earlier leader may
        describe a channel nobody was watching since. The whole cache is skipped when
        no monitor has reported or its heartbeat is older than ``heartbeat_max_age_s``.
        """
        try:
            status = await read_status(self._redis)
        except RedisError as exc:
            return CacheLookup(misses=list(addresses), skipped=f"Redis unreachable: {exc}")
        if status is None:
            return CacheLookup(misses=list(addresses), skipped="no worker has reported a PV cache")
        age = self._clock() - float(status.get("heartbeatAt") or 0)
        if age > self._heartbeat_max_age_s:
            return CacheLookup(misses=list(addresses), skipped=f"the PV cache heartbeat is {age:.0f}s old")
        leader_since = float(status.get("leaderSince") or 0)

        result = CacheLookup()
        try:
            for start in range(0, len(addresses), BATCH_SIZE):
                chunk = addresses[start : start + BATCH_SIZE]
                raws = await self._redis.hmget(VALUES_KEY, chunk)
                for address, raw in zip(chunk, raws, strict=True):
                    value = self._trusted(raw, leader_since)
                    if value is None:
                        result.misses.append(address)
                    else:
                        result.hits[address] = value
        except RedisError as exc:
            return CacheLookup(misses=list(addresses), skipped=f"Redis unreachable: {exc}")
        return result

    @staticmethod
    def _trusted(raw: bytes | str | None, leader_since: float) -> EpicsValue | None:
        if raw is None:
            return None
        try:
            cached_at, pv = decode_entry(raw)
        except (ValueError, KeyError, TypeError):
            logger.warning("Ignoring an unreadable PV cache entry")
            return None
        if cached_at < leader_since or not pv.connected:
            return None
        return _to_epics_value(pv)
