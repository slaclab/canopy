"""The PV cache writer: one elected ``canopy-worker`` monitors every registered PV.

Every worker that has ``CANOPY_SQUIRREL__PV_CACHE__ENABLED`` set runs a monitor, but
only the one holding the Redis lease leads; the others retry for the lease. The
leader subscribes to every registered setpoint and readback address through the
shared ``canopy.epics`` connector, buffers the latest value per address, and writes
the buffer to Redis on a short cadence together with its status report, which doubles
as the heartbeat readers check before trusting the cache.

A leader that can't renew its lease (another worker took it, or Redis is unreachable)
closes its subscriptions and goes back to waiting, so two workers never write the
cache at once for longer than one lease.
"""

from __future__ import annotations

import json
import time
import uuid
import socket
import asyncio
import logging
from typing import Any
from collections.abc import Callable, Awaitable

from redis.exceptions import RedisError, WatchError

from canopy.core.db import session_scope
from canopy.epics.base import PVConnector, Subscription
from canopy.contracts.pv import PVValue
from canopy.squirrel.config import PVCacheConfig
from canopy.squirrel.pv_cache.keys import (
    BATCH_SIZE,
    LEADER_KEY,
    STATUS_KEY,
    VALUES_KEY,
    encode_entry,
)
from canopy.squirrel.data.repositories import PvRepository

logger = logging.getLogger(__name__)

AddressLoader = Callable[[], Awaitable[set[str]]]

# Consecutive failed lease renewals (Redis unreachable) after which the leader steps
# down, so a worker cut off from Redis doesn't keep monitoring as a phantom leader.
_MAX_RENEW_FAILURES = 3


async def load_pv_addresses() -> set[str]:
    """Every registered setpoint and readback address (config addresses aren't snapshotted)."""
    async with session_scope() as session:
        rows = await PvRepository(session).get_all_addresses()
    return {address for _id, setpoint, readback, _config in rows for address in (setpoint, readback) if address}


def _text(value: Any) -> str | None:
    if value is None:
        return None
    return value.decode() if isinstance(value, bytes) else str(value)


class PVCacheMonitor:
    """Keeps Redis's PV cache current while this worker holds the leader lease."""

    def __init__(
        self,
        connector: PVConnector,
        redis: Any,
        config: PVCacheConfig,
        load_addresses: AddressLoader | None = None,
        *,
        instance_id: str | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._connector = connector
        self._redis = redis
        self._config = config
        # Defaults to the registered PVs in the database.
        self._load_addresses = load_addresses or load_pv_addresses
        self._instance = instance_id or uuid.uuid4().hex[:12]
        self._host = socket.gethostname()
        self._clock = clock
        self._lease_ms = int(config.lease_ttl_s * 1000)

        self._stopping = asyncio.Event()
        self._task: asyncio.Task | None = None

        # Leader-only state, cleared on every step-down.
        self._leader_since: float | None = None
        self._wanted: set[str] = set()
        self._subs: dict[str, Subscription] = {}
        self._pending: dict[str, PVValue] = {}
        self._connected: dict[str, bool] = {}
        self._seen: set[str] = set()
        self._configured = 0

    @property
    def instance_id(self) -> str:
        return self._instance

    @property
    def is_leader(self) -> bool:
        return self._leader_since is not None

    # --- lifecycle ------------------------------------------------------- #

    def start(self) -> None:
        """Run the follower/leader loop as a background task until `stop()`."""
        if self._task is None:
            self._task = asyncio.create_task(self.run(), name="squirrel-pv-cache")

    async def stop(self) -> None:
        self._stopping.set()
        if self._task is not None:
            await self._task
            self._task = None

    async def run(self) -> None:
        logger.info("PV cache monitor %s started; waiting for the leader lease", self._instance)
        while not self._stopping.is_set():
            try:
                acquired = await self._redis.set(LEADER_KEY, self._instance, nx=True, px=self._lease_ms)
            except RedisError as exc:
                logger.warning("PV cache monitor %s could not reach Redis: %s", self._instance, exc)
                acquired = False
            if acquired:
                try:
                    await self._lead()
                finally:
                    await self._step_down()
            else:
                await self._wait(self._config.lease_ttl_s / 2)
        logger.info("PV cache monitor %s stopped", self._instance)

    async def _wait(self, seconds: float) -> None:
        """Sleep, but wake at once on `stop()`."""
        try:
            await asyncio.wait_for(self._stopping.wait(), timeout=seconds)
        except TimeoutError:
            pass

    async def _lead(self) -> None:
        self._leader_since = self._clock()
        logger.info("PV cache monitor %s is the leader", self._instance)
        loops = [
            asyncio.create_task(self._renew_loop()),
            asyncio.create_task(self._reconcile_loop()),
            asyncio.create_task(self._flush_loop()),
        ]
        stop = asyncio.create_task(self._stopping.wait())
        # The renew loop is the only one that returns: when the lease is lost.
        await asyncio.wait([*loops, stop], return_when=asyncio.FIRST_COMPLETED)
        for task in [*loops, stop]:
            task.cancel()
        await asyncio.gather(*loops, stop, return_exceptions=True)

    async def _step_down(self) -> None:
        # Readers stop trusting the cache at once, rather than after heartbeat_max_age_s.
        final_report = {**self.status_report(self._clock()), "heartbeatAt": 0}
        subs = list(self._subs.values())
        self._wanted.clear()
        self._subs.clear()
        self._pending.clear()
        self._connected.clear()
        self._seen.clear()
        self._leader_since = None
        await asyncio.gather(*(sub.close() for sub in subs), return_exceptions=True)
        try:
            await self._release_lease(final_report)
        except RedisError as exc:
            logger.warning("PV cache monitor %s could not release the lease: %s", self._instance, exc)
        logger.info("PV cache monitor %s stepped down (closed %d subscriptions)", self._instance, len(subs))

    # --- lease ----------------------------------------------------------- #

    async def _renew_loop(self) -> None:
        failures = 0
        while True:
            await asyncio.sleep(self._config.lease_ttl_s / 3)
            try:
                renewed = await self._renew_lease()
            except RedisError as exc:
                failures += 1
                logger.warning("PV cache lease renewal failed (%d/%d): %s", failures, _MAX_RENEW_FAILURES, exc)
                if failures >= _MAX_RENEW_FAILURES:
                    return
                continue
            if not renewed:
                logger.warning("PV cache monitor %s lost the leader lease", self._instance)
                return
            failures = 0

    async def _renew_lease(self) -> bool:
        """Extend the lease if this instance still holds it (WATCH/MULTI, so no Lua needed)."""
        async with self._redis.pipeline(transaction=True) as pipe:
            try:
                await pipe.watch(LEADER_KEY)
                if _text(await pipe.get(LEADER_KEY)) != self._instance:
                    return False
                pipe.multi()
                pipe.pexpire(LEADER_KEY, self._lease_ms)
                await pipe.execute()
                return True
            except WatchError:
                return False

    async def _release_lease(self, final_report: dict[str, Any]) -> None:
        """Give up the lease and mark the cache unled, only if this instance still holds it."""
        async with self._redis.pipeline(transaction=True) as pipe:
            try:
                await pipe.watch(LEADER_KEY)
                if _text(await pipe.get(LEADER_KEY)) != self._instance:
                    return
                pipe.multi()
                pipe.delete(LEADER_KEY)
                pipe.set(STATUS_KEY, json.dumps(final_report))
                await pipe.execute()
            except WatchError:
                pass

    # --- subscriptions --------------------------------------------------- #

    async def _reconcile_loop(self) -> None:
        while True:
            try:
                await self.reconcile()
            except Exception:
                logger.exception("PV cache reconcile failed; keeping the current subscriptions")
            await asyncio.sleep(self._config.reconcile_interval_s)

    async def reconcile(self) -> None:
        """Match the subscriptions to the registered addresses."""
        wanted = await self._load_addresses()
        self._configured = len(wanted)
        added = sorted(wanted - self._wanted)
        removed = sorted(self._wanted - wanted)
        self._wanted = set(wanted)

        for address in removed:
            # Out of `_wanted` first, so an event already queued for it is dropped;
            # the hash entry goes only once the subscription is closed.
            sub = self._subs.pop(address, None)
            if sub is not None:
                try:
                    await sub.close()
                except Exception:
                    logger.exception("Closing the PV cache monitor for %s failed", address)
            self._pending.pop(address, None)
            self._connected.pop(address, None)
            self._seen.discard(address)
        for start in range(0, len(removed), BATCH_SIZE):
            await self._redis.hdel(VALUES_KEY, *removed[start : start + BATCH_SIZE])

        sem = asyncio.Semaphore(max(1, self._config.subscribe_concurrency))

        async def subscribe(address: str) -> None:
            async with sem:
                try:
                    sub = await self._connector.subscribe(address, lambda value: self._on_event(address, value))
                except Exception:
                    logger.exception("PV cache could not monitor %s; it will be read live", address)
                    return
                if address in self._wanted:
                    self._subs[address] = sub
                else:  # removed by a newer reconcile while subscribing
                    await sub.close()

        await asyncio.gather(*(subscribe(address) for address in added))
        if added or removed:
            logger.info(
                "PV cache now monitors %d of %d addresses (+%d, -%d)",
                len(self._subs),
                self._configured,
                len(added),
                len(removed),
            )

    def _on_event(self, address: str, value: PVValue) -> None:
        # Called on the event loop for every connect, update and disconnect.
        if address not in self._wanted:
            return
        self._pending[address] = value
        self._connected[address] = value.connected
        self._seen.add(address)

    # --- flush ----------------------------------------------------------- #

    async def _flush_loop(self) -> None:
        while True:
            await asyncio.sleep(self._config.flush_interval_s)
            try:
                await self.flush()
            except RedisError as exc:
                logger.warning("PV cache flush failed; retrying next cycle: %s", exc)
            except Exception:
                # Anything else is a bug, but it mustn't end the loop: the leader would
                # step down and re-elect itself over and over.
                logger.exception("PV cache flush failed; retrying next cycle")

    async def flush(self) -> None:
        """Write buffered values, then the status report (the heartbeat)."""
        now = self._clock()
        pending, self._pending = self._pending, {}
        items = list(pending.items())
        try:
            for start in range(0, len(items), BATCH_SIZE):
                chunk = items[start : start + BATCH_SIZE]
                await self._redis.hset(VALUES_KEY, mapping={addr: encode_entry(value, now) for addr, value in chunk})
        except RedisError:
            # Put back whatever newer events haven't superseded, so a dropped
            # disconnect isn't lost and the cache doesn't keep serving the old value.
            for address, value in items:
                if address in self._wanted:
                    self._pending.setdefault(address, value)
            raise
        await self._redis.set(STATUS_KEY, json.dumps(self.status_report(now)))

    def status_report(self, now: float) -> dict[str, Any]:
        monitored = len(self._subs)
        seen = len(self._seen & self._subs.keys())
        return {
            "instance": self._instance,
            "host": self._host,
            "leaderSince": self._leader_since,
            "heartbeatAt": now,
            "configured": self._configured,
            "monitored": monitored,
            "connected": sum(1 for address in self._subs if self._connected.get(address)),
            "seen": seen,
            "warm": monitored > 0 and seen >= monitored,
        }
