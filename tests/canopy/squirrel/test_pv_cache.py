"""The worker-side PV cache behind cached snapshots: leader election, the monitor's
writes, the reader's trust rules, and the worker wiring. All against fakeredis and a
fake connector whose monitors fire only when a test says so."""

import json
import time
import asyncio

import pytest
import fakeredis.aioredis

from canopy import worker
from tests.conftest import wait_for
from canopy.contracts.pv import PVValue
from canopy.squirrel.config import PVCacheConfig, SquirrelConfig
from canopy.contracts.sample import AlarmSeverity
from canopy.squirrel.pv_cache import (
    STATUS_KEY,
    VALUES_KEY,
    PVCacheReader,
    PVCacheMonitor,
    read_status,
)
from canopy.squirrel.pv_cache.keys import LEADER_KEY, encode_entry

FAST = PVCacheConfig(
    enabled=True, lease_ttl_s=0.3, flush_interval_s=0.02, reconcile_interval_s=0.05, heartbeat_max_age_s=5
)


def _pv(address: str, value=1.5, *, connected=True, ts_ns=1_700_000_000_000_000_000) -> PVValue:
    return PVValue(
        pv=address,
        value=value if connected else None,
        severity=AlarmSeverity.MINOR if connected else AlarmSeverity.INVALID,
        alarm_status=4 if connected else None,
        ts_ns=ts_ns,
        connected=connected,
    )


class _Sub:
    def __init__(self, connector: "FakeConnector", address: str) -> None:
        self._connector = connector
        self._address = address

    async def close(self) -> None:
        self._connector.callbacks.pop(self._address, None)
        self._connector.closed.append(self._address)


class FakeConnector:
    """Monitors that fire only on `emit`, so tests control exactly what the cache hears."""

    def __init__(self) -> None:
        self.callbacks: dict = {}
        self.closed: list[str] = []

    async def subscribe(self, pv, on_event):
        self.callbacks[pv] = on_event
        return _Sub(self, pv)

    def emit(self, value: PVValue) -> None:
        self.callbacks[value.pv](value)


def _loader(addresses: set[str]):
    async def load() -> set[str]:
        return set(addresses)

    return load


async def _leader(redis, addresses: set[str], connector=None, **kwargs) -> tuple[PVCacheMonitor, FakeConnector]:
    connector = connector or FakeConnector()
    monitor = PVCacheMonitor(connector, redis, FAST, _loader(addresses), **kwargs)
    monitor.start()
    assert await wait_for(lambda: monitor.is_leader and set(connector.callbacks) == addresses)
    return monitor, connector


# --- leader election -------------------------------------------------------- #


async def test_only_one_of_two_monitors_leads():
    redis = fakeredis.aioredis.FakeRedis()
    first = PVCacheMonitor(FakeConnector(), redis, FAST, _loader({"A"}), instance_id="one")
    second = PVCacheMonitor(FakeConnector(), redis, FAST, _loader({"A"}), instance_id="two")
    first.start()
    second.start()
    try:
        assert await wait_for(lambda: first.is_leader or second.is_leader)
        await asyncio.sleep(FAST.lease_ttl_s)  # past a renewal or two
        assert first.is_leader != second.is_leader
    finally:
        await first.stop()
        await second.stop()


async def test_a_leader_that_loses_its_lease_closes_its_monitors():
    redis = fakeredis.aioredis.FakeRedis()
    monitor, connector = await _leader(redis, {"A", "B"})
    try:
        await redis.set(LEADER_KEY, "someone-else")
        assert await wait_for(lambda: not monitor.is_leader)
        # Leadership drops before the monitors finish closing, so wait for those too.
        assert await wait_for(lambda: sorted(connector.closed) == ["A", "B"])
        # It doesn't clobber the new leader's report on the way out.
        assert (await redis.get(LEADER_KEY)) == b"someone-else"
    finally:
        await monitor.stop()


async def test_a_graceful_stop_releases_the_lease_and_marks_the_cache_unled():
    redis = fakeredis.aioredis.FakeRedis()
    monitor, _ = await _leader(redis, {"A"})
    await monitor.stop()

    assert await redis.get(LEADER_KEY) is None
    assert (await read_status(redis))["heartbeatAt"] == 0


# --- what the leader writes ---------------------------------------------------- #


async def test_flush_writes_values_disconnects_and_a_heartbeat():
    redis = fakeredis.aioredis.FakeRedis()
    monitor, connector = await _leader(redis, {"ca://A", "B"})
    try:
        connector.emit(_pv("ca://A", 2.5))
        connector.emit(_pv("B", connected=False))
        assert await wait_for(lambda: True)  # yield once
        await monitor.flush()

        a = json.loads(await redis.hget(VALUES_KEY, "ca://A"))
        b = json.loads(await redis.hget(VALUES_KEY, "B"))
        assert a["pv"]["value"] == 2.5 and a["pv"]["connected"] is True
        # A disconnect reaches Redis, so the reader stops serving B's last good value.
        assert b["pv"]["connected"] is False

        status = await read_status(redis)
        assert status["monitored"] == 2
        assert status["connected"] == 1
        assert status["seen"] == 2
        assert status["warm"] is True
        assert status["heartbeatAt"] >= status["leaderSince"]
    finally:
        await monitor.stop()


async def test_not_warm_until_every_monitored_address_has_reported():
    redis = fakeredis.aioredis.FakeRedis()
    monitor, connector = await _leader(redis, {"A", "B"})
    try:
        connector.emit(_pv("A"))
        await monitor.flush()
        status = await read_status(redis)
        assert (status["seen"], status["warm"]) == (1, False)
    finally:
        await monitor.stop()


async def test_reconcile_follows_the_registered_pvs():
    redis = fakeredis.aioredis.FakeRedis()
    addresses = {"A", "B"}

    async def load() -> set[str]:
        return set(addresses)

    connector = FakeConnector()
    monitor = PVCacheMonitor(connector, redis, FAST, load)
    monitor.start()
    try:
        assert await wait_for(lambda: set(connector.callbacks) == {"A", "B"})
        connector.emit(_pv("B"))
        await monitor.flush()
        assert await redis.hexists(VALUES_KEY, "B")

        stale_callback = connector.callbacks["B"]
        addresses.clear()
        addresses.update({"A", "C"})
        assert await wait_for(lambda: set(connector.callbacks) == {"A", "C"})
        assert "B" in connector.closed
        assert not await redis.hexists(VALUES_KEY, "B")

        # A late event from the dropped monitor doesn't bring B back.
        stale_callback(_pv("B"))
        await monitor.flush()
        assert not await redis.hexists(VALUES_KEY, "B")
    finally:
        await monitor.stop()


# --- what the reader trusts -------------------------------------------------- #


async def _write_status(redis, *, heartbeat_at: float, leader_since: float) -> None:
    await redis.set(STATUS_KEY, json.dumps({"heartbeatAt": heartbeat_at, "leaderSince": leader_since}))


async def test_reader_skips_the_cache_when_no_monitor_has_reported():
    lookup = await PVCacheReader(fakeredis.aioredis.FakeRedis(), heartbeat_max_age_s=30).lookup(["A", "B"])
    assert lookup.hits == {}
    assert lookup.misses == ["A", "B"]
    assert "no worker" in lookup.skipped


async def test_reader_skips_the_cache_when_the_heartbeat_is_stale():
    redis = fakeredis.aioredis.FakeRedis()
    now = time.time()
    await _write_status(redis, heartbeat_at=now - 120, leader_since=now - 600)
    await redis.hset(VALUES_KEY, "A", encode_entry(_pv("A"), now - 130))

    lookup = await PVCacheReader(redis, heartbeat_max_age_s=30).lookup(["A"])
    assert lookup.misses == ["A"]
    assert "heartbeat" in lookup.skipped


async def test_reader_serves_only_connected_entries_written_by_the_current_leader():
    redis = fakeredis.aioredis.FakeRedis()
    now = time.time()
    await _write_status(redis, heartbeat_at=now, leader_since=now - 10)
    await redis.hset(
        VALUES_KEY,
        mapping={
            "fresh": encode_entry(_pv("fresh", 7.0), now - 1),
            "previous-leader": encode_entry(_pv("previous-leader"), now - 60),
            "disconnected": encode_entry(_pv("disconnected", connected=False), now - 1),
            "garbage": "not json",
        },
    )

    lookup = await PVCacheReader(redis, heartbeat_max_age_s=30).lookup(
        ["fresh", "previous-leader", "disconnected", "garbage", "never-cached"]
    )

    assert lookup.skipped is None
    assert list(lookup.hits) == ["fresh"]
    assert lookup.misses == ["previous-leader", "disconnected", "garbage", "never-cached"]
    hit = lookup.hits["fresh"]
    # Same shape a live read produces: integer severity, aware timestamp.
    assert (hit.value, hit.severity, hit.status, hit.connected) == (7.0, 1, 4, True)
    assert hit.timestamp is not None and hit.timestamp.tzinfo is not None


async def test_a_new_leaders_entries_become_hits_as_its_monitors_report():
    """After a takeover, the previous leader's entries are misses until the new one rewrites them."""
    redis = fakeredis.aioredis.FakeRedis()
    old = time.time() - 60
    await redis.hset(VALUES_KEY, mapping={a: encode_entry(_pv(a), old) for a in ("A", "B")})

    monitor, connector = await _leader(redis, {"A", "B"})
    try:
        connector.emit(_pv("A"))
        await monitor.flush()
        lookup = await PVCacheReader(redis, heartbeat_max_age_s=30).lookup(["A", "B"])
        assert list(lookup.hits) == ["A"]
        assert lookup.misses == ["B"]
    finally:
        await monitor.stop()


# --- worker wiring ------------------------------------------------------------ #


@pytest.mark.parametrize("enabled", [True, False])
async def test_worker_runs_the_monitor_only_when_enabled(monkeypatch, enabled):
    monkeypatch.setattr(
        worker,
        "get_squirrel_config",
        lambda: SquirrelConfig(pv_cache=PVCacheConfig(**{**FAST.model_dump(), "enabled": enabled})),
    )

    async def no_pvs() -> set[str]:
        return set()

    monkeypatch.setattr("canopy.squirrel.pv_cache.monitor.load_pv_addresses", no_pvs)
    ctx = {"redis": fakeredis.aioredis.FakeRedis()}
    await worker.on_startup(ctx)
    try:
        monitor = ctx.get("pv_cache_monitor")
        if enabled:
            assert await wait_for(lambda: monitor.is_leader)
        else:
            assert monitor is None
    finally:
        await worker.on_shutdown(ctx)
    if enabled:
        assert not monitor.is_leader
        assert await ctx["redis"].get(LEADER_KEY) is None
