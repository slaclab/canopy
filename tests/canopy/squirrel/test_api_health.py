"""Tests for the squirrel health endpoints.

The restore-hooks status endpoint reads configuration and the worker's Redis report
only, so these run against the sub-app directly with an in-memory Redis instead of
the DB-gated ``client`` fixture.
"""

import json
import time
import asyncio
from datetime import datetime

import pytest
import pytest_asyncio
import fakeredis.aioredis
from httpx import AsyncClient, ASGITransport
from redis.exceptions import ConnectionError as RedisConnectionError

from canopy.jobs import gateway as jobs_gateway
from canopy.squirrel.app import app as squirrel_app
from canopy.squirrel.api.v1 import health
from canopy.squirrel.config import PVCacheConfig, SquirrelConfig
from canopy.squirrel.pv_cache import STATUS_KEY, VALUES_KEY
from canopy.squirrel.restore_hooks import registry, publish_worker_status

READ_HEADERS = {"X-API-Key": "test-read"}


@pytest.fixture
def redis(monkeypatch) -> fakeredis.aioredis.FakeRedis:
    """An in-memory Redis standing in for the Jobs pool the API reads the worker report from."""
    fake = fakeredis.aioredis.FakeRedis()

    async def get_pool():
        return fake

    monkeypatch.setattr(jobs_gateway, "get_pool", get_pool)
    return fake


@pytest_asyncio.fixture
async def http_client(redis) -> AsyncClient:
    """An AsyncClient over the squirrel sub-app, with no database wired in."""
    async with AsyncClient(transport=ASGITransport(app=squirrel_app), base_url="http://test") as c:
        yield c


def _configure_hooks(monkeypatch, restore_hooks: str, legacy_restore_hook: str = "") -> None:
    monkeypatch.setattr(
        registry,
        "get_squirrel_config",
        lambda: SquirrelConfig(restore_hooks=restore_hooks, restore_hook=legacy_restore_hook),
    )


async def _get_status(http_client: AsyncClient) -> dict:
    r = await http_client.get("/v1/health/restore-hooks", headers=READ_HEADERS)
    assert r.status_code == 200, r.text
    return r.json()


async def test_restore_hooks_status_requires_a_key(http_client: AsyncClient, monkeypatch):
    _configure_hooks(monkeypatch, "")
    r = await http_client.get("/v1/health/restore-hooks")
    assert r.status_code == 401


@pytest.mark.parametrize(
    ("configured", "status", "loaded", "unknown"),
    [
        ("", "disabled", [], []),
        ("magnet_trim,write_to_con", "active", ["magnet_trim", "write_to_con"], []),
        ("magnet_trim,nope", "partial", ["magnet_trim"], ["nope"]),
        ("nope", "unknown", [], ["nope"]),
    ],
)
async def test_api_view(
    http_client: AsyncClient, monkeypatch, configured: str, status: str, loaded: list[str], unknown: list[str]
):
    _configure_hooks(monkeypatch, configured)
    body = await _get_status(http_client)
    assert body["api"]["status"] == status
    assert body["api"]["loaded"] == loaded
    assert body["api"]["unknown"] == unknown
    assert body["available"] == ["magnet_trim", "write_to_con"]


async def test_no_worker_report_yet(http_client: AsyncClient, monkeypatch):
    _configure_hooks(monkeypatch, "magnet_trim")
    body = await _get_status(http_client)
    assert body["worker"] is None
    assert body["matchesApi"] is None
    assert body["workerError"] is None


async def test_worker_report_matching_the_api(http_client: AsyncClient, redis, monkeypatch):
    _configure_hooks(monkeypatch, "magnet_trim,write_to_con")
    await publish_worker_status(redis)

    body = await _get_status(http_client)
    assert body["worker"]["status"] == "active"
    assert body["worker"]["loaded"] == ["magnet_trim", "write_to_con"]
    assert body["worker"]["host"]
    assert datetime.fromisoformat(body["worker"]["reportedAt"]).tzinfo is not None
    assert body["matchesApi"] is True


async def test_worker_missing_the_variable_is_flagged(http_client: AsyncClient, redis, monkeypatch):
    _configure_hooks(monkeypatch, "")
    await publish_worker_status(redis)
    _configure_hooks(monkeypatch, "magnet_trim")

    body = await _get_status(http_client)
    assert body["api"]["status"] == "active"
    assert body["worker"]["status"] == "disabled"
    assert body["matchesApi"] is False


async def test_legacy_variable_warning_is_reported(http_client: AsyncClient, monkeypatch):
    _configure_hooks(monkeypatch, "", legacy_restore_hook="magnet_trim")
    body = await _get_status(http_client)
    assert body["api"]["status"] == "disabled"
    assert body["api"]["warnings"] == [registry.LEGACY_SETTING_WARNING]


async def test_unreachable_redis_is_reported_not_raised(http_client: AsyncClient, monkeypatch):
    _configure_hooks(monkeypatch, "magnet_trim")

    async def refuse():
        raise RedisConnectionError("Connection refused")

    monkeypatch.setattr(jobs_gateway, "get_pool", refuse)
    body = await _get_status(http_client)
    assert body["worker"] is None
    assert "Connection refused" in body["workerError"]
    assert body["api"]["status"] == "active"


async def test_slow_redis_times_out(http_client: AsyncClient, monkeypatch):
    _configure_hooks(monkeypatch, "magnet_trim")

    async def hang():
        await asyncio.sleep(10)

    monkeypatch.setattr(jobs_gateway, "get_pool", hang)
    monkeypatch.setattr(health, "WORKER_STATUS_TIMEOUT_S", 0.05)
    body = await _get_status(http_client)
    assert body["worker"] is None
    assert "TimeoutError" in body["workerError"]


async def test_restore_hooks_status_builds_no_hook(http_client: AsyncClient, monkeypatch):
    _configure_hooks(monkeypatch, "magnet_trim")
    monkeypatch.setattr(registry, "_hooks", [])
    monkeypatch.setattr(registry, "_hook_providers", None)

    await _get_status(http_client)
    assert registry._hooks == []
    assert registry._hook_providers is None


async def test_watchdog_reports_the_module(http_client: AsyncClient):
    r = await http_client.get("/v1/health/watchdog")
    assert r.status_code == 200
    assert r.json()["module"] == "squirrel"


# --- PV cache status -------------------------------------------------------- #


def _cache_config(monkeypatch, enabled: bool) -> None:
    monkeypatch.setattr(health, "get_squirrel_config", lambda: SquirrelConfig(pv_cache=PVCacheConfig(enabled=enabled)))


async def _cache_status(http_client: AsyncClient) -> dict:
    r = await http_client.get("/v1/health/pv-cache", headers=READ_HEADERS)
    assert r.status_code == 200, r.text
    return r.json()


async def _report(redis, **overrides) -> None:
    now = time.time()
    report = {
        "instance": "abc",
        "host": "worker-1",
        "leaderSince": now - 60,
        "heartbeatAt": now,
        "configured": 3,
        "monitored": 3,
        "connected": 3,
        "seen": 3,
        "warm": True,
        **overrides,
    }
    await redis.set(STATUS_KEY, json.dumps(report))


async def test_pv_cache_status_requires_a_key(http_client: AsyncClient):
    assert (await http_client.get("/v1/health/pv-cache")).status_code == 401


async def test_pv_cache_is_disabled_when_nothing_ran_it(http_client: AsyncClient, monkeypatch):
    _cache_config(monkeypatch, enabled=False)
    status = await _cache_status(http_client)
    assert (status["status"], status["enabled"]) == ("disabled", False)


async def test_pv_cache_is_down_when_enabled_but_no_worker_reported(http_client: AsyncClient, monkeypatch):
    _cache_config(monkeypatch, enabled=True)
    status = await _cache_status(http_client)
    assert status["status"] == "down"
    assert "CANOPY_SQUIRREL__PV_CACHE__ENABLED" in status["error"]


async def test_pv_cache_is_ok_with_a_fresh_warm_report(http_client: AsyncClient, redis, monkeypatch):
    _cache_config(monkeypatch, enabled=True)
    await _report(redis)
    await redis.hset(VALUES_KEY, mapping={"A": "{}", "B": "{}"})

    status = await _cache_status(http_client)
    assert status["status"] == "ok"
    assert (status["host"], status["monitored"], status["connected"], status["cached"]) == ("worker-1", 3, 3, 2)
    assert status["heartbeatAgeS"] < 5
    assert datetime.fromisoformat(status["leaderSince"]).tzinfo is not None


@pytest.mark.parametrize("overrides", [{"warm": False, "seen": 1}, {"connected": 2}])
async def test_pv_cache_is_degraded_while_warming_or_with_disconnected_pvs(
    http_client: AsyncClient, redis, monkeypatch, overrides
):
    _cache_config(monkeypatch, enabled=True)
    await _report(redis, **overrides)
    assert (await _cache_status(http_client))["status"] == "degraded"


async def test_pv_cache_is_down_when_the_heartbeat_is_stale(http_client: AsyncClient, redis, monkeypatch):
    _cache_config(monkeypatch, enabled=True)
    await _report(redis, heartbeatAt=0)
    status = await _cache_status(http_client)
    assert status["status"] == "down"
    assert "heartbeat" in status["error"]


async def test_pv_cache_is_down_when_redis_is_unreachable(http_client: AsyncClient, monkeypatch):
    _cache_config(monkeypatch, enabled=True)

    async def broken_pool():
        raise RedisConnectionError("connection refused")

    monkeypatch.setattr(jobs_gateway, "get_pool", broken_pool)
    status = await _cache_status(http_client)
    assert status["status"] == "down"
    assert "connection refused" in status["error"]
