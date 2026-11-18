"""``GET /health/ready``: 200 while the database answers, 503 while it doesn't."""

import os

import pytest
import asyncpg
from sqlalchemy.pool import NullPool
from sqlalchemy.engine import make_url
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import create_async_engine

import canopy.main
import canopy.core.health
from canopy.main import app
from canopy.core.health import database_ready

_ADMIN_DSN = os.environ.get("CANOPY_TEST_ADMIN_DSN", "postgresql://canopy:canopy@localhost:5432/postgres")


@pytest.mark.parametrize(
    ("ready", "status", "body"),
    [
        (True, 200, {"status": "ready"}),
        (False, 503, {"status": "unavailable", "database": "unreachable"}),
    ],
)
def test_ready_route_follows_the_database_check(monkeypatch, ready, status, body):
    async def check() -> bool:
        return ready

    monkeypatch.setattr(canopy.main, "database_ready", check)
    response = TestClient(app).get("/health/ready")
    assert response.status_code == status
    assert response.json() == body


@pytest.fixture
async def engine_at(monkeypatch):
    """Point the readiness check at an engine built for the test, never the app's global one."""
    engines = []

    def use(url: str) -> None:
        engine = create_async_engine(url, poolclass=NullPool)
        engines.append(engine)
        monkeypatch.setattr(canopy.core.health, "get_engine", lambda: engine)

    yield use
    for engine in engines:
        await engine.dispose()


async def test_an_unreachable_database_is_not_ready_and_warns_once(engine_at, monkeypatch, caplog):
    monkeypatch.setattr(canopy.core.health, "_database_down", False)
    engine_at("postgresql+asyncpg://canopy:canopy@127.0.0.1:1/canopy")
    assert await database_ready() is False
    assert await database_ready() is False
    assert caplog.text.count("didn't answer") == 1


async def test_a_reachable_database_is_ready(engine_at, monkeypatch, caplog):
    try:
        conn = await asyncpg.connect(_ADMIN_DSN)
    except (OSError, asyncpg.PostgresError) as e:
        pytest.skip(f"Postgres unavailable for the readiness test ({_ADMIN_DSN}): {e}")
    await conn.close()
    monkeypatch.setattr(canopy.core.health, "_database_down", True)  # as after an outage
    engine_at(make_url(_ADMIN_DSN).set(drivername="postgresql+asyncpg").render_as_string(hide_password=False))
    assert await database_ready() is True
    assert "answers again" in caplog.text
