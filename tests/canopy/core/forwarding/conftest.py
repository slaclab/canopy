"""A history head for the forwarding tests, and Postgres for those that need one.

``head`` makes this instance ``head`` (``CANOPY_ROLE=mirror``) with two arms:
``arm1`` is forwardable, at ``ARM1`` with the service key ``arm1-service-token``
(tests mock it with ``httpx_mock``); ``arm2`` has no forward settings, so its rows
are read-only here.

``database`` rebuilds the squirrel and magpie schemas in ``canopy_test`` per test and
points ``core.db``'s session maker at it, so the real app's request sessions use it.
Skips when Postgres is unreachable, like the other database tests.
"""

import os

import pytest
import asyncpg
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.pool import NullPool
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

import canopy.magpie.data.models  # noqa: F401 — register the tables on Base.metadata
import canopy.squirrel.data.models  # noqa: F401
import canopy.core.facilities.models  # noqa: F401
from canopy.core import db, security
from canopy.core.db import Base
from canopy.core.config import get_settings
from canopy.core.security import APIKey

ARM1 = "https://arm1.example.org"
ARM1_SERVICE_KEY = "arm1-service-token"


class FakeRegistry:
    def __init__(self, *keys: APIKey) -> None:
        self.keys = {key.token: key for key in keys}

    async def get(self, token: str) -> APIKey | None:
        return self.keys.get(token)


@pytest.fixture
def bind_keys():
    """Bind a registry of fake keys for the test: ``bind_keys(KEY, ...)``."""
    previous = security._registry

    def _bind(*keys: APIKey) -> None:
        security.bind_key_registry(FakeRegistry(*keys))

    yield _bind
    security._registry = previous


@pytest.fixture
def head(monkeypatch):
    """This instance as the history head ``head``, with a forwardable ``arm1`` and a read-only ``arm2``."""
    monkeypatch.setenv("CANOPY_ROLE", "mirror")
    monkeypatch.setenv("CANOPY_INSTANCE", "head")
    monkeypatch.setenv("CANOPY_ARMS__ARM1__FORWARD_URL", ARM1)
    monkeypatch.setenv("CANOPY_ARMS__ARM1__SERVICE_KEY", ARM1_SERVICE_KEY)
    monkeypatch.setenv("CANOPY_ARMS__ARM2__PUBLISHER_DSN", "host=arm2.example.org")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


TEST_DATABASE_URL = os.environ.get(
    "CANOPY_TEST_DATABASE_URL", "postgresql+asyncpg://canopy:canopy@localhost:5432/canopy_test"
)
_ADMIN_DSN = os.environ.get("CANOPY_TEST_ADMIN_DSN", "postgresql://canopy:canopy@localhost:5432/postgres")
_TEST_DB_NAME = TEST_DATABASE_URL.rsplit("/", 1)[-1].split("?")[0]
_SCHEMAS = ("squirrel", "magpie", "core")
# Of core's tables only the facility tree: the keys here are fakes.
_TABLES = [t for t in Base.metadata.sorted_tables if t.schema in _SCHEMAS[:2] or t.fullname == "core.facility"]


@pytest_asyncio.fixture
async def database(monkeypatch):
    try:
        conn = await asyncpg.connect(_ADMIN_DSN)
    except (OSError, asyncpg.PostgresError) as exc:
        pytest.skip(f"Postgres unavailable ({_ADMIN_DSN}): {exc}")
    try:
        if not await conn.fetchval("SELECT 1 FROM pg_database WHERE datname = $1", _TEST_DB_NAME):
            await conn.execute(f'CREATE DATABASE "{_TEST_DB_NAME}"')
    finally:
        await conn.close()
    engine = create_async_engine(TEST_DATABASE_URL, poolclass=NullPool)
    async with engine.begin() as conn:
        for schema in _SCHEMAS:
            await conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
        await conn.run_sync(Base.metadata.create_all, tables=_TABLES)
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False, autoflush=False)
    monkeypatch.setattr(db, "get_session_maker", lambda: maker)
    yield
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all, tables=_TABLES)
        for schema in _SCHEMAS:
            await conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
    await engine.dispose()
