"""Postgres for the forwarding tests that need one.

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
from canopy.core import db
from canopy.core.db import Base

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
