"""Fixtures for the replication commands' single-database tests.

A dedicated database, ``canopy_replication_test`` (publications and default
privileges belong to one database, so this keeps them away from ``canopy_test``),
with every registered schema rebuilt per test from ``Base.metadata``. Roles are
cluster-wide, so each test gets uniquely named ones and drops them afterwards.
Skips rather than fails when Postgres is unreachable, like the other database tests.

None of these need ``wal_level = logical``: Postgres creates a publication without
it (with a warning). Subscribing needs a second server: the tests marked
``requires_pair`` run against two (``docker-compose.yml`` here starts them).
"""

import os
import asyncio
from uuid import uuid4

import pytest
import asyncpg
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.pool import NullPool
from prometheus_client import CollectorRegistry, generate_latest
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine
from prometheus_client.parser import text_string_to_metric_families

import canopy.core.keys.models  # noqa: F401 — register every persistent module's tables
import canopy.magpie.data.models  # noqa: F401
import canopy.squirrel.data.models  # noqa: F401
import canopy.core.facilities.models  # noqa: F401
from canopy.core.db import Base
from canopy.core.replication.config import ReplicationConfig
from canopy.core.replication.metrics import ReplicationCollector
from canopy.core.replication.registry import REGISTRY

_ADMIN_DSN = os.environ.get("CANOPY_TEST_ADMIN_DSN", "postgresql://canopy:canopy@localhost:5432/postgres")
_DB_NAME = "canopy_replication_test"
DB_DSN = make_url(_ADMIN_DSN).set(database=_DB_NAME).render_as_string(hide_password=False)

# A primary (wal_level=logical) and a mirror on two servers, for the end-to-end tests.
PUBLISHER_ADMIN = os.environ.get("CANOPY_TEST_REPLICATION_PUBLISHER_DSN")
SUBSCRIBER_ADMIN = os.environ.get("CANOPY_TEST_REPLICATION_SUBSCRIBER_DSN")
# How the mirror's server reaches the primary's: libpq key=value, without dbname/user/password.
PUBLISHER_CONNINFO = os.environ.get("CANOPY_TEST_REPLICATION_PUBLISHER_CONNINFO")

requires_pair = pytest.mark.skipif(
    not (PUBLISHER_ADMIN and SUBSCRIBER_ADMIN and PUBLISHER_CONNINFO),
    reason="set CANOPY_TEST_REPLICATION_{PUBLISHER_DSN,SUBSCRIBER_DSN,PUBLISHER_CONNINFO}; see docker-compose.yml here",
)


def server_dsn(admin: str, *, database: str, user: str | None = None, password: str | None = None) -> str:
    """``admin`` pointed at ``database``, optionally as another user."""
    url = make_url(admin).set(database=database)
    if user is not None:
        url = url.set(username=user, password=password)
    return url.render_as_string(hide_password=False)


async def eventually(check, timeout: float = 60.0):
    """Await ``check()`` until it returns something truthy; fail after ``timeout`` seconds."""
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        if result := await check():
            return result
        if asyncio.get_running_loop().time() > deadline:
            pytest.fail(f"condition not met within {timeout}s")
        await asyncio.sleep(0.2)


async def build_schema(dsn: str) -> None:
    """Drop and recreate every registered schema with all of ``Base.metadata``'s tables."""
    engine = create_async_engine(
        make_url(dsn).set(drivername="postgresql+asyncpg").render_as_string(hide_password=False), poolclass=NullPool
    )
    async with engine.begin() as conn:
        for schema in REGISTRY.schemas:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
            await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        await conn.run_sync(Base.metadata.create_all)
    await engine.dispose()


async def drop_roles(conn: asyncpg.Connection, roles: list[str]) -> None:
    """Drop ``roles`` with everything granted to them (default privileges included)."""
    for role in roles:
        if await conn.fetchval("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)", role):
            await conn.execute(f'DROP OWNED BY "{role}"')
            await conn.execute(f'DROP ROLE "{role}"')


@pytest_asyncio.fixture(scope="session")
async def ensure_replication_db():
    try:
        conn = await asyncpg.connect(_ADMIN_DSN)
    except (OSError, asyncpg.PostgresError) as e:
        pytest.skip(f"Postgres unavailable for replication tests ({_ADMIN_DSN}): {e}")
    try:
        if not await conn.fetchval("SELECT 1 FROM pg_database WHERE datname = $1", _DB_NAME):
            await conn.execute(f'CREATE DATABASE "{_DB_NAME}"')
    finally:
        await conn.close()


@pytest.fixture
def config() -> ReplicationConfig:
    """Role names unique to this test, so concurrent runs on one server can't collide."""
    suffix = uuid4().hex[:8]
    return ReplicationConfig(
        replicator_role=f"rt_replicator_{suffix}",
        reader_role=f"rt_reader_{suffix}",
        app_role=f"rt_app_{suffix}",
        publisher_dsn=None,
    )


@pytest_asyncio.fixture
async def db(ensure_replication_db, config):
    """A connection (as the admin user) to the test database, with every table built."""
    await build_schema(DB_DSN)
    conn = await asyncpg.connect(DB_DSN)
    try:
        yield conn
    finally:
        for publication in REGISTRY.publications:
            await conn.execute(f'DROP PUBLICATION IF EXISTS "{publication.name}"')
        for schema in REGISTRY.schemas:
            await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await drop_roles(conn, [config.replicator_role, config.reader_role, config.app_role])
        await conn.close()


async def privileges(conn: asyncpg.Connection, role: str, table: str) -> set[str]:
    """The table privileges ``role`` holds on ``table``."""
    return {
        privilege
        for privilege in ("SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE")
        if await conn.fetchval("SELECT has_table_privilege($1, $2, $3)", role, table, privilege)
    }


def metric_samples(status: dict | None) -> dict[tuple[str, frozenset], float]:
    """What a scrape shows for ``status``: ``{(sample name, labels): value}``."""
    collector = ReplicationCollector()
    collector.status = status
    registry = CollectorRegistry()
    registry.register(collector)
    return {
        (sample.name, frozenset(sample.labels.items())): sample.value
        for family in text_string_to_metric_families(generate_latest(registry).decode())
        for sample in family.samples
    }
