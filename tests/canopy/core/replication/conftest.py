"""Fixtures for the replication tests: one database, or two arms and a head.

``db``: a dedicated database, ``canopy_replication_test`` (publications and default
privileges belong to one database, so this keeps them away from ``canopy_test``),
with every registered schema rebuilt per test from ``Base.metadata``. Roles are
cluster-wide, so each test gets uniquely named ones and drops them afterwards.
Skips rather than fails when Postgres is unreachable, like the other database tests.
None of those tests need ``wal_level = logical``: Postgres creates a publication
without it (with a warning).

``octopus``: arms (primaries) and a head (a mirror) on separate Postgres 16
servers, for the end-to-end tests. Opt-in: it skips unless the
``CANOPY_TEST_REPLICATION_*`` variables point at the servers, as
``docker-compose.yml`` in this directory starts them.
"""

import os
import asyncio
from uuid import uuid4
from contextlib import asynccontextmanager
from dataclasses import dataclass

import pytest
import asyncpg
import pytest_asyncio
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.pool import NullPool
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

import canopy.core.keys.models  # noqa: F401 — register every persistent module's tables
import canopy.magpie.data.models  # noqa: F401
import canopy.squirrel.data.models  # noqa: F401
import canopy.core.facilities.models  # noqa: F401
from canopy.core.db import Base
from canopy.core.arms import Arm, ArmsConfig, subscription_name
from canopy.core.replication.config import ReplicationConfig
from canopy.core.replication.registry import REGISTRY
from canopy.core.replication.subscriber import subscriber_status

_ADMIN_DSN = os.environ.get("CANOPY_TEST_ADMIN_DSN", "postgresql://canopy:canopy@localhost:5432/postgres")
_DB_NAME = "canopy_replication_test"
DB_DSN = make_url(_ADMIN_DSN).set(database=_DB_NAME).render_as_string(hide_password=False)


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


async def direct_grants(conn: asyncpg.Connection, role: str, table: str) -> set[str]:
    """The privileges granted on ``table`` to ``role`` itself, ignoring inherited ones."""
    rows = await conn.fetch(
        """
        SELECT a.privilege_type FROM pg_class c, aclexplode(c.relacl) a
        WHERE c.oid = $2::regclass AND a.grantee = (SELECT oid FROM pg_roles WHERE rolname = $1)
        """,
        role,
        table,
    )
    return {row["privilege_type"] for row in rows}


# --------------------------------------------------------------------------- #
# Two arms and a head
# --------------------------------------------------------------------------- #

# Per arm: its server as a superuser, from here; and how the head's server reaches
# it (libpq key=value, without dbname, user or password). Then the head's server.
ARM_SERVERS = [
    (os.environ.get("CANOPY_TEST_REPLICATION_ARM1_DSN"), os.environ.get("CANOPY_TEST_REPLICATION_ARM1_CONNINFO")),
    (os.environ.get("CANOPY_TEST_REPLICATION_ARM2_DSN"), os.environ.get("CANOPY_TEST_REPLICATION_ARM2_CONNINFO")),
]
HEAD_SERVER = os.environ.get("CANOPY_TEST_REPLICATION_HEAD_DSN")


def server_dsn(admin: str, *, database: str, user: str | None = None, password: str | None = None) -> str:
    """``admin``'s server, at ``database``, optionally as another user."""
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


@dataclass
class ArmSide:
    """One arm: a primary, set up the way prod's are (a superuser owns its tables)."""

    name: str  # the arm's CANOPY_INSTANCE; its subscription on the head is canopy_history_<name>
    admin: asyncpg.Connection  # the arm's database, as a superuser
    dsn: str  # the arm's database, as a superuser
    replicator: str  # its replication role, which the head's subscription connects as
    reader: str  # its read-only role
    publisher_dsn: str  # what the head's server uses to reach it, as the replicator (a secret)


@dataclass
class Head:
    """The head: a mirror whose tables belong to an ordinary owner role granted
    ``pg_create_subscription``, which runs the head-side commands."""

    name: str  # the head's CANOPY_INSTANCE
    admin: asyncpg.Connection  # the head's database, as a superuser
    conn: asyncpg.Connection  # the head's database, as the tables' owner
    dsn: str  # the head's database, as the tables' owner
    owner: str
    app: str  # the role the head's API would connect as


@dataclass
class Octopus:
    arms: dict[str, ArmSide]  # by arm name, in arm order
    head: Head
    config: ReplicationConfig  # role names; the same on every server
    arms_config: ArmsConfig  # what the head's settings would hold

    @property
    def arm(self) -> ArmSide:
        """The first arm: the only one in a one-arm test."""
        return next(iter(self.arms.values()))


@asynccontextmanager
async def _topology(count: int, *, schema: bool):
    """``count`` arms and a head, with every table built when ``schema`` is set.

    Every database, role and arm name is fresh; on exit the head's subscriptions
    (and so their slots on the arms), the databases and the roles are dropped.
    """
    servers = ARM_SERVERS[:count]
    if not HEAD_SERVER or not all(admin and conninfo for admin, conninfo in servers):
        pytest.skip(
            f"set CANOPY_TEST_REPLICATION_ARM{{1..{count}}}_DSN and _CONNINFO and CANOPY_TEST_REPLICATION_HEAD_DSN; "
            "see docker-compose.yml here"
        )
    suffix = uuid4().hex[:8]
    replicator, reader, owner, app = (f"rt_{role}_{suffix}" for role in ("replicator", "reader", "owner", "app"))
    replicator_pw, owner_pw = uuid4().hex, uuid4().hex
    head_db = f"rt_head_{suffix}"
    cleanup = []
    topology: Octopus | None = None

    head_server = await asyncpg.connect(HEAD_SERVER)
    try:
        await head_server.execute(f"CREATE ROLE \"{owner}\" LOGIN PASSWORD '{owner_pw}'")
        await head_server.execute(f'GRANT pg_create_subscription TO "{owner}"')
        await head_server.execute(f'CREATE ROLE "{app}" LOGIN')
        await head_server.execute(f'CREATE DATABASE "{head_db}" OWNER "{owner}"')
        head_dsn = server_dsn(HEAD_SERVER, database=head_db, user=owner, password=owner_pw)
        if schema:
            await build_schema(head_dsn)

        arms: dict[str, ArmSide] = {}
        for index, (admin, conninfo) in enumerate(servers, start=1):
            name, arm_db = f"arm{index}_{suffix}", f"rt_arm{index}_{suffix}"
            arm_server = await asyncpg.connect(admin)
            cleanup.append((arm_server, arm_db))
            await arm_server.execute(f'CREATE DATABASE "{arm_db}"')
            await arm_server.execute(f"CREATE ROLE \"{replicator}\" REPLICATION LOGIN PASSWORD '{replicator_pw}'")
            await arm_server.execute(f'CREATE ROLE "{reader}" LOGIN')
            arm_dsn = server_dsn(admin, database=arm_db)
            if schema:
                await build_schema(arm_dsn)
            arms[name] = ArmSide(
                name=name,
                admin=await asyncpg.connect(arm_dsn),
                dsn=arm_dsn,
                replicator=replicator,
                reader=reader,
                publisher_dsn=f"{conninfo} dbname={arm_db} user={replicator} password={replicator_pw}",
            )
        topology = Octopus(
            arms=arms,
            head=Head(
                name=f"head_{suffix}",
                admin=await asyncpg.connect(server_dsn(HEAD_SERVER, database=head_db)),
                conn=await asyncpg.connect(head_dsn),
                dsn=head_dsn,
                owner=owner,
                app=app,
            ),
            config=ReplicationConfig(replicator_role=replicator, reader_role=reader, app_role=app),
            arms_config=ArmsConfig(
                arms={name: Arm(publisher_dsn=SecretStr(arm.publisher_dsn)) for name, arm in arms.items()}
            ),
        )
        yield topology
    finally:
        if topology is not None:
            # Dropping a subscription also drops its slot on the arm.
            for name in topology.arms:
                await topology.head.admin.execute(f'DROP SUBSCRIPTION IF EXISTS "{subscription_name(name)}"')
            for conn in (topology.head.admin, topology.head.conn, *(arm.admin for arm in topology.arms.values())):
                await conn.close()
        for arm_server, arm_db in cleanup:
            await arm_server.execute(
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE database = $1", arm_db
            )
            await arm_server.execute(f'DROP DATABASE IF EXISTS "{arm_db}" WITH (FORCE)')
            await drop_roles(arm_server, [replicator, reader])
            await arm_server.close()
        await head_server.execute(f'DROP DATABASE IF EXISTS "{head_db}" WITH (FORCE)')
        await drop_roles(head_server, [owner, app])
        await head_server.close()


@pytest_asyncio.fixture
async def octopus(request):
    """Arms and a head on their own servers, each with every table built.

    Parametrize it indirectly with the number of arms (1 or 2; 2 by default).
    """
    async with _topology(getattr(request, "param", 2), schema=True) as topology:
        yield topology


@pytest_asyncio.fixture
async def bare_octopus(request):
    """``octopus`` with empty databases, for tests that build the schema with Alembic.

    The roles and databases are the same: the head's database belongs to its owner
    role, so a migration run as that role (``head.dsn``) owns what it creates, as in
    production. Parametrize it indirectly with the number of arms, like ``octopus``.
    """
    async with _topology(getattr(request, "param", 2), schema=False) as topology:
        yield topology


async def copied(octopus: Octopus, arm: str) -> bool:
    """Whether the head's subscription to ``arm`` has finished every table's initial copy."""
    status = await subscriber_status(octopus.head.admin, arms=octopus.arms_config.arms, only=arm)
    sub = status["subscriptions"][0]
    return sub["exists"] and sub["tables_ready"] == len(REGISTRY.replicated) and not sub["tables_not_ready"]
