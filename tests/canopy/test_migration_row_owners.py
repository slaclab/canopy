"""The ``89e21441c63c`` row-owners migration, and the rename guard in ``migrations/env.py``.

Driven through the real ``alembic`` CLI against a throwaway database, since what's
under test is the SQL the migration emits and the check ``env.py`` runs.
"""

import os
import subprocess

import pytest
import asyncpg

_ADMIN_DSN = os.environ.get("CANOPY_TEST_ADMIN_DSN", "postgresql://canopy:canopy@localhost:5432/postgres")
_TEST_DB_NAME = "canopy_row_owners_migration_test"
_TEST_DATABASE_URL = (
    os.environ.get("CANOPY_TEST_DATABASE_URL", "postgresql+asyncpg://canopy:canopy@localhost:5432/").rsplit("/", 1)[0]
    + f"/{_TEST_DB_NAME}"
)
_DSN = _TEST_DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://")

_BEFORE = "18ac64c44669"
_ROW_OWNERS = "89e21441c63c"
_OWNED = ["core.facility", "squirrel.pv", "squirrel.snapshot", "squirrel.tag_group", "magpie.config"]


def _alembic(*args: str, instance: str = "arm1", role: str = "primary") -> subprocess.CompletedProcess:
    return subprocess.run(
        ["uv", "run", "alembic", *args],
        env={**os.environ, "CANOPY_DATABASE_URL": _TEST_DATABASE_URL, "CANOPY_INSTANCE": instance, "CANOPY_ROLE": role},
        capture_output=True,
        text=True,
    )


@pytest.fixture
async def migration_db():
    try:
        conn = await asyncpg.connect(_ADMIN_DSN)
    except (OSError, asyncpg.PostgresError) as e:
        pytest.skip(f"Postgres unavailable for migration tests ({_ADMIN_DSN}): {e}")
    try:
        await conn.execute(f'DROP DATABASE IF EXISTS "{_TEST_DB_NAME}"')
        await conn.execute(f'CREATE DATABASE "{_TEST_DB_NAME}"')
    finally:
        await conn.close()
    yield
    conn = await asyncpg.connect(_ADMIN_DSN)
    try:
        await conn.execute(f'DROP DATABASE IF EXISTS "{_TEST_DB_NAME}"')
    finally:
        await conn.close()


async def _seed_before_owners(conn: asyncpg.Connection) -> None:
    """One row in each owned table, written the way a database below the revision holds them."""
    await conn.execute(
        """
        INSERT INTO core.facility (id, slug, name, created_at, updated_at)
            VALUES (gen_random_uuid(), 'site', 'Site', now(), now());
        INSERT INTO squirrel.pv (id, setpoint_address, abs_tolerance, rel_tolerance, created_at, updated_at)
            VALUES (gen_random_uuid(), 'A:SP', 0, 0, now(), now());
        INSERT INTO squirrel.snapshot (id, title, created_at, updated_at) VALUES (gen_random_uuid(), 's', now(), now());
        INSERT INTO squirrel.tag_group (id, name, created_at, updated_at) VALUES (gen_random_uuid(), 'Area', now(), now());
        INSERT INTO magpie.config (id, slug, app, data, created_at, updated_at)
            VALUES (gen_random_uuid(), 'abc', 'trace', '{}', now(), now());
        """
    )


def _run(*args: str, **kwargs) -> None:
    out = _alembic(*args, **kwargs)
    assert out.returncode == 0, out.stderr


async def test_upgrade_stamps_existing_rows_with_this_instance(migration_db):
    _run("upgrade", _BEFORE)
    conn = await asyncpg.connect(_DSN)
    try:
        await _seed_before_owners(conn)
        _run("upgrade", _ROW_OWNERS)
        for table in _OWNED:
            assert await conn.fetchval(f"SELECT array_agg(DISTINCT owner_instance) FROM {table}") == ["arm1"], table
        nullable = await conn.fetchval(
            "SELECT bool_or(is_nullable = 'YES') FROM information_schema.columns WHERE column_name = 'owner_instance'"
        )
        assert nullable is False
    finally:
        await conn.close()


async def test_the_same_address_is_allowed_once_per_owner(migration_db):
    _run("upgrade", _ROW_OWNERS)
    conn = await asyncpg.connect(_DSN)
    insert = (
        "INSERT INTO squirrel.pv (id, setpoint_address, abs_tolerance, rel_tolerance, owner_instance, created_at, "
        "updated_at) VALUES (gen_random_uuid(), 'A:SP', 0, 0, $1, now(), now())"
    )
    try:
        await conn.execute(insert, "arm1")
        await conn.execute(insert, "arm2")
        with pytest.raises(asyncpg.UniqueViolationError):
            await conn.execute(insert, "arm1")
    finally:
        await conn.close()


async def test_downgrade_refuses_while_two_owners_share_an_address(migration_db):
    _run("upgrade", _ROW_OWNERS)
    conn = await asyncpg.connect(_DSN)
    try:
        for owner in ("arm1", "arm2"):
            await conn.execute(
                "INSERT INTO squirrel.tag_group (id, name, owner_instance, created_at, updated_at) "
                "VALUES (gen_random_uuid(), 'Area', $1, now(), now())",
                owner,
            )
    finally:
        await conn.close()

    # A mirror's database: it holds two owners' rows, so the rename guard doesn't apply.
    out = _alembic("downgrade", _BEFORE, role="mirror")

    assert out.returncode != 0
    assert "squirrel.tag_group.name has values shared by more than one owner (Area)" in out.stderr


async def test_downgrade_restores_the_global_uniques(migration_db):
    _run("upgrade", _ROW_OWNERS)
    _run("downgrade", _BEFORE)
    conn = await asyncpg.connect(_DSN)
    try:
        assert await conn.fetchval(
            "SELECT indisunique FROM pg_index WHERE indexrelid = 'squirrel.ix_squirrel_pv_setpoint_address'::regclass"
        )
    finally:
        await conn.close()


async def test_a_renamed_primary_refuses_to_migrate(migration_db):
    _run("upgrade", _BEFORE)
    conn = await asyncpg.connect(_DSN)
    try:
        await _seed_before_owners(conn)
    finally:
        await conn.close()
    _run("upgrade", _ROW_OWNERS)

    renamed = _alembic("upgrade", "head", instance="arm9")

    assert renamed.returncode != 0
    assert "CANOPY_INSTANCE is 'arm9', but core.facility holds rows stamped 'arm1'" in renamed.stderr
    # A mirror holds other owners' rows by design, so it isn't checked.
    assert _alembic("current", instance="arm9", role="mirror").returncode == 0
    assert _alembic("current").returncode == 0
