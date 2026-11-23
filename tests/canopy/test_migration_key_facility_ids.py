"""The ``ddc02220891f`` migration: an API key's facility grant gets the ids its slugs name.

Driven through the real ``alembic`` CLI against a throwaway database, like
``test_migration_row_owners.py``, since what's under test is the SQL the migration runs.
"""

import os
import subprocess

import pytest
import asyncpg

_ADMIN_DSN = os.environ.get("CANOPY_TEST_ADMIN_DSN", "postgresql://canopy:canopy@localhost:5432/postgres")
_TEST_DB_NAME = "canopy_key_facility_ids_migration_test"
_TEST_DATABASE_URL = (
    os.environ.get("CANOPY_TEST_DATABASE_URL", "postgresql+asyncpg://canopy:canopy@localhost:5432/").rsplit("/", 1)[0]
    + f"/{_TEST_DB_NAME}"
)
_DSN = _TEST_DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://")

_BEFORE = "2fac58bd6437"
_FACILITY_IDS = "ddc02220891f"


def _run(*args: str) -> None:
    out = subprocess.run(
        ["uv", "run", "alembic", *args],
        env={**os.environ, "CANOPY_DATABASE_URL": _TEST_DATABASE_URL, "CANOPY_INSTANCE": "arm1"},
        capture_output=True,
        text=True,
    )
    assert out.returncode == 0, out.stderr


@pytest.fixture
async def conn():
    try:
        admin = await asyncpg.connect(_ADMIN_DSN)
    except (OSError, asyncpg.PostgresError) as e:
        pytest.skip(f"Postgres unavailable for migration tests ({_ADMIN_DSN}): {e}")
    try:
        await admin.execute(f'DROP DATABASE IF EXISTS "{_TEST_DB_NAME}"')
        await admin.execute(f'CREATE DATABASE "{_TEST_DB_NAME}"')
    finally:
        await admin.close()
    _run("upgrade", _BEFORE)
    connection = await asyncpg.connect(_DSN)
    yield connection
    await connection.close()
    admin = await asyncpg.connect(_ADMIN_DSN)
    try:
        await admin.execute(f'DROP DATABASE IF EXISTS "{_TEST_DB_NAME}"')
    finally:
        await admin.close()


async def _facility(conn: asyncpg.Connection, slug: str) -> str:
    return str(
        await conn.fetchval(
            "INSERT INTO core.facility (id, slug, name, owner_instance, created_at, updated_at) "
            "VALUES (gen_random_uuid(), $1::text, $1::text, 'arm1', now(), now()) RETURNING id",
            slug,
        )
    )


async def _key(conn: asyncpg.Connection, name: str, facilities: list[str], *, active: bool = True) -> None:
    await conn.execute(
        "INSERT INTO core.api_key (id, app_name, token_hash, is_active, read_access, write_access, admin_access, "
        "facilities, created_at, updated_at) "
        "VALUES (gen_random_uuid(), $1::text, md5($1::text), $2, true, false, false, $3, now(), now())",
        name,
        active,
        facilities,
    )


async def _grants(conn: asyncpg.Connection) -> dict[str, list[str]]:
    rows = await conn.fetch("SELECT app_name, facility_ids FROM core.api_key")
    return {row["app_name"]: [str(facility_id) for facility_id in row["facility_ids"]] for row in rows}


async def test_upgrade_binds_each_grant_to_the_facilities_its_slugs_name(conn):
    north, south = await _facility(conn, "north"), await _facility(conn, "south")
    await _key(conn, "north", ["north"])
    await _key(conn, "both", ["north", "south"])
    await _key(conn, "every", ["*"])
    await _key(conn, "none", [])
    await _key(conn, "gone", ["gone"])
    await _key(conn, "revoked", ["north"], active=False)

    _run("upgrade", _FACILITY_IDS)

    assert await _grants(conn) == {
        "north": [north],
        "both": sorted([north, south]),
        "every": [],  # the wildcard stays in facilities
        "none": [],
        "gone": [],  # a slug that names nothing matches nothing, as before
        "revoked": [north],
    }
    # The slugs stay, for display.
    assert await conn.fetchval("SELECT facilities FROM core.api_key WHERE app_name = 'both'") == ["north", "south"]


async def test_a_key_minted_after_the_upgrade_defaults_to_no_ids(conn):
    _run("upgrade", _FACILITY_IDS)

    await _key(conn, "later", [])

    assert await _grants(conn) == {"later": []}


async def test_downgrade_drops_the_ids_and_keeps_the_keys(conn):
    await _facility(conn, "north")
    await _key(conn, "north", ["north"])
    _run("upgrade", _FACILITY_IDS)

    _run("downgrade", _BEFORE)

    assert await conn.fetchval("SELECT facilities FROM core.api_key") == ["north"]
    assert not await conn.fetchval(
        "SELECT count(*) FROM information_schema.columns WHERE table_name = 'api_key' AND column_name = 'facility_ids'"
    )
