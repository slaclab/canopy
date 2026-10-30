"""Migrations released in the documented order keep a mirror in step with its primary.

Opt-in, on the pair of Postgres 16 servers the end-to-end tests use. Both sides are
built by Alembic rather than ``create_all``, and every step is a command a deploy
runs, as a subprocess: ``python -m alembic`` and ``python -m canopy.core.replication``.

``test_every_revision_in_release_order`` starts both sides at the first revision and
subscribes, then releases each later revision the way ``CONTRIBUTING.md`` describes:
the mirror migrates (``alembic upgrade``, ``grants``, ``refresh``) while the primary
keeps writing on its old schema, then the primary migrates (``alembic upgrade``,
``publish``) and the mirror's hourly ``refresh`` picks up newly published tables.
After each half the primary inserts rows and rewrites every published row, and the
mirror has to catch up without an apply error. A migration that breaks the rules (a
column the primary sends before the mirror has it, a column the mirror drops first, a
data step run on both sides) fails it.

``test_out_of_order_changes_stop_apply`` shows the failures that order prevents.
"""

import os
import sys
import json
import asyncio
import subprocess
from uuid import uuid4
from pathlib import Path
from dataclasses import dataclass

import pytest
import asyncpg
import pytest_asyncio
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy.engine import make_url

from canopy.core.replication.sql import qualified, quote_ident
from canopy.core.replication.registry import REGISTRY

from .conftest import (
    PUBLISHER_ADMIN,
    SUBSCRIBER_ADMIN,
    PUBLISHER_CONNINFO,
    drop_roles,
    eventually,
    server_dsn,
    requires_pair,
)

pytestmark = requires_pair

ROOT = Path(__file__).resolve().parents[4]
PUBLICATIONS = [publication.name for publication in REGISTRY.publications]


def revisions() -> list[str]:
    """Every revision of the shipped migration tree, oldest first."""
    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(ROOT / "migrations"))
    script = ScriptDirectory.from_config(config)
    assert len(script.get_heads()) == 1, f"the migration tree has more than one head: {script.get_heads()}"
    return [revision.revision for revision in reversed(list(script.walk_revisions()))]


@dataclass
class Side:
    """One database of the pair, driven the way its deploy drives it."""

    env: dict[str, str]  # the whole environment of its commands, so a local .env can't leak in
    conn: asyncpg.Connection  # as a superuser

    def run(self, *args: str) -> str:
        result = subprocess.run(
            [sys.executable, "-m", *args], cwd=ROOT, env=self.env, capture_output=True, text=True, timeout=300
        )
        if result.returncode != 0:
            pytest.fail(
                f"`{' '.join(args)}` on the {self.env['CANOPY_ROLE']} exited {result.returncode}:\n{result.stderr}"
            )
        return result.stdout

    def migrate(self, revision: str) -> None:
        self.run("alembic", "upgrade", revision)

    def replication(self, command: str) -> str:
        return self.run("canopy.core.replication", command)


@dataclass
class Pair:
    primary: Side
    mirror: Side
    subscription: str


def _app_url(dsn: str) -> str:
    """``CANOPY_DATABASE_URL`` for ``dsn``."""
    return make_url(dsn).set(drivername="postgresql+asyncpg").render_as_string(hide_password=False)


@pytest_asyncio.fixture
async def pair():
    """An empty database on each server; the tests build both with Alembic."""
    suffix = uuid4().hex[:8]
    pub_db, sub_db = f"rt_pub_{suffix}", f"rt_sub_{suffix}"
    replicator, reader, app = (f"rt_{name}_{suffix}" for name in ("replicator", "reader", "app"))
    replicator_pw = uuid4().hex
    instance = f"order_{suffix}"

    pub_server = await asyncpg.connect(PUBLISHER_ADMIN)
    sub_server = await asyncpg.connect(SUBSCRIBER_ADMIN)
    await pub_server.execute(f'CREATE DATABASE "{pub_db}"')
    await pub_server.execute(f"CREATE ROLE \"{replicator}\" REPLICATION LOGIN PASSWORD '{replicator_pw}'")
    await pub_server.execute(f'CREATE ROLE "{reader}" LOGIN')
    await sub_server.execute(f'CREATE DATABASE "{sub_db}"')
    await sub_server.execute(f'CREATE ROLE "{app}" LOGIN')

    common = {
        "PATH": os.environ.get("PATH", ""),
        "CANOPY_INSTANCE": instance,
        "CANOPY_REPLICATION__REPLICATOR_ROLE": replicator,
        "CANOPY_REPLICATION__READER_ROLE": reader,
        "CANOPY_REPLICATION__APP_ROLE": app,
    }
    pub_dsn = server_dsn(PUBLISHER_ADMIN, database=pub_db)
    sub_dsn = server_dsn(SUBSCRIBER_ADMIN, database=sub_db)
    pair = Pair(
        primary=Side(
            env={**common, "CANOPY_ROLE": "primary", "CANOPY_DATABASE_URL": _app_url(pub_dsn)},
            conn=await asyncpg.connect(pub_dsn),
        ),
        mirror=Side(
            env={
                **common,
                "CANOPY_ROLE": "mirror",
                "CANOPY_DATABASE_URL": _app_url(sub_dsn),
                "CANOPY_REPLICATION__PUBLISHER_DSN": (
                    f"{PUBLISHER_CONNINFO} dbname={pub_db} user={replicator} password={replicator_pw}"
                ),
            },
            conn=await asyncpg.connect(sub_dsn),
        ),
        subscription=f"canopy_history_{instance}",
    )
    try:
        yield pair
    finally:
        # Dropping the subscription also drops its slot on the primary.
        await pair.mirror.conn.execute(f'DROP SUBSCRIPTION IF EXISTS "{pair.subscription}"')
        await pair.primary.conn.close()
        await pair.mirror.conn.close()
        await pub_server.execute(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE database = $1", pub_db
        )
        await pub_server.execute(f'DROP DATABASE "{pub_db}" WITH (FORCE)')
        await sub_server.execute(f'DROP DATABASE "{sub_db}" WITH (FORCE)')
        await drop_roles(pub_server, [replicator, reader])
        await drop_roles(sub_server, [app])
        await pub_server.close()
        await sub_server.close()


async def _seed(conn: asyncpg.Connection) -> str:
    """A row in every table of the first revision; returns the PV that later rows refer to."""
    group, tag, pv, snapshot = (str(uuid4()) for _ in range(4))
    async with conn.transaction():
        await conn.execute("INSERT INTO squirrel.tag_group (id, name) VALUES ($1, 'Area')", group)
        await conn.execute("INSERT INTO squirrel.tag (id, name, group_id) VALUES ($1, 'Injector', $2)", tag, group)
        await conn.execute("INSERT INTO squirrel.pv (id, setpoint_address) VALUES ($1, 'SIM:PV:1:SETPOINT')", pv)
        await conn.execute("INSERT INTO squirrel.pv_tag (pv_id, tag_id) VALUES ($1, $2)", pv, tag)
        await conn.execute("INSERT INTO squirrel.meta_pv (pv_id) VALUES ($1)", pv)
        await conn.execute("INSERT INTO squirrel.snapshot (id, title) VALUES ($1, 'Seed')", snapshot)
        await conn.execute(
            "INSERT INTO squirrel.snapshot_value (id, snapshot_id, pv_id, pv_name) "
            "VALUES (gen_random_uuid(), $1, $2, 'SIM:PV:1:SETPOINT')",
            snapshot,
            pv,
        )
    return pv


async def _published(conn: asyncpg.Connection) -> list[str]:
    rows = await conn.fetch(
        "SELECT DISTINCT schemaname || '.' || tablename AS name FROM pg_publication_tables WHERE pubname = ANY($1)",
        PUBLICATIONS,
    )
    return sorted(row["name"] for row in rows)


async def _columns(conn: asyncpg.Connection, table: str) -> list[str]:
    rows = await conn.fetch(
        "SELECT attname FROM pg_attribute WHERE attrelid = $1::regclass AND attnum > 0 AND NOT attisdropped "
        "ORDER BY attnum",
        qualified(table),
    )
    return [row["attname"] for row in rows]


async def _write(conn: asyncpg.Connection, pv: str, label: str) -> None:
    """New rows, then a rewrite of every published row, as a primary in use would make.

    The inserts name only the first revision's columns, so they hold on every later
    schema while added columns are nullable or have a server default. Rewriting a row
    sends all its columns, so a column the mirror doesn't have stops apply.
    """
    snapshot = str(uuid4())
    async with conn.transaction():
        await conn.execute("INSERT INTO squirrel.tag_group (id, name) VALUES (gen_random_uuid(), $1)", label)
        await conn.execute("INSERT INTO squirrel.snapshot (id, title) VALUES ($1, $2)", snapshot, label)
        await conn.execute(
            "INSERT INTO squirrel.snapshot_value (id, snapshot_id, pv_id, pv_name) "
            "VALUES (gen_random_uuid(), $1, $2, 'SIM:PV:1:SETPOINT')",
            snapshot,
            pv,
        )
    for table in await _published(conn):
        column = quote_ident((await _columns(conn, table))[0])
        await conn.execute(f"UPDATE {qualified(table)} SET {column} = {column}")


async def _apply_errors(p: Pair) -> int:
    return await p.mirror.conn.fetchval(
        "SELECT coalesce(sum(apply_error_count + sync_error_count), 0) FROM pg_stat_subscription_stats "
        "WHERE subname = $1",
        p.subscription,
    )


async def _more_errors(p: Pair, count: int) -> bool:
    return await _apply_errors(p) > count


async def _differing(p: Pair) -> list[str]:
    """Published tables whose rows differ between the sides, compared on the primary's columns."""
    differing = []
    for table in await _published(p.primary.conn):
        columns = ", ".join(quote_ident(c) for c in await _columns(p.primary.conn, table))
        rows = []
        for conn in (p.primary.conn, p.mirror.conn):
            fetched = await conn.fetch(f"SELECT {columns} FROM {qualified(table)}")
            rows.append(sorted(tuple(str(v) for v in row.values()) for row in fetched))
        if rows[0] != rows[1]:
            differing.append(table)
    return differing


async def _in_step(p: Pair) -> bool:
    return not await _differing(p)


async def _caught_up(p: Pair, when: str, timeout: float = 60.0) -> None:
    """Wait until the mirror holds every published row; fail at once on an apply error."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        if errors := await _apply_errors(p):
            pytest.fail(f"{when}: the mirror's apply failed {errors} time(s); its server log has the reason")
        copying = await p.mirror.conn.fetchval(
            "SELECT count(*) FROM pg_subscription_rel r JOIN pg_subscription s ON s.oid = r.srsubid "
            "WHERE s.subname = $1 AND r.srsubstate <> 'r'",
            p.subscription,
        )
        differing = await _differing(p)
        if not (copying or differing):
            return
        if loop.time() > deadline:
            pytest.fail(
                f"{when}: the mirror didn't catch up within {timeout}s "
                f"(tables still copying: {copying}; rows differ in: {', '.join(differing) or 'none'})"
            )
        await asyncio.sleep(0.2)


async def test_every_revision_in_release_order(pair: Pair):
    p = pair
    first, *later = revisions()

    # The first release: the mirror migrates first, then the primary publishes, then
    # the one-time subscribe copies what the primary holds.
    p.mirror.migrate(first)
    p.mirror.replication("grants")
    p.mirror.replication("refresh")  # no subscription yet: a no-op, as on a first deploy
    p.primary.migrate(first)
    p.primary.replication("publish")
    pv = await _seed(p.primary.conn)
    p.mirror.replication("subscribe")
    await _caught_up(p, f"subscribed at {first}")

    for revision in later:
        p.mirror.migrate(revision)
        p.mirror.replication("grants")
        p.mirror.replication("refresh")
        await _write(p.primary.conn, pv, f"{revision}, mirror ahead")
        await _caught_up(p, f"{revision} on the mirror only")

        p.primary.migrate(revision)
        p.primary.replication("publish")
        await _write(p.primary.conn, pv, f"{revision}, both")
        p.mirror.replication("refresh")  # the hourly CronJob: copies tables first published now
        await _caught_up(p, f"{revision} on both")

    status = json.loads(p.mirror.replication("status"))["subscription"]
    assert status["exists"] and not status["tables_not_ready"]
    assert status["apply_error_count"] == 0 and status["sync_error_count"] == 0
    assert status["tables_ready"] == len(await _published(p.primary.conn))
    for side in (p.primary, p.mirror):
        assert await side.conn.fetchval("SELECT version_num FROM public.alembic_version") == later[-1]


async def test_out_of_order_changes_stop_apply(pair: Pair):
    """A column the primary sends and the mirror lacks stops apply until the mirror has it."""
    p = pair
    p.mirror.migrate("head")
    p.primary.migrate("head")
    p.primary.replication("publish")
    await _seed(p.primary.conn)
    p.mirror.replication("subscribe")
    await _caught_up(p, "subscribed at head")

    # Added on the primary first.
    await p.primary.conn.execute("ALTER TABLE squirrel.tag_group ADD COLUMN zz_added text")
    await p.primary.conn.execute("UPDATE squirrel.tag_group SET zz_added = 'new'")
    await eventually(lambda: _more_errors(p, 0))
    await p.mirror.conn.execute("ALTER TABLE squirrel.tag_group ADD COLUMN zz_added text")
    await eventually(lambda: _in_step(p))

    # Dropped on the mirror first.
    errors = await _apply_errors(p)
    await p.mirror.conn.execute("ALTER TABLE squirrel.tag_group DROP COLUMN description")
    await p.primary.conn.execute("UPDATE squirrel.tag_group SET description = 'edited'")
    await eventually(lambda: _more_errors(p, errors))
    await p.mirror.conn.execute("ALTER TABLE squirrel.tag_group ADD COLUMN description text")
    await eventually(lambda: _in_step(p))
