"""Migrations released in the documented order keep a mirror in step with its primary.

Opt-in, on the pair of Postgres 16 servers the end-to-end tests use. Both sides are
built by Alembic rather than ``create_all``, and every step is a command a deploy
runs, as a subprocess: ``python -m alembic`` and ``python -m canopy.core.replication``.

``test_every_revision_in_release_order`` starts both sides at the first revision and
subscribes, then releases each later revision the way ``CONTRIBUTING.md`` describes:
the mirror migrates (``alembic upgrade``, ``grants``, ``refresh``) while the primary
keeps writing on its old schema, then the primary migrates (``alembic upgrade``,
``publish``) and the mirror's hourly ``refresh`` picks up newly published tables.

``test_mirror_at_head_while_the_primary_steps_up`` covers a deploy that takes the
mirror through several revisions at once: the mirror goes straight to head, and the
primary then climbs one revision at a time. It starts the primary at the newest
revision a subscriber-side drop declares with ``canopy_requires_primary_at``, since
such a drop may only reach a mirror whose primary already runs that revision.

After each step the primary inserts a row into every published table, built from its
own schema at that moment, and rewrites every published row, and the mirror has to
catch up without an apply error. A migration that breaks the rules (a column the
primary sends before the mirror has it, a column the mirror drops first, a NOT NULL
column without a default, a data step run on both sides) fails them.

``test_out_of_order_changes_stop_apply`` shows the failures that order prevents.
"""

import os
import sys
import json
import asyncio
import subprocess
from uuid import uuid4
from pathlib import Path
from datetime import UTC, date, datetime
from dataclasses import dataclass

import pytest
import asyncpg
import pytest_asyncio
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy.engine import make_url

from canopy.core.replication.sql import qualified, quote_ident
from canopy.core.replication.registry import REGISTRY
from canopy.core.replication.migration import REQUIRES_PRIMARY_AT

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


def _script() -> ScriptDirectory:
    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(ROOT / "migrations"))
    script = ScriptDirectory.from_config(config)
    assert len(script.get_heads()) == 1, f"the migration tree has more than one head: {script.get_heads()}"
    return script


def revisions() -> list[str]:
    """Every revision of the shipped migration tree, oldest first."""
    return [revision.revision for revision in reversed(list(_script().walk_revisions()))]


def oldest_primary_for_mirror_at_head() -> str:
    """The first revision, or the newest one a subscriber-side drop says its primary must run."""
    ordered = revisions()
    script = _script()
    required = [getattr(script.get_revision(revision).module, REQUIRES_PRIMARY_AT, None) for revision in ordered]
    required = [revision for revision in required if revision is not None]
    return max(required, key=ordered.index) if required else ordered[0]


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


# A value for a column the generic writer can't make (a CHECK it doesn't meet, say):
# {"schema.table": {"column": callable returning the value}}.
OVERRIDES: dict[str, dict[str, object]] = {}


async def _column_specs(conn: asyncpg.Connection, table: str) -> list[asyncpg.Record]:
    return await conn.fetch(
        """
        SELECT a.attname, a.attnotnull, t.typname, t.typtype::text AS typtype, t.typcategory::text AS typcategory, a.atttypmod,
               d.adbin IS NOT NULL OR a.attidentity <> '' OR a.attgenerated <> '' AS has_default,
               (SELECT array_agg(e.enumlabel ORDER BY e.enumsortorder) FROM pg_enum e WHERE e.enumtypid = t.oid)
                   AS labels
        FROM pg_attribute a
        JOIN pg_type t ON t.oid = a.atttypid
        LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
        WHERE a.attrelid = $1::regclass AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY a.attnum
        """,
        qualified(table),
    )


async def _foreign_keys(conn: asyncpg.Connection, table: str) -> dict[str, tuple[str, str]]:
    """``{column: (parent table, parent column)}`` for ``table``'s single-column foreign keys."""
    rows = await conn.fetch(
        """
        SELECT a.attname AS column, pn.nspname || '.' || pc.relname AS parent, pa.attname AS parent_column
        FROM pg_constraint k
        JOIN pg_class pc ON pc.oid = k.confrelid JOIN pg_namespace pn ON pn.oid = pc.relnamespace
        JOIN pg_attribute a ON a.attrelid = k.conrelid AND a.attnum = k.conkey[1]
        JOIN pg_attribute pa ON pa.attrelid = k.confrelid AND pa.attnum = k.confkey[1]
        WHERE k.conrelid = $1::regclass AND k.contype = 'f' AND cardinality(k.conkey) = 1
        """,
        qualified(table),
    )
    return {row["column"]: (row["parent"], row["parent_column"]) for row in rows}


def _value(table: str, spec: asyncpg.Record):
    """A value of ``spec``'s type that an application in use could have written."""
    if (override := OVERRIDES.get(table, {}).get(spec["attname"])) is not None:
        return override()
    name, category, typmod = spec["typname"], spec["typcategory"], spec["atttypmod"]
    if spec["typtype"] == "e":
        return spec["labels"][0]
    if name == "uuid":
        return str(uuid4())
    if category == "S":  # text, varchar(n), char(n): unique enough for a unique index
        length = typmod - 4 if typmod > 4 else 32
        return uuid4().hex[:length]
    if name in ("int2", "int4", "int8"):
        return uuid4().int % 30_000 + 1
    if name in ("float4", "float8", "numeric"):
        return 0.5
    if name == "bool":
        return False
    if name == "timestamptz":
        return datetime.now(UTC)
    if name == "timestamp":
        return datetime.now()
    if name == "date":
        return date.today()
    if name in ("json", "jsonb"):
        return "{}"
    if category == "A":
        return []
    pytest.fail(f"the walk's writer has no value for {table}.{spec['attname']} ({name}); add one to OVERRIDES")


def _parents_first(tables: list[str], keys: dict[str, dict[str, tuple[str, str]]]) -> list[str]:
    ordered: list[str] = []
    remaining = sorted(tables)
    while remaining:
        ready = [t for t in remaining if not any(p in remaining and p != t for p, _ in keys[t].values())]
        assert ready, f"foreign keys form a cycle among {remaining}"
        ordered.extend(ready)
        remaining = [t for t in remaining if t not in ready]
    return ordered


async def _write(conn: asyncpg.Connection, label: str) -> None:
    """A new row in every published table, then a rewrite of every published row, as a primary in use makes.

    Each row is built from the primary's catalog at that moment: before the primary
    upgrades it has the previous release's columns, after, the new ones. Every column
    without a default gets a value, nullable ones too, as the application would fill
    them, so a later NOT NULL tightening finds nothing to reject. A foreign key points
    at the row this round wrote in the parent table (a self-reference stays NULL).
    Rewriting a row sends all its columns, so a column the mirror doesn't have stops apply.
    """
    tables = await _published(conn)
    keys = {table: await _foreign_keys(conn, table) for table in tables}
    written: dict[str, dict[str, object]] = {}
    async with conn.transaction():
        for table in _parents_first(tables, keys):
            row: dict[str, object] = {}
            for spec in await _column_specs(conn, table):
                column = spec["attname"]
                if column in keys[table]:
                    parent, parent_column = keys[table][column]
                    if parent != table and parent in written:
                        row[column] = written[parent][parent_column]
                    elif spec["attnotnull"]:
                        row[column] = await conn.fetchval(
                            f"SELECT {quote_ident(parent_column)} FROM {qualified(parent)} LIMIT 1"
                        )
                elif not spec["has_default"]:
                    row[column] = _value(table, spec)
            columns = ", ".join(quote_ident(c) for c in row)
            placeholders = ", ".join(f"${n}" for n in range(1, len(row) + 1))
            returned = await conn.fetchrow(
                f"INSERT INTO {qualified(table)} ({columns}) VALUES ({placeholders}) RETURNING *", *row.values()
            )
            written[table] = dict(returned)
    for table in tables:
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
    await _write(p.primary.conn, "seed")
    p.mirror.replication("subscribe")
    await _caught_up(p, f"subscribed at {first}")

    for revision in later:
        p.mirror.migrate(revision)
        p.mirror.replication("grants")
        p.mirror.replication("refresh")
        await _write(p.primary.conn, f"{revision}, mirror ahead")
        await _caught_up(p, f"{revision} on the mirror only")

        p.primary.migrate(revision)
        p.primary.replication("publish")
        await _write(p.primary.conn, f"{revision}, both")
        p.mirror.replication("refresh")  # the hourly CronJob: copies tables first published now
        await _caught_up(p, f"{revision} on both")

    status = json.loads(p.mirror.replication("status"))["subscription"]
    assert status["exists"] and not status["tables_not_ready"]
    assert status["apply_error_count"] == 0 and status["sync_error_count"] == 0
    assert status["tables_ready"] == len(await _published(p.primary.conn))
    for side in (p.primary, p.mirror):
        assert await side.conn.fetchval("SELECT version_num FROM public.alembic_version") == later[-1]


async def test_mirror_at_head_while_the_primary_steps_up(pair: Pair):
    """One deploy can take the mirror through every pending revision while the primary waits."""
    p = pair
    ordered = revisions()
    start = oldest_primary_for_mirror_at_head()

    for side in (p.mirror, p.primary):
        side.migrate(start)
    p.mirror.replication("grants")
    p.primary.replication("publish")
    await _write(p.primary.conn, "seed")
    p.mirror.replication("subscribe")
    await _caught_up(p, f"subscribed at {start}")

    p.mirror.migrate("head")
    p.mirror.replication("grants")
    p.mirror.replication("refresh")
    await _write(p.primary.conn, f"mirror at head, primary at {start}")
    await _caught_up(p, f"mirror at head, primary at {start}")

    for revision in ordered[ordered.index(start) + 1 :]:
        p.primary.migrate(revision)
        p.primary.replication("publish")
        p.mirror.replication("refresh")
        await _write(p.primary.conn, f"mirror at head, primary at {revision}")
        await _caught_up(p, f"mirror at head, primary at {revision}")


async def test_out_of_order_changes_stop_apply(pair: Pair):
    """A column the primary sends and the mirror lacks stops apply until the mirror has it."""
    p = pair
    p.mirror.migrate("head")
    p.primary.migrate("head")
    p.primary.replication("publish")
    await _write(p.primary.conn, "seed")
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
