"""Migrations released in the documented order keep a history head in step with its arms.

Opt-in, on the arm and head servers the end-to-end tests use (``bare_octopus``). Every
database is built by Alembic rather than ``create_all``, and every step is a command a
deploy runs, as a subprocess: ``python -m alembic`` and ``python -m canopy.core.replication``.
Each instance has its own ``CANOPY_ROLE`` and ``CANOPY_INSTANCE``; the head's commands run
as its tables' owner role and an arm's as a superuser, as in production.

``test_every_revision_in_release_order`` starts every instance at the first revision and
subscribes the head to each arm, then releases each later revision the way
``CONTRIBUTING.md`` describes: the head migrates (``alembic upgrade``, ``grants``,
``refresh``) while every arm keeps writing on its old schema, then each arm in turn
migrates (``alembic upgrade``, ``publish``), and the head's hourly ``refresh`` copies the
tables that arm has started publishing.

``test_head_at_head_while_the_arms_step_up`` covers a deploy that takes the head through
several revisions at once: the head goes straight to head, and each arm then climbs one
revision at a time. They start at the newest revision a subscriber-side drop declares
with ``canopy_requires_primary_at``, since such a drop may only reach a head whose arms
all run that revision.

After each step every instance writes a round: each arm inserts a row into every table
it publishes, built from its own schema at that moment, and rewrites every published
row, and every instance inserts a row into each of its local tables. The head then has
to hold exactly the arms' rows, compared per arm by primary key, with no apply error.
It also has to catch up right after it migrates and right after each arm migrates,
before anyone writes, so a migration's own data change must reach it by replication;
and while the head migrates, triggers record what its migration writes to replicated
tables, which may not touch any arm's rows. A migration that breaks the rules (a column
an arm sends before the head has it, a column the head drops first, a NOT NULL column
without a default, a data step run on the head as well as on the arms) fails the walk.

``test_out_of_order_changes_stop_an_arms_apply`` shows the failures that order prevents.
"""

import os
import re
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

from canopy.core.arms import subscription_name
from canopy.core.replication.sql import qualified, quote_ident, tables_present
from canopy.core.replication.registry import REGISTRY
from canopy.core.replication.migration import REQUIRES_PRIMARY_AT

from .conftest import Octopus, eventually

ROOT = Path(__file__).resolve().parents[4]
PUBLICATIONS = [publication.name for publication in REGISTRY.publications]

# A head subscribes to one arm until rows carry their owner (canopy.core.replication.subscriber).
ARMS = 1


def _script() -> ScriptDirectory:
    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(ROOT / "migrations"))
    script = ScriptDirectory.from_config(config)
    assert len(script.get_heads()) == 1, f"the migration tree has more than one head: {script.get_heads()}"
    return script


def revisions() -> list[str]:
    """Every revision of the shipped migration tree, oldest first."""
    return [revision.revision for revision in reversed(list(_script().walk_revisions()))]


def oldest_arm_revision_for_head_at_head() -> str:
    """The first revision, or the newest one a subscriber-side drop says every arm must run."""
    ordered = revisions()
    script = _script()
    required = [getattr(script.get_revision(revision).module, REQUIRES_PRIMARY_AT, None) for revision in ordered]
    required = [revision for revision in required if revision is not None]
    return max(required, key=ordered.index) if required else ordered[0]


@dataclass
class Side:
    """One instance, driven the way its deploy drives it."""

    name: str  # its CANOPY_INSTANCE
    env: dict[str, str]  # the whole environment of its commands, so a local .env can't leak in
    conn: asyncpg.Connection  # its database, as a superuser

    def run(self, *args: str) -> str:
        result = subprocess.run(
            [sys.executable, "-m", *args], cwd=ROOT, env=self.env, capture_output=True, text=True, timeout=300
        )
        if result.returncode != 0:
            pytest.fail(f"`{' '.join(args)}` on {self.name} exited {result.returncode}:\n{result.stderr}")
        return result.stdout

    def migrate(self, revision: str) -> None:
        self.run("alembic", "upgrade", revision)

    def replication(self, *args: str) -> str:
        return self.run("canopy.core.replication", *args)


@dataclass
class Walk:
    head: Side
    arms: list[Side]  # in arm order


def _app_url(dsn: str) -> str:
    """``CANOPY_DATABASE_URL`` for ``dsn``."""
    return make_url(dsn).set(drivername="postgresql+asyncpg").render_as_string(hide_password=False)


@pytest_asyncio.fixture
async def walk(bare_octopus: Octopus) -> Walk:
    """The arms and the head of ``bare_octopus``, each with the environment its deploy sets."""
    o = bare_octopus
    common = {
        "PATH": os.environ.get("PATH", ""),
        "CANOPY_REPLICATION__REPLICATOR_ROLE": o.config.replicator_role,
        "CANOPY_REPLICATION__READER_ROLE": o.config.reader_role,
        "CANOPY_REPLICATION__APP_ROLE": o.config.app_role,
    }
    arms = [
        Side(
            name=name,
            env={**common, "CANOPY_ROLE": "primary", "CANOPY_INSTANCE": name, "CANOPY_DATABASE_URL": _app_url(arm.dsn)},
            conn=arm.admin,
        )
        for name, arm in o.arms.items()
    ]
    head_env = {
        **common,
        "CANOPY_ROLE": "mirror",
        "CANOPY_INSTANCE": o.head.name,
        "CANOPY_DATABASE_URL": _app_url(o.head.dsn),
        **{f"CANOPY_ARMS__{name.upper()}__PUBLISHER_DSN": arm.publisher_dsn for name, arm in o.arms.items()},
    }
    return Walk(head=Side(name=o.head.name, env=head_env, conn=o.head.admin), arms=arms)


# --------------------------------------------------------------------------- #
# The catalog
# --------------------------------------------------------------------------- #


async def _published(conn: asyncpg.Connection) -> list[str]:
    rows = await conn.fetch(
        "SELECT DISTINCT schemaname || '.' || tablename AS name FROM pg_publication_tables WHERE pubname = ANY($1)",
        PUBLICATIONS,
    )
    return sorted(row["name"] for row in rows)


async def _subscribed(conn: asyncpg.Connection, arm: str, *, ready: bool = True) -> list[str]:
    """Tables in the head's subscription to ``arm``: those whose copy is done, or (``ready=False``) the rest."""
    rows = await conn.fetch(
        """
        SELECT n.nspname || '.' || c.relname AS name
        FROM pg_subscription_rel r
        JOIN pg_subscription s ON s.oid = r.srsubid
        JOIN pg_class c ON c.oid = r.srrelid JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE s.subname = $1 AND (r.srsubstate = 'r') = $2
        """,
        subscription_name(arm),
        ready,
    )
    return sorted(row["name"] for row in rows)


async def _columns(conn: asyncpg.Connection, table: str) -> list[str]:
    rows = await conn.fetch(
        "SELECT attname FROM pg_attribute WHERE attrelid = $1::regclass AND attnum > 0 AND NOT attisdropped "
        "ORDER BY attnum",
        qualified(table),
    )
    return [row["attname"] for row in rows]


async def _primary_key(conn: asyncpg.Connection, table: str) -> list[str]:
    rows = await conn.fetch(
        """
        SELECT a.attname FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = $1::regclass AND i.indisprimary ORDER BY array_position(i.indkey, a.attnum)
        """,
        qualified(table),
    )
    return [row["attname"] for row in rows]


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


# The first literal of a CHECK that limits one column to a list of values, as
# pg_get_constraintdef renders `col IN ('a', 'b')`: `(col = ANY (ARRAY['a'::text, ...]))`.
_CHOICES = re.compile(r"= ANY \(+ARRAY\['((?:[^']|'')*)'")


async def _check_choices(conn: asyncpg.Connection, table: str) -> dict[str, str]:
    """``{column: a value its CHECK accepts}`` for single-column CHECKs on a list of values."""
    rows = await conn.fetch(
        """
        SELECT a.attname, pg_get_constraintdef(k.oid) AS definition
        FROM pg_constraint k JOIN pg_attribute a ON a.attrelid = k.conrelid AND a.attnum = k.conkey[1]
        WHERE k.conrelid = $1::regclass AND k.contype = 'c' AND cardinality(k.conkey) = 1
        """,
        qualified(table),
    )
    choices = {}
    for row in rows:
        if match := _CHOICES.search(row["definition"]):
            choices[row["attname"]] = match.group(1).replace("''", "'")
    return choices


# --------------------------------------------------------------------------- #
# The writer
# --------------------------------------------------------------------------- #

# A value for a column the generic writer can't make (a CHECK it doesn't meet, say):
# {"schema.table": {"column": callable returning the value}}.
OVERRIDES: dict[str, dict[str, object]] = {}


def _value(table: str, spec: asyncpg.Record, choices: dict[str, str]):
    """A value of ``spec``'s type that an application in use could have written."""
    if (override := OVERRIDES.get(table, {}).get(spec["attname"])) is not None:
        return override()
    if (choice := choices.get(spec["attname"])) is not None:
        return choice
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
    if name == "bytea":
        return b""
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


async def _insert_rows(conn: asyncpg.Connection, tables: list[str]) -> None:
    """A new row in each of ``tables``, built from the catalog at this moment, as an application in use writes it.

    Every column without a default gets a value, nullable ones too, so a later NOT NULL
    tightening finds nothing to reject. A foreign key points at the row this call wrote
    in the parent table (a self-reference stays NULL).
    """
    keys = {table: await _foreign_keys(conn, table) for table in tables}
    written: dict[str, dict[str, object]] = {}
    async with conn.transaction():
        for table in _parents_first(tables, keys):
            choices = await _check_choices(conn, table)
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
                    row[column] = _value(table, spec, choices)
            columns = ", ".join(quote_ident(c) for c in row)
            placeholders = ", ".join(f"${n}" for n in range(1, len(row) + 1))
            returned = await conn.fetchrow(
                f"INSERT INTO {qualified(table)} ({columns}) VALUES ({placeholders}) RETURNING *", *row.values()
            )
            written[table] = dict(returned)


async def _local_tables(conn: asyncpg.Connection) -> list[str]:
    present = await tables_present(conn, REGISTRY.schemas)
    return [table for table in REGISTRY.local if table in present]


async def _write(w: Walk) -> None:
    """One round of writes, as instances in use make them.

    Each arm inserts a row into every table it publishes, from its own schema at that
    moment (before an arm upgrades it has the previous release's columns), then rewrites
    every published row: a rewrite sends all its columns, so a column the head doesn't
    have stops apply. Every instance inserts a row into each of its local tables, so a
    local table's migration always runs over rows.
    """
    await _forget_schemas(w)
    for arm in w.arms:
        published = await _published(arm.conn)
        await _insert_rows(arm.conn, published)
        for table in published:
            column = quote_ident((await _columns(arm.conn, table))[0])
            await arm.conn.execute(f"UPDATE {qualified(table)} SET {column} = {column}")
    for side in (w.head, *w.arms):
        await _insert_rows(side.conn, await _local_tables(side.conn))


# --------------------------------------------------------------------------- #
# The head's view
# --------------------------------------------------------------------------- #


async def _forget_schemas(w: Walk) -> None:
    """Drop asyncpg's cached statements: a migration since they were prepared can change their rows' shape."""
    for side in (w.head, *w.arms):
        await side.conn.reload_schema_state()


async def _errors(w: Walk, arm: Side) -> int:
    return await w.head.conn.fetchval(
        "SELECT coalesce(sum(apply_error_count + sync_error_count), 0) FROM pg_stat_subscription_stats "
        "WHERE subname = $1",
        subscription_name(arm.name),
    )


async def _more_errors(w: Walk, arm: Side, count: int) -> bool:
    return await _errors(w, arm) > count


async def _rows(conn: asyncpg.Connection, table: str, columns: list[str], key: list[str]) -> dict[tuple, tuple]:
    """``{primary key: row}`` over ``columns``, every value as text so both sides compare alike."""
    selected = ", ".join(quote_ident(c) for c in [*key, *columns])
    rows = await conn.fetch(f"SELECT {selected} FROM {qualified(table)}")
    return {
        tuple(str(v) for v in list(row.values())[: len(key)]): tuple(str(v) for v in list(row.values())[len(key) :])
        for row in rows
    }


async def _differing(w: Walk) -> list[str]:
    """How the head differs from its arms, on the tables whose copy is done.

    Per arm, every row the arm holds must be on the head, equal on the arm's columns;
    and the head must hold no row that no arm holds.
    """
    differing: list[str] = []
    held: dict[str, set[tuple]] = {}
    for arm in w.arms:
        ready = set(await _subscribed(w.head.conn, arm.name))
        for table in await _published(arm.conn):
            if table not in ready:
                continue
            key = await _primary_key(arm.conn, table)
            columns = await _columns(arm.conn, table)
            if missing := sorted(set(columns) - set(await _columns(w.head.conn, table))):
                differing.append(f"{table}: the head lacks {', '.join(missing)} from {arm.name}")
                continue
            mine = await _rows(arm.conn, table, columns, key)
            on_head = await _rows(w.head.conn, table, columns, key)
            if wrong := [k for k, row in mine.items() if on_head.get(k) != row]:
                differing.append(f"{table}: {len(wrong)} of {arm.name}'s rows missing or different on the head")
            held.setdefault(table, set()).update(mine)
    for table, keys in sorted(held.items()):
        on_head = await _rows(w.head.conn, table, [], await _primary_key(w.head.conn, table))
        if extra := set(on_head) - keys:
            differing.append(f"{table}: {len(extra)} row(s) on the head that no arm holds")
    return differing


async def _in_step(w: Walk) -> bool:
    return not await _differing(w)


async def _caught_up(w: Walk, when: str, *, timeout: float = 60.0) -> None:
    """Wait until the head holds every arm's rows; fail at once on an apply or copy error."""
    await _forget_schemas(w)
    for arm in w.arms:
        if leaked := sorted(set(await _published(arm.conn)) & set(REGISTRY.local)):
            pytest.fail(f"{when}: {arm.name} publishes local tables: {', '.join(leaked)}")
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        for arm in w.arms:
            if errors := await _errors(w, arm):
                pytest.fail(
                    f"{when}: the head's apply from {arm.name} failed {errors} time(s); "
                    "the head's server log has the reason"
                )
        copying = {arm.name: await _subscribed(w.head.conn, arm.name, ready=False) for arm in w.arms}
        differing = await _differing(w)
        if not (any(copying.values()) or differing):
            return
        if loop.time() > deadline:
            pytest.fail(
                f"{when}: the head didn't catch up within {timeout}s "
                f"(still copying: {copying}; differing: {'; '.join(differing) or 'none'})"
            )
        await asyncio.sleep(0.2)


async def _migrate_head(w: Walk, revision: str) -> None:
    """``alembic upgrade`` on the head while triggers record what its migration writes, then ``grants`` and ``refresh``.

    The triggers fire for the migration's own INSERT, UPDATE and DELETE statements but
    not for apply, which runs with ``session_replication_role = replica``, nor for a
    table rewrite. Every row on the head belongs to an arm, so a data step that ran on
    the head as well as on the arms (rows each arm's own migration changes, and its
    result replicates) shows up here.
    """
    conn = w.head.conn
    tables = sorted(set(REGISTRY.replicated) & await tables_present(conn, REGISTRY.schemas))
    await conn.execute(
        """
        CREATE SCHEMA walk_audit;
        CREATE TABLE walk_audit.writes (tbl text, op text);
        CREATE FUNCTION walk_audit.record() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER
        SET search_path = pg_catalog AS $$
        BEGIN
            INSERT INTO walk_audit.writes VALUES (TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME, TG_OP);
            RETURN NULL;
        END $$;
        """
    )
    for table in tables:
        await conn.execute(
            f"CREATE TRIGGER walk_audit AFTER INSERT OR UPDATE OR DELETE ON {qualified(table)} "
            "FOR EACH ROW EXECUTE FUNCTION walk_audit.record()"
        )
    try:
        w.head.migrate(revision)
        writes = await conn.fetch("SELECT tbl, op, count(*) AS n FROM walk_audit.writes GROUP BY tbl, op ORDER BY 1, 2")
    finally:
        await conn.execute("DROP SCHEMA walk_audit CASCADE")  # the function, and with it every trigger
    if writes:
        pytest.fail(
            f"migrating the head to {revision} wrote arms' rows: "
            + ", ".join(f"{row['op']} x{row['n']} on {row['tbl']}" for row in writes)
            + ". A data step on a replicated table runs on the arms only, under `if on_publisher():`"
        )
    w.head.replication("grants")
    w.head.replication("refresh")


# --------------------------------------------------------------------------- #
# The walks
# --------------------------------------------------------------------------- #


def _subscribe_at(w: Walk, revision: str) -> None:
    """Every instance at ``revision``, the head first, each arm publishing; then the head subscribes."""
    w.head.migrate(revision)
    w.head.replication("grants")
    w.head.replication("refresh")  # no subscription yet: a no-op, as on a first deploy
    for arm in w.arms:
        arm.migrate(revision)
        arm.replication("publish")


@pytest.mark.parametrize("bare_octopus", [ARMS], indirect=True)
async def test_every_revision_in_release_order(walk: Walk):
    w = walk
    first, *later = revisions()

    # The first release: the head migrates first, then every arm publishes, then the
    # one-time subscribe copies what each arm holds.
    _subscribe_at(w, first)
    await _write(w)
    w.head.replication("subscribe")
    await _caught_up(w, f"subscribed at {first}")

    for revision in later:
        await _migrate_head(w, revision)
        await _caught_up(w, f"{revision} on the head, before anyone writes")
        await _write(w)
        await _caught_up(w, f"{revision} on the head only")

        for arm in w.arms:
            arm.migrate(revision)
            arm.replication("publish")
            await _caught_up(w, f"{revision} on {arm.name}, before anyone writes")
            await _write(w)
            w.head.replication("refresh")  # the hourly CronJob: copies tables first published now
            await _caught_up(w, f"{revision} on the head and {arm.name}")

    status = json.loads(w.head.replication("status"))
    assert [sub["arm"] for sub in status["subscriptions"]] == [arm.name for arm in w.arms]
    assert status["unexpected"] == []
    for arm, sub in zip(w.arms, status["subscriptions"]):
        assert sub["exists"] and not sub["tables_not_ready"]
        assert sub["apply_error_count"] == 0 and sub["sync_error_count"] == 0
        assert sub["tables_ready"] == len(await _published(arm.conn))
    for side in (w.head, *w.arms):
        assert await side.conn.fetchval("SELECT version_num FROM public.alembic_version") == later[-1]


@pytest.mark.parametrize("bare_octopus", [ARMS], indirect=True)
async def test_head_at_head_while_the_arms_step_up(walk: Walk):
    """One deploy can take the head through every pending revision while its arms wait."""
    w = walk
    ordered = revisions()
    start = oldest_arm_revision_for_head_at_head()

    _subscribe_at(w, start)
    await _write(w)
    w.head.replication("subscribe")
    await _caught_up(w, f"subscribed at {start}")

    await _migrate_head(w, "head")
    await _caught_up(w, f"the head at head, the arms at {start}, before anyone writes")
    await _write(w)
    await _caught_up(w, f"the head at head, the arms at {start}")

    for arm in w.arms:
        for revision in ordered[ordered.index(start) + 1 :]:
            arm.migrate(revision)
            arm.replication("publish")
            w.head.replication("refresh")
            await _write(w)
            await _caught_up(w, f"the head at head, {arm.name} at {revision}")


@pytest.mark.parametrize("bare_octopus", [ARMS], indirect=True)
async def test_out_of_order_changes_stop_an_arms_apply(walk: Walk):
    """A column an arm sends and the head lacks stops that arm's apply until the head has it."""
    w = walk
    arm = w.arms[0]
    _subscribe_at(w, "head")
    await _write(w)
    w.head.replication("subscribe")
    await _caught_up(w, "subscribed at head")

    # Added on the arm first.
    await arm.conn.execute("ALTER TABLE squirrel.tag_group ADD COLUMN zz_added text")
    await arm.conn.execute("UPDATE squirrel.tag_group SET zz_added = 'new'")
    await eventually(lambda: _more_errors(w, arm, 0))
    await w.head.conn.execute("ALTER TABLE squirrel.tag_group ADD COLUMN zz_added text")
    await eventually(lambda: _in_step(w))

    # Dropped on the head first.
    errors = await _errors(w, arm)
    await w.head.conn.execute("ALTER TABLE squirrel.tag_group DROP COLUMN description")
    await arm.conn.execute("UPDATE squirrel.tag_group SET description = 'edited'")
    await eventually(lambda: _more_errors(w, arm, errors))
    await w.head.conn.execute("ALTER TABLE squirrel.tag_group ADD COLUMN description text")
    await eventually(lambda: _in_step(w))


async def test_the_writer_meets_a_check_on_a_list_of_values(db):
    """A column limited to a list of values, like the outbox's state, gets one of them."""
    await db.execute("CREATE TABLE squirrel.zz_check (id uuid PRIMARY KEY, state varchar(16) NOT NULL, body bytea)")
    await db.execute("ALTER TABLE squirrel.zz_check ADD CHECK (state IN ('pending', 'it''s', 'failed'))")
    assert await _check_choices(db, "squirrel.zz_check") == {"state": "pending"}
    await _insert_rows(db, ["squirrel.zz_check"])
    assert await db.fetchval("SELECT state FROM squirrel.zz_check") == "pending"
