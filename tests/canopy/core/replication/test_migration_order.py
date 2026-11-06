"""Migrations released in the documented order keep a history head in step with its arms.

Opt-in, on the arm and head servers the end-to-end tests use (``bare_octopus``, two
arms). Every database is built by Alembic rather than ``create_all``. Each instance
has its own ``CANOPY_ROLE`` and ``CANOPY_INSTANCE``, and every step is a command a
deploy runs, as a subprocess (``python -m alembic``, ``python -m canopy.core.replication``),
except ``subscribe`` and ``refresh``: they also read each arm through its connection
string, which names the arm as the head's server sees it, so they run in-process with
the fixture's way of reaching the arms. The head's commands run as its tables' owner
role and an arm's as a superuser, as in production.

Replication starts at ``REPLICATION_BASE``, the ownership revision: the replication
code first shipped with it, and a deploy migrates an arm before ``publish``, which
comes before the head's ``subscribe``, so no subscription can exist below it. Below
the base each arm climbs alone, one revision at a time, writing as an instance in use.

``test_every_revision_in_release_order`` then subscribes the head to both arms at the
base and releases each later revision the way ``CONTRIBUTING.md`` describes: the
head migrates (``alembic upgrade``, ``grants``, ``refresh``) while both arms keep
writing on their old schema, then each arm in turn migrates (``alembic upgrade``,
``publish``), and the head's hourly ``refresh`` copies the tables that arm has started
publishing.

``test_head_at_head_while_the_arms_step_up`` covers a deploy that takes the head through
several revisions at once and arms at different revisions: the head goes straight to
head, a second arm is subscribed while the first streams, then the first arm climbs
one revision at a time while the second waits, and the second jumps to head in one
deploy. They start at the base, or at the newest revision a subscriber-side drop
declares with ``canopy_requires_primary_at``, since such a drop may only reach a head
whose arms all run that revision.

After each step every instance writes a round. Each arm inserts a row into every table
it publishes, built from its own schema at that moment and stamped with its own name,
then rewrites every published row. The head inserts rows it owns into every replicated
table, and every instance inserts a row into each local table. A column unique per
owner gets the same value from every instance in a round. The head then has to hold
exactly each arm's rows plus its own, compared per arm by primary key, with every row
stamped by its owner and no apply error. It also has to catch up right after it
migrates and right after each arm migrates, before anyone writes, so a migration's own
data change must reach it by replication. While the head migrates, triggers record what
its migration writes to replicated tables, which must be rows the head owns. A migration
that breaks the rules (a column an arm sends before the head has it, a column the head
drops first, a NOT NULL column without a default, a data step that changes another
owner's rows) fails the walk.

``test_out_of_order_changes_stop_only_that_arms_apply`` shows the failures that order prevents.
"""

import os
import re
import sys
import json
import asyncio
import subprocess
from uuid import uuid4
from hashlib import md5
from pathlib import Path
from datetime import UTC, date, datetime
from dataclasses import field, dataclass
from collections.abc import Callable, Awaitable

import pytest
import asyncpg
import pytest_asyncio
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy.engine import make_url

from canopy.core.arms import Arm, subscription_name
from canopy.core.replication.sql import (
    qualified,
    owner_join,
    quote_ident,
    tables_present,
)
from canopy.core.replication.registry import REGISTRY
from canopy.core.replication.migration import REQUIRES_PRIMARY_AT
from canopy.core.replication.subscriber import refresh, subscribe

from .conftest import Octopus, eventually

ROOT = Path(__file__).resolve().parents[4]
PUBLICATIONS = [publication.name for publication in REGISTRY.publications]

REPLICATION_BASE = "89e21441c63c"
"""The oldest revision a database with a subscription can run: the ownership revision."""

OWNER = "owner_instance"
ROOTS = sorted(table for table, path in REGISTRY.owner_paths.items() if path.column)


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
    """The base, or the newest revision a subscriber-side drop says every arm must run, if later."""
    ordered = revisions()
    script = _script()
    required = [getattr(script.get_revision(revision).module, REQUIRES_PRIMARY_AT, None) for revision in ordered]
    return max([REPLICATION_BASE, *(revision for revision in required if revision is not None)], key=ordered.index)


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
    head_dsn: str  # the head's database as its tables' owner, which runs its commands
    configured: dict[str, Arm]  # the head's CANOPY_ARMS
    connect_arm: Callable[[str], Awaitable[asyncpg.Connection]]
    rounds: int = field(default=0)  # write rounds so far

    async def subscribe(self, only: str | None = None) -> None:
        """``subscribe`` on the head, every arm or ``only``."""
        conn = await asyncpg.connect(self.head_dsn)
        try:
            await subscribe(
                conn, role="mirror", head=self.head.name, arms=self.configured, only=only, connect_arm=self.connect_arm
            )
        finally:
            await conn.close()

    async def refresh(self) -> None:
        """``refresh`` on the head, every arm: the hourly CronJob."""
        conn = await asyncpg.connect(self.head_dsn)
        try:
            await refresh(conn, role="mirror", arms=self.configured, connect_arm=self.connect_arm)
        finally:
            await conn.close()


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
    return Walk(
        head=Side(name=o.head.name, env=head_env, conn=o.head.admin),
        arms=arms,
        head_dsn=o.head.dsn,
        configured=o.arms_config.arms,
        connect_arm=o.connect_arm,
    )


# --------------------------------------------------------------------------- #
# The catalog
# --------------------------------------------------------------------------- #


async def _published(conn: asyncpg.Connection) -> list[str]:
    rows = await conn.fetch(
        "SELECT DISTINCT schemaname || '.' || tablename AS name FROM pg_publication_tables WHERE pubname = ANY($1)",
        PUBLICATIONS,
    )
    return sorted(row["name"] for row in rows)


async def _present(conn: asyncpg.Connection, tables: list[str]) -> list[str]:
    present = await tables_present(conn, REGISTRY.schemas)
    return [table for table in tables if table in present]


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


async def _unique_per_owner(conn: asyncpg.Connection, table: str) -> set[str]:
    """Columns of ``table`` in a unique index together with the owner column: unique per owner, not everywhere."""
    rows = await conn.fetch(
        """
        SELECT a.attname, o.attname AS owner
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        LEFT JOIN pg_attribute o ON o.attrelid = i.indrelid AND o.attnum = ANY(i.indkey) AND o.attname = $2
        WHERE i.indrelid = $1::regclass AND i.indisunique AND NOT i.indisprimary
        """,
        qualified(table),
        OWNER,
    )
    return {row["attname"] for row in rows if row["owner"] is not None and row["attname"] != OWNER}


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


def _text(seed: str, typmod: int) -> str:
    length = typmod - 4 if typmod > 4 else 32
    return md5(seed.encode()).hexdigest()[:length]


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
        return _text(uuid4().hex, typmod)
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


async def _insert_rows(
    conn: asyncpg.Connection,
    tables: list[str],
    *,
    owner: str | None = None,
    shared: dict[str, set[str]] | None = None,
    round_: int = 0,
) -> None:
    """A new row in each of ``tables``, built from the catalog at this moment, as an application in use writes it.

    Every column without a default gets a value, nullable ones too, so a later NOT NULL
    tightening finds nothing to reject. A root table's owner column gets ``owner``. A
    column in ``shared`` gets the same value from every instance in a round. A foreign
    key points at the row this call wrote in the parent table (a self-reference stays
    NULL), so child rows follow their parent's owner.
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
                elif column == OWNER and table in ROOTS and owner is not None:
                    row[column] = owner
                elif column in (shared or {}).get(table, ()):
                    row[column] = _text(f"{round_}:{table}:{column}", spec["atttypmod"])
                elif not spec["has_default"]:
                    row[column] = _value(table, spec, choices)
            columns = ", ".join(quote_ident(c) for c in row)
            placeholders = ", ".join(f"${n}" for n in range(1, len(row) + 1))
            returned = await conn.fetchrow(
                f"INSERT INTO {qualified(table)} ({columns}) VALUES ({placeholders}) RETURNING *", *row.values()
            )
            written[table] = dict(returned)


async def _rewrite(conn: asyncpg.Connection, tables: list[str]) -> None:
    """Rewrite every row of ``tables``: an UPDATE sends all its columns, so one the head lacks stops apply."""
    for table in tables:
        column = quote_ident((await _columns(conn, table))[0])
        await conn.execute(f"UPDATE {qualified(table)} SET {column} = {column}")


async def _forget_schemas(w: Walk) -> None:
    """Drop asyncpg's cached statements: a migration since they were prepared can change their rows' shape."""
    for side in (w.head, *w.arms):
        await side.conn.reload_schema_state()


async def _write(w: Walk, *, arms: list[Side] | None = None, head: bool = True) -> None:
    """One round of writes, as instances in use make them.

    Each arm (or each of ``arms``) inserts a row into every table it publishes, from its
    own schema at that moment, then rewrites every published row. The head inserts rows
    it owns into every replicated table. Every instance inserts a row into each of its
    local tables, so a local table's migration always runs over rows.
    """
    await _forget_schemas(w)
    w.rounds += 1
    replicated = await _present(w.head.conn, REGISTRY.replicated)
    shared = {table: await _unique_per_owner(w.head.conn, table) for table in replicated}
    for arm in w.arms if arms is None else arms:
        published = await _published(arm.conn)
        await _insert_rows(arm.conn, published, owner=arm.name, shared=shared, round_=w.rounds)
        await _rewrite(arm.conn, published)
        await _insert_rows(arm.conn, await _present(arm.conn, REGISTRY.local))
    if head:
        await _insert_rows(w.head.conn, replicated, owner=w.head.name, shared=shared, round_=w.rounds)
        await _insert_rows(w.head.conn, await _present(w.head.conn, REGISTRY.local))


async def _climb_to_the_base(w: Walk) -> None:
    """Each arm, alone, from the first revision to the base, writing as an instance in use after each."""
    for arm in w.arms:
        for revision in revisions()[: revisions().index(REPLICATION_BASE) + 1]:
            arm.migrate(revision)
            await arm.conn.reload_schema_state()
            await _insert_rows(arm.conn, await _present(arm.conn, REGISTRY.replicated), owner=arm.name)
            await _insert_rows(arm.conn, await _present(arm.conn, REGISTRY.local))


# --------------------------------------------------------------------------- #
# The head's view
# --------------------------------------------------------------------------- #


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


async def _owned(conn: asyncpg.Connection, table: str, key: list[str], owner: str) -> set[tuple]:
    """The primary keys of ``table``'s rows that ``owner`` owns, through the registry's owner path."""
    join, owner_sql = owner_join(REGISTRY, table, alias="t")
    selected = ", ".join(f"t.{quote_ident(c)}" for c in key)
    rows = await conn.fetch(f"SELECT {selected} FROM {qualified(table)} t {join} WHERE {owner_sql} = $1", owner)
    return {tuple(str(v) for v in row.values()) for row in rows}


async def _differing(w: Walk, *, arms: list[Side] | None = None) -> list[str]:
    """How the head differs from its arms (or ``arms``), on the tables whose copy is done.

    Per arm, every row the arm holds must be on the head, equal on the arm's columns,
    its owner stamp included. The head must hold no row beyond every subscribed arm's
    and its own, and stamp no root row with any other name.
    """
    differing: list[str] = []
    held: dict[str, set[tuple]] = {}
    for arm in w.arms if arms is None else arms:
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
    if arms is not None:
        return differing
    for table, keys in sorted(held.items()):
        key = await _primary_key(w.head.conn, table)
        on_head = set(await _rows(w.head.conn, table, [], key))
        if extra := on_head - keys - await _owned(w.head.conn, table, key, w.head.name):
            differing.append(f"{table}: {len(extra)} row(s) on the head that neither an arm nor the head owns")
    owners = {w.head.name, *(arm.name for arm in w.arms)}
    for table in await _present(w.head.conn, ROOTS):
        stamps = {row[OWNER] for row in await w.head.conn.fetch(f"SELECT DISTINCT {OWNER} FROM {qualified(table)}")}
        if strange := sorted(str(stamp) for stamp in stamps - owners):
            differing.append(f"{table}: rows on the head stamped {', '.join(strange)}")
    return differing


async def _in_step(w: Walk, *, arms: list[Side] | None = None) -> bool:
    return not await _differing(w, arms=arms)


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


async def _owner_of(w: Walk, table: str, row: dict, written: dict[tuple[str, str], str | None]) -> str | None:
    """Who owns ``row`` of ``table`` on the head: its stamp, or its parent's, followed to the root."""
    for parent, key in REGISTRY.owner_chain(table):
        if row.get(key) is None:
            return None
        if (parent, row[key]) in written:  # the migration changed or deleted the parent too
            return written[parent, row[key]]
        table, row = (
            parent,
            dict(await w.head.conn.fetchrow(f"SELECT * FROM {qualified(parent)} WHERE id = $1", row[key]) or {}),
        )
    return row.get(OWNER)


async def _migrate_head(w: Walk, revision: str) -> None:
    """``alembic upgrade`` on the head while triggers record what its migration writes, then ``grants`` and ``refresh``.

    The triggers fire for the migration's own INSERT, UPDATE and DELETE statements but
    not for apply, which runs with ``session_replication_role = replica``, nor for a
    table rewrite. Every row they record must be one the head owns: a data step that
    changed an arm's rows on the head as well as on the arm would show up here.
    """
    conn = w.head.conn
    tables = await _present(conn, REGISTRY.replicated)
    await conn.execute(
        """
        CREATE SCHEMA walk_audit;
        CREATE TABLE walk_audit.writes (tbl text, op text, old jsonb, new jsonb);
        CREATE FUNCTION walk_audit.record() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER
        SET search_path = pg_catalog AS $$
        BEGIN
            INSERT INTO walk_audit.writes VALUES (
                TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME, TG_OP,
                CASE WHEN TG_OP <> 'INSERT' THEN to_jsonb(OLD) END,
                CASE WHEN TG_OP <> 'DELETE' THEN to_jsonb(NEW) END);
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
        writes = [
            (row["tbl"], row["op"], json.loads(row["old"] or "null"), json.loads(row["new"] or "null"))
            for row in await conn.fetch("SELECT tbl, op, old::text, new::text FROM walk_audit.writes")
        ]
    finally:
        await conn.execute("DROP SCHEMA walk_audit CASCADE")  # the function, and with it every trigger
    await conn.reload_schema_state()
    root_rows = {
        (table, version["id"]): version.get(OWNER)
        for table, _, old, new in writes
        for version in (old, new)
        if version is not None and table in ROOTS
    }
    foreign: dict[tuple[str, str], int] = {}
    for table, op, old, new in writes:
        for version in (old, new):
            if version is not None and await _owner_of(w, table, version, root_rows) != w.head.name:
                foreign[table, op] = foreign.get((table, op), 0) + 1
    if foreign:
        pytest.fail(
            f"migrating the head to {revision} wrote rows it doesn't own: "
            + ", ".join(f"{op} x{n} on {table}" for (table, op), n in sorted(foreign.items()))
            + f". A data step changes only the rows the instance being migrated owns ({OWNER} = local_instance())"
        )
    w.head.replication("grants")
    await w.refresh()


# --------------------------------------------------------------------------- #
# The walks
# --------------------------------------------------------------------------- #


async def _start_at(w: Walk, revision: str, *, only: str | None = None) -> None:
    """Every instance at ``revision``, each arm climbing to the base first; then the head subscribes."""
    await _climb_to_the_base(w)
    w.head.migrate(revision)
    w.head.replication("grants")
    for arm in w.arms:
        arm.migrate(revision)
        arm.replication("publish")
    await _write(w)
    await w.subscribe(only=only)
    await _caught_up(w, f"subscribed at {revision}")


@pytest.mark.parametrize("bare_octopus", [2], indirect=True)
async def test_every_revision_in_release_order(walk: Walk):
    w = walk
    ordered = revisions()
    await _start_at(w, REPLICATION_BASE)

    for revision in ordered[ordered.index(REPLICATION_BASE) + 1 :]:
        await _migrate_head(w, revision)
        await _caught_up(w, f"{revision} on the head, before anyone writes")
        await _write(w)
        await _caught_up(w, f"{revision} on the head only")

        for arm in w.arms:
            arm.migrate(revision)
            arm.replication("publish")
            await _caught_up(w, f"{revision} on {arm.name}, before anyone writes")
            await _write(w)
            await w.refresh()  # the hourly CronJob: copies tables first published now
            await _caught_up(w, f"{revision} on the head and {arm.name}")

    status = json.loads(w.head.replication("status"))
    assert [sub["arm"] for sub in status["subscriptions"]] == [arm.name for arm in w.arms]
    assert status["unexpected"] == []
    for arm, sub in zip(w.arms, status["subscriptions"]):
        assert sub["exists"] and not sub["tables_not_ready"]
        assert sub["apply_error_count"] == 0 and sub["sync_error_count"] == 0
        assert sub["tables_ready"] == len(await _published(arm.conn))
    for side in (w.head, *w.arms):
        assert await side.conn.fetchval("SELECT version_num FROM public.alembic_version") == ordered[-1]


@pytest.mark.parametrize("bare_octopus", [2], indirect=True)
async def test_head_at_head_while_the_arms_step_up(walk: Walk):
    """One deploy can take the head through every pending revision while its arms wait, each at its own revision."""
    w = walk
    first, second = w.arms
    ordered = revisions()
    start = oldest_arm_revision_for_head_at_head()
    await _start_at(w, start, only=first.name)

    await _migrate_head(w, "head")
    await _caught_up(w, f"the head at head, the arms at {start}, before anyone writes")
    await _write(w)
    await _caught_up(w, f"the head at head, the arms at {start}")

    # A second arm joins a head that holds the first arm's rows and its own.
    await w.subscribe(only=second.name)
    await _caught_up(w, f"{second.name} subscribed to a running head")

    for revision in ordered[ordered.index(start) + 1 :]:
        first.migrate(revision)
        first.replication("publish")
        await w.refresh()
        await _write(w)
        await _caught_up(w, f"the head at head, {first.name} at {revision}, {second.name} at {start}")

    # The second arm missed every release: one deploy takes it to head.
    second.migrate("head")
    second.replication("publish")
    await w.refresh()
    await _write(w)
    await _caught_up(w, "every instance at head")


@pytest.mark.parametrize("bare_octopus", [2], indirect=True)
async def test_out_of_order_changes_stop_only_that_arms_apply(walk: Walk):
    """A column an arm sends and the head lacks stops that arm's apply, and only that arm's, until the head has it."""
    w = walk
    first, second = w.arms
    await _start_at(w, "head")

    # Added on an arm first.
    await first.conn.execute("ALTER TABLE squirrel.tag_group ADD COLUMN zz_added text")
    await first.conn.execute("UPDATE squirrel.tag_group SET zz_added = 'new'")
    await eventually(lambda: _more_errors(w, first, 0))
    await _write(w, arms=[second], head=False)
    await eventually(lambda: _in_step(w, arms=[second]))
    await w.head.conn.execute("ALTER TABLE squirrel.tag_group ADD COLUMN zz_added text")
    await eventually(lambda: _in_step(w))

    # Dropped on the head first.
    errors = await _errors(w, first)
    await w.head.conn.execute("ALTER TABLE squirrel.tag_group DROP COLUMN description")
    await first.conn.execute("UPDATE squirrel.tag_group SET description = 'edited'")
    await eventually(lambda: _more_errors(w, first, errors))
    assert not await _errors(w, second)
    await w.head.conn.execute("ALTER TABLE squirrel.tag_group ADD COLUMN description text")
    # The head lost every owner's values in the drop: the first arm's come back with the
    # change it was stuck on, the second arm's only once it sends its rows again.
    assert not await _in_step(w)
    await _rewrite(second.conn, ["squirrel.tag_group"])
    await eventually(lambda: _in_step(w))


def test_the_base_is_the_ownership_revision():
    """Replication starts where rows first carry their owner, and nothing below asks for a primary revision."""
    ordered = revisions()
    script = _script()
    module = script.get_revision(REPLICATION_BASE).module
    assert "owner_instance" in Path(module.__file__).read_text()
    below = ordered[: ordered.index(REPLICATION_BASE)]
    assert not [rev for rev in below if getattr(script.get_revision(rev).module, REQUIRES_PRIMARY_AT, None)]


async def test_the_writer_meets_a_check_on_a_list_of_values(db):
    """A column limited to a list of values, like the outbox's state, gets one of them."""
    await db.execute("CREATE TABLE squirrel.zz_check (id uuid PRIMARY KEY, state varchar(16) NOT NULL, body bytea)")
    await db.execute("ALTER TABLE squirrel.zz_check ADD CHECK (state IN ('pending', 'it''s', 'failed'))")
    assert await _check_choices(db, "squirrel.zz_check") == {"state": "pending"}
    await _insert_rows(db, ["squirrel.zz_check"])
    assert await db.fetchval("SELECT state FROM squirrel.zz_check") == "pending"
