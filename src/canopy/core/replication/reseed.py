"""``reseed``: fill a mirror's gap from its primary, then hand over to replication at one WAL position.

After the primary's slot is lost, or the mirror is restored to an earlier point in
time, the changes the mirror is missing are no longer in any slot. ``reseed --since
<ts>`` copies them from the primary, read as the reader role:

* every reference table, fully reconciled: rows the primary no longer has are
  deleted, the rest are inserted or updated;
* every time-series parent the primary has, inserted or updated; parents the primary
  no longer has are never deleted, since once the primary prunes old rows history is
  the only copy;
* the children of each parent the mirror was missing, and of each parent created (or,
  where the table has ``updated_at``, updated) at or after ``--since``, reconciled per
  parent.

It runs with the subscription disabled, creating it disabled and without the initial
copy when there is none. The hand-over:

1. Take ``B = pg_current_xact_id()`` on the primary and wait until no lower xid is
   still running. Read ``x0 = pg_current_wal_insert_lsn()``. Open a repeatable-read
   snapshot whose first statement takes another xid, ``X``.
2. ``X = B + 1`` means no other transaction got an xid in between. Everything that
   committed before ``x0`` is then visible in the snapshot, and nothing committed
   between ``x0`` and the snapshot. Otherwise the attempt is dropped and the cut is
   tried again, for up to ``--cut-wait`` seconds; a primary that never pauses writing
   that long is refused, and nothing changes.
3. Copy from that snapshot into the mirror in one transaction. Inside it, unique
   values the primary has moved between rows are set aside first, parents are written
   before children, and rows the primary no longer has are deleted last, children
   first.
4. Once the copy has committed, advance the subscription's replication origin to
   ``x0`` and enable the subscription. Apply resumes with the first transaction that
   committed after ``x0``: nothing is replayed and nothing is lost.

The origin's progress isn't transactional, so it moves only after the copy is safely
in. If a run fails, the subscription stays disabled: run ``reseed`` again, and never
enable the subscription by hand.

``--since`` must not be later than the newest snapshot the mirror holds. Missing
parents are found by id whatever their time, but children of a parent both sides hold
are re-read only from ``--since`` on. Earlier is always safe, only slower.

Run it as a superuser of the mirror's database, or as the tables' and
subscription's owner with EXECUTE on ``pg_replication_origin_advance(text, pg_lsn)``.
"""

from __future__ import annotations

import sys
import json
import asyncio
import logging
import argparse
from datetime import UTC, datetime
from dataclasses import dataclass
from collections.abc import Mapping, Callable, Awaitable

import asyncpg

from canopy.core.config import get_settings
from canopy.core.replication.sql import (
    Refused,
    ReplicationError,
    redact,
    qualified,
    asyncpg_dsn,
    quote_ident,
    is_superuser,
    require_role,
    tables_present,
)
from canopy.core.replication.drift import (
    SESSION,
    TIME_SERIES_BUCKETS,
    Bucketing,
    primary_key,
    table_columns,
)
from canopy.core.replication.config import ReplicationConfig, get_replication_config
from canopy.core.replication.registry import REGISTRY, TableClass, ReplicationRegistry
from canopy.core.replication.subscriber import (
    subscribe,
    _subscription,
    subscription_name,
    _subscribed_tables,
)

logger = logging.getLogger(__name__)

CUT_WAIT_S = 60.0
"""Default for ``--cut-wait``: how long to keep trying for an exact cut before refusing."""

WORKER_WAIT_S = 10.0
"""How long to wait for a just-disabled subscription's apply worker to exit."""

CHUNK_ROWS = 10_000


@dataclass(frozen=True)
class Copy:
    """One table to re-seed and which of its rows."""

    table: str
    kind: TableClass
    columns: tuple[str, ...]  # present on both sides, in the mirror's order
    key: tuple[str, ...]
    bucketing: Bucketing | None = None  # time series
    updated_column: str | None = None  # time-series parent: also selected by this, when present
    parent_id: str = "id"  # time-series child: the parent's primary key column


async def _foreign_keys(conn: asyncpg.Connection, tables: list[str]) -> set[tuple[str, str]]:
    """``(child, parent)`` for each foreign key between two of ``tables``."""
    rows = await conn.fetch(
        """
        SELECT cn.nspname || '.' || c.relname AS child, pn.nspname || '.' || p.relname AS parent
        FROM pg_constraint k
        JOIN pg_class c ON c.oid = k.conrelid JOIN pg_namespace cn ON cn.oid = c.relnamespace
        JOIN pg_class p ON p.oid = k.confrelid JOIN pg_namespace pn ON pn.oid = p.relnamespace
        WHERE k.contype = 'f'
        """
    )
    wanted = set(tables)
    return {
        (row["child"], row["parent"])
        for row in rows
        if row["child"] in wanted and row["parent"] in wanted and row["child"] != row["parent"]
    }


def parents_first(tables: list[str], foreign_keys: set[tuple[str, str]]) -> list[str]:
    """``tables`` ordered so every table comes after the tables it references.

    Raises
    ------
    ReplicationError
        When the foreign keys form a cycle.
    """
    ordered: list[str] = []
    remaining = sorted(tables)
    while remaining:
        ready = [t for t in remaining if not any(c == t and p in remaining for c, p in foreign_keys)]
        if not ready:
            raise ReplicationError(f"foreign keys form a cycle among {', '.join(remaining)}")
        ordered.extend(ready)
        remaining = [t for t in remaining if t not in ready]
    return ordered


async def _newest_parent(conn: asyncpg.Connection, tables: set[str]) -> datetime | None:
    """The newest timestamp of any time-series parent row the mirror holds."""
    newest = None
    for table, bucketing in TIME_SERIES_BUCKETS.items():
        if bucketing.parent is None and table in tables:
            value = await conn.fetchval(f"SELECT max({quote_ident(bucketing.time_column)}) FROM {qualified(table)}")
            if value is not None and (newest is None or value > newest):
                newest = value
    return newest


async def _plan(
    primary: asyncpg.Connection, mirror: asyncpg.Connection, ready: list[str], registry: ReplicationRegistry
) -> list[Copy]:
    copies = {}
    for table in ready:
        kind = registry.tables[table]
        on_primary = set(await table_columns(primary, table))
        columns = tuple(c for c in await table_columns(mirror, table) if c in on_primary)
        key = tuple(await primary_key(mirror, table))
        if kind is not TableClass.TIME_SERIES:
            copies[table] = Copy(table, kind, columns, key)
            continue
        if (bucketing := TIME_SERIES_BUCKETS.get(table)) is None:
            raise Refused(f"time-series table {table} has no entry in TIME_SERIES_BUCKETS")
        if bucketing.parent is None:
            updated = "updated_at" if "updated_at" in columns else None
            copies[table] = Copy(table, kind, columns, key, bucketing=bucketing, updated_column=updated)
        elif bucketing.parent in ready:
            (parent_id,) = await primary_key(mirror, bucketing.parent)
            copies[table] = Copy(table, kind, columns, key, bucketing=bucketing, parent_id=parent_id)
        else:
            raise Refused(f"{table} is ready but its parent {bucketing.parent} isn't; run `refresh` and wait")
    order = parents_first(list(copies), await _foreign_keys(mirror, list(copies)))
    return [copies[table] for table in order]


def _select(copy: Copy) -> str:
    """The query that reads ``copy``'s rows on the primary; a child's takes its parents' ids as ``$1``."""
    columns = ", ".join(quote_ident(c) for c in copy.columns)
    query = f"SELECT {columns} FROM {qualified(copy.table)}"
    if copy.kind is TableClass.TIME_SERIES and copy.bucketing.parent is not None:
        query += f" WHERE {quote_ident(copy.bucketing.parent_key)} = ANY($1)"
    return query


async def _selected_parents(mirror: asyncpg.Connection, copy: Copy, temp: str, since: datetime) -> list:
    """Ids of the parents whose children get reconciled: those the mirror lacks, and recent ones."""
    key = quote_ident(copy.key[0])
    recent = f"x.{quote_ident(copy.bucketing.time_column)} >= $1"
    if copy.updated_column:
        recent += f" OR x.{quote_ident(copy.updated_column)} >= $1"
    rows = await mirror.fetch(
        f"SELECT x.{key} AS id FROM {quote_ident(temp)} x WHERE {recent} "
        f"OR NOT EXISTS (SELECT 1 FROM {qualified(copy.table)} t WHERE t.{key} = x.{key})",
        since,
    )
    return [row["id"] for row in rows]


def _matches(left: str, right: str, key: tuple[str, ...]) -> str:
    return " AND ".join(f"{left}.{quote_ident(k)} = {right}.{quote_ident(k)}" for k in key)


async def _load(
    primary: asyncpg.Connection, mirror: asyncpg.Connection, copy: Copy, temp: str, query: str, args: list
) -> int:
    """Stream ``copy``'s rows from the primary into the mirror's temp table ``temp``."""
    columns = ", ".join(quote_ident(c) for c in copy.columns)
    await mirror.execute(
        f"CREATE TEMP TABLE {quote_ident(temp)} ON COMMIT DROP AS SELECT {columns} FROM {qualified(copy.table)} "
        "WITH NO DATA"
    )
    total, chunk = 0, []
    async for record in primary.cursor(query, *args, prefetch=CHUNK_ROWS):
        chunk.append(tuple(record.values()))
        if len(chunk) == CHUNK_ROWS:
            await mirror.copy_records_to_table(temp, records=chunk, columns=list(copy.columns))
            total, chunk = total + len(chunk), []
    if chunk:
        await mirror.copy_records_to_table(temp, records=chunk, columns=list(copy.columns))
        total += len(chunk)
    return total


async def _delete_missing(mirror: asyncpg.Connection, copy: Copy, temp: str, parents: list | None) -> int:
    """Delete the mirror rows in ``copy``'s scope that the primary's copy lacks."""
    args: list = []
    if copy.kind is not TableClass.TIME_SERIES:
        scope = ""
    elif copy.bucketing.parent is None:
        return 0  # parents are never deleted: the primary may have pruned them
    else:
        scope, args = f"t.{quote_ident(copy.bucketing.parent_key)} = ANY($1) AND ", [parents]
    status = await mirror.execute(
        f"DELETE FROM {qualified(copy.table)} t "
        f"WHERE {scope}NOT EXISTS (SELECT 1 FROM {quote_ident(temp)} x WHERE {_matches('x', 't', copy.key)})",
        *args,
    )
    return int(status.split()[-1])


async def _unique_indexes(conn: asyncpg.Connection, table: str) -> list[list[str]]:
    """The columns of each unique index on ``table`` other than its primary key (expression indexes left out)."""
    rows = await conn.fetch(
        """
        SELECT array_agg(a.attname ORDER BY k.n) AS columns, bool_or(a.attname IS NULL) AS expression
        FROM pg_index i
        JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, n) ON true
        LEFT JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum AND k.attnum > 0
        WHERE i.indrelid = $1::regclass AND i.indisunique AND NOT i.indisprimary
        GROUP BY i.indexrelid
        """,
        qualified(table),
    )
    return [list(row["columns"]) for row in rows if not row["expression"]]


async def _placeholder(conn: asyncpg.Connection, copy: Copy, column: str) -> str:
    """SQL for a stand-in value of ``column`` that no real row holds, unique per row."""
    spec = await conn.fetchrow(
        """
        SELECT a.attnotnull, t.typcategory::text AS category, a.atttypmod,
               EXISTS (SELECT 1 FROM pg_constraint c WHERE c.conrelid = a.attrelid AND c.contype = 'c'
                       AND a.attnum = ANY(c.conkey)) AS checked
        FROM pg_attribute a JOIN pg_type t ON t.oid = a.atttypid
        WHERE a.attrelid = $1::regclass AND a.attname = $2
        """,
        qualified(copy.table),
        column,
    )
    if not spec["attnotnull"]:
        return "NULL"
    if spec["category"] != "S" or spec["checked"]:
        raise Refused(
            f"{copy.table}.{column} holds a unique value the primary has moved to another row, and reseed can't "
            "set it aside (it isn't nullable text without a CHECK); reconcile that row by hand"
        )
    length = spec["atttypmod"] - 4 if spec["atttypmod"] > 4 else 33
    key = " || ',' || ".join(f"t.{quote_ident(k)}::text" for k in copy.key)
    return f"left(chr(1) || md5({key}), {length})"


async def _set_aside(mirror: asyncpg.Connection, copy: Copy, temp: str) -> int:
    """Give a stand-in to each mirror row holding a unique value the primary gives another row.

    Every such row is rewritten or deleted later in the same transaction, so the stand-in
    never outlives it; a row that would be kept as it is (history the primary no longer
    has) is refused instead.
    """
    parked = 0
    for columns in await _unique_indexes(mirror, copy.table):
        if not set(columns) <= set(copy.columns):
            continue
        same = " AND ".join(f"x.{quote_ident(c)} = t.{quote_ident(c)}" for c in columns)
        mine = ", ".join(f"t.{quote_ident(k)}" for k in copy.key)
        theirs = ", ".join(f"x.{quote_ident(k)}" for k in copy.key)
        clash = f"EXISTS (SELECT 1 FROM {quote_ident(temp)} x WHERE {same} AND ({theirs}) IS DISTINCT FROM ({mine}))"
        if not await mirror.fetchval(f"SELECT EXISTS (SELECT 1 FROM {qualified(copy.table)} t WHERE {clash})"):
            continue
        kept = f"EXISTS (SELECT 1 FROM {quote_ident(temp)} x WHERE {_matches('x', 't', copy.key)})"
        if copy.kind is TableClass.TIME_SERIES and await mirror.fetchval(
            f"SELECT EXISTS (SELECT 1 FROM {qualified(copy.table)} t WHERE {clash} AND NOT {kept})"
        ):
            raise Refused(
                f"{copy.table} keeps a row the primary no longer has whose {', '.join(columns)} the primary now "
                "gives another row; reconcile it by hand"
            )
        assignments = ", ".join([f"{quote_ident(c)} = {await _placeholder(mirror, copy, c)}" for c in columns])
        status = await mirror.execute(f"UPDATE {qualified(copy.table)} t SET {assignments} WHERE {clash}")
        parked += int(status.split()[-1])
    return parked


async def _upsert(mirror: asyncpg.Connection, copy: Copy, temp: str) -> int:
    """Insert the primary's rows, or update the mirror's where they differ."""
    columns = ", ".join(quote_ident(c) for c in copy.columns)
    key = ", ".join(quote_ident(k) for k in copy.key)
    others = [c for c in copy.columns if c not in copy.key]
    if others:
        assignments = ", ".join(f"{quote_ident(c)} = EXCLUDED.{quote_ident(c)}" for c in others)
        mine = ", ".join(f"t.{quote_ident(c)}" for c in others)
        theirs = ", ".join(f"EXCLUDED.{quote_ident(c)}" for c in others)
        conflict = f"DO UPDATE SET {assignments} WHERE ({mine}) IS DISTINCT FROM ({theirs})"
    else:
        conflict = "DO NOTHING"
    status = await mirror.execute(
        f"INSERT INTO {qualified(copy.table)} AS t ({columns}) SELECT {columns} FROM {quote_ident(temp)} "
        f"ON CONFLICT ({key}) {conflict}"
    )
    return int(status.split()[-1])


async def _xid(conn: asyncpg.Connection) -> int:
    return int(await conn.fetchval("SELECT pg_current_xact_id()::text"))


async def _exact_cut(
    primary: asyncpg.Connection,
    deadline: float,
    before_snapshot: Callable[[], Awaitable[object]] | None = None,
) -> tuple[str, asyncpg.transaction.Transaction, int]:
    """``x0``, an open repeatable-read transaction on ``primary`` that is exact for it, and the attempts made.

    Raises
    ------
    Refused
        The primary never paused long enough before ``deadline``. Nothing was changed.
    """
    running = "SELECT count(*) FROM pg_snapshot_xip(pg_current_snapshot()) x WHERE x < $1::text::xid8"
    loop = asyncio.get_running_loop()
    attempts = 0
    while True:
        attempts += 1
        bound = await _xid(primary)
        # Everything that held an xid before `bound` must finish first. A snapshot's own
        # in-progress list stops below its xmax (the newest finished xid + 1), so ask by xid.
        while await primary.fetchval(running, str(bound)):
            if loop.time() > deadline:
                raise Refused(
                    "a write transaction on the primary stayed open past --cut-wait; nothing was changed. "
                    "Retry once it ends"
                )
            await asyncio.sleep(0.1)
        x0 = await primary.fetchval("SELECT pg_current_wal_insert_lsn()::text")
        if before_snapshot is not None:
            await before_snapshot()
        snapshot = primary.transaction(isolation="repeatable_read", readonly=True)
        await snapshot.start()
        if await _xid(primary) == bound + 1:  # nobody else took an xid between the bound and the snapshot
            return x0, snapshot, attempts
        await snapshot.rollback()
        if loop.time() > deadline:
            raise Refused(
                "the primary kept writing, so no exact cut was found within --cut-wait; nothing was changed. "
                "Pause writes on the primary briefly, or retry"
            )
        await asyncio.sleep(0.05)


async def _wait_for_no_worker(mirror: asyncpg.Connection, subid: int) -> None:
    deadline = asyncio.get_running_loop().time() + WORKER_WAIT_S
    while await mirror.fetchval(
        "SELECT count(*) FROM pg_stat_subscription WHERE subid = $1 AND pid IS NOT NULL", subid
    ):
        if asyncio.get_running_loop().time() > deadline:
            raise Refused("the subscription is disabled but an apply worker is still running; retry in a moment")
        await asyncio.sleep(0.2)


async def reseed(
    primary: asyncpg.Connection,
    mirror: asyncpg.Connection,
    *,
    role: str,
    instance: str | None,
    config: ReplicationConfig,
    since: datetime,
    cut_wait_s: float = CUT_WAIT_S,
    registry: ReplicationRegistry = REGISTRY,
    hooks: Mapping[str, Callable[[], Awaitable[object]]] | None = None,
) -> dict:
    """Copy what the mirror is missing since ``since``, then resume replication at the cut; see the module docstring.

    Parameters
    ----------
    primary : asyncpg.Connection
        Read-only connection to the primary.
    mirror : asyncpg.Connection
        Connection to this mirror's database (see the module docstring for the role).
    role, instance : str, str | None
        ``CANOPY_ROLE`` (must be a mirror) and ``CANOPY_INSTANCE`` (names the subscription).
    config : ReplicationConfig
        Supplies ``publisher_dsn`` when the subscription has to be created.
    since : datetime
        Reconcile the children of parents created or updated from this time on.
    cut_wait_s : float
        How long to keep trying for an exact cut.
    hooks : Mapping[str, Callable[[], Awaitable]] | None
        For tests: awaited at ``before_snapshot``, ``after_snapshot``, ``before_commit``
        (inside the copy transaction) and ``after_commit`` (before the origin moves).

    Returns
    -------
    dict
        The subscription and whether it was ``created``; the ``cut_lsn`` replication
        resumed after and the ``cut_attempts`` it took; per table, rows ``read`` from
        the primary, and ``set_aside``, ``written`` and ``deleted`` on the mirror; and
        tables ``skipped`` (not ready: their own initial copy runs once the
        subscription is enabled).

    Raises
    ------
    Refused
        Wrong role, missing privileges, an enabled subscription, ``since`` later than
        the mirror's newest snapshot, no exact cut within ``cut_wait_s``, or a unique
        value it can't set aside. Nothing was copied.
    """
    hooks = hooks or {}
    require_role("reseed", "mirror", role)
    name = subscription_name(instance)
    if not await is_superuser(mirror) and not await mirror.fetchval(
        "SELECT has_function_privilege('pg_replication_origin_advance(text, pg_lsn)', 'EXECUTE')"
    ):
        raise Refused("reseed needs a superuser, or EXECUTE on pg_replication_origin_advance(text, pg_lsn)")

    present = set(registry.replicated) & await tables_present(mirror, registry.schemas)
    if (newest := await _newest_parent(mirror, present)) is not None and since > newest:
        raise Refused(
            f"--since {since.isoformat()} is later than the newest snapshot here ({newest.isoformat()}); "
            "changes in between would be lost. Use a time no later than that"
        )

    sub = await _subscription(mirror, name)
    created = sub is None
    if created:
        await subscribe(mirror, role=role, instance=instance, config=config, registry=registry, copy_data=False)
        sub = await _subscription(mirror, name)
    elif sub["subenabled"]:
        raise Refused(f"subscription {name} is enabled; disable it first (ALTER SUBSCRIPTION {name} DISABLE)")
    await _wait_for_no_worker(mirror, sub["oid"])

    states = await _subscribed_tables(mirror, sub["oid"])
    ready = sorted(t for t, state in states.items() if state == "ready" and t in registry.replicated)
    skipped = {t: state for t, state in sorted(states.items()) if state != "ready"}
    copies = await _plan(primary, mirror, ready, registry)

    x0, snapshot, attempts = await _exact_cut(
        primary, asyncio.get_running_loop().time() + cut_wait_s, hooks.get("before_snapshot")
    )
    tables: dict[str, dict] = {}
    try:
        snapshot_lsn = await primary.fetchval("SELECT pg_current_wal_insert_lsn()::text")
        if "after_snapshot" in hooks:
            await hooks["after_snapshot"]()
        async with mirror.transaction():
            temps = {copy.table: f"reseed_{n}" for n, copy in enumerate(copies)}
            selected: dict[str, list] = {}  # parent table -> ids whose children are reconciled
            for copy in copies:
                args: list = []
                if copy.kind is TableClass.TIME_SERIES and copy.bucketing.parent is not None:
                    args = [selected[copy.bucketing.parent]]
                read = await _load(primary, mirror, copy, temps[copy.table], _select(copy), args)
                if copy.kind is TableClass.TIME_SERIES and copy.bucketing.parent is None:
                    selected[copy.table] = await _selected_parents(mirror, copy, temps[copy.table], since)
                tables[copy.table] = {"read": read}
            for copy in copies:
                tables[copy.table]["set_aside"] = await _set_aside(mirror, copy, temps[copy.table])
            for copy in copies:  # parents first, so rows already point at their new parents ...
                tables[copy.table]["written"] = await _upsert(mirror, copy, temps[copy.table])
            for copy in reversed(copies):  # ... before old ones are deleted, children first
                parents = selected.get(copy.bucketing.parent) if copy.bucketing else None
                tables[copy.table]["deleted"] = await _delete_missing(mirror, copy, temps[copy.table], parents)
            if "before_commit" in hooks:
                await hooks["before_commit"]()
    finally:
        await snapshot.rollback()

    for table, counts in tables.items():
        logger.info(
            "Re-seeded %s: %d rows read, %d set aside, %d written, %d deleted",
            table,
            counts["read"],
            counts["set_aside"],
            counts["written"],
            counts["deleted"],
        )
    # Only now that the copy is in: origin progress isn't transactional.
    if "after_commit" in hooks:
        await hooks["after_commit"]()
    await mirror.execute("SELECT pg_replication_origin_advance($1, $2::text::pg_lsn)", f"pg_{sub['oid']}", x0)
    await mirror.execute(f"ALTER SUBSCRIPTION {quote_ident(name)} ENABLE")
    logger.info("Subscription %s enabled; replication resumes after %s", name, x0)
    return {
        "subscription": name,
        "created": created,
        "since": since.isoformat(),
        "cut_lsn": x0,
        "cut_attempts": attempts,
        "snapshot_lsn": snapshot_lsn,
        "tables": tables,
        "skipped": skipped,
    }


def parse_since(value: str) -> datetime:
    """An ISO 8601 time for ``--since``; one without a zone is UTC."""
    try:
        moment = datetime.fromisoformat(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"not an ISO 8601 time: {value!r}") from None
    return moment if moment.tzinfo else moment.replace(tzinfo=UTC)


async def run(args: argparse.Namespace) -> int:
    """``reseed``: 0 done, 1 a database step failed, 2 refused (nothing copied)."""
    settings = get_settings()
    config = get_replication_config()
    reader_dsn = config.reader_dsn.get_secret_value() if config.reader_dsn else None
    secrets = [reader_dsn, config.publisher_dsn.get_secret_value() if config.publisher_dsn else None]
    primary = mirror = None
    try:
        require_role("reseed", "mirror", settings.role)
        if not reader_dsn:
            raise Refused(
                "set CANOPY_REPLICATION__READER_DSN to the primary's read-only connection URI (from a secret)"
            )
        primary = await asyncpg.connect(reader_dsn, server_settings={**SESSION, "default_transaction_read_only": "on"})
        mirror = await asyncpg.connect(asyncpg_dsn(settings.database_url), server_settings=SESSION)
        report = await reseed(
            primary,
            mirror,
            role=settings.role,
            instance=settings.instance,
            config=config,
            since=args.since,
            cut_wait_s=args.cut_wait,
        )
    except Refused as exc:
        logger.error("refused: %s", redact(str(exc), secrets))
        return 2
    except Exception as exc:
        logger.error("reseed failed: %s", redact(str(exc) or repr(exc), secrets))
        return 1
    finally:
        for conn in (primary, mirror):
            if conn is not None:
                await conn.close()
    print(json.dumps(report, indent=2), file=sys.stdout)
    return 0
