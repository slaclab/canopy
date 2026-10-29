"""``reseed``: fill a mirror's gap from its primary, then hand over to replication at one WAL position.

After the primary's slot is lost, or the mirror is restored to an earlier point in
time, the changes the mirror is missing are no longer in any slot. ``reseed --since
<ts>`` copies them from the primary, read as the reader role:

* every reference table, fully reconciled: rows the primary no longer has are
  deleted, the rest are inserted or updated;
* time-series parents created (or, where the table has ``updated_at``, updated) at or
  after ``--since``, and all their children, which are reconciled per parent. Parents
  the primary no longer has are never deleted: once the primary prunes old rows,
  history is the only copy.

It runs with the subscription disabled, creating it disabled and without the initial
copy when there is none. The hand-over works like this:

1. ``x0 = pg_current_wal_insert_lsn()`` on the primary, then wait for every
   transaction in progress after that to finish, then open a repeatable-read
   snapshot. Every transaction that committed before ``x0`` is visible in it.
2. Copy the rows above from that snapshot into the mirror, in one transaction that
   also advances the subscription's replication origin to ``x0`` and enables it, so
   apply resumes with the first transaction that committed after ``x0``.

Nothing committed before ``x0`` is replayed, and nothing after it is lost. The one
overlap: a write transaction already open on the primary when the cut starts (a
snapshot being saved, say) commits after ``x0``, so it is streamed, and the cut waits
for it, so it is copied too. If it inserted a row, apply stops on the duplicate key.
That transaction is already in place, so skip it with ``ALTER SUBSCRIPTION ... SKIP
(lsn = ...)``, taking the LSN from the "finished at" in the mirror's log. The report's
``open_at_cut`` says how many there were; with none, only a transaction committing in
the few milliseconds before the snapshot opens can be.
(The other way round, not waiting, would lose a transaction caught mid-commit without
a trace. Closing the gap exactly takes the snapshot the replication protocol exports
when it creates a slot, which plain SQL can't reach.)

``--since`` must not be later than the newest snapshot the mirror holds: the origin
advance discards every change between the mirror's last applied transaction and
``x0``, and only what ``--since`` covers is copied in its place. Earlier is always
safe, only slower.

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
from collections.abc import Callable, Awaitable

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
"""How long the cut waits for the primary's in-progress transactions before giving up."""

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


def _select(copy: Copy, copies: dict[str, Copy]) -> str:
    """The query that reads ``copy``'s rows on the primary; a time-series one takes ``since`` as ``$1``."""
    columns = ", ".join(quote_ident(c) for c in copy.columns)
    if copy.kind is not TableClass.TIME_SERIES:
        return f"SELECT {columns} FROM {qualified(copy.table)}"

    def recent(alias: str, parent: Copy) -> str:
        condition = f"{alias}{quote_ident(parent.bucketing.time_column)} >= $1"
        if parent.updated_column:
            condition += f" OR {alias}{quote_ident(parent.updated_column)} >= $1"
        return condition

    if copy.bucketing.parent is None:
        return f"SELECT {columns} FROM {qualified(copy.table)} WHERE {recent('', copy)}"
    parent = copies[copy.bucketing.parent]
    child_columns = ", ".join(f"c.{quote_ident(col)}" for col in copy.columns)
    return (
        f"SELECT {child_columns} FROM {qualified(copy.table)} c JOIN {qualified(parent.table)} p "
        f"ON p.{quote_ident(copy.parent_id)} = c.{quote_ident(copy.bucketing.parent_key)} WHERE {recent('p.', parent)}"
    )


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


async def _delete_missing(mirror: asyncpg.Connection, copy: Copy, temp: str, temps: dict[str, str]) -> int:
    """Delete the mirror rows in ``copy``'s scope that the primary's copy lacks."""
    if copy.kind is not TableClass.TIME_SERIES:
        scope = ""
    elif copy.bucketing.parent is None:
        return 0  # parents are never deleted: the primary may have pruned them
    else:
        parent_temp = quote_ident(temps[copy.bucketing.parent])
        scope = f"t.{quote_ident(copy.bucketing.parent_key)} IN (SELECT {quote_ident(copy.parent_id)} FROM {parent_temp}) AND "
    status = await mirror.execute(
        f"DELETE FROM {qualified(copy.table)} t "
        f"WHERE {scope}NOT EXISTS (SELECT 1 FROM {quote_ident(temp)} x WHERE {_matches('x', 't', copy.key)})"
    )
    return int(status.split()[-1])


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


async def _cut(primary: asyncpg.Connection) -> tuple[str, int]:
    """``x0``, once every transaction that might have committed before it is visible, and how many were open."""
    x0 = await primary.fetchval("SELECT pg_current_wal_insert_lsn()::text")
    # Every transaction that held an xid when x0 was read has a lower one than this.
    # A snapshot's in-progress list stops below its xmax (the newest finished xid + 1),
    # which can leave out the newest open transactions, so ask by xid instead: once
    # none below this one is running, every commit before x0 is visible.
    bound = await primary.fetchval("SELECT pg_current_xact_id()::text")
    running = "SELECT count(*) FROM pg_snapshot_xip(pg_current_snapshot()) x WHERE x < $1::text::xid8"
    open_at_cut = await primary.fetchval(running, bound)
    deadline = asyncio.get_running_loop().time() + CUT_WAIT_S
    while await primary.fetchval(running, bound):
        if asyncio.get_running_loop().time() > deadline:
            raise Refused(
                f"a write transaction on the primary has been open for over {CUT_WAIT_S:.0f} s; nothing was changed, "
                "retry once it ends"
            )
        await asyncio.sleep(0.1)
    return x0, open_at_cut


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
    registry: ReplicationRegistry = REGISTRY,
    after_snapshot: Callable[[], Awaitable[object]] | None = None,
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
        Copy time-series parents from this time on, with their children.
    after_snapshot : Callable[[], Awaitable] | None
        Awaited once the primary's snapshot is taken; for tests.

    Returns
    -------
    dict
        The subscription and whether it was ``created``; the ``cut_lsn`` replication
        resumed after; per table, rows ``read`` from the primary, ``written`` and
        ``deleted`` on the mirror; and tables ``skipped`` (not ready: their own
        initial copy runs once the subscription is enabled).

    Raises
    ------
    Refused
        Wrong role, missing privileges, an enabled subscription, ``since`` later than
        the mirror's newest snapshot, or the primary held a transaction open too long.
        Nothing was copied.
    """
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
    by_table = {copy.table: copy for copy in copies}

    x0, open_at_cut = await _cut(primary)
    if open_at_cut:
        logger.warning(
            "%d write transactions were open on the primary at the cut; if apply stops on a duplicate key "
            "after this, skip that transaction (see `reseed` in the history runbooks)",
            open_at_cut,
        )
    tables: dict[str, dict] = {}
    async with primary.transaction(isolation="repeatable_read", readonly=True):
        snapshot_lsn = await primary.fetchval("SELECT pg_current_wal_insert_lsn()::text")
        if after_snapshot is not None:
            await after_snapshot()
        async with mirror.transaction():
            temps = {copy.table: f"reseed_{n}" for n, copy in enumerate(copies)}
            for copy in copies:
                args = [since] if copy.kind is TableClass.TIME_SERIES else []
                read = await _load(primary, mirror, copy, temps[copy.table], _select(copy, by_table), args)
                tables[copy.table] = {"read": read}
            for copy in reversed(copies):  # children first
                tables[copy.table]["deleted"] = await _delete_missing(mirror, copy, temps[copy.table], temps)
            for copy in copies:  # parents first
                tables[copy.table]["written"] = await _upsert(mirror, copy, temps[copy.table])
            await mirror.execute("SELECT pg_replication_origin_advance($1, $2::text::pg_lsn)", f"pg_{sub['oid']}", x0)
            await mirror.execute(f"ALTER SUBSCRIPTION {quote_ident(name)} ENABLE")

    for table, counts in tables.items():
        logger.info(
            "Re-seeded %s: %d rows read, %d written, %d deleted",
            table,
            counts["read"],
            counts["written"],
            counts["deleted"],
        )
    logger.info("Subscription %s enabled; replication resumes after %s", name, x0)
    return {
        "subscription": name,
        "created": created,
        "since": since.isoformat(),
        "cut_lsn": x0,
        "open_at_cut": open_at_cut,
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
            primary, mirror, role=settings.role, instance=settings.instance, config=config, since=args.since
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
