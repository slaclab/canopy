"""``reseed``: fill the head's gap in one arm from that arm, then hand over to replication at one WAL position.

After an arm's slot is lost, or the head is restored to an earlier point in time,
the changes the head is missing from that arm are no longer in any slot. ``reseed
--arm <arm> --since <ts>`` copies them from the arm, read as its reader role
(``CANOPY_ARMS__<arm>__READER_DSN``). Only that arm's subscription and the rows it
owns are touched (``sql.owner_join``); other arms keep streaming, and their rows and
the head's own are never read, written or deleted:

* every reference table, fully reconciled: rows the arm no longer has are
  deleted, the rest are inserted or updated;
* every time-series parent the arm has, inserted or updated; parents the arm
  no longer has are never deleted, since once the arm prunes old rows the head is
  the only copy;
* the children of each parent the head was missing, and of each parent created (or,
  where the table has ``updated_at``, updated) at or after ``--since``, reconciled per
  parent.

It runs with that arm's subscription disabled, creating it disabled and without the
initial copy when there is none. The hand-over:

1. Take ``B = pg_current_xact_id()`` on the arm and wait until no lower xid is
   still running. Read ``x0 = pg_current_wal_insert_lsn()``. Open a repeatable-read
   snapshot whose first statement takes another xid, ``X``.
2. ``X = B + 1`` means no other transaction got an xid in between. Everything that
   committed before ``x0`` is then visible in the snapshot, and nothing committed
   between ``x0`` and the snapshot. Otherwise the attempt is dropped and the cut is
   tried again, for up to ``--cut-wait`` seconds; an arm that never pauses writing
   that long is refused, and nothing changes.
3. Copy from that snapshot into the head in one transaction. Inside it, unique
   values the arm has moved between rows are set aside first, parents are written
   before children, and rows the arm no longer has are deleted last, children
   first.
4. Once the copy has committed, advance the arm's subscription's replication origin to
   ``x0`` and enable the subscription. Apply resumes with the first transaction that
   committed after ``x0``: nothing is replayed and nothing is lost.

The origin's progress isn't transactional, so it moves only after the copy is safely
in. If a run fails, the arm's subscription stays disabled: run ``reseed`` again, and
never enable the subscription by hand.

A key, or a value unique across owners (a facility or Magpie slug), that another
owner's row already holds here is refused, naming that owner: never overwritten, set
aside or deleted. So is deleting a row another owner's row still references.

``--since`` must not be later than the newest snapshot the head holds from that arm. Missing
parents are found by id whatever their time, but children of a parent both sides hold
are re-read only from ``--since`` on. Earlier is always safe, only slower.

Run it as a superuser of the head's database, or as the tables' and
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

from canopy.core.arms import Arm, subscription_name
from canopy.core.config import get_settings
from canopy.core.replication.sql import (
    Refused,
    ReplicationError,
    redact,
    qualified,
    owner_join,
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
    references,
    primary_key,
    table_columns,
    connect_reader,
    configured_arms,
)
from canopy.core.replication.registry import REGISTRY, TableClass, ReplicationRegistry
from canopy.core.replication.subscriber import (
    subscribe,
    _subscription,
    _selected_arms,
    _subscribed_tables,
)

logger = logging.getLogger(__name__)

RESEED_SESSION = {**SESSION, "application_name": "canopy-reseed"}

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
    owners: tuple[str, str] = ("", "")  # sql.owner_join for alias t
    bucketing: Bucketing | None = None  # time-series parent
    updated_column: str | None = None  # time-series parent: also selected by this, when present
    parent: str | None = None  # time-series child: its owner-path parent
    parent_key: str | None = None  # time-series child: its column holding the parent's id

    @property
    def time_series_parent(self) -> bool:
        return self.kind is TableClass.TIME_SERIES and self.parent is None

    @property
    def time_series_child(self) -> bool:
        return self.kind is TableClass.TIME_SERIES and self.parent is not None


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


async def _newest_parent(
    conn: asyncpg.Connection, tables: set[str], owner: str, registry: ReplicationRegistry
) -> datetime | None:
    """The newest timestamp of any time-series parent row ``owner`` owns here."""
    newest = None
    for table, bucketing in TIME_SERIES_BUCKETS.items():
        if table in tables:
            join_sql, owner_sql = owner_join(registry, table, alias="t")
            value = await conn.fetchval(
                f"SELECT max(t.{quote_ident(bucketing.time_column)}) FROM {qualified(table)} t {join_sql} "
                f"WHERE {owner_sql} = $1",
                owner,
            )
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
        owners = owner_join(registry, table, alias="t")
        if blocked := [parent for parent, _ in registry.owner_chain(table) if parent not in ready]:
            raise Refused(f"{table} is ready but its owner-path parent {blocked[0]} isn't; run `refresh` and wait")
        if kind is not TableClass.TIME_SERIES:
            copies[table] = Copy(table, kind, columns, key, owners)
        elif (bucketing := TIME_SERIES_BUCKETS.get(table)) is not None:
            updated = "updated_at" if "updated_at" in columns else None
            copies[table] = Copy(table, kind, columns, key, owners, bucketing=bucketing, updated_column=updated)
        elif (path := registry.owner_paths.get(table)) is not None and path.parent in TIME_SERIES_BUCKETS:
            copies[table] = Copy(table, kind, columns, key, owners, parent=path.parent, parent_key=path.key)
        else:
            raise Refused(
                f"time-series table {table} has no entry in TIME_SERIES_BUCKETS, and its owner-path parent "
                "isn't a time-series table that has one"
            )
    order = parents_first(list(copies), await _foreign_keys(mirror, list(copies)))
    return [copies[table] for table in order]


def _select(copy: Copy) -> str:
    """The query that reads the arm's rows of ``copy`` (owner ``$1``); a child's takes its parents' ids as ``$2``.

    On the arm the owner filter is a safety belt: an arm holds only its own rows.
    """
    join_sql, owner_sql = copy.owners
    columns = ", ".join(f"t.{quote_ident(c)}" for c in copy.columns)
    query = f"SELECT {columns} FROM {qualified(copy.table)} t {join_sql} WHERE {owner_sql} = $1"
    if copy.time_series_child:
        query += f" AND t.{quote_ident(copy.parent_key)} = ANY($2)"
    return query


def _owned(copy: Copy, owner_param: str, registry: ReplicationRegistry) -> str:
    """SQL true for a row ``t`` of ``copy.table`` that the arm (``owner_param``) owns: for deletes and updates."""
    join_sql, owner_sql = owner_join(registry, copy.table, alias="x")
    mine = ", ".join(f"t.{quote_ident(k)}" for k in copy.key)
    theirs = ", ".join(f"x.{quote_ident(k)}" for k in copy.key)
    return f"({mine}) IN (SELECT {theirs} FROM {qualified(copy.table)} x {join_sql} WHERE {owner_sql} = {owner_param})"


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


async def _refuse_other_owners_references(
    mirror: asyncpg.Connection,
    copy: Copy,
    temp: str,
    owner: str,
    references: list[tuple],
    registry: ReplicationRegistry,
) -> None:
    """Refuse when a row about to be deleted is still referenced by a row of another owner.

    The same-owner rules make this practically impossible; the refusal names the owner
    rather than leave a foreign key to fail, or a row to dangle, on the head.
    """
    if copy.key != ("id",):
        return  # only rows with an id are referenced
    doomed = (
        f"SELECT t.id FROM {qualified(copy.table)} t WHERE {_owned(copy, '$1', registry)} "
        f"AND NOT EXISTS (SELECT 1 FROM {quote_ident(temp)} y WHERE y.id = t.id)"
    )
    for child, column, parent, parent_column in references:
        if parent != copy.table or parent_column != "id":
            continue
        join_sql, owner_sql = owner_join(registry, child, alias="c")
        rows = await mirror.fetch(
            f"SELECT DISTINCT {owner_sql} AS owner FROM {qualified(child)} c {join_sql} "
            f"WHERE {owner_sql} <> $1 AND c.{quote_ident(column)} IN ({doomed}) ORDER BY 1",
            owner,
        )
        if rows:
            raise Refused(
                f"{copy.table} rows the arm no longer has are still referenced here by {child}.{column} rows of "
                f"{', '.join(row['owner'] for row in rows)}; reconcile those by hand"
            )


async def _delete_missing(
    mirror: asyncpg.Connection, copy: Copy, temp: str, parents: list | None, owner: str, registry: ReplicationRegistry
) -> int:
    """Delete the head's rows of the arm (``owner``) in ``copy``'s scope that the arm's copy lacks."""
    if copy.time_series_parent:
        return 0  # parents are never deleted: the arm may have pruned them
    scope, args = _owned(copy, "$1", registry), [owner]
    if copy.time_series_child:
        scope += f" AND t.{quote_ident(copy.parent_key)} = ANY($2)"
        args.append(parents)
    status = await mirror.execute(
        f"DELETE FROM {qualified(copy.table)} t "
        f"WHERE {scope} AND NOT EXISTS (SELECT 1 FROM {quote_ident(temp)} x WHERE {_matches('x', 't', copy.key)})",
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


async def _set_aside(
    mirror: asyncpg.Connection, copy: Copy, temp: str, owner: str, registry: ReplicationRegistry
) -> int:
    """Give a stand-in to each of the arm's rows here holding a unique value the arm gives another row.

    Every such row is rewritten or deleted later in the same transaction, so the stand-in
    never outlives it; a row that would be kept as it is (history the arm no longer
    has) is refused instead. A row of another owner holding the value is never touched:
    that is refused, naming the owner (only values unique across owners can clash so).
    """
    parked = 0
    join_sql, owner_sql = copy.owners
    owner_column = getattr(registry.owner_paths.get(copy.table), "column", None)
    for columns in await _unique_indexes(mirror, copy.table):
        if not set(columns) <= set(copy.columns):
            continue
        same = " AND ".join(f"x.{quote_ident(c)} = t.{quote_ident(c)}" for c in columns)
        mine = ", ".join(f"t.{quote_ident(k)}" for k in copy.key)
        theirs = ", ".join(f"x.{quote_ident(k)}" for k in copy.key)
        clash = f"EXISTS (SELECT 1 FROM {quote_ident(temp)} x WHERE {same} AND ({theirs}) IS DISTINCT FROM ({mine}))"
        if not await mirror.fetchval(f"SELECT EXISTS (SELECT 1 FROM {qualified(copy.table)} t WHERE {clash})"):
            continue
        if foreign := await mirror.fetch(
            f"SELECT DISTINCT {owner_sql} AS owner FROM {qualified(copy.table)} t {join_sql} "
            f"WHERE {owner_sql} <> $1 AND {clash} ORDER BY 1",
            owner,
        ):
            raise Refused(
                f"{copy.table}.{', '.join(columns)}: a value the arm gives one of its rows is held here by a row of "
                f"{', '.join(row['owner'] for row in foreign)}; rename one of them on its owner"
            )
        mine_owned = _owned(copy, "$1", registry)
        kept = f"EXISTS (SELECT 1 FROM {quote_ident(temp)} x WHERE {_matches('x', 't', copy.key)})"
        if copy.kind is TableClass.TIME_SERIES and await mirror.fetchval(
            f"SELECT EXISTS (SELECT 1 FROM {qualified(copy.table)} t WHERE {mine_owned} AND {clash} AND NOT {kept})",
            owner,
        ):
            raise Refused(
                f"{copy.table} keeps a row the arm no longer has whose {', '.join(columns)} the arm now "
                "gives another row; reconcile it by hand"
            )
        # The owner column is never parked: the clash already matches it, so parking the
        # other columns frees a per-owner constraint, and the row keeps its owner for
        # the owner-scoped delete that follows.
        park = [c for c in columns if c != owner_column]
        assignments = ", ".join([f"{quote_ident(c)} = {await _placeholder(mirror, copy, c)}" for c in park])
        status = await mirror.execute(
            f"UPDATE {qualified(copy.table)} t SET {assignments} WHERE {mine_owned} AND {clash}", owner
        )
        parked += int(status.split()[-1])
    return parked


async def _refuse_other_owners_keys(mirror: asyncpg.Connection, copy: Copy, temp: str, owner: str) -> None:
    """Refuse when a key the arm has is held here by another owner's row; it is never overwritten."""
    join_sql, owner_sql = copy.owners
    if rows := await mirror.fetch(
        f"SELECT DISTINCT {owner_sql} AS owner FROM {qualified(copy.table)} t {join_sql} WHERE {owner_sql} <> $1 "
        f"AND EXISTS (SELECT 1 FROM {quote_ident(temp)} x WHERE {_matches('x', 't', copy.key)}) ORDER BY 1",
        owner,
    ):
        raise Refused(
            f"{copy.table}: rows the arm has are held here under the same key by "
            f"{', '.join(row['owner'] for row in rows)}; nothing was copied. Reconcile that row by hand"
        )


async def _upsert(mirror: asyncpg.Connection, copy: Copy, temp: str) -> int:
    """Insert the arm's rows, or update the head's where they differ (after ``_refuse_other_owners_keys``)."""
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
    arm: str,
    arms: Mapping[str, Arm],
    head: str | None,
    since: datetime,
    cut_wait_s: float = CUT_WAIT_S,
    registry: ReplicationRegistry = REGISTRY,
    hooks: Mapping[str, Callable[[], Awaitable[object]]] | None = None,
) -> dict:
    """Copy what the head is missing from ``arm`` since ``since``, then resume its replication at the cut.

    See the module docstring.

    Parameters
    ----------
    primary : asyncpg.Connection
        Read-only connection to the arm, as its reader role.
    mirror : asyncpg.Connection
        Connection to the head's database (see the module docstring for the role).
    role : str
        ``CANOPY_ROLE``; must be ``"mirror"``.
    arm : str
        The arm re-seeded; names its subscription (``canopy_history_<arm>``).
    arms : Mapping[str, Arm]
        The configured arms; ``arm``'s ``publisher_dsn`` is used when its subscription
        has to be created.
    head : str | None
        The head's own ``CANOPY_INSTANCE``.
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
        Wrong role, an arm not configured, missing privileges, an enabled
        subscription, ``since`` later than the head's newest snapshot from the arm, no exact cut within ``cut_wait_s``, or a unique
        value it can't set aside. Nothing was copied.
    """
    hooks = hooks or {}
    require_role("reseed", "mirror", role)
    _selected_arms(arms, arm, head)
    name = subscription_name(arm)
    if not await is_superuser(mirror) and not await mirror.fetchval(
        "SELECT has_function_privilege('pg_replication_origin_advance(text, pg_lsn)', 'EXECUTE')"
    ):
        raise Refused("reseed needs a superuser, or EXECUTE on pg_replication_origin_advance(text, pg_lsn)")

    present = set(registry.replicated) & await tables_present(mirror, registry.schemas)
    if (newest := await _newest_parent(mirror, present, arm, registry)) is not None and since > newest:
        raise Refused(
            f"--since {since.isoformat()} is later than the newest snapshot here from {arm} ({newest.isoformat()}); "
            "changes in between would be lost. Use a time no later than that"
        )

    sub = await _subscription(mirror, name)
    created = sub is None
    if created:
        await subscribe(mirror, role=role, head=head, arms=arms, only=arm, registry=registry, copy_data=False)
        sub = await _subscription(mirror, name)
    elif sub["subenabled"]:
        raise Refused(f"subscription {name} is enabled; disable it first (ALTER SUBSCRIPTION {name} DISABLE)")
    await _wait_for_no_worker(mirror, sub["oid"])

    states = await _subscribed_tables(mirror, sub["oid"])
    ready = sorted(t for t, state in states.items() if state == "ready" and t in registry.replicated)
    skipped = {t: state for t, state in sorted(states.items()) if state != "ready"}
    copies = await _plan(primary, mirror, ready, registry)
    refs = await references(mirror, registry)

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
                args: list = [arm]
                if copy.time_series_child:
                    args.append(selected[copy.parent])
                read = await _load(primary, mirror, copy, temps[copy.table], _select(copy), args)
                if copy.time_series_parent:
                    selected[copy.table] = await _selected_parents(mirror, copy, temps[copy.table], since)
                tables[copy.table] = {"read": read}
            # The copy is one transaction, so a refusal here or in _set_aside leaves nothing written.
            for copy in copies:
                await _refuse_other_owners_keys(mirror, copy, temps[copy.table], arm)
                if not copy.time_series_parent:
                    await _refuse_other_owners_references(mirror, copy, temps[copy.table], arm, refs, registry)
            for copy in copies:
                tables[copy.table]["set_aside"] = await _set_aside(mirror, copy, temps[copy.table], arm, registry)
            for copy in copies:  # parents first, so rows already point at their new parents ...
                tables[copy.table]["written"] = await _upsert(mirror, copy, temps[copy.table])
            for copy in reversed(copies):  # ... before old ones are deleted, children first
                parents = selected.get(copy.parent) if copy.time_series_child else None
                tables[copy.table]["deleted"] = await _delete_missing(
                    mirror, copy, temps[copy.table], parents, arm, registry
                )
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
    logger.info("Subscription %s enabled; replication from %s resumes after %s", name, arm, x0)
    return {
        "arm": arm,
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
    """``reseed --arm``: 0 done, 1 a database step failed, 2 refused (nothing copied)."""
    settings = get_settings()
    secrets: list[str] = []
    primary = mirror = None
    try:
        arms = configured_arms()
        secrets = [
            dsn.get_secret_value() for arm in arms.values() for dsn in (arm.reader_dsn, arm.publisher_dsn) if dsn
        ]
        require_role("reseed", "mirror", settings.role)
        (name,) = _selected_arms(arms, args.arm, settings.instance)
        primary = await connect_reader(name, arms[name], RESEED_SESSION)
        mirror = await asyncpg.connect(asyncpg_dsn(settings.database_url), server_settings=RESEED_SESSION)
        report = await reseed(
            primary,
            mirror,
            role=settings.role,
            arm=name,
            arms=arms,
            head=settings.instance,
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
