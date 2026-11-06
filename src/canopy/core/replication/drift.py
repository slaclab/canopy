"""``drift-check``: compare the head's copy of each arm with that arm, table by table.

Logical replication skips an UPDATE or DELETE whose row the head doesn't have,
without an error, so a copy can drift with nothing failing. This check runs on the
head (hourly, after ``refresh``), reads each arm as its read-only reader role
(``CANOPY_ARMS__<ARM>__READER_DSN``), and reports where the two differ. Only the rows
the arm owns are compared, on both sides (``sql.owner_join``): the head's own rows and
other arms' are never part of an arm's figures.

* reference tables: the row count, and an md5 of every row in primary-key order over
  the columns both sides have (a column the head got first isn't drift). Keys are
  UUIDs, which sort the same on any server; a text key could sort differently under
  another collation and show drift that isn't there (a test keeps keys non-text);
* time-series tables: row counts per UTC day, by the table's own timestamp
  (``TIME_SERIES_BUCKETS``) or, for a child, its owner-path parent's.

Tables are compared once the arm's subscription holds them in state ready. A table
the arm publishes that isn't ready, or isn't subscribed at all, is reported as not
checked.

A difference may only be lag, or a write landing between the two reads, so a table
that differs is drift only when the comparison is conclusive:

1. Read the arm in a repeatable-read snapshot, with ``L``, the WAL position inside it.
2. Read the head. Equal figures mean in step.
3. Otherwise wait for the arm's slot (the head's subscription) to confirm ``L``
   (``confirmed_flush_lsn``), then read the head again, then the arm.
4. Drift when the slot confirmed ``L``, the head still differs, and the arm's second
   read equals its first. When the arm changed or the slot didn't confirm, the table is
   tried again, up to ``ATTEMPTS`` times within ``--settle`` seconds, and is then
   reported as inconclusive.

After the arms, the head itself is checked: every reference between replicated tables
(a foreign key, or one declared in ``REFERENCES``) must find its parent. Apply doesn't
fire foreign-key triggers, so only the head can hold a row whose parent is gone. A NULL
reference points at nothing and is never dangling. A dangling row belongs to its owner:
its own, its owner-path parent's, or else the owner of another parent it references
that survives. It is that arm's drift; a row nobody can be found to own (or owned by
the head or an instance that isn't a configured arm) is the head's, and fails every run.

``--days`` limits time-series buckets to recent days; once an arm prunes old rows,
only its window can match. ``--arm`` checks one arm; the head check runs every time.

Exit status, for each arm: 0 when every table it publishes was compared and is in
step; 1 drift found; 2 the check couldn't be completed (a table not ready or not
subscribed, nothing to compare, an inconclusive comparison, the arm unreachable, or
an error). An arm that fails is reported so and the others are still checked. The run
exits 1 if any arm drifted or the head holds a dangling row, else 2 if any arm wasn't
checked, else 0. A scheduled heartbeat passes only on 0. The report goes to stdout as
JSON (``{"status", "arms": {arm: report}, "head"}``); drifting tables and a summary
per arm go to the log.
"""

from __future__ import annotations

import sys
import json
import asyncio
import logging
import argparse
from types import MappingProxyType
from datetime import UTC, date, datetime, timedelta
from dataclasses import dataclass
from collections.abc import Mapping, Callable, Awaitable

import asyncpg
import pydantic

from canopy.core.arms import Arm, get_arms_config, subscription_name
from canopy.core.config import get_settings
from canopy.core.replication.sql import (
    Refused,
    redact,
    qualified,
    owner_join,
    asyncpg_dsn,
    quote_ident,
    require_role,
    tables_present,
)
from canopy.core.replication.registry import REGISTRY, TableClass, ReplicationRegistry
from canopy.core.replication.subscriber import (
    _subscription,
    _selected_arms,
    _subscribed_tables,
)

logger = logging.getLogger(__name__)

ATTEMPTS = 3
"""Comparisons of a table that came out inconclusive, before it is reported so."""


@dataclass(frozen=True)
class Bucketing:
    """How a time-series table's rows are counted per day.

    Attributes
    ----------
    time_column : str
        The table's timestamp the day comes from.
    """

    time_column: str


# Every time-series table is dated by an entry here: its own, or the first one up its
# owner path (a test checks), so a new one can't ship unchecked. Snapshot values have
# no timestamp of their own and are dated by their snapshot.
TIME_SERIES_BUCKETS: Mapping[str, Bucketing] = MappingProxyType(
    {
        "squirrel.snapshot": Bucketing(time_column="created_at"),
    }
)


def dated_by(table: str, registry: ReplicationRegistry = REGISTRY) -> tuple[str, int] | None:
    """The table whose ``TIME_SERIES_BUCKETS`` entry dates ``table``'s rows, and its hop up the owner path.

    Hop 0 is ``table`` itself; hop ``n`` is the parent ``sql.owner_join`` aliases ``<alias>_o<n>``.
    ``None`` when neither ``table`` nor any table up its owner path has an entry.
    """
    chain = [table] + [parent for parent, _ in registry.owner_chain(table)]
    for hop, candidate in enumerate(chain):
        if candidate in TIME_SERIES_BUCKETS:
            return candidate, hop
    return None


# References between replicated tables that have no foreign key by design (they cross
# module schemas). Every other reference is a foreign key in the catalog; a test fails
# on an `*_id` column that is neither, so a new one can't be missed.
REFERENCES: Mapping[tuple[str, str], tuple[str, str]] = MappingProxyType(
    {
        ("squirrel.snapshot", "facility_id"): ("core.facility", "id"),
        ("squirrel.pv_facility", "facility_id"): ("core.facility", "id"),
    }
)

# The same text for the same values on both sides, whatever the servers' defaults.
SESSION = {
    "application_name": "canopy-drift-check",
    "TimeZone": "UTC",
    "DateStyle": "ISO, YMD",
    "IntervalStyle": "postgres",
    "extra_float_digits": "1",
    "bytea_output": "hex",
}


@dataclass(frozen=True)
class Check:
    """One table to compare and how."""

    table: str
    kind: TableClass
    owner_join: tuple[str, str] = ("", "")  # sql.owner_join for alias t: the arm's rows only
    columns: tuple[str, ...] = ()  # reference: hashed, in this order, on both sides
    key: tuple[str, ...] = ()  # reference: the primary key the rows are ordered by
    bucketing: Bucketing | None = None  # time series
    dated_alias: str = "t"  # time series: the alias whose time column dates a row


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
        SELECT a.attname
        FROM pg_index i
        JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, n) ON true
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
        WHERE i.indrelid = $1::regclass AND i.indisprimary
        ORDER BY k.n
        """,
        qualified(table),
    )
    return [row["attname"] for row in rows]


async def plan(
    primary: asyncpg.Connection,
    mirror: asyncpg.Connection,
    *,
    arm: str,
    registry: ReplicationRegistry = REGISTRY,
) -> tuple[list[Check], dict[str, str], str | None]:
    """The tables to compare, those not checked with the reason, and the subscription's slot.

    The publications come from the subscription itself, so this works whichever
    publications the primary uses.

    Raises
    ------
    Refused
        No subscription here, or a ready time-series table without a ``TIME_SERIES_BUCKETS`` entry.
    """
    name = subscription_name(arm)
    if (sub := await _subscription(mirror, name)) is None:
        raise Refused(f"no subscription {name} here; there is nothing to compare yet")
    states = await _subscribed_tables(mirror, sub["oid"])
    skipped = {table: state for table, state in sorted(states.items()) if state != "ready"}
    published = await primary.fetch(
        "SELECT DISTINCT schemaname || '.' || tablename AS name FROM pg_publication_tables WHERE pubname = ANY($1)",
        list(sub["subpublications"]),
    )
    for row in published:
        if row["name"] in registry.replicated and row["name"] not in states:
            skipped[row["name"]] = "published but not subscribed yet (run refresh)"
    ready = sorted(table for table, state in states.items() if state == "ready" and table in registry.replicated)

    checks = []
    for table in ready:
        kind = registry.tables[table]
        owners = owner_join(registry, table, alias="t")
        if blocked := [parent for parent, _ in registry.owner_chain(table) if parent not in ready]:
            skipped[table] = f"owner-path parent {blocked[0]} not ready"
            continue
        if kind is TableClass.TIME_SERIES:
            if (dating := dated_by(table, registry)) is None:
                raise Refused(
                    f"time-series table {table} has no entry in TIME_SERIES_BUCKETS, nor any table up its owner path"
                )
            root, hop = dating
            alias = "t" if hop == 0 else f"t_o{hop}"
            checks.append(Check(table, kind, owners, bucketing=TIME_SERIES_BUCKETS[root], dated_alias=alias))
        else:
            on_primary = set(await _columns(primary, table))
            # One column list, in one order, for both sides' row hashes.
            columns = tuple(c for c in await _columns(mirror, table) if c in on_primary)
            checks.append(Check(table, kind, owners, columns=columns, key=tuple(await _primary_key(mirror, table))))
    return checks, dict(sorted(skipped.items())), sub["subslotname"]


async def _observe_reference(conn: asyncpg.Connection, check: Check, owner: str) -> dict:
    join_sql, owner_sql = check.owner_join
    row = ", ".join(f"t.{quote_ident(c)}" for c in check.columns)
    order = ", ".join(f"t.{quote_ident(c)}" for c in check.key)
    record = await conn.fetchrow(
        f"SELECT count(*) AS rows, md5(coalesce(string_agg(ROW({row})::text, E'\\n' ORDER BY {order}), '')) AS md5 "
        f"FROM {qualified(check.table)} t {join_sql} WHERE {owner_sql} = $1",
        owner,
    )
    return {"rows": record["rows"], "md5": record["md5"]}


async def _observe_buckets(conn: asyncpg.Connection, check: Check, since: date | None, owner: str) -> dict[str, int]:
    # The owner join is an inner join, so a child whose parent is gone has no day;
    # the head check reports it.
    join_sql, owner_sql = check.owner_join
    stamp = f"{check.dated_alias}.{quote_ident(check.bucketing.time_column)}"
    query = (
        f"SELECT ({stamp} AT TIME ZONE 'UTC')::date AS day, count(*) AS n "
        f"FROM {qualified(check.table)} t {join_sql} WHERE {owner_sql} = $1"
    )
    args: list = [owner]
    if since is not None:
        query += f" AND {stamp} >= $2"
        args.append(datetime.combine(since, datetime.min.time(), UTC))
    rows = await conn.fetch(query + " GROUP BY 1", *args)
    return {row["day"].isoformat(): row["n"] for row in rows}


async def observe(conn: asyncpg.Connection, checks: list[Check], since: date | None, owner: str) -> dict[str, dict]:
    """One side's figures for ``owner``'s rows in ``checks``, all read in one repeatable-read snapshot."""
    return (await _observe(conn, checks, since, owner))[0]


async def _observe(
    conn: asyncpg.Connection, checks: list[Check], since: date | None, owner: str
) -> tuple[dict[str, dict], str]:
    """``observe``, plus the WAL position inside the snapshot: every commit it sees ends before it."""
    observed = {}
    async with conn.transaction(isolation="repeatable_read", readonly=True):
        lsn = await conn.fetchval("SELECT pg_current_wal_insert_lsn()::text")
        for check in checks:
            if check.kind is TableClass.TIME_SERIES:
                observed[check.table] = await _observe_buckets(conn, check, since, owner)
            else:
                observed[check.table] = await _observe_reference(conn, check, owner)
    return observed, lsn


def compare(check: Check, primary: dict, mirror: dict) -> dict | None:
    """How ``check.table`` differs between the sides' figures, or ``None`` when it doesn't."""
    if check.kind is not TableClass.TIME_SERIES:
        if primary == mirror:
            return None
        return {"kind": "reference", "primary": primary, "mirror": mirror}
    differing = [
        {"day": day, "primary": primary.get(day, 0), "mirror": mirror.get(day, 0)}
        for day in sorted(set(primary) | set(mirror))
        if primary.get(day, 0) != mirror.get(day, 0)
    ]
    if not differing:
        return None
    return {
        "kind": "time_series",
        "days": differing,
        "missing_rows": sum(max(d["primary"] - d["mirror"], 0) for d in differing),
        "extra_rows": sum(max(d["mirror"] - d["primary"], 0) for d in differing),
    }


async def _confirmed(primary: asyncpg.Connection, slot: str | None, lsn: str, deadline: float) -> bool:
    """Wait until the mirror's slot on the primary confirms ``lsn``, until ``deadline``."""
    loop = asyncio.get_running_loop()
    while True:
        if slot is not None and await primary.fetchval(
            "SELECT confirmed_flush_lsn >= $2::text::pg_lsn FROM pg_replication_slots WHERE slot_name = $1",
            slot,
            lsn,
        ):
            return True
        if loop.time() >= deadline:
            return False
        await asyncio.sleep(min(0.5, max(deadline - loop.time(), 0)))


async def _compare(
    primary: asyncpg.Connection,
    mirror: asyncpg.Connection,
    checks: list[Check],
    since: date | None,
    slot: str | None,
    deadline: float,
    after_mirror_read: Callable[[], Awaitable[object]] | None,
    owner: str,
) -> tuple[dict[str, dict], dict[str, str]]:
    """One conclusive-or-not comparison of ``checks``: ``(drift, inconclusive)``, both by table."""
    before, lsn = await _observe(primary, checks, since, owner)
    on_mirror = await observe(mirror, checks, since, owner)
    differing = [c for c in checks if compare(c, before[c.table], on_mirror[c.table]) is not None]
    if not differing:
        return {}, {}
    confirmed = await _confirmed(primary, slot, lsn, deadline)
    on_mirror = await observe(mirror, differing, since, owner)
    if after_mirror_read is not None:
        await after_mirror_read()
    after, _ = await _observe(primary, differing, since, owner)
    drift, inconclusive = {}, {}
    for check in differing:
        if (difference := compare(check, before[check.table], on_mirror[check.table])) is None:
            continue  # it was lag
        if not confirmed:
            inconclusive[check.table] = "the mirror didn't confirm the primary's position in time"
        elif after[check.table] != before[check.table]:
            inconclusive[check.table] = "the primary changed while it was being compared"
        else:
            drift[check.table] = difference
    return drift, inconclusive


def status(report: dict) -> str:
    """``in_step``, ``drift`` or ``not_checked``; the exit status is 0, 1 or 2 accordingly."""
    if report["drift"]:
        return "drift"
    if report["skipped"] or report["inconclusive"] or not report["compared"]:
        return "not_checked"
    return "in_step"


EXIT = {"in_step": 0, "drift": 1, "not_checked": 2}


async def drift_check(
    primary: asyncpg.Connection,
    mirror: asyncpg.Connection,
    *,
    arm: str,
    days: int | None = None,
    settle_s: float = 30.0,
    registry: ReplicationRegistry = REGISTRY,
    after_mirror_read: Callable[[], Awaitable[object]] | None = None,
) -> dict:
    """Compare the mirror with the primary; see the module docstring.

    Parameters
    ----------
    primary : asyncpg.Connection
        Read-only connection to the primary.
    mirror : asyncpg.Connection
        Connection to this mirror's database.
    arm : str
        The arm compared: names the subscription (``canopy_history_<arm>``), and only
        the rows it owns are compared.
    days : int | None
        Compare time-series rows dated in the last ``days`` UTC days only (today included).
    settle_s : float
        Seconds the check may spend waiting for the mirror to confirm the primary's
        position and trying inconclusive tables again.
    after_mirror_read : Callable[[], Awaitable[object]] | None
        Awaited between a differing table's second mirror read and the primary's second
        read; for tests.

    Returns
    -------
    dict
        ``status`` (``in_step``, ``drift`` or ``not_checked``) and ``in_step``; the
        tables ``compared``; those ``skipped`` (not ready or not subscribed) with the
        reason; those ``inconclusive`` with the reason; ``attempts``; and ``drift``: per
        table, how it differs.
    """
    since = None if days is None else datetime.now(UTC).date() - timedelta(days=days - 1)
    checks, skipped, slot = await plan(primary, mirror, arm=arm, registry=registry)
    deadline = asyncio.get_running_loop().time() + settle_s
    drift: dict[str, dict] = {}
    inconclusive: dict[str, str] = {}
    pending, attempts = checks, 0
    while pending and attempts < ATTEMPTS:
        attempts += 1
        found, inconclusive = await _compare(
            primary, mirror, pending, since, slot, deadline, after_mirror_read, owner=arm
        )
        drift.update(found)
        if asyncio.get_running_loop().time() >= deadline:
            break
        pending = [c for c in pending if c.table in inconclusive]
    report = {
        "since": since.isoformat() if since else None,
        "compared": [check.table for check in checks],
        "skipped": skipped,
        "inconclusive": dict(sorted(inconclusive.items())),
        "attempts": attempts,
        "drift": dict(sorted(drift.items())),
    }
    report["status"] = status(report)
    report["in_step"] = report["status"] == "in_step"
    return report


def _log_arm(arm: str, report: dict) -> None:
    for table, difference in report["drift"].items():
        if difference["kind"] == "reference":
            logger.warning(
                "Drift in %s from %s: %d rows on the arm, %d on the head, row hashes %s",
                table,
                arm,
                difference["primary"]["rows"],
                difference["mirror"]["rows"],
                "equal" if difference["primary"]["md5"] == difference["mirror"]["md5"] else "differ",
                extra={"arm": arm},
            )
        else:
            days = [d["day"] for d in difference["days"]]
            logger.warning(
                "Drift in %s from %s: %d rows missing and %d extra on the head over %d days%s",
                table,
                arm,
                difference["missing_rows"],
                difference["extra_rows"],
                len(days),
                f" (oldest {days[0]})" if days else "",
                extra={"arm": arm},
            )
    if report.get("error"):
        logger.warning("%s not checked: %s", arm, report["error"], extra={"arm": arm})
    elif not report["compared"]:
        logger.warning("%s: compared nothing; no subscribed table is ready yet", arm, extra={"arm": arm})
    if report["skipped"]:
        logger.warning(
            "%s, not checked: %s",
            arm,
            ", ".join(f"{t} ({s})" for t, s in report["skipped"].items()),
            extra={"arm": arm},
        )
    if report["inconclusive"]:
        logger.warning(
            "%s, inconclusive: %s",
            arm,
            ", ".join(f"{t} ({s})" for t, s in report["inconclusive"].items()),
            extra={"arm": arm},
        )
    summary = {
        "arm": arm,
        "status": report["status"],
        "in_step": report["in_step"],
        "drift_tables": len(report["drift"]),
        "compared": len(report["compared"]),
        "not_checked": len(report["skipped"]) + len(report["inconclusive"]),
    }
    if report["status"] == "in_step":
        logger.info("drift-check %s: %d tables in step", arm, len(report["compared"]), extra=summary)
    elif report["status"] == "drift":
        tables = list(report["drift"]) + [f"{d['table']} (dangling)" for d in report.get("dangling", [])]
        logger.warning("drift-check %s: drift in %s", arm, ", ".join(tables), extra=summary)
    else:
        logger.warning("drift-check %s: couldn't check every table; see above", arm, extra=summary)


def _log_dangling(found: dict, arm: str | None) -> None:
    logger.warning(
        "Dangling on the head: %d rows of %s, owned by %s, whose %s finds no %s",
        found["rows"],
        found["table"],
        found["owner"] or "nobody known",
        found["column"],
        found["parent"],
        extra={"arm": arm} if arm else {},
    )


def _log_report(report: dict) -> None:
    for arm, arm_report in report["arms"].items():
        for found in arm_report["dangling"]:
            _log_dangling(found, arm)
        _log_arm(arm, arm_report)
    for found in report["head"]["dangling"]:
        _log_dangling(found, None)


async def references(conn: asyncpg.Connection, registry: ReplicationRegistry = REGISTRY) -> list[tuple]:
    """``(table, column, parent, parent_column)`` for each reference between replicated tables here."""
    rows = await conn.fetch(
        """
        SELECT cn.nspname || '.' || c.relname AS child, ca.attname AS column,
               pn.nspname || '.' || p.relname AS parent, pa.attname AS parent_column
        FROM pg_constraint k
        JOIN pg_class c ON c.oid = k.conrelid JOIN pg_namespace cn ON cn.oid = c.relnamespace
        JOIN pg_class p ON p.oid = k.confrelid JOIN pg_namespace pn ON pn.oid = p.relnamespace
        JOIN pg_attribute ca ON ca.attrelid = k.conrelid AND ca.attnum = k.conkey[1]
        JOIN pg_attribute pa ON pa.attrelid = k.confrelid AND pa.attnum = k.confkey[1]
        WHERE k.contype = 'f' AND cardinality(k.conkey) = 1
        """
    )
    replicated = set(registry.replicated) & await tables_present(conn, registry.schemas)
    found = {tuple(row.values()) for row in rows if row["child"] in replicated and row["parent"] in replicated}
    found |= {
        (table, column, parent, parent_column)
        for (table, column), (parent, parent_column) in REFERENCES.items()
        if table in replicated and parent in replicated
    }
    return sorted(found)


async def dangling(mirror: asyncpg.Connection, registry: ReplicationRegistry = REGISTRY) -> list[dict]:
    """Rows on the head whose reference points at a parent that isn't there, by owner.

    Apply runs with ``session_replication_role = replica``, so the head's foreign keys
    don't stop a change that leaves a row behind. A NULL reference points at nothing
    and is never dangling. Each row's ``owner`` is the first found of: its own owner
    through its owner path (a root's own column, or the chain of parents), then the
    owner of each other parent it references that survives, through that parent's
    owner path. ``None`` when no parent survives to tell.

    Returns
    -------
    list[dict]
        ``table``, ``column``, ``parent``, ``owner`` and ``rows``, one entry per
        dangling reference and owner.
    """
    refs = await references(mirror, registry)
    found = []
    for table, column, parent, parent_column in refs:
        join_sql, owner_sql = owner_join(registry, table, alias="x")
        candidates = [f"(SELECT {owner_sql} FROM {qualified(table)} x {join_sql} WHERE x.ctid = t.ctid)"]
        for other_table, other_column, other_parent, other_parent_column in refs:
            if other_table != table or other_column == column:
                continue
            join_p, owner_p = owner_join(registry, other_parent, alias="p")
            candidates.append(
                f"(SELECT {owner_p} FROM {qualified(other_parent)} p {join_p} "
                f"WHERE p.{quote_ident(other_parent_column)} = t.{quote_ident(other_column)})"
            )
        rows = await mirror.fetch(
            f"SELECT coalesce({', '.join(candidates)}) AS owner, count(*) AS rows FROM {qualified(table)} t "
            f"WHERE t.{quote_ident(column)} IS NOT NULL AND NOT EXISTS (SELECT 1 FROM {qualified(parent)} q "
            f"WHERE q.{quote_ident(parent_column)} = t.{quote_ident(column)}) GROUP BY 1 ORDER BY 1 NULLS LAST"
        )
        found += [
            {"table": table, "column": column, "parent": parent, "owner": row["owner"], "rows": row["rows"]}
            for row in rows
        ]
    return found


def _not_checked(reason: str) -> dict:
    return {
        "status": "not_checked",
        "in_step": False,
        "error": reason,
        "compared": [],
        "skipped": {},
        "inconclusive": {},
        "attempts": 0,
        "drift": {},
        "dangling": [],
    }


def overall(statuses: list[str]) -> str:
    """Drift anywhere wins, then anything not checked; in step only when everything is."""
    if "drift" in statuses:
        return "drift"
    return "not_checked" if "not_checked" in statuses else "in_step"


async def _check_arm(mirror: asyncpg.Connection, name: str, arm: Arm, *, days, settle_s, secrets) -> dict:
    """One arm's report; a failure to reach or read it makes that arm not checked, not the run."""
    if arm.reader_dsn is None or not arm.reader_dsn.get_secret_value():
        return _not_checked(f"set CANOPY_ARMS__{name.upper()}__READER_DSN to this arm's read-only URI (from a secret)")
    primary = None
    try:
        primary = await asyncpg.connect(
            arm.reader_dsn.get_secret_value(), server_settings={**SESSION, "default_transaction_read_only": "on"}
        )
        return await drift_check(primary, mirror, arm=name, days=days, settle_s=settle_s)
    except Refused as exc:
        return _not_checked(str(exc))
    except Exception as exc:  # still "couldn't check", never 1 ("drift")
        return _not_checked(redact(str(exc) or repr(exc), secrets))
    finally:
        if primary is not None:
            await primary.close()


async def check_arms(
    mirror: asyncpg.Connection,
    arms: Mapping[str, Arm],
    names: list[str],
    *,
    days: int | None = None,
    settle_s: float = 30.0,
    registry: ReplicationRegistry = REGISTRY,
) -> dict:
    """Each of ``names`` compared with its arm, plus one integrity check of the head.

    Returns
    -------
    dict
        ``status`` over everything (see ``overall``); ``arms``: each arm's report, as
        ``drift_check`` returns it, plus the ``dangling`` rows it owns (which make it
        drift); and ``head``: the dangling rows no configured arm owns, and its ``status``.
    """
    secrets = [arm.reader_dsn.get_secret_value() for arm in arms.values() if arm.reader_dsn]
    reports = {
        name: await _check_arm(mirror, name, arms[name], days=days, settle_s=settle_s, secrets=secrets)
        for name in names
    }
    head_rows = []
    for row in await dangling(mirror, registry):
        if row["owner"] in reports:
            reports[row["owner"]].setdefault("dangling", []).append(row)
        elif row["owner"] not in arms:  # nobody, the head itself, or an instance that isn't an arm
            head_rows.append(row)
        # else: another configured arm's row; that arm's own run reports it
    for report in reports.values():
        report.setdefault("dangling", [])
        if report["dangling"]:
            report["status"], report["in_step"] = "drift", False
    head = {"status": "drift" if head_rows else "in_step", "dangling": head_rows}
    return {
        "status": overall([report["status"] for report in reports.values()] + [head["status"]]),
        "arms": reports,
        "head": head,
    }


async def run(args: argparse.Namespace) -> int:
    """``drift-check``: 0 every arm in step, 1 drift found, 2 not everything could be checked."""
    settings = get_settings()
    try:
        arms = get_arms_config().arms
    except pydantic.ValidationError as exc:
        # Only the messages: the error's own text would echo the settings it was given.
        logger.error("refused: CANOPY_ARMS is invalid: %s", "; ".join(error["msg"] for error in exc.errors()))
        return 2
    mirror = None
    try:
        require_role("drift-check", "mirror", settings.role)
        names = _selected_arms(arms, args.arm, settings.instance)
        mirror = await asyncpg.connect(asyncpg_dsn(settings.database_url), server_settings=SESSION)
        report = await check_arms(mirror, arms, names, days=args.days, settle_s=args.settle)
    except Refused as exc:
        logger.error("refused: %s", exc)
        return 2
    except Exception as exc:  # the head itself couldn't be read: still "couldn't check"
        logger.error("drift-check failed: %s", redact(str(exc) or repr(exc)))
        return 2
    finally:
        if mirror is not None:
            await mirror.close()
    print(json.dumps(report, indent=2), file=sys.stdout)
    _log_report(report)
    return EXIT[report["status"]]
