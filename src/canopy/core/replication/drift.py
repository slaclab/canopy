"""``drift-check``: compare a mirror's copy with its primary, table by table.

Logical replication skips an UPDATE or DELETE whose row the mirror doesn't have,
without an error, so a copy can drift with nothing failing. This check runs on the
mirror (hourly, after ``refresh``), reads the primary as the read-only reader role,
and reports where the two differ:

* reference tables: the row count, and an md5 of every row in primary-key order over
  the columns both sides have (a column the mirror got first isn't drift). Keys are
  UUIDs, which sort the same on any server; a text key could sort differently under
  another collation and show drift that isn't there (a test keeps keys non-text);
* time-series tables: row counts per UTC day, by the table's own timestamp or its
  parent's (``TIME_SERIES_BUCKETS``), plus rows whose parent is missing, which only a
  mirror can hold, since apply doesn't enforce foreign keys.

Only tables the subscription holds in state ready are compared. A difference can be
replication lag, so the tables that differ are read again after a pause, in new
transactions, and only what still differs is drift. ``--days`` limits time-series
buckets to recent days; once the primary prunes old rows, only its window can match.

Exit status: 0 in step, 1 drift found, 2 the check couldn't run. The report goes to
stdout as JSON; each drifting table and a summary go to the log.
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

from canopy.core.config import get_settings
from canopy.core.replication.sql import (
    Refused,
    ReplicationError,
    redact,
    qualified,
    asyncpg_dsn,
    quote_ident,
    require_role,
)
from canopy.core.replication.config import get_replication_config
from canopy.core.replication.registry import REGISTRY, TableClass, ReplicationRegistry
from canopy.core.replication.subscriber import (
    _subscription,
    subscription_name,
    _subscribed_tables,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Bucketing:
    """How a time-series table's rows are counted per day.

    Attributes
    ----------
    time_column : str
        Timestamp the day comes from: this table's, or the parent's when ``parent`` is set.
    parent : str | None
        ``"schema.table"`` whose timestamp dates this table's rows.
    parent_key : str | None
        This table's column holding the parent's primary key.
    """

    time_column: str
    parent: str | None = None
    parent_key: str | None = None


# Every time-series table in the registry needs an entry (a test checks), so a new
# one can't ship unchecked. Children are dated by their parent: snapshot values have
# no timestamp of their own.
TIME_SERIES_BUCKETS: Mapping[str, Bucketing] = MappingProxyType(
    {
        "squirrel.snapshot": Bucketing(time_column="created_at"),
        "squirrel.snapshot_value": Bucketing(
            time_column="created_at", parent="squirrel.snapshot", parent_key="snapshot_id"
        ),
    }
)

ORPHANS = "no parent"
"""Bucket for rows whose parent row is missing."""

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
    columns: tuple[str, ...] = ()  # reference: hashed, in this order, on both sides
    key: tuple[str, ...] = ()  # reference: the primary key the rows are ordered by
    bucketing: Bucketing | None = None  # time series
    parent_id: str = "id"  # time series with a parent: the parent's primary key column


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
    instance: str | None,
    registry: ReplicationRegistry = REGISTRY,
) -> tuple[list[Check], dict[str, str]]:
    """The tables to compare, and those skipped with the reason.

    Raises
    ------
    Refused
        No subscription here, or a ready time-series table without a ``TIME_SERIES_BUCKETS`` entry.
    """
    name = subscription_name(instance)
    if (sub := await _subscription(mirror, name)) is None:
        raise Refused(f"no subscription {name} here; there is nothing to compare yet")
    states = await _subscribed_tables(mirror, sub["oid"])
    skipped = {table: state for table, state in sorted(states.items()) if state != "ready"}
    ready = sorted(table for table, state in states.items() if state == "ready" and table in registry.replicated)

    checks = []
    for table in ready:
        kind = registry.tables[table]
        if kind is TableClass.TIME_SERIES:
            if (bucketing := TIME_SERIES_BUCKETS.get(table)) is None:
                raise Refused(f"time-series table {table} has no entry in TIME_SERIES_BUCKETS")
            if bucketing.parent is None:
                checks.append(Check(table, kind, bucketing=bucketing))
            elif bucketing.parent not in ready:
                skipped[table] = f"parent {bucketing.parent} not ready"
            else:
                (parent_id,) = await _primary_key(mirror, bucketing.parent)
                checks.append(Check(table, kind, bucketing=bucketing, parent_id=parent_id))
        else:
            on_primary = set(await _columns(primary, table))
            # One column list, in one order, for both sides' row hashes.
            columns = tuple(c for c in await _columns(mirror, table) if c in on_primary)
            checks.append(Check(table, kind, columns=columns, key=tuple(await _primary_key(mirror, table))))
    return checks, skipped


async def _observe_reference(conn: asyncpg.Connection, check: Check) -> dict:
    row = ", ".join(quote_ident(c) for c in check.columns)
    order = ", ".join(quote_ident(c) for c in check.key)
    record = await conn.fetchrow(
        f"SELECT count(*) AS rows, md5(coalesce(string_agg(ROW({row})::text, E'\\n' ORDER BY {order}), '')) AS md5 "
        f"FROM {qualified(check.table)}"
    )
    return {"rows": record["rows"], "md5": record["md5"]}


async def _observe_buckets(conn: asyncpg.Connection, check: Check, since: date | None) -> dict[str, int]:
    bucketing = check.bucketing
    time_column = quote_ident(bucketing.time_column)
    if bucketing.parent is None:
        query = f"SELECT ({time_column} AT TIME ZONE 'UTC')::date AS day, count(*) AS n FROM {qualified(check.table)}"
        if since is not None:
            query += f" WHERE {time_column} >= $1"
    else:
        query = (
            f"SELECT (p.{time_column} AT TIME ZONE 'UTC')::date AS day, count(*) AS n "
            f"FROM {qualified(check.table)} c LEFT JOIN {qualified(bucketing.parent)} p "
            f"ON p.{quote_ident(check.parent_id)} = c.{quote_ident(bucketing.parent_key)}"
        )
        if since is not None:
            query += f" WHERE p.{quote_ident(check.parent_id)} IS NULL OR p.{time_column} >= $1"
    query += " GROUP BY 1"
    args = [] if since is None else [datetime.combine(since, datetime.min.time(), UTC)]
    rows = await conn.fetch(query, *args)
    return {ORPHANS if row["day"] is None else row["day"].isoformat(): row["n"] for row in rows}


async def observe(conn: asyncpg.Connection, checks: list[Check], since: date | None) -> dict[str, dict]:
    """One side's figures for ``checks``, all read in one repeatable-read snapshot."""
    observed = {}
    async with conn.transaction(isolation="repeatable_read", readonly=True):
        for check in checks:
            if check.kind is TableClass.TIME_SERIES:
                observed[check.table] = await _observe_buckets(conn, check, since)
            else:
                observed[check.table] = await _observe_reference(conn, check)
    return observed


def compare(check: Check, primary: dict, mirror: dict) -> dict | None:
    """How ``check.table`` differs between the sides' figures, or ``None`` when it doesn't."""
    if check.kind is not TableClass.TIME_SERIES:
        if primary == mirror:
            return None
        return {"kind": "reference", "primary": primary, "mirror": mirror}
    days = sorted((set(primary) | set(mirror)) - {ORPHANS})
    differing = [
        {"day": day, "primary": primary.get(day, 0), "mirror": mirror.get(day, 0)}
        for day in days
        if primary.get(day, 0) != mirror.get(day, 0)
    ]
    orphans = {"primary": primary.get(ORPHANS, 0), "mirror": mirror.get(ORPHANS, 0)}
    if not differing and orphans["primary"] == orphans["mirror"]:
        return None
    return {
        "kind": "time_series",
        "days": differing,
        "orphans": orphans,
        "missing_rows": sum(max(d["primary"] - d["mirror"], 0) for d in differing),
        "extra_rows": sum(max(d["mirror"] - d["primary"], 0) for d in differing),
    }


async def _differences(
    primary: asyncpg.Connection, mirror: asyncpg.Connection, checks: list[Check], since: date | None
) -> dict[str, dict]:
    # Primary first: the mirror, read a moment later, has had time to apply what the primary showed.
    on_primary = await observe(primary, checks, since)
    on_mirror = await observe(mirror, checks, since)
    found = {}
    for check in checks:
        if (difference := compare(check, on_primary[check.table], on_mirror[check.table])) is not None:
            found[check.table] = difference
    return found


async def drift_check(
    primary: asyncpg.Connection,
    mirror: asyncpg.Connection,
    *,
    instance: str | None,
    days: int | None = None,
    settle: Callable[[], Awaitable[object]] | None = None,
    registry: ReplicationRegistry = REGISTRY,
) -> dict:
    """Compare the mirror with the primary; see the module docstring.

    Parameters
    ----------
    primary : asyncpg.Connection
        Read-only connection to the primary.
    mirror : asyncpg.Connection
        Connection to this mirror's database.
    instance : str | None
        ``CANOPY_INSTANCE``; names the subscription.
    days : int | None
        Compare time-series rows dated in the last ``days`` UTC days only (today included).
    settle : Callable[[], Awaitable[object]] | None
        Awaited between the first comparison and the re-check of what differed; no
        re-check when ``None``.

    Returns
    -------
    dict
        ``in_step``; the tables ``compared``; those ``skipped`` with the reason;
        ``rechecked`` (differed on the first read and were read again after ``settle``;
        empty without it); and ``drift``: per table, how it
        still differs.
    """
    since = None if days is None else datetime.now(UTC).date() - timedelta(days=days - 1)
    checks, skipped = await plan(primary, mirror, instance=instance, registry=registry)
    drift = await _differences(primary, mirror, checks, since)
    rechecked = []
    if drift and settle is not None:
        rechecked = sorted(drift)
        await settle()
        drift = await _differences(primary, mirror, [c for c in checks if c.table in drift], since)
    return {
        "in_step": not drift,
        "since": since.isoformat() if since else None,
        "compared": [check.table for check in checks],
        "skipped": skipped,
        "rechecked": rechecked,
        "drift": drift,
    }


def _log_report(report: dict) -> None:
    for table, difference in report["drift"].items():
        if difference["kind"] == "reference":
            logger.warning(
                "Drift in %s: %d rows on the primary, %d on the mirror, row hashes %s",
                table,
                difference["primary"]["rows"],
                difference["mirror"]["rows"],
                "equal" if difference["primary"]["md5"] == difference["mirror"]["md5"] else "differ",
            )
        else:
            days = [d["day"] for d in difference["days"]]
            logger.warning(
                "Drift in %s: %d rows missing and %d extra on the mirror over %d days%s; rows without a parent: "
                "%d on the primary, %d on the mirror",
                table,
                difference["missing_rows"],
                difference["extra_rows"],
                len(days),
                f" (oldest {days[0]})" if days else "",
                difference["orphans"]["primary"],
                difference["orphans"]["mirror"],
            )
    if not report["compared"]:
        logger.warning("drift-check compared nothing: no subscribed table is ready yet")
    if report["skipped"]:
        logger.info("Not compared (not ready): %s", ", ".join(f"{t} ({s})" for t, s in report["skipped"].items()))
    summary = {"in_step": report["in_step"], "drift_tables": len(report["drift"]), "compared": len(report["compared"])}
    if report["in_step"]:
        logger.info("drift-check: %d tables in step", len(report["compared"]), extra=summary)
    else:
        logger.warning("drift-check: drift in %s", ", ".join(report["drift"]), extra=summary)


async def run(args: argparse.Namespace) -> int:
    """``drift-check``: 0 in step, 1 drift found, 2 the check couldn't run."""
    settings = get_settings()
    config = get_replication_config()
    reader_dsn = config.reader_dsn.get_secret_value() if config.reader_dsn else None
    secrets = [reader_dsn]
    primary = mirror = None
    try:
        require_role("drift-check", "mirror", settings.role)
        if not reader_dsn:
            raise Refused(
                "set CANOPY_REPLICATION__READER_DSN to the primary's read-only connection URI (from a secret)"
            )
        primary = await asyncpg.connect(reader_dsn, server_settings={**SESSION, "default_transaction_read_only": "on"})
        mirror = await asyncpg.connect(asyncpg_dsn(settings.database_url), server_settings=SESSION)
        report = await drift_check(
            primary,
            mirror,
            instance=settings.instance,
            days=args.days,
            settle=(lambda: asyncio.sleep(args.settle)) if args.settle > 0 else None,
        )
    except Refused as exc:
        logger.error("refused: %s", exc)
        return 2
    except (ReplicationError, OSError, asyncpg.PostgresError, asyncpg.InterfaceError, ValueError) as exc:
        logger.error("drift-check failed: %s", redact(str(exc), secrets))
        return 2
    except Exception as exc:  # anything else is still "couldn't check", never 1 ("drift")
        logger.error("drift-check failed unexpectedly: %s", redact(repr(exc), secrets))
        return 2
    finally:
        for conn in (primary, mirror):
            if conn is not None:
                await conn.close()
    print(json.dumps(report, indent=2), file=sys.stdout)
    _log_report(report)
    return 0 if report["in_step"] else 1
