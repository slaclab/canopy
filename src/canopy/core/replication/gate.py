"""The checks a primary passes before deleting old rows that only its history mirror will keep.

A primary that prunes old snapshots relies on its history mirror holding them. The
prune job calls ``check_prune_gate`` before each snapshot it deletes; one failed
check stops the run, and the job reports every check that failed, by name:

``instance_set``, ``window_set``
    ``CANOPY_INSTANCE`` names the mirror's slot; ``CANOPY_WINDOW_DAYS`` says how much
    the primary keeps.
``no_delete_publication``
    No publication carrying a pruned table publishes deletes (or truncates). This is
    what keeps a prune delete off the mirror: decoded under any publication set, it
    is either filtered out or, if the mirror still asks for a dropped publication,
    stops replication before it is applied.
``slot_active``
    The mirror's slot exists and a walsender holds it.
``streams_split``
    That walsender streams publications that between them carry every pruned table:
    the mirror has switched to the split layout and still receives new rows.
``wal_status_reserved``
    The slot's WAL is within ``max_wal_size``, not at risk of being removed.
``lag``
    The mirror's flush lag and the age of its last reply are within ``max_lag_s``.
``heartbeat_fresh``, ``heartbeat_ok``
    The mirror reported its checks within ``heartbeat_max_age_h`` (by this
    database's clock), and its backup, WAL archive and drift check all passed.

Slot facts come from ``publisher_status``, so the gate and ``status`` read the same
numbers. Seeing what a walsender streams needs a superuser or ``pg_read_all_stats``.
"""

from __future__ import annotations

from dataclasses import field, dataclass
from collections.abc import Mapping, Sequence

import asyncpg

from canopy.core.replication.sql import Refused
from canopy.core.replication.config import PruneConfig, ReplicationConfig
from canopy.core.replication.registry import REGISTRY, ReplicationRegistry
from canopy.core.replication.publisher import publisher_status
from canopy.core.replication.subscriber import subscription_name


@dataclass(frozen=True)
class GateResult:
    """The outcome of the gate: an empty ``failures`` means every check passed.

    Attributes
    ----------
    failures : dict[str, str]
        Each failed check's name and why it failed.
    """

    failures: dict[str, str] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return not self.failures


def evaluate_gate(
    *,
    slot_name: str,
    tables: Sequence[str],
    status: Mapping,
    delete_publications: Mapping[str, list[str]],
    heartbeat: Mapping | None,
    config: PruneConfig,
) -> GateResult:
    """Judge the gate from facts already read; see the module docstring for each check.

    Parameters
    ----------
    slot_name : str
        The mirror's slot (its subscription's name).
    tables : Sequence[str]
        The ``schema.table`` names the caller will delete from.
    status : Mapping
        ``publisher_status`` output.
    delete_publications : Mapping[str, list[str]]
        For each pruned table, the publications that carry it and publish deletes or truncates.
    heartbeat : Mapping | None
        The mirror's ``core.history_heartbeat`` row plus ``age_h``, or ``None`` when it never reported.
    config : PruneConfig
        Lag and heartbeat limits.

    Returns
    -------
    GateResult
        The checks that failed.
    """
    failures: dict[str, str] = {}
    if delete_publications:
        failures["no_delete_publication"] = "; ".join(
            f"{table} is published with deletes by {', '.join(publications)}"
            for table, publications in sorted(delete_publications.items())
        )

    slot = next((s for s in status["slots"] if s["slot_name"] == slot_name), None)
    if slot is None:
        failures["slot_active"] = f"no slot {slot_name}; is the mirror subscribed?"
    elif not slot["active"]:
        failures["slot_active"] = f"slot {slot_name} has no walsender; the mirror isn't streaming"
    else:
        streamed = slot["walsender_publications"]
        carried = {
            table
            for publication in status["publications"]
            if streamed and publication["name"] in streamed and publication["exists"]
            for table in publication["tables"]
        }
        if streamed is None:
            failures["streams_split"] = "can't see what the walsender streams; needs a superuser or pg_read_all_stats"
        elif missing := sorted(set(tables) - carried):
            failures["streams_split"] = f"the mirror streams {', '.join(streamed)}, which carry no {', '.join(missing)}"
        flush_lag, reply_age = slot["walsender_flush_lag_s"], slot["walsender_reply_age_s"]
        if reply_age is None:
            failures["lag"] = "the mirror hasn't replied to its walsender yet"
        elif reply_age > config.max_lag_s:
            failures["lag"] = f"the mirror last replied {reply_age:.0f} s ago (limit {config.max_lag_s:g} s)"
        elif flush_lag is not None and flush_lag > config.max_lag_s:
            failures["lag"] = f"flush lag is {flush_lag:.0f} s (limit {config.max_lag_s:g} s)"
    if slot is not None and slot["wal_status"] != "reserved":
        failures["wal_status_reserved"] = f"slot {slot_name} has wal_status {slot['wal_status']}"

    if heartbeat is None:
        failures["heartbeat_fresh"] = f"no heartbeat from {slot_name}"
    else:
        if heartbeat["age_h"] > config.heartbeat_max_age_h:
            failures["heartbeat_fresh"] = (
                f"the last heartbeat is {heartbeat['age_h']:.1f} h old (limit {config.heartbeat_max_age_h:g} h)"
            )
        if failed := [check for check in ("backup_ok", "wal_archive_ok", "drift_ok") if not heartbeat[check]]:
            failures["heartbeat_ok"] = f"the mirror reported failed checks: {', '.join(failed)}"
    return GateResult(failures)


async def check_prune_gate(
    conn: asyncpg.Connection,
    *,
    instance: str | None,
    window_days: int | None,
    tables: Sequence[str],
    config: PruneConfig,
    replication: ReplicationConfig,
    registry: ReplicationRegistry = REGISTRY,
) -> GateResult:
    """Read the primary's replication state and judge the gate for deleting from ``tables``.

    Parameters
    ----------
    conn : asyncpg.Connection
        Connection to the primary; a superuser, or a role with ``pg_read_all_stats``.
    instance : str | None
        ``CANOPY_INSTANCE``, which names the mirror's subscription and slot.
    window_days : int | None
        ``CANOPY_WINDOW_DAYS``.
    tables : Sequence[str]
        The ``schema.table`` names the caller will delete from.
    config : PruneConfig
        Lag and heartbeat limits.
    replication : ReplicationConfig
        Passed through to ``publisher_status``.
    registry : ReplicationRegistry
        The publications ``publisher_status`` reports.

    Returns
    -------
    GateResult
        The checks that failed; ``instance_set`` or ``window_set`` alone when a setting is missing.
    """
    preconditions: dict[str, str] = {}
    if not window_days or window_days <= 0:
        preconditions["window_set"] = "set CANOPY_WINDOW_DAYS to the days of snapshots this primary keeps"
    try:
        slot_name = subscription_name(instance)
    except Refused as exc:
        preconditions["instance_set"] = str(exc)
    if preconditions:
        return GateResult(preconditions)

    status = await publisher_status(conn, config=replication, registry=registry)
    rows = await conn.fetch(
        """
        SELECT t.schemaname || '.' || t.tablename AS name, array_agg(p.pubname::text ORDER BY p.pubname) AS publications
        FROM pg_publication p
        JOIN pg_publication_tables t ON t.pubname = p.pubname
        WHERE (p.pubdelete OR p.pubtruncate) AND t.schemaname || '.' || t.tablename = ANY($1::text[])
        GROUP BY 1
        """,
        list(tables),
    )
    try:
        heartbeat = await conn.fetchrow(
            """
            SELECT backup_ok, wal_archive_ok, drift_ok, applied_lsn,
                   extract(epoch FROM clock_timestamp() - written_at)::float8 / 3600 AS age_h
            FROM core.history_heartbeat WHERE name = $1
            """,
            slot_name,
        )
    except asyncpg.UndefinedTableError:
        heartbeat = None
    return evaluate_gate(
        slot_name=slot_name,
        tables=tables,
        status=status,
        delete_publications={row["name"]: list(row["publications"]) for row in rows},
        heartbeat=dict(heartbeat) if heartbeat else None,
        config=config,
    )
