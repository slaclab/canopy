"""The primary's side: keep the publications in step with the registry, and report on them.

``publish`` runs after ``alembic upgrade head`` on every deploy of a primary. It is
idempotent: a run that finds the publications already right changes nothing.

A primary that will prune old rows switches from the single layout to the split
one, once, in three steps:

1. ``publish --split`` creates ``canopy_ref`` and ``canopy_ts`` beside ``canopy_history``.
2. ``switch`` on the mirror moves its subscription to them.
3. ``publish --split`` again drops ``canopy_history``, but only once every mirror's
   walsender is seen streaming the split publications.

From then on, plain ``publish`` keeps the split pair in step. A deploy never
switches layouts by itself.
"""

from __future__ import annotations

import re
import logging

import asyncpg

from canopy.core.replication.sql import (
    Refused,
    jsonable,
    qualified,
    quote_ident,
    role_exists,
    require_role,
    quote_literal,
    tables_present,
)
from canopy.core.replication.config import ReplicationConfig
from canopy.core.replication.registry import (
    EXCLUDED,
    REGISTRY,
    Publication,
    ReplicationRegistry,
    split_name,
)

logger = logging.getLogger(__name__)

_OPERATIONS = ("insert", "update", "delete", "truncate")
# The walsender's command text, as pg_stat_activity shows it to a privileged role:
# START_REPLICATION SLOT "s" LOGICAL 0/0 (proto_version '4', publication_names '"a","b"')
_PUBLICATION_NAMES = re.compile(r"publication_names '((?:[^']|'')*)'")
_HEARTBEAT_TABLE = "core.history_heartbeat"


async def publication_layout(conn: asyncpg.Connection, registry: ReplicationRegistry = REGISTRY) -> str:
    """Which publication layout this database has.

    Returns
    -------
    str
        ``"none"`` before the first ``publish``, ``"single"`` with only ``canopy_history``,
        ``"switching"`` with both layouts during the switch, ``"split"`` after it.
    """
    existing = {
        row["pubname"]
        for row in await conn.fetch(
            "SELECT pubname FROM pg_publication WHERE pubname = ANY($1::text[])",
            [p.name for p in registry.all_publications],
        )
    }
    single = any(p.name in existing for p in registry.publications)
    split = any(p.name in existing for p in registry.split_publications)
    return {(False, False): "none", (True, False): "single", (True, True): "switching", (False, True): "split"}[
        (single, split)
    ]


def walsender_publications(command: str | None) -> list[str] | None:
    """The publications a walsender streams, read from its ``START_REPLICATION`` command.

    ``None`` when the command isn't visible (``<insufficient privilege>`` for a role
    without ``pg_read_all_stats``) or names no publications.
    """
    if not command or not (match := _PUBLICATION_NAMES.search(command)):
        return None
    names = match.group(1).replace("''", "'")
    return [name.strip().strip('"').replace('""', '"') for name in names.split(",") if name.strip()]


async def _subscription_walsenders(conn: asyncpg.Connection) -> list[dict]:
    """Each history mirror's slot here, whether a walsender holds it, and what it streams."""
    rows = await conn.fetch(
        """
        SELECT s.slot_name, s.active, a.query AS command
        FROM pg_replication_slots s
        LEFT JOIN pg_stat_activity a ON a.pid = s.active_pid
        WHERE s.slot_type = 'logical' AND s.database = current_database()
          AND s.slot_name LIKE 'canopy\\_history\\_%'
        ORDER BY s.slot_name
        """
    )
    return [
        {"slot": row["slot_name"], "active": row["active"], "publications": walsender_publications(row["command"])}
        for row in rows
    ]


def _not_switched(walsenders: list[dict], registry: ReplicationRegistry) -> list[str]:
    """Why ``canopy_history`` can't be dropped yet: one reason per mirror not streaming the split pair."""
    if not walsenders:
        return ["no history mirror's slot exists here, so none can be seen to have switched"]
    split = {p.name for p in registry.split_publications}
    reasons = []
    for walsender in walsenders:
        if not walsender["active"]:
            reasons.append(f"slot {walsender['slot']} has no walsender (its mirror isn't connected)")
        elif walsender["publications"] is None:
            reasons.append(
                f"can't read what slot {walsender['slot']} streams; publish needs a superuser or pg_read_all_stats"
            )
        elif not set(walsender["publications"]) <= split:
            reasons.append(f"slot {walsender['slot']} still streams {', '.join(walsender['publications'])}")
    return reasons


async def _publication_state(conn: asyncpg.Connection, name: str) -> tuple[set[str], set[str]] | None:
    """``(tables, operations)`` of publication ``name``, or ``None`` when it doesn't exist."""
    row = await conn.fetchrow(
        "SELECT pubinsert, pubupdate, pubdelete, pubtruncate FROM pg_publication WHERE pubname = $1", name
    )
    if row is None:
        return None
    operations = {op for op, on in zip(_OPERATIONS, row.values(), strict=True) if on}
    tables = await conn.fetch(
        "SELECT schemaname || '.' || tablename AS name FROM pg_publication_tables WHERE pubname = $1", name
    )
    return {t["name"] for t in tables}, operations


async def _sync_publication(conn: asyncpg.Connection, publication: Publication, desired: list[str]) -> dict:
    """Create or alter one publication so it carries exactly ``desired``."""
    name = quote_ident(publication.name)
    operations = quote_literal(", ".join(publication.operations))
    state = await _publication_state(conn, publication.name)
    created = state is None
    if created:
        await conn.execute(f"CREATE PUBLICATION {name} WITH (publish = {operations})")
        logger.info("Created publication %s", publication.name)
        current_tables, current_operations = set(), set(publication.operations)
    else:
        current_tables, current_operations = state

    added = sorted(set(desired) - current_tables)
    removed = sorted(current_tables - set(desired))
    if desired and (added or removed):
        await conn.execute(f"ALTER PUBLICATION {name} SET TABLE {', '.join(qualified(t) for t in desired)}")
    elif removed:
        await conn.execute(f"ALTER PUBLICATION {name} DROP TABLE {', '.join(qualified(t) for t in removed)}")
    operations_changed = current_operations != set(publication.operations)
    if operations_changed:
        await conn.execute(f"ALTER PUBLICATION {name} SET (publish = {operations})")

    if added:
        logger.info("Publication %s: added %s", publication.name, ", ".join(added))
    if removed:
        logger.info("Publication %s: removed %s", publication.name, ", ".join(removed))
    if operations_changed:
        logger.info("Publication %s: now publishes %s", publication.name, ", ".join(publication.operations))
    if not (created or added or removed or operations_changed):
        logger.info("Publication %s: no change", publication.name)
    return {"created": created, "added": added, "removed": removed, "operations_changed": operations_changed}


async def _grant_heartbeat(conn: asyncpg.Connection, role: str, present: set[str]) -> None:
    """Leave ``role`` able to read and write ``core.history_heartbeat`` and no other registry table."""
    ident = quote_ident(role)
    await conn.execute(f"GRANT USAGE ON SCHEMA core TO {ident}")
    await conn.execute(f"REVOKE ALL ON TABLE {', '.join(qualified(t) for t in sorted(present))} FROM {ident}")
    await conn.execute(f"GRANT SELECT, INSERT, UPDATE ON TABLE {qualified(_HEARTBEAT_TABLE)} TO {ident}")
    logger.info("Role %s: may write %s only", role, _HEARTBEAT_TABLE)


async def _grant_published(conn: asyncpg.Connection, role: str, published: list[str], present: set[str]) -> None:
    """Leave ``role`` with SELECT on exactly ``published`` among the tables in the registry's schemas."""
    ident = quote_ident(role)
    schemas = sorted({split_name(t)[0] for t in published})
    await conn.execute(f"GRANT USAGE ON SCHEMA {', '.join(quote_ident(s) for s in schemas)} TO {ident}")
    await conn.execute(f"REVOKE ALL ON TABLE {', '.join(qualified(t) for t in sorted(present))} FROM {ident}")
    await conn.execute(f"GRANT SELECT ON TABLE {', '.join(qualified(t) for t in published)} TO {ident}")
    logger.info("Role %s: SELECT on the %d published tables only", role, len(published))


async def publish(
    conn: asyncpg.Connection,
    *,
    role: str,
    config: ReplicationConfig,
    registry: ReplicationRegistry = REGISTRY,
    split: bool = False,
) -> dict:
    """Create or update the registry's publications and the replication roles' grants.

    Runs in one transaction: either every publication and grant is brought in line
    or nothing changes. Registered tables the database doesn't have yet are skipped
    with a notice. The replicator and reader roles get SELECT on exactly the
    published tables (every other table in those schemas is revoked, so neither can
    read API-key hashes); the heartbeat role may write ``core.history_heartbeat``
    only. A role that doesn't exist yet is skipped with a notice, and the next run
    grants it.

    Keeps whichever layout the database has. ``split`` moves to the split layout:
    the first run creates the split pair beside ``canopy_history``; a later run, once
    every mirror streams the split pair, drops ``canopy_history``.

    Parameters
    ----------
    conn : asyncpg.Connection
        Connection to the primary as a role that may create publications and owns the tables.
    role : str
        This instance's ``CANOPY_ROLE``; must be ``"primary"``.
    config : ReplicationConfig
        Names of the replicator and reader roles.
    registry : ReplicationRegistry
        Table classes and publications to apply.
    split : bool
        Move to the split layout (``--split``).

    Returns
    -------
    dict
        The ``layout`` afterwards; per publication, what changed; per role,
        ``"granted"`` or ``"absent"``; the registered tables that don't exist here;
        and, when this run dropped ``canopy_history``, the WAL position just after.

    Raises
    ------
    Refused
        On a mirror; when the registry's schemas hold a table it doesn't classify (a
        table history would silently miss); or, for ``split`` with both layouts
        present, when some mirror isn't yet streaming the split pair. Nothing is changed.
    """
    require_role("publish", "primary", role)
    present = await tables_present(conn, registry.schemas)
    unclassified = sorted(present - set(registry.tables) - EXCLUDED)
    if unclassified:
        raise Refused(
            f"unclassified tables: {', '.join(unclassified)}. Add them to "
            "canopy.core.replication.registry before publishing"
        )
    missing = sorted(set(registry.replicated) - present)
    if missing:
        logger.info("Not in this database yet, skipped: %s", ", ".join(missing))
    if (wal_level := await conn.fetchval("SHOW wal_level")) != "logical":
        logger.warning("wal_level is %s; a subscription can't stream changes until it is logical", wal_level)

    layout = await publication_layout(conn, registry)
    drop_single = split and layout == "switching"
    if drop_single and (reasons := _not_switched(await _subscription_walsenders(conn), registry)):
        names = ", ".join(p.name for p in registry.publications)
        raise Refused(
            f"{names} is still needed: {'; '.join(reasons)}. Run `switch` on the mirror, "
            "then `publish --split` here again"
        )
    if split or layout in ("switching", "split"):
        publications = registry.split_publications + (
            () if layout in ("none", "split") or drop_single else registry.publications
        )
    else:
        publications = registry.publications

    result: dict = {"publications": {}, "roles": {}, "missing": missing}
    async with conn.transaction():
        published: set[str] = set()
        for publication in publications:
            desired = [t for t in registry.published(publication) if t in present]
            published.update(desired)
            result["publications"][publication.name] = await _sync_publication(conn, publication, desired)
        if drop_single:
            for publication in registry.publications:
                await conn.execute(f"DROP PUBLICATION {quote_ident(publication.name)}")
                result["publications"][publication.name] = {"dropped": True}
        for grantee in (config.replicator_role, config.reader_role):
            if not await role_exists(conn, grantee):
                logger.info("Role %s doesn't exist yet; no grants made for it", grantee)
                result["roles"][grantee] = "absent"
            elif published:
                await _grant_published(conn, grantee, sorted(published), present)
                result["roles"][grantee] = "granted"
        if not await role_exists(conn, config.heartbeat_role):
            logger.info("Role %s doesn't exist yet; no grants made for it", config.heartbeat_role)
            result["roles"][config.heartbeat_role] = "absent"
        elif _HEARTBEAT_TABLE in present:
            await _grant_heartbeat(conn, config.heartbeat_role, present)
            result["roles"][config.heartbeat_role] = "granted"
    if drop_single:
        result["dropped_at_lsn"] = await conn.fetchval("SELECT pg_current_wal_lsn()::text")
        logger.info(
            "Dropped %s at %s; the split layout is complete",
            ", ".join(p.name for p in registry.publications),
            result["dropped_at_lsn"],
        )
    elif split and layout != "split":
        logger.info(
            "Split publications are in place. Next: run `switch` on the mirror, then `publish --split` here again"
        )
    result["layout"] = await publication_layout(conn, registry)
    return result


async def publisher_status(
    conn: asyncpg.Connection, *, config: ReplicationConfig, registry: ReplicationRegistry = REGISTRY
) -> dict:
    """The primary's replication state, as JSON-ready data.

    Returns
    -------
    dict
        ``wal_level`` and the current WAL position; the publication ``layout`` (see
        ``publication_layout``) and each publication it uses against the registry
        (``missing``: registered and present but not published; ``unexpected``:
        published but not registered); whether the replication roles exist; and every
        logical slot in this database, with the WAL it retains. Slots named
        ``pg_<oid>_sync_...`` belong to an initial table copy and are normally
        short-lived; one that stays inactive is holding WAL for a failed copy. For a
        slot a walsender holds: the publications it streams (``None`` when this role
        may not see them), its flush lag (``None`` once caught up and idle) and the
        age of the mirror's last reply, in seconds.
    """
    present = await tables_present(conn, registry.schemas)
    layout = await publication_layout(conn, registry)
    publications = []
    in_use = {
        "none": registry.publications,
        "single": registry.publications,
        "switching": registry.all_publications,
        "split": registry.split_publications,
    }[layout]
    for publication in in_use:
        desired = {t for t in registry.published(publication) if t in present}
        state = await _publication_state(conn, publication.name)
        tables, operations = state if state else (set(), set())
        publications.append(
            {
                "name": publication.name,
                "exists": state is not None,
                "operations": [op for op in _OPERATIONS if op in operations],
                "tables": sorted(tables),
                "missing": sorted(desired - tables),
                "unexpected": sorted(tables - desired),
            }
        )
    slots = await conn.fetch(
        """
        SELECT s.slot_name,
               CASE WHEN s.slot_name ~ '^pg_[0-9]+_sync_' THEN 'tablesync'
                    WHEN s.slot_name LIKE 'canopy\\_history\\_%' THEN 'subscription'
                    ELSE 'other' END AS kind,
               s.active, s.active_pid, s.wal_status, s.safe_wal_size,
               s.restart_lsn::text AS restart_lsn, s.confirmed_flush_lsn::text AS confirmed_flush_lsn,
               pg_wal_lsn_diff(pg_current_wal_lsn(), s.restart_lsn) AS retained_bytes,
               pg_wal_lsn_diff(pg_current_wal_lsn(), s.confirmed_flush_lsn) AS lag_bytes,
               st.spill_bytes, st.stream_bytes,
               r.state AS walsender_state, r.reply_time AS walsender_reply_time,
               extract(epoch FROM r.flush_lag)::float8 AS walsender_flush_lag_s,
               extract(epoch FROM clock_timestamp() - r.reply_time)::float8 AS walsender_reply_age_s,
               a.query AS walsender_command
        FROM pg_replication_slots s
        LEFT JOIN pg_stat_replication_slots st ON st.slot_name = s.slot_name
        LEFT JOIN pg_stat_replication r ON r.pid = s.active_pid
        LEFT JOIN pg_stat_activity a ON a.pid = s.active_pid
        WHERE s.slot_type = 'logical' AND s.database = current_database()
        ORDER BY s.slot_name
        """
    )
    slot_rows = []
    for row in slots:
        slot = jsonable(row)
        slot["walsender_publications"] = walsender_publications(slot.pop("walsender_command"))
        slot_rows.append(slot)
    roles = (config.replicator_role, config.reader_role, config.heartbeat_role)
    return {
        "side": "publisher",
        "wal_level": await conn.fetchval("SHOW wal_level"),
        "current_lsn": await conn.fetchval("SELECT pg_current_wal_lsn()::text"),
        "layout": layout,
        "publications": publications,
        "roles": {name: await role_exists(conn, name) for name in roles},
        "slots": slot_rows,
    }
