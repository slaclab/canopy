"""The primary's side: keep the publications in step with the registry, and report on them.

``publish`` runs after ``alembic upgrade head`` on every deploy of a primary. It is
idempotent: a run that finds the publications already right changes nothing.
"""

from __future__ import annotations

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


async def _grant_published(conn: asyncpg.Connection, role: str, published: list[str], present: set[str]) -> None:
    """Leave ``role`` with SELECT on exactly ``published`` among the tables in the registry's schemas."""
    ident = quote_ident(role)
    schemas = sorted({split_name(t)[0] for t in published})
    await conn.execute(f"GRANT USAGE ON SCHEMA {', '.join(quote_ident(s) for s in schemas)} TO {ident}")
    await conn.execute(f"REVOKE ALL ON TABLE {', '.join(qualified(t) for t in sorted(present))} FROM {ident}")
    await conn.execute(f"GRANT SELECT ON TABLE {', '.join(qualified(t) for t in published)} TO {ident}")
    logger.info("Role %s: SELECT on the %d published tables only", role, len(published))


async def publish(
    conn: asyncpg.Connection, *, role: str, config: ReplicationConfig, registry: ReplicationRegistry = REGISTRY
) -> dict:
    """Create or update the registry's publications and the replication roles' grants.

    Runs in one transaction: either every publication and grant is brought in line
    or nothing changes. Registered tables the database doesn't have yet are skipped
    with a notice. The replicator and reader roles get SELECT on exactly the
    published tables (every other table in those schemas is revoked, so neither can
    read API-key hashes); a role that doesn't exist yet is skipped with a notice,
    and the next run grants it.

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

    Returns
    -------
    dict
        Per publication, what changed; per role, ``"granted"`` or ``"absent"``; and the
        registered tables that don't exist here.

    Raises
    ------
    Refused
        On a mirror, or when the registry's schemas hold a table it doesn't classify
        (a table history would silently miss). Nothing is changed.
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

    result: dict = {"publications": {}, "roles": {}, "missing": missing}
    async with conn.transaction():
        published: set[str] = set()
        for publication in registry.publications:
            desired = [t for t in registry.published(publication) if t in present]
            published.update(desired)
            result["publications"][publication.name] = await _sync_publication(conn, publication, desired)
        for grantee in (config.replicator_role, config.reader_role):
            if not await role_exists(conn, grantee):
                logger.info("Role %s doesn't exist yet; no grants made for it", grantee)
                result["roles"][grantee] = "absent"
            elif published:
                await _grant_published(conn, grantee, sorted(published), present)
                result["roles"][grantee] = "granted"
    return result


async def publisher_status(
    conn: asyncpg.Connection, *, config: ReplicationConfig, registry: ReplicationRegistry = REGISTRY
) -> dict:
    """The primary's replication state, as JSON-ready data.

    Returns
    -------
    dict
        ``wal_level`` and the current WAL position; each publication against the
        registry (``missing``: registered and present but not published;
        ``unexpected``: published but not registered); whether the replication roles
        exist; and every logical slot in this database, with the WAL it retains.
        Slots named ``pg_<oid>_sync_...`` belong to an initial table copy and are
        normally short-lived; one that stays inactive is holding WAL for a failed copy.
    """
    present = await tables_present(conn, registry.schemas)
    publications = []
    for publication in registry.publications:
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
               r.state AS walsender_state, r.reply_time AS walsender_reply_time
        FROM pg_replication_slots s
        LEFT JOIN pg_stat_replication_slots st ON st.slot_name = s.slot_name
        LEFT JOIN pg_stat_replication r ON r.pid = s.active_pid
        WHERE s.slot_type = 'logical' AND s.database = current_database()
        ORDER BY s.slot_name
        """
    )
    return {
        "side": "publisher",
        "wal_level": await conn.fetchval("SHOW wal_level"),
        "current_lsn": await conn.fetchval("SELECT pg_current_wal_lsn()::text"),
        "publications": publications,
        "roles": {name: await role_exists(conn, name) for name in (config.replicator_role, config.reader_role)},
        "slots": [jsonable(row) for row in slots],
    }
