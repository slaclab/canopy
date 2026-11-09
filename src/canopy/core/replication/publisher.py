"""An arm's side: keep the publications in step with the registry, and report on them.

``publish`` runs after ``alembic upgrade head`` on every deploy of an arm. It is
idempotent: a run that finds the publications already right changes nothing.
"""

from __future__ import annotations

import logging
from dataclasses import field, dataclass
from collections.abc import Mapping

import asyncpg

from canopy.core.arms import _SUBSCRIPTION_PREFIX
from canopy.core.replication.sql import (
    Refused,
    jsonable,
    qualified,
    quote_ident,
    role_exists,
    require_role,
    quote_literal,
    tables_present,
    require_confined,
)
from canopy.core.replication.config import ReplicationConfig
from canopy.core.replication.registry import (
    RETIRED,
    EXCLUDED,
    REGISTRY,
    Publication,
    ReplicationRegistry,
    split_name,
)

logger = logging.getLogger(__name__)

_OPERATIONS = ("insert", "update", "delete", "truncate")


@dataclass(frozen=True)
class PublicationState:
    """A publication as this database has it.

    Attributes
    ----------
    name : str
        Publication name.
    exists : bool
        Whether it exists. When it doesn't, every other field is empty.
    all_tables : bool
        ``FOR ALL TABLES``: it publishes every table, local ones included, and can't be altered.
    operations : frozenset[str]
        Changes it publishes: any of insert, update, delete, truncate.
    tables : frozenset[str]
        ``"schema.table"`` for every table it publishes, however the table got there.
    schemas : frozenset[str]
        Schemas it publishes whole (``FOR TABLES IN SCHEMA``).
    restricted : Mapping[str, dict]
        Tables published only in part, keyed like ``tables``: ``{"row_filter": str | None,
        "columns": list[str] | None}``, where ``None`` means no filter, or every column.
    """

    name: str
    exists: bool = False
    all_tables: bool = False
    operations: frozenset[str] = frozenset()
    tables: frozenset[str] = frozenset()
    schemas: frozenset[str] = frozenset()
    restricted: Mapping[str, dict] = field(default_factory=dict)


async def publication_state(conn: asyncpg.Connection, name: str) -> PublicationState:
    """Publication ``name`` as this database has it; ``exists=False`` when it doesn't."""
    row = await conn.fetchrow(
        "SELECT oid, puballtables, pubinsert, pubupdate, pubdelete, pubtruncate FROM pg_publication WHERE pubname = $1",
        name,
    )
    if row is None:
        return PublicationState(name)
    flags = (row["pubinsert"], row["pubupdate"], row["pubdelete"], row["pubtruncate"])
    tables = await conn.fetch(
        "SELECT schemaname || '.' || tablename AS name FROM pg_publication_tables WHERE pubname = $1", name
    )
    schemas = await conn.fetch(
        """
        SELECT n.nspname FROM pg_publication_namespace pn JOIN pg_namespace n ON n.oid = pn.pnnspid
        WHERE pn.pnpubid = $1
        """,
        row["oid"],
    )
    restricted = await conn.fetch(
        """
        SELECT n.nspname || '.' || c.relname AS name, pg_get_expr(r.prqual, r.prrelid) AS row_filter,
               CASE WHEN r.prattrs IS NULL THEN NULL ELSE ARRAY(
                   SELECT a.attname::text FROM unnest(r.prattrs::int2[]) WITH ORDINALITY AS u(attnum, position)
                   JOIN pg_attribute a ON a.attrelid = r.prrelid AND a.attnum = u.attnum
                   ORDER BY u.position) END AS columns
        FROM pg_publication_rel r
        JOIN pg_class c ON c.oid = r.prrelid JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE r.prpubid = $1 AND (r.prqual IS NOT NULL OR r.prattrs IS NOT NULL)
        """,
        row["oid"],
    )
    return PublicationState(
        name=name,
        exists=True,
        all_tables=row["puballtables"],
        operations=frozenset(op for op, on in zip(_OPERATIONS, flags, strict=True) if on),
        tables=frozenset(t["name"] for t in tables),
        schemas=frozenset(s["nspname"] for s in schemas),
        restricted={
            r["name"]: {"row_filter": r["row_filter"], "columns": None if r["columns"] is None else list(r["columns"])}
            for r in restricted
        },
    )


async def _sync_publication(conn: asyncpg.Connection, publication: Publication, desired: list[str]) -> dict:
    """Create or alter one publication so it carries exactly ``desired``, every row and column of each.

    A plain ``SET TABLE`` replaces the whole membership: it drops row filters and
    column lists, and schemas published whole, as well as tables that don't belong.
    """
    name = quote_ident(publication.name)
    operations = quote_literal(", ".join(publication.operations))
    state = await publication_state(conn, publication.name)
    if state.all_tables:
        raise Refused(
            f"publication {publication.name} is FOR ALL TABLES, so it publishes local tables such as core.api_key "
            f"and can't be altered. Drop it by hand (DROP PUBLICATION {name}) once no mirror streams it, then run "
            "publish again"
        )
    created = not state.exists
    if created:
        await conn.execute(f"CREATE PUBLICATION {name} WITH (publish = {operations})")
        logger.info("Created publication %s", publication.name)
        current_operations = set(publication.operations)
    else:
        current_operations = set(state.operations)

    added = sorted(set(desired) - state.tables)
    removed = sorted(state.tables - set(desired))
    cleared = sorted(set(state.restricted) & set(desired))
    schemas_removed = sorted(state.schemas)
    if desired and (added or removed or cleared or schemas_removed):
        await conn.execute(f"ALTER PUBLICATION {name} SET TABLE {', '.join(qualified(t) for t in desired)}")
    elif removed:
        # Nothing left to publish, and SET TABLE needs at least one table.
        if schemas_removed:
            await conn.execute(
                f"ALTER PUBLICATION {name} DROP TABLES IN SCHEMA {', '.join(quote_ident(s) for s in schemas_removed)}"
            )
        if remaining := (await publication_state(conn, publication.name)).tables:
            await conn.execute(f"ALTER PUBLICATION {name} DROP TABLE {', '.join(qualified(t) for t in remaining)}")
    operations_changed = current_operations != set(publication.operations)
    if operations_changed:
        await conn.execute(f"ALTER PUBLICATION {name} SET (publish = {operations})")

    if added:
        logger.info("Publication %s: added %s", publication.name, ", ".join(added))
    if removed:
        logger.info("Publication %s: removed %s", publication.name, ", ".join(removed))
    if cleared:
        logger.warning(
            "Publication %s: cleared row filters or column lists on %s. A mirror that copied them while restricted "
            "holds only part of them; re-seed those tables there",
            publication.name,
            ", ".join(cleared),
        )
    if schemas_removed:
        logger.info(
            "Publication %s: lists its tables instead of publishing schemas %s whole",
            publication.name,
            ", ".join(schemas_removed),
        )
    if operations_changed:
        logger.info("Publication %s: now publishes %s", publication.name, ", ".join(publication.operations))
    if not (created or added or removed or cleared or schemas_removed or operations_changed):
        logger.info("Publication %s: no change", publication.name)
    return {
        "created": created,
        "added": added,
        "removed": removed,
        "cleared": cleared,
        "schemas_removed": schemas_removed,
        "operations_changed": operations_changed,
    }


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
    with a notice. Each publication ends up carrying whole tables: row filters,
    column lists and schemas published whole are replaced by the plain table list.
    The replicator and reader roles get SELECT on exactly the published tables
    (every other table in those schemas is revoked, so neither can read API-key
    hashes); a role that doesn't exist yet is skipped with a notice, and the next
    run grants it. Afterwards each role must be able to do no more than that,
    counting what it inherits from other roles or PUBLIC; ``publish`` refuses rather
    than change those shared grants.

    Parameters
    ----------
    conn : asyncpg.Connection
        Connection to this arm as a role that may create publications and owns the tables.
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
        On a mirror; when the registry's schemas hold a table it doesn't classify (a
        table history would silently miss); when a publication is ``FOR ALL TABLES``;
        or when the replicator or reader role can reach more than SELECT on the
        published tables (as a superuser, through a table owner, another role or
        PUBLIC). Nothing is changed.
    """
    require_role("publish", "primary", role)
    present = await tables_present(conn, registry.schemas)
    unclassified = sorted(present - set(registry.tables) - EXCLUDED)
    if unclassified:
        raise Refused(
            f"unclassified tables: {', '.join(unclassified)}. Add them to "
            "canopy.core.replication.registry before publishing"
        )
    missing = sorted(set(registry.replicated) - present - RETIRED)
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
                await require_confined(
                    conn,
                    grantee,
                    tables=present,
                    allowed={table: {"SELECT"} for table in published},
                    command="publish",
                    policy="SELECT on the published tables",
                )
                result["roles"][grantee] = "granted"
    return result


async def publisher_status(
    conn: asyncpg.Connection, *, config: ReplicationConfig, registry: ReplicationRegistry = REGISTRY
) -> dict:
    """This arm's replication state, as JSON-ready data.

    Returns
    -------
    dict
        ``wal_level`` and the current WAL position; each publication against the
        registry (``missing``: registered and present but not published;
        ``unexpected``: published but not registered; ``restricted``: tables published
        only in part, by a row filter or column list, keyed like ``tables``; ``schemas``
        published whole; ``all_tables``); whether the replication roles
        exist; and every logical slot in this database, with the WAL it retains.
        Slots named ``pg_<oid>_sync_...`` belong to an initial table copy and are
        normally short-lived; one that stays inactive is holding WAL for a failed copy.
    """
    present = await tables_present(conn, registry.schemas)
    publications = []
    for publication in registry.publications:
        desired = {t for t in registry.published(publication) if t in present}
        state = await publication_state(conn, publication.name)
        publications.append(
            {
                "name": publication.name,
                "exists": state.exists,
                "operations": [op for op in _OPERATIONS if op in state.operations],
                "tables": sorted(state.tables),
                "missing": sorted(desired - state.tables),
                "unexpected": sorted(state.tables - desired),
                "restricted": dict(sorted(state.restricted.items())),
                "schemas": sorted(state.schemas),
                "all_tables": state.all_tables,
            }
        )
    slots = await conn.fetch(
        """
        SELECT s.slot_name,
               CASE WHEN s.slot_name ~ '^pg_[0-9]+_sync_' THEN 'tablesync'
                    WHEN starts_with(s.slot_name, $1) THEN 'subscription'
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
        """,
        _SUBSCRIPTION_PREFIX,
    )
    return {
        "side": "publisher",
        "wal_level": await conn.fetchval("SHOW wal_level"),
        "current_lsn": await conn.fetchval("SELECT pg_current_wal_lsn()::text"),
        "publications": publications,
        "roles": {name: await role_exists(conn, name) for name in (config.replicator_role, config.reader_role)},
        "slots": [jsonable(row) for row in slots],
    }
