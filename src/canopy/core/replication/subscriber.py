"""The mirror's side: subscribe to the primary, pick up new tables, grant the app role, report.

Order on a mirror's deploy: ``alembic upgrade head``, then ``grants``, then
``refresh``. ``subscribe`` runs once, after the first migration, and is a no-op
afterwards. The mirror always migrates before its primary, so it never lacks a
table the primary publishes.

Two commands reach the primary over ``CANOPY_REPLICATION__HEARTBEAT_DSN`` rather
than through the subscription: ``heartbeat`` records this mirror's backup,
WAL-archive and drift checks there, and ``switch`` (once, when the primary moves to
the split layout) reads its slot's position before moving the subscription.

Postgres runs ``CREATE SUBSCRIPTION`` (when it creates the slot) and
``ALTER SUBSCRIPTION ... REFRESH`` only outside a transaction, which is how asyncpg
executes a bare statement.
"""

from __future__ import annotations

import re
import asyncio
import logging

import asyncpg

from canopy.core.replication.sql import (
    Refused,
    ReplicationError,
    redact,
    jsonable,
    non_empty,
    qualified,
    quote_ident,
    role_exists,
    is_superuser,
    require_role,
    table_owners,
    quote_literal,
    tables_present,
    schemas_present,
)
from canopy.core.replication.config import ReplicationConfig
from canopy.core.replication.registry import EXCLUDED, REGISTRY, ReplicationRegistry

logger = logging.getLogger(__name__)

_SUBSCRIPTION_PREFIX = "canopy_history_"
_INSTANCE_NAME = re.compile(r"[a-z0-9_]+")
_MAX_IDENTIFIER_BYTES = 63
_TABLE_STATES = {"i": "initialize", "d": "copying", "f": "copied", "s": "synchronized", "r": "ready"}


def subscription_name(instance: str | None) -> str:
    """``canopy_history_<instance>``, the subscription (and the slot on the primary) for this mirror.

    Raises
    ------
    Refused
        When ``CANOPY_INSTANCE`` is unset or isn't lowercase letters, digits and underscores.
    """
    if not instance:
        raise Refused("set CANOPY_INSTANCE to this instance's name; the subscription is named after it")
    if not _INSTANCE_NAME.fullmatch(instance):
        raise Refused(f"CANOPY_INSTANCE={instance!r} must be lowercase letters, digits and underscores")
    name = _SUBSCRIPTION_PREFIX + instance
    if len(name.encode()) > _MAX_IDENTIFIER_BYTES:
        raise Refused(f"subscription name {name!r} exceeds Postgres's {_MAX_IDENTIFIER_BYTES}-byte limit")
    return name


async def _subscription(conn: asyncpg.Connection, name: str) -> asyncpg.Record | None:
    """The subscription's catalog row in this database. Never reads ``subconninfo``.

    ``"char"`` columns are cast to text: asyncpg returns them as bytes.
    """
    return await conn.fetchrow(
        """
        SELECT oid, subname, subenabled, subslotname, subpublications, substream::text AS substream,
               subowner::regrole::text AS owner
        FROM pg_subscription
        WHERE subname = $1 AND subdbid = (SELECT oid FROM pg_database WHERE datname = current_database())
        """,
        name,
    )


async def _subscribed_tables(conn: asyncpg.Connection, subid: int) -> dict[str, str]:
    """``{"schema.table": state}`` for each table the subscription holds."""
    rows = await conn.fetch(
        """
        SELECT n.nspname || '.' || c.relname AS name, r.srsubstate::text AS state
        FROM pg_subscription_rel r
        JOIN pg_class c ON c.oid = r.srrelid JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE r.srsubid = $1
        """,
        subid,
    )
    return {row["name"]: _TABLE_STATES.get(row["state"], row["state"]) for row in rows}


def _layout(publications: list[str], registry: ReplicationRegistry) -> str | None:
    """``"single"`` or ``"split"`` when ``publications`` are exactly that layout's, else ``None``."""
    for layout, expected in (("single", registry.publications), ("split", registry.split_publications)):
        if sorted(publications) == sorted(p.name for p in expected):
            return layout
    return None


def _warn_on_drift(sub: asyncpg.Record, registry: ReplicationRegistry) -> None:
    """Log where an existing subscription differs from what ``subscribe`` would create."""
    if _layout(sub["subpublications"], registry) is None:
        logger.warning(
            "Subscription %s reads publications %s; the registry expects %s or %s",
            sub["subname"],
            ", ".join(sub["subpublications"]),
            ", ".join(p.name for p in registry.publications),
            ", ".join(p.name for p in registry.split_publications),
        )
    if sub["substream"] != "p":
        logger.warning(
            "Subscription %s doesn't stream in parallel (streaming = %s); large transactions spill on the primary",
            sub["subname"],
            sub["substream"],
        )


async def _require_empty(conn: asyncpg.Connection, tables: list[str], action: str) -> None:
    """Refuse ``action`` when any of ``tables`` already has rows.

    The initial copy into a non-empty table fails on duplicate keys, leaves the table
    stuck mid-copy and keeps a slot on the primary that holds WAL until someone drops it.
    """
    if occupied := await non_empty(conn, tables):
        raise Refused(
            f"{action} would copy into tables that already have rows: {', '.join(occupied)}. "
            "Empty them first (the copy would fail on duplicate keys and leave a slot holding WAL on the primary)"
        )


async def subscribe(
    conn: asyncpg.Connection,
    *,
    role: str,
    instance: str | None,
    config: ReplicationConfig,
    registry: ReplicationRegistry = REGISTRY,
    copy_data: bool = True,
    split: bool = False,
) -> dict:
    """Create this mirror's subscription to the primary, copying every published table.

    Idempotent: when the subscription exists, logs where it differs from what this
    would create and changes nothing. Doesn't wait for the initial copy; ``status``
    shows its progress.

    With ``copy_data=False`` (used by ``reseed``) nothing is copied, the tables may
    already hold rows, and the subscription starts disabled: its slot keeps the
    primary's changes from now on while ``reseed`` fills in what came before.

    Run it as the role that owns the tables, granted ``pg_create_subscription``:
    Postgres 16 applies changes as each table's owner, which then needs no superuser.
    As a superuser it also stops this session's failed statements from being written
    to the server log, which would otherwise record the connection string.

    Parameters
    ----------
    conn : asyncpg.Connection
        Connection to the mirror's database.
    role : str
        This instance's ``CANOPY_ROLE``; must be ``"mirror"``.
    instance : str | None
        ``CANOPY_INSTANCE``; names the subscription.
    config : ReplicationConfig
        Supplies ``publisher_dsn``.
    registry : ReplicationRegistry
        Publications to subscribe to, and the tables that must be empty first.
    copy_data : bool
        ``False`` for a disabled subscription without the initial copy.
    split : bool
        Subscribe to the split pair, for a primary that has already switched layouts.

    Returns
    -------
    dict
        ``name``, ``created`` and, when just created, the table states.

    Raises
    ------
    Refused
        Wrong role, missing settings, Postgres older than 16, insufficient privileges,
        or a replicated table that already has rows.
    ReplicationError
        ``CREATE SUBSCRIPTION`` failed (message redacted).
    """
    require_role("subscribe", "mirror", role)
    name = subscription_name(instance)
    if config.publisher_dsn is None or not config.publisher_dsn.get_secret_value():
        raise Refused("set CANOPY_REPLICATION__PUBLISHER_DSN to the primary's connection string (from a secret)")
    dsn = config.publisher_dsn.get_secret_value()
    if int(await conn.fetchval("SHOW server_version_num")) < 160000:
        raise Refused("subscribe needs Postgres 16 or later (streaming = parallel)")

    if (sub := await _subscription(conn, name)) is not None:
        logger.info("Subscription %s already exists; nothing to do", name)
        _warn_on_drift(sub, registry)
        return {"name": name, "created": False}

    present = await tables_present(conn, registry.schemas)
    if missing := sorted(set(registry.replicated) - present):
        logger.info("Not in this database yet, not copied: %s", ", ".join(missing))
    if copy_data:
        await _require_empty(conn, sorted(set(registry.replicated) & present), "subscribe")

    superuser = await is_superuser(conn)
    if not superuser and not await conn.fetchval(
        "SELECT pg_has_role(current_user, 'pg_create_subscription', 'MEMBER') "
        "AND has_database_privilege(current_database(), 'CREATE')"
    ):
        raise Refused(
            "creating a subscription needs a superuser, or membership in pg_create_subscription "
            "plus CREATE on the database"
        )
    if await conn.fetchval("SHOW standard_conforming_strings") != "on":
        raise Refused("standard_conforming_strings must be on to quote the connection string safely")
    if superuser:
        await conn.execute("SET log_min_error_statement = panic")
    else:
        logger.info(
            "Not a superuser, so a failed CREATE SUBSCRIPTION may be written to the server log "
            "with its connection string; set log_min_error_statement = panic on this role to prevent it"
        )

    publications = ", ".join(quote_ident(p.name) for p in registry.layout(split))
    options = "copy_data = true" if copy_data else "copy_data = false, enabled = false"
    try:
        await conn.execute(
            f"CREATE SUBSCRIPTION {quote_ident(name)} CONNECTION {quote_literal(dsn)} "
            f"PUBLICATION {publications} WITH ({options}, streaming = parallel)"
        )
    except asyncpg.PostgresError as exc:
        raise ReplicationError(f"CREATE SUBSCRIPTION failed: {redact(str(exc), [dsn])}") from None
    sub = await _subscription(conn, name)
    tables = await _subscribed_tables(conn, sub["oid"])
    if copy_data:
        logger.info("Created subscription %s; initial copy of %d tables started", name, len(tables))
    else:
        logger.info("Created subscription %s, disabled and without an initial copy", name)
    return {"name": name, "created": True, "tables": tables}


async def refresh(
    conn: asyncpg.Connection, *, role: str, instance: str | None, registry: ReplicationRegistry = REGISTRY
) -> dict:
    """Pick up tables the primary has started publishing, with their initial copy.

    A no-op with a notice when the subscription doesn't exist yet: the first deploy's
    migration Job runs before the one-time ``subscribe``.

    Raises
    ------
    Refused
        Wrong role, or a table about to get its initial copy already has rows.
    """
    require_role("refresh", "mirror", role)
    name = subscription_name(instance)
    if (sub := await _subscription(conn, name)) is None:
        logger.info("No subscription %s yet; nothing to refresh (run `subscribe` first)", name)
        return {"name": name, "refreshed": False}
    _warn_on_drift(sub, registry)

    before = await _subscribed_tables(conn, sub["oid"])
    present = await tables_present(conn, registry.schemas)
    await _require_empty(conn, sorted((set(registry.replicated) & present) - set(before)), "refresh")
    await conn.execute(f"ALTER SUBSCRIPTION {quote_ident(name)} REFRESH PUBLICATION WITH (copy_data = true)")
    after = await _subscribed_tables(conn, sub["oid"])
    added, removed = sorted(set(after) - set(before)), sorted(set(before) - set(after))
    if added:
        logger.info("Subscription %s: copying newly published %s", name, ", ".join(added))
    if removed:
        logger.info("Subscription %s: no longer receives %s", name, ", ".join(removed))
    if not (added or removed):
        logger.info("Subscription %s: no change", name)
    return {"name": name, "refreshed": True, "added": added, "removed": removed}


async def grants(
    conn: asyncpg.Connection, *, role: str, config: ReplicationConfig, registry: ReplicationRegistry = REGISTRY
) -> dict:
    """Give the mirror's app role read access to replicated tables and read-write on local ones.

    In one transaction: USAGE on the registry's schemas; on replicated tables every
    privilege revoked, then SELECT; SELECT, INSERT, UPDATE and DELETE on local tables
    (DB-key auth writes ``core.api_key.last_used_at``, and key management writes keys);
    and default privileges, for each owner of the registered tables, so a table a
    later migration creates is readable at once. Run it after ``alembic upgrade head``,
    as the tables' owner, so a new local table also gets its write grant.

    Raises
    ------
    Refused
        Wrong role, the app role doesn't exist, or it is a superuser or owns a registered
        table (grants mean nothing to it, and a revoke would strip an owner's own rights).
    """
    require_role("grants", "mirror", role)
    app = config.app_role
    if not await role_exists(conn, app):
        raise Refused(f"app role {app} doesn't exist; create it first (CANOPY_REPLICATION__APP_ROLE names it)")
    if await is_superuser(conn, app):
        raise Refused(f"app role {app} is a superuser, so grants can't limit it")
    present = await tables_present(conn, registry.schemas)
    registered = sorted(set(registry.tables) & present)
    owners = await table_owners(conn, registered)
    if owned := sorted(table for table, owner in owners.items() if owner == app):
        raise Refused(f"app role {app} owns {', '.join(owned)}; it must not own the tables it is limited on")
    if unclassified := sorted(present - set(registry.tables) - EXCLUDED):
        logger.warning("Unclassified tables, left to default privileges: %s", ", ".join(unclassified))

    replicated = sorted(set(registry.replicated) & present)
    local = sorted(set(registry.local) & present)
    schemas = await schemas_present(conn, registry.schemas)
    ident = quote_ident(app)
    async with conn.transaction():
        if schemas:
            await conn.execute(f"GRANT USAGE ON SCHEMA {', '.join(quote_ident(s) for s in schemas)} TO {ident}")
        if replicated:
            tables = ", ".join(qualified(t) for t in replicated)
            await conn.execute(f"REVOKE ALL ON TABLE {tables} FROM {ident}")
            await conn.execute(f"GRANT SELECT ON TABLE {tables} TO {ident}")
        if local:
            await conn.execute(
                f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {', '.join(qualified(t) for t in local)} TO {ident}"
            )
        for owner in sorted(set(owners.values())):
            await conn.execute(
                f"ALTER DEFAULT PRIVILEGES FOR ROLE {quote_ident(owner)} "
                f"IN SCHEMA {', '.join(quote_ident(s) for s in schemas)} GRANT SELECT ON TABLES TO {ident}"
            )
    logger.info(
        "Granted %s: SELECT on %d replicated tables, read-write on %s", app, len(replicated), ", ".join(local) or "none"
    )
    return {
        "role": app,
        "replicated": replicated,
        "local": local,
        "default_privileges_for": sorted(set(owners.values())),
    }


async def applied_lsn(conn: asyncpg.Connection, subscription: str) -> str | None:
    """The primary's WAL position of the last transaction ``subscription`` applied here.

    ``pg_replication_origin_status.remote_lsn``, as in ``subscriber_status``: the end of
    that transaction's commit record. ``None`` when the subscription doesn't exist or
    hasn't applied anything yet.

    Raises
    ------
    asyncpg.InsufficientPrivilegeError
        When the role can't read ``pg_replication_origin_status`` (superuser only,
        unless granted).
    """
    return await conn.fetchval(
        """
        SELECT o.remote_lsn::text
        FROM pg_replication_origin_status o
        JOIN pg_subscription s ON o.external_id = 'pg_' || s.oid::text
        WHERE s.subname = $1 AND s.subdbid = (SELECT oid FROM pg_database WHERE datname = current_database())
        """,
        subscription,
    )


async def subscriber_status(
    conn: asyncpg.Connection, *, instance: str | None, registry: ReplicationRegistry = REGISTRY
) -> dict:
    """The mirror's replication state, as JSON-ready data.

    ``applied_lsn`` is ``pg_replication_origin_status.remote_lsn``: the primary's WAL
    position of the last transaction applied here. It stays put while the primary is
    idle, so it is a position, not a lag. The view is readable only by superusers
    unless granted, so ``applied_lsn`` is ``None`` with ``applied_lsn_error`` set when
    it can't be read. The connection string (``subconninfo``) is never read.
    """
    name = subscription_name(instance)
    status: dict = {"side": "subscriber", "subscription": {"name": name, "exists": False}}
    if (sub := await _subscription(conn, name)) is None:
        return status
    worker = await conn.fetchrow(
        """
        SELECT pid, received_lsn::text AS received_lsn, last_msg_send_time, last_msg_receipt_time,
               latest_end_lsn::text AS latest_end_lsn, latest_end_time
        FROM pg_stat_subscription WHERE subid = $1 AND relid IS NULL AND leader_pid IS NULL
        """,
        sub["oid"],
    )
    errors = await conn.fetchrow(
        "SELECT apply_error_count, sync_error_count FROM pg_stat_subscription_stats WHERE subid = $1", sub["oid"]
    )
    applied_lsn, applied_lsn_error = None, None
    try:
        applied_lsn = await conn.fetchval(
            "SELECT remote_lsn::text FROM pg_replication_origin_status WHERE external_id = 'pg_' || $1::oid::text",
            sub["oid"],
        )
    except asyncpg.InsufficientPrivilegeError:
        applied_lsn_error = "pg_replication_origin_status needs a superuser or an explicit grant"
    tables = await _subscribed_tables(conn, sub["oid"])
    status["subscription"] = {
        "name": name,
        "exists": True,
        "enabled": sub["subenabled"],
        "owner": sub["owner"],
        "publications": list(sub["subpublications"]),
        "layout": (layout := _layout(sub["subpublications"], registry)),
        "expected_publications": [p.name for p in registry.layout(layout == "split")],
        "streaming": sub["substream"],
        "slot_name": sub["subslotname"],
        "worker": jsonable(worker) if worker else None,
        "applied_lsn": applied_lsn,
        "applied_lsn_error": applied_lsn_error,
        "apply_error_count": errors["apply_error_count"] if errors else 0,
        "sync_error_count": errors["sync_error_count"] if errors else 0,
        "tables_ready": sum(state == "ready" for state in tables.values()),
        "tables_not_ready": {table: state for table, state in sorted(tables.items()) if state != "ready"},
    }
    return status


async def _connect_primary(config: ReplicationConfig, command: str) -> tuple[asyncpg.Connection, str]:
    """A connection to the primary over ``heartbeat_dsn``, and the DSN (for redaction)."""
    if config.heartbeat_dsn is None or not config.heartbeat_dsn.get_secret_value():
        raise Refused(f"{command} needs CANOPY_REPLICATION__HEARTBEAT_DSN, a URI to the primary as the heartbeat role")
    dsn = config.heartbeat_dsn.get_secret_value()
    try:
        return await asyncpg.connect(dsn, server_settings={"application_name": "canopy-replication"}), dsn
    except (OSError, asyncpg.PostgresError) as exc:
        raise ReplicationError(f"cannot connect to the primary: {redact(str(exc), [dsn])}") from None


async def switch(
    conn: asyncpg.Connection,
    *,
    role: str,
    instance: str | None,
    config: ReplicationConfig,
    registry: ReplicationRegistry = REGISTRY,
    timeout: float = 120.0,
) -> dict:
    """Move this mirror's subscription from ``canopy_history`` to the split pair.

    Run it once, after ``publish --split`` on the primary has created the split pair.
    It waits until the subscription's slot on the primary has confirmed a WAL position
    taken after the pair existed: a subscription whose slot is still behind a
    publication's creation fails to find it and retries forever. Then it sets the
    publications without copying, since every table already has its rows. A no-op when
    the subscription already reads the split pair.

    Parameters
    ----------
    conn : asyncpg.Connection
        Connection to the mirror's database, as the subscription's owner.
    role : str
        This instance's ``CANOPY_ROLE``; must be ``"mirror"``.
    instance : str | None
        ``CANOPY_INSTANCE``; names the subscription.
    config : ReplicationConfig
        Supplies ``heartbeat_dsn``, used here only to read the slot's position.
    registry : ReplicationRegistry
        The split publications to move to.
    timeout : float
        Seconds to wait for the slot to catch up.

    Returns
    -------
    dict
        ``name``, ``switched`` and, when just switched, the position waited for.

    Raises
    ------
    Refused
        Wrong role; no subscription; it is disabled; the primary lacks the split pair;
        or the slot didn't catch up within ``timeout``. Nothing is changed.
    ReplicationError
        The primary couldn't be reached (message redacted).
    """
    require_role("switch", "mirror", role)
    name = subscription_name(instance)
    if (sub := await _subscription(conn, name)) is None:
        raise Refused(f"no subscription {name}; on a mirror of a split primary run `subscribe --split` instead")
    target = [p.name for p in registry.split_publications]
    if _layout(sub["subpublications"], registry) == "split":
        logger.info("Subscription %s already reads %s; nothing to do", name, ", ".join(target))
        return {"name": name, "switched": False}
    if not sub["subenabled"]:
        raise Refused(f"subscription {name} is disabled; enable it so its slot can catch up, then switch")

    primary, dsn = await _connect_primary(config, "switch")
    try:
        absent = [
            publication
            for publication in target
            if not await primary.fetchval(
                "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)", publication
            )
        ]
        if absent:
            raise Refused(f"the primary has no {', '.join(absent)} yet; run `publish --split` there first")
        position = await primary.fetchval("SELECT pg_current_wal_lsn()::text")
        deadline = asyncio.get_running_loop().time() + timeout
        while not await primary.fetchval(
            "SELECT confirmed_flush_lsn >= $1::text::pg_lsn FROM pg_replication_slots WHERE slot_name = $2",
            position,
            sub["subslotname"],
        ):
            if asyncio.get_running_loop().time() >= deadline:
                raise Refused(
                    f"slot {sub['subslotname']} hasn't confirmed {position} within {timeout:g} s; "
                    "check the subscription is streaming (`status`) and try again"
                )
            await asyncio.sleep(1)
    except asyncpg.PostgresError as exc:
        raise ReplicationError(f"reading the primary failed: {redact(str(exc), [dsn])}") from None
    finally:
        await primary.close()

    publications = ", ".join(quote_ident(p) for p in target)
    await conn.execute(
        f"ALTER SUBSCRIPTION {quote_ident(name)} SET PUBLICATION {publications} WITH (copy_data = false)"
    )
    logger.info(
        "Subscription %s now reads %s (slot confirmed %s). Next: `publish --split` on the primary",
        name,
        ", ".join(target),
        position,
    )
    return {"name": name, "switched": True, "publications": target, "confirmed": position}


async def heartbeat(
    conn: asyncpg.Connection,
    *,
    role: str,
    instance: str | None,
    config: ReplicationConfig,
    backup_ok: bool,
    wal_archive_ok: bool,
    drift_ok: bool,
    registry: ReplicationRegistry = REGISTRY,
) -> dict:
    """Record this mirror's latest checks in the primary's ``core.history_heartbeat``.

    The CronJob runs it after the backup, WAL-archive and drift checks, passing each
    result; a check not passed as OK is recorded as failed. The primary's prune job
    deletes nothing while the latest heartbeat is old or reports a failure.
    ``applied_lsn`` comes from ``subscriber_status`` and is ``None`` when this role
    may not read it. The row is keyed by the subscription's name and timed by the
    primary's clock.

    Raises
    ------
    Refused
        Wrong role, no subscription, or no ``heartbeat_dsn``.
    ReplicationError
        The primary couldn't be reached or refused the write (message redacted).
    """
    require_role("heartbeat", "mirror", role)
    status = (await subscriber_status(conn, instance=instance, registry=registry))["subscription"]
    if not status["exists"]:
        raise Refused(f"no subscription {status['name']}; a mirror reports checks only once it replicates")
    row = {
        "name": status["name"],
        "backup_ok": backup_ok,
        "wal_archive_ok": wal_archive_ok,
        "drift_ok": drift_ok,
        "applied_lsn": status["applied_lsn"],
    }
    primary, dsn = await _connect_primary(config, "heartbeat")
    try:
        written_at = await primary.fetchval(
            """
            INSERT INTO core.history_heartbeat AS h (name, backup_ok, wal_archive_ok, drift_ok, applied_lsn, written_at)
            VALUES ($1, $2, $3, $4, $5, now())
            ON CONFLICT (name) DO UPDATE SET
                backup_ok = excluded.backup_ok, wal_archive_ok = excluded.wal_archive_ok,
                drift_ok = excluded.drift_ok, applied_lsn = excluded.applied_lsn, written_at = excluded.written_at
            RETURNING h.written_at
            """,
            *row.values(),
        )
    except asyncpg.PostgresError as exc:
        raise ReplicationError(f"writing the heartbeat failed: {redact(str(exc), [dsn])}") from None
    finally:
        await primary.close()
    level = logging.INFO if backup_ok and wal_archive_ok and drift_ok else logging.WARNING
    logger.log(
        level,
        "Heartbeat %s: backup %s, WAL archive %s, drift %s, applied %s",
        row["name"],
        *("ok" if ok else "FAILED" for ok in (backup_ok, wal_archive_ok, drift_ok)),
        row["applied_lsn"] or "unknown",
    )
    return {**row, "written_at": written_at.isoformat()}
