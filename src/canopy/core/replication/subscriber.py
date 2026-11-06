"""The head's side: subscribe to each arm, pick up new tables, grant the app role, report.

A head (``CANOPY_ROLE=mirror``) holds one subscription per arm, named
``canopy_history_<arm>`` (``canopy.core.arms``). ``subscribe``, ``refresh`` and
``subscriber_status`` act on every configured arm, or on the one named with
``only``; one arm failing doesn't stop the rest.

Order on a head's deploy: ``alembic upgrade head``, then ``grants``, then
``refresh``. ``subscribe`` runs once per arm, after the first migration, and is a
no-op for an arm already subscribed. The head always migrates before its arms, so
it never lacks a table an arm publishes.

Postgres runs ``CREATE SUBSCRIPTION`` (when it creates the slot) and
``ALTER SUBSCRIPTION ... REFRESH`` only outside a transaction, which is how asyncpg
executes a bare statement.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping

import asyncpg

from canopy.core.arms import Arm, subscription_name
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
    require_confined,
    owner_memberships,
)
from canopy.core.replication.config import ReplicationConfig
from canopy.core.replication.registry import EXCLUDED, REGISTRY, ReplicationRegistry

logger = logging.getLogger(__name__)

_TABLE_STATES = {"i": "initialize", "d": "copying", "f": "copied", "s": "synchronized", "r": "ready"}


def _selected_arms(arms: Mapping[str, Arm], only: str | None, head: str | None = None) -> list[str]:
    """The arms a command acts on: every configured arm, or ``only``.

    Raises
    ------
    Refused
        When no arm is configured, ``only`` isn't one of them, or an arm has the
        head's own name (an instance can't subscribe to itself).
    """
    if not arms:
        raise Refused("no arms are configured; set CANOPY_ARMS__<arm>__PUBLISHER_DSN for each arm")
    if only is not None and only not in arms:
        raise Refused(f"no arm named {only!r} is configured (configured: {', '.join(sorted(arms))})")
    if head is not None and head in arms:
        raise Refused(f"arm {head!r} has this head's own name (CANOPY_INSTANCE); an instance can't subscribe to itself")
    return [only] if only is not None else sorted(arms)


async def _history_subscriptions(conn: asyncpg.Connection) -> list[str]:
    """Names of every ``canopy_history_*`` subscription in this database."""
    rows = await conn.fetch(
        """
        SELECT subname FROM pg_subscription
        WHERE subname LIKE 'canopy\\_history\\_%'
          AND subdbid = (SELECT oid FROM pg_database WHERE datname = current_database())
        ORDER BY subname
        """
    )
    return [row["subname"] for row in rows]


def _unexpected(subscriptions: list[str], arms: Mapping[str, Arm]) -> list[str]:
    """History subscriptions whose arm isn't configured (an arm that was removed)."""
    expected = {subscription_name(arm) for arm in arms}
    return [name for name in subscriptions if name not in expected]


def _in_order(results: list[dict], arms: list[str]) -> list[dict]:
    """Per-arm results in arm order, whatever order they were produced in."""
    return sorted(results, key=lambda result: arms.index(result["arm"]))


def _warn_on_unexpected(subscriptions: list[str], arms: Mapping[str, Arm]) -> None:
    for name in _unexpected(subscriptions, arms):
        logger.warning(
            "Subscription %s belongs to no configured arm; drop it if that arm was removed (see the history guide)",
            name,
        )


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


def _warn_on_drift(sub: asyncpg.Record, registry: ReplicationRegistry) -> None:
    """Log where an existing subscription differs from what ``subscribe`` would create."""
    expected = sorted(p.name for p in registry.publications)
    if sorted(sub["subpublications"]) != expected:
        logger.warning(
            "Subscription %s reads publications %s; the registry expects %s",
            sub["subname"],
            ", ".join(sub["subpublications"]),
            ", ".join(expected),
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
    head: str,
    arms: Mapping[str, Arm],
    only: str | None = None,
    registry: ReplicationRegistry = REGISTRY,
) -> dict:
    """Create the head's subscription to each arm, copying every table the arm publishes.

    Idempotent: an arm already subscribed gets a log of where its subscription
    differs from what this would create, and nothing changes. Every selected arm is
    checked before any subscription is created; a ``CREATE SUBSCRIPTION`` that then
    fails for one arm is reported and the others still go ahead. Doesn't wait for
    the initial copies; ``status`` shows their progress.

    Until rows carry their owner, a head subscribes to one arm: a second arm's copy
    would land in tables that already hold the first arm's rows, and the checks
    that tell those apart arrive with the owner column.

    Run it as the role that owns the tables, granted ``pg_create_subscription``:
    Postgres 16 applies changes as each table's owner, which then needs no superuser.
    As a superuser it also stops this session's failed statements from being written
    to the server log, which would otherwise record a connection string.

    Parameters
    ----------
    conn : asyncpg.Connection
        Connection to the head's database.
    role : str
        This instance's ``CANOPY_ROLE``; must be ``"mirror"``.
    head : str
        The head's own ``CANOPY_INSTANCE``; no arm may share it.
    arms : Mapping[str, Arm]
        The configured arms, keyed by name; each needs a ``publisher_dsn``.
    only : str | None
        Subscribe to this arm only.
    registry : ReplicationRegistry
        Publications to subscribe to, and the tables that must be empty first.

    Returns
    -------
    dict
        ``subscriptions``: per arm, ``arm``, ``name``, ``created`` and, when just
        created, the table states.

    Raises
    ------
    Refused
        Wrong role, no arm (or an unknown one), an arm without a ``publisher_dsn`` or
        with the head's name, Postgres older than 16, insufficient privileges, a
        second arm, or a replicated table that already has rows. Nothing was created.
    ReplicationError
        ``CREATE SUBSCRIPTION`` failed for at least one arm (messages redacted); the
        others were created.
    """
    require_role("subscribe", "mirror", role)
    selected = _selected_arms(arms, only, head)
    if lacking := [
        arm for arm in selected if not (arms[arm].publisher_dsn and arms[arm].publisher_dsn.get_secret_value())
    ]:
        raise Refused("set CANOPY_ARMS__<arm>__PUBLISHER_DSN (from a secret) for " + ", ".join(lacking))
    if int(await conn.fetchval("SHOW server_version_num")) < 160000:
        raise Refused("subscribe needs Postgres 16 or later (streaming = parallel)")

    existing = await _history_subscriptions(conn)
    _warn_on_unexpected(existing, arms)
    results = []
    to_create = []
    for arm in selected:
        name = subscription_name(arm)
        if name in existing:
            logger.info("Subscription %s already exists; nothing to do", name)
            _warn_on_drift(await _subscription(conn, name), registry)
            results.append({"arm": arm, "name": name, "created": False})
        else:
            to_create.append(arm)
    if not to_create:
        return {"subscriptions": _in_order(results, selected)}

    if len(set(existing) | {subscription_name(arm) for arm in to_create}) > 1:
        raise Refused(
            "a head subscribes to one arm until rows carry their owner: a second arm's copy would land in "
            "tables that already hold the first arm's rows"
        )
    present = await tables_present(conn, registry.schemas)
    if missing := sorted(set(registry.replicated) - present):
        logger.info("Not in this database yet, not copied: %s", ", ".join(missing))
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

    publications = ", ".join(quote_ident(p.name) for p in registry.publications)
    failures = []
    for arm in to_create:
        name = subscription_name(arm)
        dsn = arms[arm].publisher_dsn.get_secret_value()
        try:
            await conn.execute(
                f"CREATE SUBSCRIPTION {quote_ident(name)} CONNECTION {quote_literal(dsn)} "
                f"PUBLICATION {publications} WITH (copy_data = true, streaming = parallel)"
            )
        except asyncpg.PostgresError as exc:
            message = redact(str(exc), [dsn])
            logger.error("Arm %s: CREATE SUBSCRIPTION failed: %s", arm, message)
            failures.append(f"{arm}: {message}")
            continue
        sub = await _subscription(conn, name)
        tables = await _subscribed_tables(conn, sub["oid"])
        logger.info("Created subscription %s; initial copy of %d tables started", name, len(tables))
        results.append({"arm": arm, "name": name, "created": True, "tables": tables})
    if failures:
        raise ReplicationError(f"CREATE SUBSCRIPTION failed for {'; '.join(failures)}")
    return {"subscriptions": _in_order(results, selected)}


async def refresh(
    conn: asyncpg.Connection,
    *,
    role: str,
    arms: Mapping[str, Arm],
    only: str | None = None,
    registry: ReplicationRegistry = REGISTRY,
) -> dict:
    """Pick up tables each arm has started publishing, with their initial copy.

    Each arm is refreshed on its own: one that can't be reached is reported and the
    others still go ahead. An arm without a subscription yet gets a notice, not an
    error: the first deploy's migration Job runs before the one-time ``subscribe``.

    Returns
    -------
    dict
        ``subscriptions``: per arm, ``arm``, ``name``, ``refreshed`` and, when
        refreshed, the tables ``added`` and ``removed``.

    Raises
    ------
    Refused
        Wrong role, no arm (or an unknown one), or a table about to get its initial
        copy already has rows. Nothing was refreshed.
    ReplicationError
        The refresh failed for at least one arm; the others were refreshed.
    """
    require_role("refresh", "mirror", role)
    selected = _selected_arms(arms, only)
    existing = await _history_subscriptions(conn)
    _warn_on_unexpected(existing, arms)
    present = await tables_present(conn, registry.schemas)

    pending = []
    results = []
    for arm in selected:
        name = subscription_name(arm)
        if name not in existing:
            logger.info("No subscription %s yet; nothing to refresh (run `subscribe` first)", name)
            results.append({"arm": arm, "name": name, "refreshed": False})
            continue
        sub = await _subscription(conn, name)
        _warn_on_drift(sub, registry)
        before = await _subscribed_tables(conn, sub["oid"])
        await _require_empty(conn, sorted((set(registry.replicated) & present) - set(before)), "refresh")
        pending.append((arm, name, sub, before))

    failures = []
    for arm, name, sub, before in pending:
        try:
            await conn.execute(f"ALTER SUBSCRIPTION {quote_ident(name)} REFRESH PUBLICATION WITH (copy_data = true)")
        except asyncpg.PostgresError as exc:
            logger.error("Arm %s: refresh failed: %s", arm, redact(str(exc)))
            failures.append(f"{arm}: {redact(str(exc))}")
            continue
        after = await _subscribed_tables(conn, sub["oid"])
        added, removed = sorted(set(after) - set(before)), sorted(set(before) - set(after))
        if added:
            logger.info("Subscription %s: copying newly published %s", name, ", ".join(added))
        if removed:
            logger.info("Subscription %s: no longer receives %s", name, ", ".join(removed))
        if not (added or removed):
            logger.info("Subscription %s: no change", name)
        results.append({"arm": arm, "name": name, "refreshed": True, "added": added, "removed": removed})
    if failures:
        raise ReplicationError(f"refresh failed for {'; '.join(failures)}")
    return {"subscriptions": _in_order(results, selected)}


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

    Afterwards the app role must be able to write no replicated table, counting what
    it inherits from other roles or PUBLIC: a write there would diverge from the
    primary or break replication. ``grants`` refuses rather than change those shared
    grants, and the refusal rolls back its own.

    Raises
    ------
    Refused
        Wrong role; the app role doesn't exist; it is a superuser, owns a registered
        table or belongs to a role that does (grants can't limit it, and a revoke would
        strip an owner's own rights); or, after the grants, it can still do more than
        read replicated tables and read and write local ones.
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
    if memberships := await owner_memberships(conn, app, registered):
        raise Refused(
            f"app role {app} belongs to {', '.join(memberships)}, which owns registered tables, so it can act as "
            "their owner; remove that membership"
        )
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
        # Unclassified tables keep the SELECT that default privileges give them.
        read_write = {"SELECT", "INSERT", "UPDATE", "DELETE"}
        await require_confined(
            conn,
            app,
            tables=present,
            allowed={table: (read_write if table in local else {"SELECT"}) for table in present},
            command="grants",
            policy="SELECT on replicated tables and read-write on local ones",
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


async def _arm_status(conn: asyncpg.Connection, arm: str, registry: ReplicationRegistry) -> dict:
    """One arm's subscription state; see ``subscriber_status``."""
    name = subscription_name(arm)
    if (sub := await _subscription(conn, name)) is None:
        return {"arm": arm, "name": name, "exists": False}
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
    return {
        "arm": arm,
        "name": name,
        "exists": True,
        "enabled": sub["subenabled"],
        "owner": sub["owner"],
        "publications": list(sub["subpublications"]),
        "expected_publications": [p.name for p in registry.publications],
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


async def subscriber_status(
    conn: asyncpg.Connection,
    *,
    arms: Mapping[str, Arm],
    only: str | None = None,
    registry: ReplicationRegistry = REGISTRY,
) -> dict:
    """The head's replication state per arm, as JSON-ready data.

    ``applied_lsn`` is ``pg_replication_origin_status.remote_lsn``: the arm's WAL
    position of the last transaction applied here. It stays put while the arm is
    idle, so it is a position, not a lag. The view is readable only by superusers
    unless granted, so ``applied_lsn`` is ``None`` with ``applied_lsn_error`` set when
    it can't be read. Connection strings (``subconninfo``) are never read.

    Returns
    -------
    dict
        ``subscriptions``: one entry per configured arm (or ``only``), with its
        ``arm`` and ``name`` and, when it exists, its state. ``unexpected``: history
        subscriptions in this database whose arm isn't configured.

    With no arm configured it still reports, so a head whose arms were all taken
    out of its settings shows their subscriptions under ``unexpected``.

    Raises
    ------
    Refused
        ``only`` isn't a configured arm.
    """
    selected = _selected_arms(arms, only) if arms or only is not None else []
    return {
        "side": "subscriber",
        "subscriptions": [await _arm_status(conn, arm, registry) for arm in selected],
        "unexpected": _unexpected(await _history_subscriptions(conn), arms),
    }
