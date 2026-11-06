"""The head's side: subscribe to each arm, pick up new tables, grant the app role, report.

A head (``CANOPY_ROLE=mirror``) holds one subscription per arm, named
``canopy_history_<arm>`` (``canopy.core.arms``). ``subscribe``, ``refresh`` and
``subscriber_status`` act on every configured arm, or on the one named with
``only``; one arm failing doesn't stop the rest.

Order on a head's deploy: ``alembic upgrade head``, then ``grants``, then
``refresh``. ``subscribe`` runs once per arm, after the first migration, and is a
no-op for an arm already subscribed. The head always migrates before its arms, so
it never lacks a table an arm publishes.

Two commands reach one arm's Postgres over its ``CANOPY_ARMS__<arm>__HEARTBEAT_DSN``
rather than through the subscription: ``heartbeat`` records the head's backup,
WAL-archive and drift checks for that arm there, and ``switch`` (once, when the arm
moves to the split layout) reads the arm's slot position before moving the
subscription. Arms may differ: one on the split pair and pruning, another on
``canopy_history``.

Postgres runs ``CREATE SUBSCRIPTION`` (when it creates the slot) and
``ALTER SUBSCRIPTION ... REFRESH`` only outside a transaction, which is how asyncpg
executes a bare statement.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping, Callable, Awaitable

import asyncpg

from canopy.core.arms import Arm, subscription_name
from canopy.core.replication.sql import (
    Refused,
    ReplicationError,
    redact,
    jsonable,
    qualified,
    owner_join,
    quote_ident,
    role_exists,
    is_superuser,
    require_role,
    table_owners,
    quote_literal,
    tables_present,
    conninfo_as_uri,
    schemas_present,
    require_confined,
    owner_memberships,
    tables_with_rows_of,
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


ArmConnector = Callable[[str], Awaitable[asyncpg.Connection]]


def _arm_connector(arms: Mapping[str, Arm]) -> ArmConnector:
    """Open a connection to an arm with its own connection string, as the subscription will."""

    async def connect(arm: str) -> asyncpg.Connection:
        dsn = arms[arm].publisher_dsn
        if dsn is None or not dsn.get_secret_value():
            raise Refused(f"set CANOPY_ARMS__<arm>__PUBLISHER_DSN (from a secret) for {arm}")
        return await asyncpg.connect(
            conninfo_as_uri(dsn.get_secret_value()), server_settings={"application_name": "canopy-replication"}
        )

    return connect


async def _require_unowned(
    conn: asyncpg.Connection, registry: ReplicationRegistry, tables: list[str], arm: str, action: str
) -> None:
    """Refuse ``action`` when the head already holds rows ``arm`` owns in any of ``tables``.

    The initial copy brings every row the arm has, so a row of the arm's already here
    is a duplicate key: the copy fails, the table sticks mid-copy, and the slot on the
    arm holds WAL until someone drops it. Rows of the head's own, or of other arms,
    are expected and don't block it.
    """
    if occupied := await tables_with_rows_of(conn, registry, tables, arm):
        raise Refused(
            f"{action} for arm {arm} would copy into tables that already hold that arm's rows: {', '.join(occupied)}. "
            "Remove them first, or re-seed the arm (the copy would fail on duplicate keys and leave a slot "
            "holding WAL on the arm)"
        )


async def _require_no_shared_key_clash(
    conn: asyncpg.Connection,
    connect: ArmConnector,
    registry: ReplicationRegistry,
    tables: list[str],
    arm: str,
    action: str,
) -> None:
    """Refuse ``action`` when ``arm`` holds a value of a key unique across owners that the head already has.

    Facility and Magpie slugs are unique everywhere, so an arm's row whose slug the
    head already holds (for its own row or another arm's) would stop the copy, and
    then replication from that arm. Only keys of ``tables`` are checked: those are
    the tables this run copies. A clash that streams in later shows as an apply error.

    Raises
    ------
    Refused
        On any clash, naming each value and the owner of the row already here.
    OSError, asyncpg.PostgresError
        The arm can't be read.
    """
    keyed = [(table, column) for table in tables for column in registry.shared_keys.get(table, ())]
    if not keyed:
        return
    clashes = []
    arm_conn = await connect(arm)
    try:
        for table, column in keyed:
            # Every row the arm publishes is copied, whoever it names as owner.
            theirs = [row[0] for row in await arm_conn.fetch(f"SELECT {quote_ident(column)} FROM {qualified(table)}")]
            if not theirs:
                continue
            join_sql, owner_sql = owner_join(registry, table, alias="t")
            rows = await conn.fetch(
                f"SELECT t.{quote_ident(column)} AS value, {owner_sql} AS owner FROM {qualified(table)} t {join_sql} "
                f"WHERE {owner_sql} <> $1 AND t.{quote_ident(column)} = ANY($2::text[]) ORDER BY 1",
                arm,
                theirs,
            )
            clashes += [f"{table}.{column} {row['value']!r} (here, owned by {row['owner']})" for row in rows]
    finally:
        await arm_conn.close()
    if clashes:
        raise Refused(
            f"{action} for arm {arm} would copy keys that must be unique and are already taken: {'; '.join(clashes)}. "
            "Rename one of each pair first; the copy would stop on the clash"
        )


async def subscribe(
    conn: asyncpg.Connection,
    *,
    role: str,
    head: str,
    arms: Mapping[str, Arm],
    only: str | None = None,
    registry: ReplicationRegistry = REGISTRY,
    connect_arm: ArmConnector | None = None,
    copy_data: bool = True,
    split: bool = False,
) -> dict:
    """Create the head's subscription to each arm, copying every table the arm publishes.

    Idempotent: an arm already subscribed gets a log of where its subscription
    differs from what this would create, and nothing changes. Every selected arm is
    checked before any subscription is created; a ``CREATE SUBSCRIPTION`` that then
    fails for one arm is reported and the others still go ahead. Doesn't wait for
    the initial copies; ``status`` shows their progress.

    Before copying, each arm is checked two ways. The head must hold none of the
    arm's own rows yet (its own rows and other arms' are expected). And none of the
    arm's values of a key unique across owners (facility and Magpie slugs) may
    already be taken here. An arm that can't be reached for the second check is
    reported and skipped.

    With ``copy_data=False`` (used by ``reseed``) nothing is copied, so neither check
    applies: the head may already hold the arm's rows, and the subscription starts
    disabled. Its slot keeps the arm's changes from now on while ``reseed`` fills in
    what came before.

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
        Publications to subscribe to, and the tables and owner paths the checks use.
    connect_arm : ArmConnector | None
        Opens a connection to an arm for the key check; by default with the arm's own
        ``publisher_dsn``, the way the subscription connects.
    copy_data : bool
        ``False`` for disabled subscriptions without the initial copy.
    split : bool
        Subscribe to the split pair, for an arm that has already switched layouts.
        Layouts are per arm, so this needs ``only``.

    Returns
    -------
    dict
        ``subscriptions``: per arm, ``arm``, ``name``, ``created`` and, when just
        created, the table states.

    Raises
    ------
    Refused
        Wrong role, no arm (or an unknown one), ``split`` without ``only``, an arm
        without a ``publisher_dsn`` or with the head's name, Postgres older than 16,
        insufficient privileges, rows of the arm already here, or a key clash.
        Nothing was created.
    ReplicationError
        An arm couldn't be reached for its check, or ``CREATE SUBSCRIPTION`` failed
        for it (messages redacted); the other arms were created.
    """
    require_role("subscribe", "mirror", role)
    if split and only is None:
        raise Refused("the split layout is one arm's; name it with --arm")
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

    present = await tables_present(conn, registry.schemas)
    if missing := sorted(set(registry.replicated) - present):
        logger.info("Not in this database yet, not copied: %s", ", ".join(missing))
    copied = sorted(set(registry.replicated) & present)
    connect = connect_arm or _arm_connector(arms)
    secrets = [arms[arm].publisher_dsn.get_secret_value() for arm in to_create]
    failures = []
    checked = []
    for arm in to_create:
        if not copy_data:  # nothing is copied, so nothing can clash; reseed checks what it writes
            checked.append(arm)
            continue
        await _require_unowned(conn, registry, copied, arm, "subscribe")
        try:
            await _require_no_shared_key_clash(conn, connect, registry, copied, arm, "subscribe")
        except (OSError, asyncpg.PostgresError) as exc:
            message = redact(str(exc), secrets)
            logger.error("Arm %s: can't read it to check its unique keys: %s", arm, message)
            failures.append(f"{arm}: can't read it to check its unique keys: {message}")
            continue
        checked.append(arm)

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
    for arm in checked:
        name = subscription_name(arm)
        dsn = arms[arm].publisher_dsn.get_secret_value()
        try:
            await conn.execute(
                f"CREATE SUBSCRIPTION {quote_ident(name)} CONNECTION {quote_literal(dsn)} "
                f"PUBLICATION {publications} WITH ({options}, streaming = parallel)"
            )
        except asyncpg.PostgresError as exc:
            message = redact(str(exc), [dsn])
            logger.error("Arm %s: CREATE SUBSCRIPTION failed: %s", arm, message)
            failures.append(f"{arm}: {message}")
            continue
        sub = await _subscription(conn, name)
        tables = await _subscribed_tables(conn, sub["oid"])
        if copy_data:
            logger.info("Created subscription %s; initial copy of %d tables started", name, len(tables))
        else:
            logger.info("Created subscription %s, disabled and without an initial copy", name)
        results.append({"arm": arm, "name": name, "created": True, "tables": tables})
    if failures:
        raise ReplicationError(f"subscribe failed for {'; '.join(failures)}")
    return {"subscriptions": _in_order(results, selected)}


async def refresh(
    conn: asyncpg.Connection,
    *,
    role: str,
    arms: Mapping[str, Arm],
    only: str | None = None,
    registry: ReplicationRegistry = REGISTRY,
    connect_arm: ArmConnector | None = None,
) -> dict:
    """Pick up tables each arm has started publishing, with their initial copy.

    Each arm is refreshed on its own: one that can't be reached is reported and the
    others still go ahead. An arm without a subscription yet gets a notice, not an
    error: the first deploy's migration Job runs before the one-time ``subscribe``.
    Tables about to get their initial copy get ``subscribe``'s two checks first:
    none of the arm's rows here yet, and no clash on a key unique across owners.

    Returns
    -------
    dict
        ``subscriptions``: per arm, ``arm``, ``name``, ``refreshed`` and, when
        refreshed, the tables ``added`` and ``removed``.

    Raises
    ------
    Refused
        Wrong role, no arm (or an unknown one), or a table about to get its initial
        copy already holds the arm's rows or clashes on a unique key. Nothing was
        refreshed.
    ReplicationError
        The refresh failed for at least one arm, or one couldn't be reached for its
        check; the others were refreshed.
    """
    require_role("refresh", "mirror", role)
    selected = _selected_arms(arms, only)
    existing = await _history_subscriptions(conn)
    _warn_on_unexpected(existing, arms)
    present = await tables_present(conn, registry.schemas)

    connect = connect_arm or _arm_connector(arms)
    secrets = [a.publisher_dsn.get_secret_value() for a in arms.values() if a.publisher_dsn]
    failures = []
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
        new = sorted((set(registry.replicated) & present) - set(before))
        await _require_unowned(conn, registry, new, arm, "refresh")
        try:
            await _require_no_shared_key_clash(conn, connect, registry, new, arm, "refresh")
        except (OSError, asyncpg.PostgresError) as exc:
            message = redact(str(exc), secrets)
            logger.error("Arm %s: can't read it to check its unique keys: %s", arm, message)
            failures.append(f"{arm}: can't read it to check its unique keys: {message}")
            continue
        pending.append((arm, name, sub, before))

    for arm, name, sub, before in pending:
        try:
            await conn.execute(f"ALTER SUBSCRIPTION {quote_ident(name)} REFRESH PUBLICATION WITH (copy_data = true)")
        except asyncpg.PostgresError as exc:
            logger.error("Arm %s: refresh failed: %s", arm, redact(str(exc), secrets))
            failures.append(f"{arm}: {redact(str(exc), secrets)}")
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


_POLICIES = ("canopy_read", "canopy_insert", "canopy_update", "canopy_delete")


async def _limit_writes_to_the_head(conn: asyncpg.Connection, table: str, column: str, app: str, head: str) -> None:
    """Row-level security on one root table: the app role reads every row, writes only the head's.

    Enabled, not forced: the table's owner, which applies every arm's changes,
    isn't bound by it. Policies are dropped and created again, so a renamed app role
    or head is picked up. An UPDATE or DELETE of an arm's row matches no row, and an
    INSERT of one is an error.
    """
    name, ident, own = qualified(table), quote_ident(app), f"{quote_ident(column)} = {quote_literal(head)}"
    await conn.execute(f"ALTER TABLE {name} ENABLE ROW LEVEL SECURITY")
    await conn.execute(f"ALTER TABLE {name} NO FORCE ROW LEVEL SECURITY")
    for policy in _POLICIES:
        await conn.execute(f"DROP POLICY IF EXISTS {quote_ident(policy)} ON {name}")
    await conn.execute(f"CREATE POLICY canopy_read ON {name} FOR SELECT TO {ident} USING (true)")
    await conn.execute(f"CREATE POLICY canopy_insert ON {name} FOR INSERT TO {ident} WITH CHECK ({own})")
    await conn.execute(f"CREATE POLICY canopy_update ON {name} FOR UPDATE TO {ident} USING ({own}) WITH CHECK ({own})")
    await conn.execute(f"CREATE POLICY canopy_delete ON {name} FOR DELETE TO {ident} USING ({own})")


async def grants(
    conn: asyncpg.Connection,
    *,
    role: str,
    head: str,
    config: ReplicationConfig,
    registry: ReplicationRegistry = REGISTRY,
) -> dict:
    """Let the head's app role read every row and write only the head's own.

    A head holds its arms' rows and its own in the same tables, and its API writes
    its own rows (decision: a mirror writes only what it owns). In one transaction:

    - USAGE on the registry's schemas;
    - on replicated tables, every privilege revoked, then SELECT, INSERT, UPDATE and
      DELETE;
    - on each root table (one that names its row's owner), row-level security: the
      app role reads every row and may insert, update or delete only rows whose
      owner is ``head``;
    - SELECT, INSERT, UPDATE and DELETE on local tables (DB-key auth writes
      ``core.api_key.last_used_at``, and key management writes keys);
    - default privileges, for each owner of the registered tables, so a table a later
      migration creates is readable at once.

    With row-level security on, a role other than the tables' owner, a superuser or
    the app role sees no rows of the root tables; give any other reader its own
    policy. Child tables (a snapshot's values, a PV's tags and facilities, a tag
    group's tags) get plain grants: the head writes values by ``COPY``, which row-level
    security refuses. The services check a child's owner through its parent, and
    the drift check catches what they miss. Run ``grants`` after ``alembic upgrade
    head``, as the tables' owner, so a new table gets its grants too.

    Afterwards the app role must hold nothing more, counting what it inherits from
    other roles or PUBLIC. ``grants`` refuses rather than change those shared grants,
    and the refusal rolls back its own.

    Raises
    ------
    Refused
        Wrong role; the app role doesn't exist; it is a superuser, bypasses row-level
        security, owns a registered table or belongs to a role that does (none of
        which a policy can limit); or, after the grants, it holds more than they give.
    """
    require_role("grants", "mirror", role)
    app = config.app_role
    if not await role_exists(conn, app):
        raise Refused(f"app role {app} doesn't exist; create it first (CANOPY_REPLICATION__APP_ROLE names it)")
    if await is_superuser(conn, app):
        raise Refused(f"app role {app} is a superuser, so grants can't limit it")
    if await conn.fetchval("SELECT rolbypassrls FROM pg_roles WHERE rolname = $1", app):
        raise Refused(f"app role {app} has BYPASSRLS, so it could write the arms' rows; remove it")
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
    roots = [table for table in replicated if not registry.owner_chain(table)]
    local = sorted(set(registry.local) & present)
    schemas = await schemas_present(conn, registry.schemas)
    ident = quote_ident(app)
    read_write = {"SELECT", "INSERT", "UPDATE", "DELETE"}
    async with conn.transaction():
        if schemas:
            await conn.execute(f"GRANT USAGE ON SCHEMA {', '.join(quote_ident(s) for s in schemas)} TO {ident}")
        if replicated:
            tables = ", ".join(qualified(t) for t in replicated)
            await conn.execute(f"REVOKE ALL ON TABLE {tables} FROM {ident}")
            await conn.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {tables} TO {ident}")
        for table in roots:
            await _limit_writes_to_the_head(conn, table, registry.owner_paths[table].column, app, head)
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
        await require_confined(
            conn,
            app,
            tables=present,
            allowed={table: (read_write if table in registry.tables else {"SELECT"}) for table in present},
            command="grants",
            policy="read-write on registered tables (rows of other owners blocked by row-level security)",
        )
    logger.info(
        "Granted %s: read-write on %d replicated tables (writes to %s's rows only on %s) and on %s",
        app,
        len(replicated),
        head,
        ", ".join(roots) or "none",
        ", ".join(local) or "no local tables",
    )
    return {
        "role": app,
        "replicated": replicated,
        "row_security": roots,
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


async def _connect_arm(name: str, arm: Arm, command: str) -> tuple[asyncpg.Connection, str]:
    """A connection to arm ``name`` over its ``heartbeat_dsn``, and the DSN (for redaction)."""
    if arm.heartbeat_dsn is None or not arm.heartbeat_dsn.get_secret_value():
        raise Refused(
            f"{command} needs CANOPY_ARMS__{name.upper()}__HEARTBEAT_DSN, a URI to arm {name} as its heartbeat role"
        )
    dsn = arm.heartbeat_dsn.get_secret_value()
    try:
        return await asyncpg.connect(dsn, server_settings={"application_name": "canopy-replication"}), dsn
    except (OSError, asyncpg.PostgresError) as exc:
        raise ReplicationError(f"cannot connect to arm {name}: {redact(str(exc), [dsn])}") from None


async def switch(
    conn: asyncpg.Connection,
    *,
    role: str,
    arm: str,
    arms: Mapping[str, Arm],
    registry: ReplicationRegistry = REGISTRY,
    timeout: float = 120.0,
) -> dict:
    """Move the head's subscription to ``arm`` from ``canopy_history`` to the split pair.

    Run it once per arm, after ``publish --split`` on that arm has created the split
    pair. It waits until the subscription's slot on the arm has confirmed a WAL
    position taken after the pair existed: a subscription whose slot is still behind
    a publication's creation fails to find it and retries forever. Then it sets the
    publications without copying, since every table already has its rows. A no-op
    when the subscription already reads the split pair. Other arms are untouched.

    Parameters
    ----------
    conn : asyncpg.Connection
        Connection to the head's database, as the subscription's owner.
    role : str
        This instance's ``CANOPY_ROLE``; must be ``"mirror"``.
    arm : str
        The arm whose subscription moves.
    arms : Mapping[str, Arm]
        The configured arms; ``arm``'s ``heartbeat_dsn`` is used only to read the slot's position.
    registry : ReplicationRegistry
        The split publications to move to.
    timeout : float
        Seconds to wait for the slot to catch up.

    Returns
    -------
    dict
        ``arm``, ``name``, ``switched`` and, when just switched, the position waited for.

    Raises
    ------
    Refused
        Wrong role; an unknown arm; no subscription; it is disabled; the arm lacks the
        split pair; or the slot didn't catch up within ``timeout``. Nothing is changed.
    ReplicationError
        The arm couldn't be reached (message redacted).
    """
    require_role("switch", "mirror", role)
    _selected_arms(arms, arm)
    name = subscription_name(arm)
    if (sub := await _subscription(conn, name)) is None:
        raise Refused(f"no subscription {name}; for an arm already split, run `subscribe --split --arm {arm}` instead")
    target = [p.name for p in registry.split_publications]
    if _layout(sub["subpublications"], registry) == "split":
        logger.info("Subscription %s already reads %s; nothing to do", name, ", ".join(target))
        return {"arm": arm, "name": name, "switched": False}
    if not sub["subenabled"]:
        raise Refused(f"subscription {name} is disabled; enable it so its slot can catch up, then switch")

    primary, dsn = await _connect_arm(arm, arms[arm], "switch")
    try:
        absent = [
            publication
            for publication in target
            if not await primary.fetchval(
                "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)", publication
            )
        ]
        if absent:
            raise Refused(f"arm {arm} has no {', '.join(absent)} yet; run `publish --split` there first")
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
                    f"check the subscription is streaming (`status --arm {arm}`) and try again"
                )
            await asyncio.sleep(1)
    except asyncpg.PostgresError as exc:
        raise ReplicationError(f"reading arm {arm} failed: {redact(str(exc), [dsn])}") from None
    finally:
        await primary.close()

    publications = ", ".join(quote_ident(p) for p in target)
    await conn.execute(
        f"ALTER SUBSCRIPTION {quote_ident(name)} SET PUBLICATION {publications} WITH (copy_data = false)"
    )
    logger.info(
        "Subscription %s now reads %s (slot confirmed %s). Next: `publish --split` on arm %s",
        name,
        ", ".join(target),
        position,
        arm,
    )
    return {"arm": arm, "name": name, "switched": True, "publications": target, "confirmed": position}


async def heartbeat(
    conn: asyncpg.Connection,
    *,
    role: str,
    arm: str,
    arms: Mapping[str, Arm],
    backup_ok: bool,
    wal_archive_ok: bool,
    drift_ok: bool,
    registry: ReplicationRegistry = REGISTRY,
) -> dict:
    """Record the head's latest checks for ``arm`` in that arm's ``core.history_heartbeat``.

    The head's CronJob runs it per arm, after the backup, WAL-archive and drift
    checks, passing each result: the backup and WAL archive are the head's own, so
    every arm gets the same two, and the drift check is that arm's. A check not
    passed as OK is recorded as failed. The drift check is also recorded as failed
    while the arm's subscription is disabled or any of its tables is still copying:
    those tables weren't compared, and rows pruned before their copy would never
    reach history. The arm's prune job deletes nothing while the latest heartbeat is
    old, predates the current stream, or reports a failure. ``applied_lsn`` comes
    from ``subscriber_status`` and is ``None`` when this role may not read it. The
    row is keyed by the subscription's name (the arm's slot) and timed by the arm's clock.

    Raises
    ------
    Refused
        Wrong role, an unknown arm, no subscription to it, or no ``heartbeat_dsn`` for it.
    ReplicationError
        The arm couldn't be reached or refused the write (message redacted).
    """
    require_role("heartbeat", "mirror", role)
    [status] = (await subscriber_status(conn, arms=arms, only=arm, registry=registry))["subscriptions"]
    if not status["exists"]:
        raise Refused(f"no subscription {status['name']}; the head reports an arm's checks only once it replicates it")
    if drift_ok and not status["enabled"]:
        logger.warning("Subscription %s is disabled; recording the drift check as failed", status["name"])
        drift_ok = False
    elif drift_ok and status["tables_not_ready"]:
        logger.warning(
            "Tables still copying from arm %s (%s); recording the drift check as failed",
            arm,
            ", ".join(f"{table}: {state}" for table, state in status["tables_not_ready"].items()),
        )
        drift_ok = False
    row = {
        "name": status["name"],
        "backup_ok": backup_ok,
        "wal_archive_ok": wal_archive_ok,
        "drift_ok": drift_ok,
        "applied_lsn": status["applied_lsn"],
    }
    primary, dsn = await _connect_arm(arm, arms[arm], "heartbeat")
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
        raise ReplicationError(f"writing the heartbeat to arm {arm} failed: {redact(str(exc), [dsn])}") from None
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
    return {"arm": arm, **row, "written_at": written_at.isoformat()}
