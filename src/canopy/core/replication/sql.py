"""Shared pieces of the replication commands: errors, quoting, redaction, catalog reads, privilege checks.

DDL takes no bind parameters, so identifiers and the subscription's connection
string are spliced into statements here, quoted. The connection string carries a
password, and Postgres error text can quote fragments of it, so every message
that leaves the CLI goes through ``redact`` first.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING
from decimal import Decimal
from datetime import datetime
from urllib.parse import quote, unquote, urlencode
from collections.abc import Mapping, Iterable

import asyncpg
from sqlalchemy.engine import make_url

if TYPE_CHECKING:
    from canopy.core.replication.registry import ReplicationRegistry

_REDACTED = "***"


class ReplicationError(Exception):
    """A database step failed. The message is safe to print: secrets are redacted."""


class Refused(ReplicationError):
    """The command won't run here (wrong role, or a failed precondition). Nothing was changed."""


def require_role(command: str, required: str, role: str) -> None:
    """Refuse ``command`` unless this instance's ``CANOPY_ROLE`` is ``required``.

    Raises
    ------
    Refused
        When ``role`` differs from ``required``.
    """
    if role != required:
        raise Refused(f"`{command}` runs on a {required} (CANOPY_ROLE={required}); this instance is a {role}")


# --------------------------------------------------------------------------- #
# Quoting
# --------------------------------------------------------------------------- #


def quote_ident(name: str) -> str:
    """Quote one SQL identifier (a schema, table, role, publication or subscription name)."""
    if not name or "\x00" in name:
        raise ValueError(f"invalid identifier {name!r}")
    return '"' + name.replace('"', '""') + '"'


def qualified(name: str) -> str:
    """Quote ``"schema.table"`` as ``"schema"."table"``."""
    schema, _, table = name.partition(".")
    return f"{quote_ident(schema)}.{quote_ident(table)}"


def quote_literal(value: str) -> str:
    """Quote a SQL string literal. Correct only with ``standard_conforming_strings = on``."""
    if "\x00" in value:
        raise ValueError("a SQL literal cannot contain NUL")
    return "'" + value.replace("'", "''") + "'"


def asyncpg_dsn(database_url: str) -> str:
    """Turn the app's SQLAlchemy URL (``postgresql+asyncpg://...``) into a DSN asyncpg accepts."""
    return make_url(database_url).set(drivername="postgresql").render_as_string(hide_password=False)


_CONNINFO_PAIR = re.compile(r"(\w+)\s*=\s*('(?:[^'\\]|\\.)*'|\S+)")


def conninfo_as_uri(conninfo: str) -> str:
    """A libpq connection string as a URI, the one form asyncpg reads.

    An arm's connection string may be key=value (``host=... dbname=...``) or a URI;
    Postgres takes either for a subscription, and this lets the CLI open the same
    connection itself. Keys other than host, port, dbname, user and password become
    query parameters (``sslmode``, ``sslrootcert`` and the like).
    """
    if "://" in conninfo:
        return conninfo
    pairs = {}
    for key, value in _CONNINFO_PAIR.findall(conninfo):
        if len(value) >= 2 and value[0] == value[-1] == "'":
            value = re.sub(r"\\(.)", r"\1", value[1:-1])
        pairs[key] = value
    user, password = pairs.pop("user", ""), pairs.pop("password", None)
    credentials = quote(user, safe="") + (f":{quote(password, safe='')}" if password is not None else "")
    host = pairs.pop("host", "")
    port = f":{pairs.pop('port')}" if "port" in pairs else ""
    database = quote(pairs.pop("dbname", ""), safe="")
    query = f"?{urlencode(pairs)}" if pairs else ""
    return f"postgresql://{credentials}{'@' if credentials else ''}{host}{port}/{database}{query}"


def owner_join(registry: ReplicationRegistry, table: str, *, alias: str) -> tuple[str, str]:
    """SQL that finds the owner of each row of ``table``, aliased ``alias``.

    Returns ``(join_sql, owner_sql)``: the joins from ``table`` up its owner chain,
    and the column holding the owner. A root needs no join; a child joins one parent
    per hop, aliased ``<alias>_o1``, ``<alias>_o2``, … so several tables can share a
    query. Filter one owner's rows with ``FROM <table> <alias> <join_sql> WHERE
    <owner_sql> = $1``, or look a row's owner up with ``SELECT <owner_sql> …``.

    Raises
    ------
    ValueError
        When ``table`` has no owner path in ``registry``.
    """
    joins = []
    current = alias
    for hop, (parent, key) in enumerate(registry.owner_chain(table), start=1):
        parent_alias = f"{alias}_o{hop}"
        joins.append(f"JOIN {qualified(parent)} {parent_alias} ON {parent_alias}.id = {current}.{quote_ident(key)}")
        current = parent_alias
    column = registry.owner_paths[registry.owner_root(table)].column
    return " ".join(joins), f"{current}.{quote_ident(column)}"


# --------------------------------------------------------------------------- #
# Redaction
# --------------------------------------------------------------------------- #

# password=value in a key=value conninfo; the value may be single-quoted with \' escapes.
_KV_PASSWORD = re.compile(r"(password\s*=\s*)('(?:[^'\\]|\\.)*'|\S+)", re.IGNORECASE)
# Everything after password= up to the next key=, for a value mistakenly left unquoted.
_KV_PASSWORD_SPAN = re.compile(r"(password\s*=\s*)(.*?)(?=\s+\w+\s*=|\s*$)", re.IGNORECASE)
# scheme://user:password@ in a URI.
_URI_PASSWORD = re.compile(r"(\w+://[^:/@\s]*:)([^@\s]*)(@)")
# ?password=value or &password=value in a URI's query string.
_QUERY_PASSWORD = re.compile(r"([?&]password=)([^&\s]*)", re.IGNORECASE)


def _passwords(dsn: str) -> set[str]:
    """Every password-looking value in ``dsn``, raw and decoded."""
    found: set[str] = set()
    for pattern in (_KV_PASSWORD, _KV_PASSWORD_SPAN):
        for match in pattern.finditer(dsn):
            value = match.group(2)
            found.add(value)
            if len(value) >= 2 and value[0] == value[-1] == "'":
                found.add(re.sub(r"\\(.)", r"\1", value[1:-1]))
    for pattern in (_URI_PASSWORD, _QUERY_PASSWORD):
        for match in pattern.finditer(dsn):
            found.update({match.group(2), unquote(match.group(2))})
    # libpq reports a bad token by quoting it, so a password with spaces can come back in pieces.
    found.update(part for value in list(found) for part in value.split())
    return {value for value in found if value}


def redact(message: str, secrets: Iterable[str | None] = ()) -> str:
    """Remove ``secrets`` (connection strings), their passwords and any password-shaped text.

    Parameters
    ----------
    message : str
        Text about to be logged or printed.
    secrets : Iterable[str | None]
        Connection strings in play; ``None`` entries are skipped.

    Returns
    -------
    str
        ``message`` with each secret, each password inside one, and any ``password=``
        or ``user:password@`` value replaced by ``***``.
    """
    for secret in secrets:
        if not secret:
            continue
        for value in sorted({secret, *_passwords(secret)}, key=len, reverse=True):
            message = message.replace(value, _REDACTED)
    message = _KV_PASSWORD.sub(rf"\g<1>{_REDACTED}", message)
    message = _URI_PASSWORD.sub(rf"\g<1>{_REDACTED}\g<3>", message)
    return _QUERY_PASSWORD.sub(rf"\g<1>{_REDACTED}", message)


# --------------------------------------------------------------------------- #
# Catalog reads
# --------------------------------------------------------------------------- #


async def tables_present(conn: asyncpg.Connection, schemas: list[str]) -> set[str]:
    """``"schema.table"`` for every table that exists in ``schemas``."""
    rows = await conn.fetch(
        """
        SELECT n.nspname || '.' || c.relname AS name
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind IN ('r', 'p') AND n.nspname = ANY($1::text[])
        """,
        schemas,
    )
    return {row["name"] for row in rows}


async def schemas_present(conn: asyncpg.Connection, schemas: list[str]) -> list[str]:
    """The subset of ``schemas`` that exists, sorted."""
    rows = await conn.fetch("SELECT nspname FROM pg_namespace WHERE nspname = ANY($1::text[])", schemas)
    return sorted(row["nspname"] for row in rows)


async def role_exists(conn: asyncpg.Connection, role: str) -> bool:
    return await conn.fetchval("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)", role)


async def is_superuser(conn: asyncpg.Connection, role: str | None = None) -> bool:
    """Whether ``role`` (the session user when omitted) is a superuser."""
    if role is None:
        return await conn.fetchval("SELECT rolsuper FROM pg_roles WHERE rolname = current_user")
    return bool(await conn.fetchval("SELECT rolsuper FROM pg_roles WHERE rolname = $1", role))


async def table_owners(conn: asyncpg.Connection, tables: Iterable[str]) -> dict[str, str]:
    """``{"schema.table": owner}`` for each of ``tables`` that exists."""
    rows = await conn.fetch(
        """
        SELECT n.nspname || '.' || c.relname AS name, pg_get_userbyid(c.relowner) AS owner
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname || '.' || c.relname = ANY($1::text[])
        """,
        list(tables),
    )
    return {row["name"]: row["owner"] for row in rows}


_TABLE_PRIVILEGES = ("SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER")
# Privileges that can also be granted on single columns; has_table_privilege misses those.
_COLUMN_PRIVILEGES = ("SELECT", "INSERT", "UPDATE", "REFERENCES")


async def access_outside(
    conn: asyncpg.Connection, role: str, *, tables: Iterable[str], allowed: Mapping[str, set[str]]
) -> list[str]:
    """Every privilege ``role`` can use on ``tables`` beyond ``allowed``, however it holds it.

    Counts what Postgres would let the role do, not just its own grants: privileges
    inherited from roles it belongs to, ones granted to PUBLIC, the
    ``pg_read_all_data`` and ``pg_write_all_data`` roles, and grants on single columns.

    Parameters
    ----------
    conn : asyncpg.Connection
        Connection to the database holding ``tables``.
    role : str
        Role to check.
    tables : Iterable[str]
        ``"schema.table"`` names to check, each of which must exist.
    allowed : Mapping[str, set[str]]
        Privileges ``role`` may hold, per table; a table not listed allows none.

    Returns
    -------
    list[str]
        One entry per privilege held beyond ``allowed``, such as
        ``"SELECT on core.api_key (granted to PUBLIC)"``, sorted by table.
    """
    names = sorted(tables)
    rows = await conn.fetch(
        """
        SELECT t.name, p.privilege,
               CASE WHEN p.privilege = ANY($4::text[]) THEN has_any_column_privilege($1, t.ref, p.privilege)
                    ELSE has_table_privilege($1, t.ref, p.privilege) END AS held,
               CASE WHEN p.privilege = ANY($4::text[]) THEN has_any_column_privilege('public', t.ref, p.privilege)
                    ELSE has_table_privilege('public', t.ref, p.privilege) END AS public
        FROM unnest($2::text[], $3::text[]) AS t(name, ref)
        CROSS JOIN unnest($5::text[]) WITH ORDINALITY AS p(privilege, position)
        ORDER BY t.name, p.position
        """,
        role,
        names,
        [qualified(name) for name in names],
        list(_COLUMN_PRIVILEGES),
        list(_TABLE_PRIVILEGES),
    )
    return [
        f"{row['privilege']} on {row['name']}" + (" (granted to PUBLIC)" if row["public"] else "")
        for row in rows
        if row["held"] and row["privilege"] not in allowed.get(row["name"], set())
    ]


async def owner_memberships(conn: asyncpg.Connection, role: str, tables: Iterable[str]) -> list[str]:
    """Owners of ``tables`` that ``role`` belongs to, directly or through other roles.

    Membership is enough to act as the owner (``SET ROLE``), even where the role
    doesn't inherit the owner's privileges, so it counts whatever its options.
    """
    owners = sorted(set((await table_owners(conn, tables)).values()))
    rows = await conn.fetch(
        "SELECT owner FROM unnest($2::text[]) AS o(owner) WHERE owner <> $1 AND pg_has_role($1, owner, 'MEMBER')",
        role,
        owners,
    )
    return [row["owner"] for row in rows]


async def require_confined(
    conn: asyncpg.Connection,
    role: str,
    *,
    tables: Iterable[str],
    allowed: Mapping[str, set[str]],
    command: str,
    policy: str,
) -> None:
    """Refuse unless ``role`` can do no more on ``tables`` than ``allowed``.

    Run it after the command's own grants, inside its transaction, so the refusal
    rolls them back. It changes no grants itself: access that comes from PUBLIC or
    another role is shared, so the fix belongs where it was granted.

    Raises
    ------
    Refused
        When ``role`` is a superuser, belongs to an owner of ``tables``, or holds a
        privilege beyond ``allowed``; the message names each one.
    """
    tables = sorted(tables)
    if await is_superuser(conn, role):
        problems = ["it is a superuser"]
    else:
        problems = [
            f"it belongs to {owner}, which owns Canopy tables" for owner in await owner_memberships(conn, role, tables)
        ]
        problems += await access_outside(conn, role, tables=tables, allowed=allowed)
    if problems:
        raise Refused(
            f"role {role} must have no more than {policy}, but {'; '.join(problems)}. Remove that access where it "
            f"is granted (PUBLIC, or the other role); {command} doesn't change shared grants. Nothing was changed"
        )


async def tables_with_rows_of(
    conn: asyncpg.Connection, registry: ReplicationRegistry, tables: Iterable[str], owner: str
) -> list[str]:
    """Those of ``tables`` holding at least one row owned by ``owner``, sorted."""
    found = []
    for name in sorted(tables):
        join_sql, owner_sql = owner_join(registry, name, alias="t")
        if await conn.fetchval(
            f"SELECT EXISTS (SELECT 1 FROM {qualified(name)} t {join_sql} WHERE {owner_sql} = $1)", owner
        ):
            found.append(name)
    return found


def jsonable(record: asyncpg.Record | dict) -> dict:
    """A row as a JSON-ready dict: timestamps as ISO strings, numerics as numbers."""
    out = {}
    for key, value in dict(record).items():
        if isinstance(value, datetime):
            value = value.isoformat()
        elif isinstance(value, Decimal):
            value = int(value) if value == value.to_integral_value() else float(value)
        out[key] = value
    return out
