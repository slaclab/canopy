"""Shared pieces of the replication commands: errors, quoting, redaction, catalog reads.

DDL takes no bind parameters, so identifiers and the subscription's connection
string are spliced into statements here, quoted. The connection string carries a
password, and Postgres error text can quote fragments of it, so every message
that leaves the CLI goes through ``redact`` first.
"""

from __future__ import annotations

import re
from decimal import Decimal
from datetime import datetime
from urllib.parse import unquote
from collections.abc import Iterable

import asyncpg
from sqlalchemy.engine import make_url

from canopy.core.replication.migration import side_for_role

_REDACTED = "***"


class ReplicationError(Exception):
    """A database step failed. The message is safe to print: secrets are redacted."""


class Refused(ReplicationError):
    """The command won't run here (wrong role, or a failed precondition). Nothing was changed."""


# The side a command names, in ``side_for_role``'s terms.
_SIDES = {"primary": "publisher", "mirror": "subscriber"}


def require_role(command: str, required: str, role: str) -> None:
    """Refuse ``command`` unless this instance is on the ``required`` side.

    ``required`` is ``"primary"`` or ``"mirror"``. The side comes from
    ``migration.side_for_role``: every role but ``primary``, ``forward`` included,
    serves a mirror's database.

    Raises
    ------
    Refused
        When ``role`` is on the other side.
    """
    if side_for_role(role) != _SIDES[required]:
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


async def non_empty(conn: asyncpg.Connection, tables: Iterable[str]) -> list[str]:
    """Those of ``tables`` holding at least one row, sorted."""
    found = []
    for name in sorted(tables):
        if await conn.fetchval(f"SELECT EXISTS (SELECT 1 FROM {qualified(name)})"):
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
