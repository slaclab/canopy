"""Finding the owner of a routed write on a history head, from the head's copy.

The head holds every arm's rows with their ``owner_instance``, so the owner of a
row, a facility or a PV address is one query here. The SQL comes from the
replication registry's owner paths (``sql.owner_join``), so this module imports no
module's models.
"""

from __future__ import annotations

import json
from uuid import UUID

from sqlalchemy import text

from canopy.core.db import session_scope
from canopy.core.errors import FacilitySlugConflict
from canopy.core.replication.sql import qualified, owner_join, quote_ident
from canopy.core.forwarding.routes import (
    Row,
    Address,
    Facility,
    NoTarget,
    Facilities,
    WriteRoute,
)
from canopy.core.replication.registry import REGISTRY


def parse_body(body: bytes) -> object:
    """The request's JSON body, or ``None`` when it isn't JSON (the app answers that one)."""
    try:
        return json.loads(body)
    except ValueError:
        return None


def _field(obj: object, name: str) -> object:
    """``name`` from a JSON object, by its camelCase alias or its field name, as the DTOs accept."""
    if not isinstance(obj, dict):
        return None
    if name in obj:
        return obj[name]
    snake = "".join(f"_{char.lower()}" if char.isupper() else char for char in name)
    return obj.get(snake)


async def owner_of(
    route: WriteRoute, params: dict[str, str], body: object, *, requested: str | None, local: str
) -> str | None:
    """The instance ``route``'s write belongs to, or ``None`` when only the app here can answer it.

    Read from this instance's copy. ``requested`` is the ``?owner=`` the caller named,
    for a create that takes one; ``local`` is this instance's name. ``None`` hands the
    request to the app, whose services answer it: a row or facility this instance
    doesn't hold (404), facilities of two owners (``owner_mismatch``), a requested
    owner the target contradicts, or a PV address several other instances hold
    (``not_owner``).
    """
    rule = route.owner
    if isinstance(rule, NoTarget):
        return requested
    if isinstance(rule, Row):
        return await _row_owner(rule, params[rule.param])
    if isinstance(rule, Address):
        address = _field(body, rule.field)
        if not isinstance(address, str):
            return None
        owners = await _address_owners(address)
        if requested is not None:
            return requested if not owners or requested in owners else None
        if local in owners:
            return local
        return next(iter(owners)) if len(owners) == 1 else None
    if isinstance(rule, Facility):
        value = _field(body, rule.field)
        slugs = [value] if isinstance(value, str) else []
        named = value is not None
    elif isinstance(rule, Facilities):
        rows = body if rule.rows else [body]
        named_lists = [_field(row, rule.field) for row in (rows if isinstance(rows, list) else [])]
        slugs = [slug for named in named_lists if isinstance(named, list) for slug in named if isinstance(slug, str)]
        named = bool(slugs)
    else:
        raise TypeError(f"unknown owner rule {rule!r}")
    if not named:
        return requested
    owners = set((await _facility_owners(slugs)).values()) if slugs else set()
    if len(owners) != 1:
        return None
    owner = owners.pop()
    return owner if requested is None or requested == owner else None


async def require_unused(route: WriteRoute, body: object) -> None:
    """Refuse a forwarded create whose value of a key unique across owners is taken here.

    The head holds every arm's rows, so a value it holds under any owner would make
    the arm's new row clash on arrival and stop replication from that arm.

    Raises
    ------
    FacilitySlugConflict
        When the value is already here.
    """
    if route.unique is None:
        return
    table, column = route.unique
    value = _field(body, column)
    if not isinstance(value, str):
        return
    join_sql, owner_sql = owner_join(REGISTRY, table, alias="t")
    async with session_scope() as session:
        owner = await session.scalar(
            text(f"SELECT {owner_sql} FROM {qualified(table)} t {join_sql} WHERE t.{quote_ident(column)} = :value"),
            {"value": value},
        )
    if owner is not None:
        raise FacilitySlugConflict(
            f"'{value}' is already taken here, by a facility owned by '{owner}'; slugs are unique across every "
            "instance this one holds"
        )


async def _row_owner(rule: Row, value: str) -> str | None:
    if rule.column == "id":
        try:
            value = str(UUID(value))
        except ValueError:
            return None  # the app answers a malformed id with its own 404
        condition = f"t.{quote_ident(rule.column)} = CAST(:value AS uuid)"
    else:
        condition = f"t.{quote_ident(rule.column)} = :value"
    join_sql, owner_sql = owner_join(REGISTRY, rule.table, alias="t")
    async with session_scope() as session:
        return await session.scalar(
            text(f"SELECT {owner_sql} FROM {qualified(rule.table)} t {join_sql} WHERE {condition}"),
            {"value": value},
        )


async def _facility_owners(slugs: list[str]) -> dict[str, str]:
    """The owner of each of ``slugs`` that names a facility here."""
    async with session_scope() as session:
        rows = await session.execute(
            text("SELECT slug, owner_instance FROM core.facility WHERE slug = ANY(:slugs)"),
            {"slugs": sorted(set(slugs))},
        )
        return {row.slug: row.owner_instance for row in rows}


async def _address_owners(address: str) -> set[str]:
    """The owners of the live PVs holding ``address`` as any of their addresses."""
    async with session_scope() as session:
        rows = await session.execute(
            text(
                "SELECT DISTINCT owner_instance FROM squirrel.pv WHERE deleted_at IS NULL "
                "AND (setpoint_address = :a OR readback_address = :a OR config_address = :a)"
            ),
            {"a": address},
        )
        return {row.owner_instance for row in rows}


async def snapshot_is_live(snapshot_id: str) -> bool:
    """Whether this instance holds ``snapshot_id`` and it isn't deleted.

    An arm that answers 404 for a snapshot the head still holds has pruned it: the
    head keeps history the arm no longer has.
    """
    try:
        snapshot_id = str(UUID(snapshot_id))
    except ValueError:
        return False
    async with session_scope() as session:
        return bool(
            await session.scalar(
                text("SELECT 1 FROM squirrel.snapshot WHERE id = CAST(:id AS uuid) AND deleted_at IS NULL"),
                {"id": snapshot_id},
            )
        )
