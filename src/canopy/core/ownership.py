"""Row owners: which instance may write a row, in every role.

Five tables carry ``owner_instance``: ``core.facility``, ``squirrel.pv``,
``squirrel.snapshot``, ``squirrel.tag_group`` and ``magpie.config``. It holds the
``CANOPY_INSTANCE`` of the instance that created the row. It is set on insert and
never changed afterwards, and replication carries it to a mirror with the row.
Child rows follow their parent: a snapshot's values, a PV's tags, facilities and
meta-PV marker, and a tag group's tags. Related rows share one owner: a facility
and its parent, a PV and its facilities and tags, a snapshot and its facility and
PVs.

An instance writes only rows it owns. The services check before they write, and
answer ``not_owner`` (403) for another owner's row, or ``owner_mismatch`` (409) for
a request that links rows of different owners. A ``before_flush`` listener is the
backstop for an ORM write that a service check missed.

A create's owner is the owner of what it is created in: a facility's parent, a
PV's facilities, a snapshot's facility, a tag's group. A create with none of those
belongs to the instance that receives it. Creating a root facility, a PV, a PV
import, a snapshot, a meta-PV marker, a tag group, a tag import or a Magpie config
also takes ``?owner=``, which an instance accepts
only with its own name; on a mirror, the router forwards a create naming an arm
before it gets here.
"""

from __future__ import annotations

from typing import Any, Annotated
from itertools import chain
from collections.abc import Iterable

from fastapi import Query
from sqlalchemy import String, event
from sqlalchemy.orm import Mapped, Session, mapped_column

from canopy.core.config import INSTANCE_NAME_PATTERN, get_settings
from canopy.core.errors import NotOwner, OwnerMismatch


def local_instance() -> str:
    """This instance's name, the owner it stamps on every row it creates."""
    return get_settings().instance


class OwnedMixin:
    """``owner_instance``: the instance that created the row, set on insert and never changed."""

    owner_instance: Mapped[str] = mapped_column(String(48), nullable=False, default=local_instance)


OwnerQuery = Annotated[
    str | None,
    Query(
        pattern=INSTANCE_NAME_PATTERN,
        description=(
            "The instance that will own the new row, for a create with no facility or parent to take "
            "the owner from. Defaults to this instance. An instance refuses with not_owner a create it "
            "would not own itself; a mirror's router may forward a create meant for an arm."
        ),
    ),
]


def require_owned(owner: str, what: str) -> None:
    """Refuse with ``not_owner`` unless this instance owns ``what``."""
    if owner != local_instance():
        raise NotOwner(f"{what} is owned by '{owner}'; write it there")


def one_owner(owners: Iterable[str], what: str) -> str | None:
    """The single owner of ``owners``, or ``None`` when there are none.

    Raises
    ------
    OwnerMismatch
        When ``owners`` holds more than one name.
    """
    distinct = set(owners)
    if len(distinct) > 1:
        raise OwnerMismatch(f"{what} belong to different owners ({', '.join(sorted(distinct))})")
    return next(iter(distinct), None)


def require_create_owner(target_owner: str | None, requested: str | None, what: str) -> None:
    """Refuse with ``not_owner`` a create this instance wouldn't own.

    ``target_owner`` is the owner of what the row is created in (``None`` when it
    has no facility or parent); ``requested`` is the ``?owner=`` the caller named.
    An instance accepts only its own name as ``requested``.
    """
    if requested is not None:
        require_owned(requested, what)
    if target_owner is not None:
        require_owned(target_owner, what)


def require_same_owner(owner: str, others: Iterable[str], what: str) -> None:
    """Refuse with ``owner_mismatch`` when any of ``others`` isn't ``owner``."""
    strangers = sorted(set(others) - {owner})
    if strangers:
        raise OwnerMismatch(f"{what} must belong to '{owner}', not {', '.join(repr(s) for s in strangers)}")


def _owner_of(session: Session, row: Any) -> str | None:
    """The owner of an ORM row, or ``None`` for a row outside the owned tables.

    A new row's ``owner_instance`` is still ``None`` until the column default fills
    it in with this instance. A child names its parent with ``__owner_parent__``,
    a ``(model, foreign-key attribute)`` pair.
    """
    if isinstance(row, OwnedMixin):
        return row.owner_instance or local_instance()
    parent = getattr(type(row), "__owner_parent__", None)
    if parent is None:
        return None
    model, attribute = parent()
    key = getattr(row, attribute)
    if key is None:
        return None  # no parent yet: the foreign key refuses the row, not this check
    owner_row = session.get(model, key)
    return owner_row.owner_instance if owner_row is not None else None


@event.listens_for(Session, "before_flush")
def _refuse_foreign_writes(session: Session, flush_context: Any, instances: Any) -> None:
    """The backstop: no ORM flush inserts, updates or deletes another owner's row."""
    for row in chain(session.new, session.dirty, session.deleted):
        owner = _owner_of(session, row)
        if owner is not None and owner != local_instance():
            table = row.__table__.fullname
            raise NotOwner(f"{table} row is owned by '{owner}'; write it there")
