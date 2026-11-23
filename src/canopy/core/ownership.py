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
backstop for a flushed ORM write (``session.add``, an attribute change,
``session.delete``) that a service check missed. It judges a stored row by its
stored stamp, refuses any change to that stamp, and checks a child's parent before
and after the change. Statements such as ``insert()``, ``update()``, ``delete()`` and
``COPY`` don't flush, so they go around it; the services check those.

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
from itertools import chain, batched
from collections.abc import Iterable

from fastapi import Query
from sqlalchemy import String, event, select, inspect
from sqlalchemy.orm import Mapped, Session, InstanceState, mapped_column

from canopy.core.config import INSTANCE_NAME_PATTERN, get_settings
from canopy.core.errors import NotOwner, OwnerMismatch


def local_instance() -> str:
    """This instance's name, the owner it stamps on every row it creates."""
    return get_settings().instance


class OwnedMixin:
    """``owner_instance``: the instance that created the row, set on insert and never changed."""

    # active_history: a change is compared with the stored stamp even when it wasn't loaded.
    owner_instance: Mapped[str] = mapped_column(String(48), nullable=False, default=local_instance, active_history=True)


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


_LOOKUP_BATCH = 5000
"""Parent keys per owner lookup, well under Postgres's bind-parameter limit."""


def _stored_owner(state: InstanceState, table: str) -> str:
    """An owned row's stamp: the one it is inserted with, or the stored one, which never changes."""
    row = state.obj()
    if not state.has_identity:
        return row.owner_instance or local_instance()
    history = state.attrs.owner_instance.history
    if history.has_changes():
        stored = history.deleted[0] if history.deleted else row.owner_instance
        raise NotOwner(f"{table} row is owned by '{stored}', and its owner_instance never changes")
    return row.owner_instance


def _referenced(state: InstanceState, model: type, attribute: str) -> tuple[set[Any], set[str]]:
    """The ``model`` rows a child names, before and after this flush.

    Returns the keys in ``attribute`` (now, and before a change), and the stamps of
    stored ``model`` rows attached through a relationship, whose key the flush
    hasn't copied into ``attribute`` yet. A new parent is checked as a row itself.
    """
    keys = {getattr(state.obj(), attribute), *state.attrs[attribute].history.deleted} - {None}
    owners: set[str] = set()
    for relationship in state.mapper.relationships:
        if relationship.mapper.class_ is model and any(c.key == attribute for c in relationship.local_columns):
            history = state.attrs[relationship.key].history  # loads nothing
            for parent in chain(history.added, history.unchanged, history.deleted):
                if parent is not None and inspect(parent).has_identity:
                    owners.add(parent.owner_instance)
    return keys, owners


def _foreign_owner(session: Session, model: Any, keys: set[Any]) -> str | None:
    """An owner other than this instance among the stored ``model`` rows with these keys."""
    here = local_instance()
    with session.no_autoflush:
        for chunk in batched(sorted(keys), _LOOKUP_BATCH):
            owner = session.execute(
                select(model.owner_instance).where(model.id.in_(chunk), model.owner_instance != here).limit(1)
            ).scalar()
            if owner is not None:
                return owner
    return None


@event.listens_for(Session, "before_flush")
def _refuse_foreign_writes(session: Session, flush_context: Any, instances: Any) -> None:
    """The backstop: no ORM flush inserts, updates or deletes another owner's row.

    A child names its owner's row with ``__owner_parent__``, a ``(model, foreign-key
    attribute)`` pair, and other rows that must share that owner with
    ``__owner_peers__``, a tuple of such pairs. Their keys are looked up together,
    one query per model.
    """
    here = local_instance()
    lookups: dict[tuple[Any, str], set[Any]] = {}
    for row in chain(session.new, session.dirty, session.deleted):
        state, table = inspect(row), row.__table__.fullname
        if isinstance(row, OwnedMixin):
            if (owner := _stored_owner(state, table)) != here:
                raise NotOwner(f"{table} row is owned by '{owner}'; write it there")
            continue
        parent = getattr(type(row), "__owner_parent__", None)
        peers = getattr(type(row), "__owner_peers__", None)
        for model, attribute in ((parent(),) if parent else ()) + (peers() if peers else ()):
            keys, owners = _referenced(state, model, attribute)
            if foreign := owners - {here}:
                raise NotOwner(f"{table} row is owned by '{min(foreign)}'; write it there")
            lookups.setdefault((model, table), set()).update(keys)
    for (model, table), keys in lookups.items():
        if keys and (owner := _foreign_owner(session, model, keys)) is not None:
            raise NotOwner(f"{table} row is owned by '{owner}'; write it there")
