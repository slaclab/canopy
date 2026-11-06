"""Which side of replication a migration runs on, for steps that belong to one side only.

A history head runs the same Alembic tree as each of its arms, and migrates before
every one of them, so most migrations run on all of them. A few steps must not:

* Data steps (INSERT, UPDATE or DELETE on a replicated table) run on the publishers
  (the arms) only. Replication carries each arm's result to the head; an INSERT run on
  the head as well stops apply with a duplicate key.
* Indexes that only history queries need run on the subscriber (the head) only, and
  stay out of ``Base.metadata`` so ``alembic check`` on an arm still passes.
* A dropped column or table leaves the publishers (the arms) first and the subscriber
  later, once every arm runs the publisher-side drop and the head has applied every
  row that still carried it. The subscriber-side revision names the publisher-side
  one in a module-level ``canopy_requires_primary_at = "<revision>"``
  (``REQUIRES_PRIMARY_AT``); the migration-order test honours it, and deploying it
  needs the check in ``CONTRIBUTING.md``.

``migrations/env.py`` records the side from ``CANOPY_ROLE``: a ``primary`` (an arm)
is a publisher and a ``mirror`` (a head) the subscriber. Use it as::

    from canopy.core.replication.migration import on_publisher

    def upgrade() -> None:
        if on_publisher():
            op.execute("UPDATE squirrel.pv SET ...")

The rules are in ``CONTRIBUTING.md`` ("Database Migrations").
"""

from __future__ import annotations

from typing import Literal

from alembic import op

Side = Literal["publisher", "subscriber"]

OPTION = "canopy_replication_side"
"""Key ``migrations/env.py`` stores the side under, in the migration context's options."""

REQUIRES_PRIMARY_AT = "canopy_requires_primary_at"
"""Module attribute of a subscriber-side drop: the publisher-side revision every arm must already run."""


def side_for_role(role: str) -> Side:
    """The replication side of a database served with ``CANOPY_ROLE=role``: an arm publishes, a head subscribes."""
    return "publisher" if role == "primary" else "subscriber"


def migration_side() -> Side:
    """The side of the database the running migration is applied to.

    Raises
    ------
    RuntimeError
        When the migration environment didn't record a side (it wasn't run through
        ``migrations/env.py``), so a side-specific step can't guess.
    """
    side = op.get_context().opts.get(OPTION)
    if side not in ("publisher", "subscriber"):
        raise RuntimeError(
            f"no replication side recorded for this migration (expected migrations/env.py to set {OPTION})"
        )
    return side


def on_publisher() -> bool:
    """Whether this migration runs on an arm, a database a head replicates from."""
    return migration_side() == "publisher"


def on_subscriber() -> bool:
    """Whether this migration runs on a head's database, which receives its arms' rows."""
    return migration_side() == "subscriber"
