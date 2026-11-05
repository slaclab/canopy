"""Which side of replication a migration runs on, for steps that belong to one side only.

A history mirror runs the same Alembic tree as its primary, a release ahead of it,
so most migrations run on both. A few steps must not:

* Data steps (INSERT, UPDATE or DELETE on a replicated table) run on the publisher
  only. Replication carries their result to the mirror; an INSERT run on both sides
  stops apply with a duplicate key.
* Indexes that only history queries need run on the subscriber only, and stay out
  of ``Base.metadata`` so ``alembic check`` on a primary still passes.
* A dropped column or table leaves the publisher first and the subscriber later, once
  the primary runs the publisher-side drop and the subscriber has applied every row
  that still carried it. The subscriber-side revision names the publisher-side one in
  a module-level ``canopy_requires_primary_at = "<revision>"`` (``REQUIRES_PRIMARY_AT``);
  the migration-order test honours it, and deploying it needs the check in
  ``CONTRIBUTING.md``.

``migrations/env.py`` records the side from ``CANOPY_ROLE``: ``primary`` is the
publisher, and every other role serves a subscriber's database. Use it as::

    from canopy.core.replication.migration import on_publisher

    def upgrade() -> None:
        if on_publisher():
            op.execute("UPDATE squirrel.pv SET ...")

The rules are in ``CONTRIBUTING.md`` ("Database migrations").
"""

from __future__ import annotations

from typing import Literal

from alembic import op

Side = Literal["publisher", "subscriber"]

OPTION = "canopy_replication_side"
"""Key ``migrations/env.py`` stores the side under, in the migration context's options."""

REQUIRES_PRIMARY_AT = "canopy_requires_primary_at"
"""Module attribute of a subscriber-side drop: the publisher-side revision its primary must already run."""


def side_for_role(role: str) -> Side:
    """The replication side of a database served with ``CANOPY_ROLE=role``."""
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
    """Whether this migration runs on a primary, the database a mirror replicates from."""
    return migration_side() == "publisher"


def on_subscriber() -> bool:
    """Whether this migration runs on a mirror's database, which receives the primary's rows."""
    return migration_side() == "subscriber"
