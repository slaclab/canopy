"""Squirrel ORM models — the ``squirrel`` Postgres schema.

Every table lives in the ``squirrel`` schema (schema-per-module)
and reuses the shared ``canopy.core.db`` declarative base + mixins so the single
Alembic tree autogenerates across modules. Cross-*module* references are opaque ids,
never cross-schema foreign keys — but the foreign keys *within* squirrel
(snapshot→value, pv↔tag, …) port as-is.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    Text,
    Float,
    Index,
    Table,
    Column,
    String,
    Boolean,
    Integer,
    DateTime,
    Identity,
    BigInteger,
    ForeignKey,
    CheckConstraint,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.orm import Mapped, relationship, mapped_column
from sqlalchemy.dialects.postgresql import UUID, JSONB

from canopy.core.db import Base, UUIDMixin, TimestampMixin, SoftDeleteMixin
from canopy.core.ownership import OwnedMixin

SQUIRREL_SCHEMA = "squirrel"


def _fk(target: str) -> str:
    """Schema-qualify an intra-squirrel foreign-key target (e.g. ``pv.id``)."""
    return f"{SQUIRREL_SCHEMA}.{target}"


# --------------------------------------------------------------------------- #
# PV registry + tagging
# --------------------------------------------------------------------------- #

# Many-to-many between pv and tag. Composite PK (pv_id, tag_id); both sides
# cascade so deleting either end removes the link rows at the database.
pv_tag = Table(
    "pv_tag",
    Base.metadata,
    Column("pv_id", UUID(as_uuid=False), ForeignKey(_fk("pv.id"), ondelete="CASCADE"), primary_key=True),
    Column("tag_id", UUID(as_uuid=False), ForeignKey(_fk("tag.id"), ondelete="CASCADE"), primary_key=True),
    schema=SQUIRREL_SCHEMA,
)


class Pv(UUIDMixin, TimestampMixin, OwnedMixin, SoftDeleteMixin, Base):
    """A registered PV: its address(es), owning device, and compare tolerances.

    Address resolution (the three ``*_address`` columns + ``device``) is kept
    independent of the comparison policy (``abs_tolerance``/``rel_tolerance``) so a
    later Raccoon directory delegation can resolve addresses externally while the
    tolerances stay local. Only ``setpoint_address`` is unique, per owner: the same
    PV can be registered on two instances, and a mirror holds both rows. The
    at-least-one-address rule is enforced in the service layer, not the DB.

    Deleting a PV sets ``deleted_at``: the row, its tags and its values in past
    snapshots stay, and registering the same identifying address again brings the
    row back.
    """

    __tablename__ = "pv"
    __table_args__ = (
        UniqueConstraint("owner_instance", "setpoint_address", name="uq_pv_owner_instance_setpoint_address"),
        {"schema": SQUIRREL_SCHEMA},
    )

    setpoint_address: Mapped[str | None] = mapped_column(String(255), index=True)
    readback_address: Mapped[str | None] = mapped_column(String(255), index=True)
    config_address: Mapped[str | None] = mapped_column(String(255), index=True)
    device: Mapped[str | None] = mapped_column(String(255), index=True)
    description: Mapped[str | None] = mapped_column(Text)
    abs_tolerance: Mapped[float] = mapped_column(Float, nullable=False, server_default=text("0.0"))
    rel_tolerance: Mapped[float] = mapped_column(Float, nullable=False, server_default=text("0.0"))

    tags: Mapped[list[Tag]] = relationship("Tag", secondary=pv_tag, back_populates="pvs", lazy="selectin")
    # No ORM cascade: a PV's values belong to the snapshots that captured them.
    snapshot_values: Mapped[list[SnapshotValue]] = relationship(
        "SnapshotValue", back_populates="pv", passive_deletes=True
    )


class TagGroup(UUIDMixin, TimestampMixin, OwnedMixin, Base):
    """A named group of tags (e.g. ``Region``); the name is unique per owner."""

    __tablename__ = "tag_group"
    __table_args__ = (
        UniqueConstraint("owner_instance", "name", name="uq_tag_group_owner_instance_name"),
        {"schema": SQUIRREL_SCHEMA},
    )

    name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    description: Mapped[str | None] = mapped_column(Text)

    tags: Mapped[list[Tag]] = relationship("Tag", back_populates="group", cascade="all, delete-orphan", lazy="selectin")


class Tag(UUIDMixin, TimestampMixin, Base):
    """A tag within a group. Per-group name uniqueness is enforced in the repo."""

    __tablename__ = "tag"
    __table_args__ = {"schema": SQUIRREL_SCHEMA}

    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    group_id: Mapped[str] = mapped_column(
        UUID(as_uuid=False), ForeignKey(_fk("tag_group.id"), ondelete="CASCADE"), nullable=False, index=True
    )

    group: Mapped[TagGroup] = relationship("TagGroup", back_populates="tags", lazy="selectin")
    pvs: Mapped[list[Pv]] = relationship("Pv", secondary=pv_tag, back_populates="tags")

    @staticmethod
    def __owner_parent__() -> tuple[type[TagGroup], str]:
        """A tag is owned by its group's owner (``canopy.core.ownership``)."""
        return TagGroup, "group_id"


# --------------------------------------------------------------------------- #
# Facilities
# --------------------------------------------------------------------------- #

# Many-to-many: shared infrastructure PVs legitimately belong to more than one facility.
# Unlike ``pv_tag``, ``facility_id`` gets its own index — every PV/snapshot search filters
# by facility, so it's the hot side of the join, not the PK-leading side. It's an opaque
# reference to ``core.facility.id``; facilities live in the ``core`` schema, resolved via
# ``canopy.core.facilities.gateway``, so there's no cross-schema foreign key.
pv_facility = Table(
    "pv_facility",
    Base.metadata,
    Column("pv_id", UUID(as_uuid=False), ForeignKey(_fk("pv.id"), ondelete="CASCADE"), primary_key=True),
    Column("facility_id", UUID(as_uuid=False), primary_key=True, index=True),
    schema=SQUIRREL_SCHEMA,
)


# --------------------------------------------------------------------------- #
# Snapshots
# --------------------------------------------------------------------------- #


class Snapshot(UUIDMixin, TimestampMixin, OwnedMixin, SoftDeleteMixin, Base):
    """A captured machine state: metadata + one ``SnapshotValue`` per read address.

    Its facility and every PV its values reference share its ``owner_instance``.
    Deleting a snapshot sets ``deleted_at`` and keeps its values; the row keeps its
    owner. ``retain`` exempts it from any pruning of old snapshots. ``imported_from`` is
    the id of the snapshot it was imported from, usually on a history mirror; no foreign
    key, since that snapshot may no longer exist here.
    """

    __tablename__ = "snapshot"
    __table_args__ = (
        # Serves the newest-first keyset listing (scanned backward) and range scans by
        # creation time, such as a cutoff for pruning old snapshots.
        Index("ix_squirrel_snapshot_created_at_id", "created_at", "id"),
        Index("ix_squirrel_snapshot_owner_instance", "owner_instance"),
        {"schema": SQUIRREL_SCHEMA},
    )

    title: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    description: Mapped[str | None] = mapped_column(Text)
    created_by: Mapped[str | None] = mapped_column(String(255))
    # Opaque reference to ``core.facility.id`` — no cross-schema foreign key; resolved
    # via ``canopy.core.facilities.gateway``.
    facility_id: Mapped[str | None] = mapped_column(UUID(as_uuid=False), index=True)
    retain: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    imported_from: Mapped[str | None] = mapped_column(UUID(as_uuid=False))

    values: Mapped[list[SnapshotValue]] = relationship(
        "SnapshotValue", back_populates="snapshot", cascade="all, delete-orphan", lazy="selectin"
    )


class SnapshotValue(UUIDMixin, Base):
    """One captured reading.

    ``setpoint_value``/``readback_value`` are free-form JSONB blobs (shaped
    ``{"value", "status", "severity", "timestamp", "units", "precision"}``) so
    metadata fidelity can grow without a migration. ``pv_name`` denormalizes the
    captured address for display/query without joining ``pv``. There are no row
    timestamps — the ``timestamp`` column holds the capture time, not a row audit.
    """

    __tablename__ = "snapshot_value"
    __table_args__ = {"schema": SQUIRREL_SCHEMA}

    snapshot_id: Mapped[str] = mapped_column(
        UUID(as_uuid=False), ForeignKey(_fk("snapshot.id"), ondelete="CASCADE"), nullable=False, index=True
    )
    pv_id: Mapped[str] = mapped_column(
        UUID(as_uuid=False), ForeignKey(_fk("pv.id"), ondelete="CASCADE"), nullable=False, index=True
    )
    pv_name: Mapped[str] = mapped_column(String(255), nullable=False)
    setpoint_value: Mapped[dict | None] = mapped_column(JSONB)
    readback_value: Mapped[dict | None] = mapped_column(JSONB)
    status: Mapped[int | None] = mapped_column(Integer)
    severity: Mapped[int | None] = mapped_column(Integer)
    timestamp: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    snapshot: Mapped[Snapshot] = relationship("Snapshot", back_populates="values")
    pv: Mapped[Pv] = relationship("Pv", back_populates="snapshot_values")


# --------------------------------------------------------------------------- #
# Meta-PVs
# --------------------------------------------------------------------------- #


class MetaPv(Base):
    """Marks a PV as a 'meta' PV shown specially on snapshots.

    Keyed directly by ``pv_id`` (no surrogate id, no timestamps). The FK
    intentionally has no cascade — a meta marker must be removed before its PV can
    be deleted.
    """

    __tablename__ = "meta_pv"
    __table_args__ = {"schema": SQUIRREL_SCHEMA}

    pv_id: Mapped[str] = mapped_column(UUID(as_uuid=False), ForeignKey(_fk("pv.id")), primary_key=True)
    display_name: Mapped[str] = mapped_column(String(32), nullable=False, server_default=text("''"))
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))


# --------------------------------------------------------------------------- #
# Snapshot outbox (a history head only; never replicated)
# --------------------------------------------------------------------------- #

OUTBOX_STATES = ("pending", "delivered", "failed")


class SnapshotOutbox(Base):
    """A snapshot the head captured for an arm it couldn't hand it to yet.

    The row holds the snapshot's bundle until the arm has saved it under the same
    id and the snapshot has replicated back to the head; then it goes. The registry
    classifies the table ``local``: an arm never receives the head's queue, and an
    arm's copy of a snapshot can't collide with a row here.

    ``state`` is ``pending`` (waiting, or backing off after a transient failure),
    ``delivered`` (the arm saved it; waiting for replication) or ``failed`` (the arm
    refused it, for the reason in ``last_error``). Each arm's rows are delivered in
    ``seq`` order. ``facility_id`` is ``None`` for a snapshot of every PV the arm
    owns, and has no foreign key: ``core.facility`` is a replica on the head, and a
    reference from a local row would stop an arm's facility delete from applying,
    and with it that arm's replication.

    ``caller`` is who the capture was taken for, as the capture job carried it:
    delivery sends that user and facility grant, not whatever holds when it runs.
    ``strict`` asks the arm to refuse a bundle it would save with values left out.
    """

    __tablename__ = "snapshot_outbox"
    __table_args__ = (
        CheckConstraint(f"state IN {OUTBOX_STATES!r}", name="ck_snapshot_outbox_state"),
        Index("ix_squirrel_snapshot_outbox_arm_state_seq", "arm", "state", "seq"),
        {"schema": SQUIRREL_SCHEMA},
    )

    id: Mapped[str] = mapped_column(UUID(as_uuid=False), primary_key=True)
    seq: Mapped[int] = mapped_column(BigInteger, Identity(), nullable=False)
    arm: Mapped[str] = mapped_column(String(48), nullable=False)
    facility_id: Mapped[str | None] = mapped_column(UUID(as_uuid=False))
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    created_by: Mapped[str | None] = mapped_column(String(255))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    bundle: Mapped[dict] = mapped_column(JSONB, nullable=False)
    value_count: Mapped[int] = mapped_column(Integer, nullable=False)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    caller: Mapped[dict] = mapped_column(JSONB, nullable=False)
    strict: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))
    state: Mapped[str] = mapped_column(String(16), nullable=False, server_default=text("'pending'"))
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    queued_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    next_attempt_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    last_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    failed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error: Mapped[str | None] = mapped_column(Text)
