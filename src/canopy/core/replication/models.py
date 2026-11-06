"""Replication bookkeeping tables — the ``core`` Postgres schema, never replicated.

``core.history_heartbeat`` lives on each arm and is written by its history head:
``python -m canopy.core.replication heartbeat --arm <arm>`` runs on the head after
its backup, WAL-archive and drift checks and records their outcome for that arm
here, over a connection as a role that may write only this table. The arm's prune
job reads it before deleting anything, so the arm never removes old rows while the
head's copy of them is unchecked. The registry classifies it ``local``: the head
must not receive an arm's heartbeat rows.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import Text, String, Boolean, DateTime, func
from sqlalchemy.orm import Mapped, mapped_column

from canopy.core.db import Base

CORE_SCHEMA = "core"


class HistoryHeartbeat(Base):
    """The latest checks the head reported for this arm, keyed by its subscription name.

    ``applied_lsn`` is this arm's WAL position the head had applied, as text
    (``0/16B3748``), or ``None`` when the head's role may not read it.
    ``written_at`` is set by this arm's clock, so freshness is judged on one clock.
    """

    __tablename__ = "history_heartbeat"
    __table_args__ = {"schema": CORE_SCHEMA}

    name: Mapped[str] = mapped_column(String(63), primary_key=True)
    backup_ok: Mapped[bool] = mapped_column(Boolean, nullable=False)
    wal_archive_ok: Mapped[bool] = mapped_column(Boolean, nullable=False)
    drift_ok: Mapped[bool] = mapped_column(Boolean, nullable=False)
    applied_lsn: Mapped[str | None] = mapped_column(Text)
    written_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
