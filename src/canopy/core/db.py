"""Shared async database foundation: engine, session, and ORM base/mixins.

Any module which needs persistence can reuse this engine and ``Base``. The
architecture is one Postgres with one schema per module — set per model via
``__table_args__ = {"schema": "<module>"}``. All modules' ORM metadata lives on
this single ``Base.metadata`` so the one Alembic tree (``migrations/``) can
autogenerate across modules.

Routes and services take their request session through ``SessionDep``;
``get_session`` explains its scope.
"""

from __future__ import annotations

from uuid import uuid4
from typing import Annotated
from datetime import datetime
from contextlib import asynccontextmanager
from collections.abc import AsyncGenerator

from fastapi import Depends
from sqlalchemy import DateTime, func, text, select
from sqlalchemy.orm import Mapped, DeclarativeBase, mapped_column
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.dialects.postgresql import UUID

from canopy.core.config import get_settings
from canopy.core.forwarding.context import ForwardedWrite, forwarded_write


class Base(DeclarativeBase):
    """Declarative base shared by every module's ORM models."""


class UUIDMixin:
    """String UUID primary key (``as_uuid=False`` keeps ids JSON/opaque-ref friendly)."""

    id: Mapped[str] = mapped_column(UUID(as_uuid=False), primary_key=True, default=lambda: str(uuid4()))


class TimestampMixin:
    """``created_at`` / ``updated_at`` managed by the database."""

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


class SoftDeleteMixin:
    """``deleted_at``: set when a user deletes the row, which then stays in the table.

    Reads skip rows with ``deleted_at`` set. Deleting is an UPDATE, so it still reaches
    a history head through a publication that carries inserts and updates only, and anything
    that refers to the row (snapshot values referring to a PV) keeps working.
    """

    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class BaseRepository[ModelType: Base]:
    """Base repository with common CRUD operations (flush-not-commit).

    Each repository takes an ``AsyncSession`` and only ever ``flush()``es — the
    transaction boundary (commit / rollback) is owned by the caller (``get_session``
    request dep, or ``session_scope`` in a worker task). Shared here so every
    persistent module reuses one definition.
    """

    def __init__(self, model: type[ModelType], session: AsyncSession):
        self.model = model
        self.session = session

    async def get_by_id(self, id: str) -> ModelType | None:
        return await self.session.get(self.model, id)

    async def get_all(self) -> list[ModelType]:
        result = await self.session.execute(select(self.model))
        return list(result.scalars().all())

    async def create(self, obj: ModelType) -> ModelType:
        self.session.add(obj)
        await self.session.flush()
        await self.session.refresh(obj)
        return obj

    async def update(self, obj: ModelType) -> ModelType:
        await self.session.flush()
        await self.session.refresh(obj)
        return obj

    async def delete(self, obj: ModelType) -> None:
        await self.session.delete(obj)
        await self.session.flush()

    async def count(self) -> int:
        result = await self.session.execute(select(func.count()).select_from(self.model))
        return result.scalar() or 0


_engine: AsyncEngine | None = None
_session_maker: async_sessionmaker[AsyncSession] | None = None


def get_engine() -> AsyncEngine:
    """Return the process-wide async engine, building it lazily on first use.

    Lazy construction keeps importing this module side-effect free (no
    ``DATABASE_URL`` required at import time) so unit tests that never touch
    Postgres can import models freely. The same engine serves both the
    ``canopy-api`` process and ``canopy-worker``.
    """
    global _engine, _session_maker
    if _engine is None:
        settings = get_settings()
        _engine = create_async_engine(
            settings.database_url,
            pool_size=settings.database_pool_size,
            max_overflow=settings.database_max_overflow,
            echo=settings.debug,
        )
        _session_maker = async_sessionmaker(_engine, class_=AsyncSession, expire_on_commit=False, autoflush=False)
    return _engine


def get_session_maker() -> async_sessionmaker[AsyncSession]:
    if _session_maker is None:
        get_engine()
    assert _session_maker is not None
    return _session_maker


@asynccontextmanager
async def session_scope() -> AsyncGenerator[AsyncSession, None]:
    """A session with explicit transaction control — it does NOT commit for you.

    The caller decides when (or whether) to commit, so a worker task can make a
    multi-step write all-or-nothing and an Arq retry (``max_tries``) never re-runs
    against partially-committed state. Rolls back if the block raises.
    """
    async with get_session_maker()() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI request dependency: a session that commits once on success and rolls
    back on error. Background/worker code that needs its own transaction boundaries
    should use ``session_scope`` instead.

    Routes and services take it through ``SessionDep``, not ``Depends(get_session)``.
    ``SessionDep`` is function-scoped: FastAPI's default ``"request"`` scope runs the
    code after ``yield`` only once the response has been sent, so the client would get
    its 200 before the commit lands (a read-after-write race) and a failed commit would
    never reach it. Function scope commits before the response starts.

    Every dependency in a request that takes ``SessionDep`` shares one session and one
    transaction, because FastAPI only reuses a dependency declared the same way, scope
    included. A module that needs a different policy can still declare its own.

    For a write a history head forwarded on someone's behalf, the commit's WAL position
    is recorded for ``X-Canopy-Commit-LSN`` (see ``_commit_forwarded``).
    """
    async with session_scope() as session:
        yield session
        if (write := forwarded_write.get()) is not None and write.on_behalf_of is not None:
            await _commit_forwarded(session, write)
        else:
            await session.commit()


async def _commit_forwarded(session: AsyncSession, write: ForwardedWrite) -> None:
    """Commit, and if the transaction wrote anything, record a WAL position at or past its commit.

    The position is the end of the last WAL record when it is read, straight after the
    commit: the commit record's own end, which is what the head reports as applied once
    it has the change (``pg_replication_origin_status.remote_lsn``). It is never before
    that end, so the head never reports a write as applied before it is. Other
    sessions' WAL can land in between, and then only the arm's next published write
    takes the head past it. A transaction that wrote nothing has no xid and gets no
    position, so a read or a job enqueue never makes the head wait.

    A route that always answers with a position (``ForwardedWrite.report_position``,
    the captured import) gets the current WAL position when it wrote nothing: a
    replay of a write already committed, which is at or before it.
    """
    wrote = await session.scalar(text("SELECT pg_current_xact_id_if_assigned() IS NOT NULL"))
    await session.commit()
    if wrote or write.report_position:
        current = "pg_current_wal_insert_lsn()" if wrote else "pg_current_wal_lsn()"
        row = (await session.execute(text(_POSITION.format(current=current)))).one()
        write.commit_lsn = _lsn_text(_record_end(int(row.position), int(row.page), int(row.segment)))


_POSITION = """
SELECT {current} - '0/0'::pg_lsn AS position,
       (SELECT setting::bigint FROM pg_settings WHERE name = 'wal_block_size') AS page,
       (SELECT setting::bigint FROM pg_settings WHERE name = 'wal_segment_size') AS segment
"""

# Bytes of a WAL page's header: the long one opens each segment (SizeOfXLogLongPHD), the
# short one every other page (SizeOfXLogShortPHD), on a 64-bit build.
_LONG_PAGE_HEADER = 40
_SHORT_PAGE_HEADER = 24


def _record_end(position: int, page: int, segment: int) -> int:
    """The end of the last WAL record written before ``position``, an insert position.

    The two differ only when that record ended a page: the insert position then points
    past the next page's header, where the next record would start, while the record's
    end, which a subscriber reports as applied, is the page boundary.
    """
    if position % segment == _LONG_PAGE_HEADER:
        return position - _LONG_PAGE_HEADER
    if position % page == _SHORT_PAGE_HEADER:
        return position - _SHORT_PAGE_HEADER
    return position


def _lsn_text(position: int) -> str:
    """``position`` as ``pg_lsn`` text (``"16/B374D848"``)."""
    return f"{position >> 32:X}/{position & 0xFFFFFFFF:X}"


SessionDep = Annotated[AsyncSession, Depends(get_session, scope="function")]


async def dispose_engine() -> None:
    """Dispose the engine on shutdown. Call from the app / worker lifespan."""
    global _engine, _session_maker
    if _engine is not None:
        await _engine.dispose()
        _engine = None
        _session_maker = None
