"""Deletes on a head, as its API runs them: the app role, under row-level security.

The head's policies let the app role read every row but update only the head's own,
so ``SELECT … FOR UPDATE`` doesn't return an arm's row there. A delete has to find
and judge the row before it locks it, or an arm's PV looks missing.
"""

from contextlib import asynccontextmanager
from collections.abc import AsyncIterator

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from canopy.core.errors import NotOwner
from canopy.squirrel.services.pv_service import PvService

from .conftest import HEAD_SERVER, Octopus, server_dsn, _subscribed

one_arm = pytest.mark.parametrize("octopus", [1], indirect=True)


@asynccontextmanager
async def _as_the_heads_api(o: Octopus) -> AsyncIterator[AsyncSession]:
    """A session on the head's database as its app role, the way its API connects."""
    url = make_url(server_dsn(HEAD_SERVER, database=make_url(o.head.dsn).database)).set(drivername="postgresql+asyncpg")
    engine = create_async_engine(url)
    try:
        async with AsyncSession(engine) as session:
            await session.execute(text(f'SET ROLE "{o.config.app_role}"'))
            yield session
            await session.rollback()
    finally:
        await engine.dispose()


@one_arm
async def test_deleting_an_arms_pv_on_the_head_is_refused_by_its_owner(octopus: Octopus, as_instance):
    o = octopus
    ids = await _subscribed(o)

    async with _as_the_heads_api(o) as session:
        with as_instance(o.head.name), pytest.raises(NotOwner, match=o.arm.name):
            await PvService(session).delete(ids["pv"])

    assert await o.head.conn.fetchval("SELECT deleted_at FROM squirrel.pv WHERE id = $1", ids["pv"]) is None
