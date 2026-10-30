"""Read-your-writes for forward mode, on two Postgres 16 servers.

The position the primary reports for a forwarded write (``core.db.get_session``,
``X-Canopy-Commit-LSN``) must be one the mirror's ``applied_lsn`` reaches once it has
applied that write, read by a non-superuser app role with the two grants the docs
name. Opt-in like ``test_replication_e2e.py``, whose ``pair`` fixture it reuses.
"""

from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.pool import NullPool
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from canopy.core import db
from canopy.core.config import get_settings
from canopy.core.forwarding.client import lsn_value, wait_until_applied
from canopy.core.forwarding.context import ForwardedWrite, forwarded_write
from canopy.core.replication.publisher import publish
from canopy.core.replication.subscriber import (
    grants,
    subscribe,
    applied_lsn,
    subscriber_status,
)

from .conftest import (
    PUBLISHER_ADMIN,
    SUBSCRIBER_ADMIN,
    eventually,
    server_dsn,
    requires_pair,
)
from .test_replication_e2e import pair  # noqa: F401 — the two-server fixture
from .test_replication_e2e import Pair, _copied

pytestmark = requires_pair


async def _forwarded_write(pub_dsn: str, group_id: str) -> str | None:
    """Insert a tag group through the app's request session, as a trusted forwarded request."""
    engine = create_async_engine(
        make_url(pub_dsn).set(drivername="postgresql+asyncpg").render_as_string(hide_password=False),
        poolclass=NullPool,
    )
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False, autoflush=False)
    write = ForwardedWrite(on_behalf_of="operator@site_a")
    token = forwarded_write.set(write)
    original = db.get_session_maker
    db.get_session_maker = lambda: maker
    try:
        sessions = db.get_session()
        session = await anext(sessions)
        await session.execute(
            text("INSERT INTO squirrel.tag_group (id, name) VALUES (:id, 'Forwarded')"), {"id": group_id}
        )
        with pytest.raises(StopAsyncIteration):
            await anext(sessions)
    finally:
        db.get_session_maker = original
        forwarded_write.reset(token)
        await engine.dispose()
    return write.commit_lsn


async def test_the_mirror_reports_a_forwarded_write_applied(pair: Pair, monkeypatch):  # noqa: F811
    p = pair
    await publish(p.pub, role="primary", config=p.config)
    await grants(p.sub, role="forward", config=p.config)
    await subscribe(p.sub, role="forward", instance=p.instance, config=p.config)
    await eventually(lambda: _copied(p))

    group_id = str(uuid4())
    pub_db = await p.pub.fetchval("SELECT current_database()")
    commit_lsn = await _forwarded_write(server_dsn(PUBLISHER_ADMIN, database=pub_db), group_id)
    assert commit_lsn is not None

    # The forward instance's API, as its app role.
    app_password = uuid4().hex
    await p.sub_admin.execute(f"ALTER ROLE \"{p.config.app_role}\" PASSWORD '{app_password}'")
    sub_db = await p.sub.fetchval("SELECT current_database()")
    monkeypatch.setenv(
        "CANOPY_DATABASE_URL",
        make_url(server_dsn(SUBSCRIBER_ADMIN, database=sub_db, user=p.config.app_role, password=app_password))
        .set(drivername="postgresql+asyncpg")
        .render_as_string(hide_password=False),
    )
    monkeypatch.setenv("CANOPY_INSTANCE", p.instance)
    get_settings.cache_clear()
    await db.dispose_engine()
    try:
        # Without the grants the position can't be read, so the answer is "stale", not an error.
        assert await wait_until_applied(commit_lsn, 0.5) is False

        await p.sub_admin.execute(f'GRANT SELECT ON pg_replication_origin_status TO "{p.config.app_role}"')
        await p.sub_admin.execute(
            f'GRANT EXECUTE ON FUNCTION pg_show_replication_origin_status() TO "{p.config.app_role}"'
        )
        assert await wait_until_applied(commit_lsn, 30.0) is True
    finally:
        await db.dispose_engine()
        get_settings.cache_clear()

    assert await p.sub.fetchval("SELECT name FROM squirrel.tag_group WHERE id = $1", group_id) == "Forwarded"
    applied = await applied_lsn(p.sub_admin, f"canopy_history_{p.instance}")
    assert lsn_value(applied) >= lsn_value(commit_lsn)
    status = await subscriber_status(p.sub_admin, instance=p.instance)
    assert status["subscription"]["applied_lsn"] == applied
