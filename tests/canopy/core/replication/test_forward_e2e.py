"""Read-your-writes for a head's forwarded writes, on two arms and a head (Postgres 16).

The position an arm reports for a forwarded write (``core.db.get_session``,
``X-Canopy-Commit-LSN``) must be one the head's subscription to that arm reaches once
it has applied the write, read by the head's non-superuser app role with the two
grants the docs name. Opt-in like the other end-to-end tests (the ``octopus`` fixture).
"""

from uuid import uuid4
from datetime import UTC, datetime

import pytest
from sqlalchemy import text
from sqlalchemy.pool import NullPool
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from canopy.core import db
from canopy.core.arms import subscription_name
from canopy.core.config import get_settings
from canopy.core.forwarding.client import lsn_value, wait_until_applied
from canopy.core.forwarding.context import ForwardedWrite, forwarded_write
from canopy.squirrel.schemas.snapshot import BundleValueDTO, SnapshotBundleDTO
from canopy.core.replication.subscriber import applied_lsn
from canopy.squirrel.services.snapshot_service import SnapshotService

from .conftest import Octopus, eventually, server_dsn, _subscribed


async def _forwarded_write(arm_dsn: str, *statements: tuple[str, dict]) -> str | None:
    """Run ``statements`` through the arm's request session, as a trusted forwarded request."""
    engine = create_async_engine(
        make_url(arm_dsn).set(drivername="postgresql+asyncpg").render_as_string(hide_password=False),
        poolclass=NullPool,
    )
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False, autoflush=False)
    write = ForwardedWrite(on_behalf_of="operator@head")
    token = forwarded_write.set(write)
    original = db.get_session_maker
    db.get_session_maker = lambda: maker
    try:
        sessions = db.get_session()
        session = await anext(sessions)
        for sql, params in statements:
            await session.execute(text(sql), params)
        with pytest.raises(StopAsyncIteration):
            await anext(sessions)
    finally:
        db.get_session_maker = original
        forwarded_write.reset(token)
        await engine.dispose()
    return write.commit_lsn


@pytest.fixture
async def head_app(octopus: Octopus, monkeypatch):
    """The head's API database connection, as its app role, the way the API would open it."""
    head = octopus.head
    password = uuid4().hex
    await head.admin.execute(f"ALTER ROLE \"{head.app}\" PASSWORD '{password}'")
    database = make_url(head.dsn).database
    monkeypatch.setenv(
        "CANOPY_DATABASE_URL",
        make_url(server_dsn(head.dsn, database=database, user=head.app, password=password))
        .set(drivername="postgresql+asyncpg")
        .render_as_string(hide_password=False),
    )
    monkeypatch.setenv("CANOPY_ROLE", "mirror")
    monkeypatch.setenv("CANOPY_INSTANCE", head.name)
    get_settings.cache_clear()
    await db.dispose_engine()
    yield
    await db.dispose_engine()
    get_settings.cache_clear()


async def test_a_head_waits_on_the_arm_that_took_the_write(octopus: Octopus, head_app):
    arm1, arm2 = octopus.arms.values()
    arm1_ids = await _subscribed(octopus, arm1.name)
    await _subscribed(octopus, arm2.name)
    head = octopus.head

    # A PV edit and a snapshot delete the head forwarded to arm1, saved there.
    commit_lsn = await _forwarded_write(
        arm1.dsn,
        ("UPDATE squirrel.pv SET description = 'forwarded' WHERE id = CAST(:id AS uuid)", {"id": arm1_ids["pv"]}),
        (
            "UPDATE squirrel.snapshot SET deleted_at = now() WHERE id = CAST(:id AS uuid)",
            {"id": arm1_ids["snapshot"]},
        ),
    )
    assert commit_lsn is not None
    arm1_sub, arm2_sub = subscription_name(arm1.name), subscription_name(arm2.name)

    # Without the grants the position can't be read, so the answer is "stale", not an error.
    assert await wait_until_applied(commit_lsn, 0.5, subscription=arm1_sub) is False

    await head.admin.execute(f'GRANT SELECT ON pg_replication_origin_status TO "{head.app}"')
    await head.admin.execute(f'GRANT EXECUTE ON FUNCTION pg_show_replication_origin_status() TO "{head.app}"')
    assert await wait_until_applied(commit_lsn, 30.0, subscription=arm1_sub) is True

    # A wait cut short by its timeout hands its connection back to the pool in working
    # order: the next wait, on the same pool, still reads the position.
    assert await wait_until_applied("FFFFFFFF/0", 0.3, subscription=arm1_sub) is False
    assert await wait_until_applied(commit_lsn, 5.0, subscription=arm1_sub) is True

    pv = await head.conn.fetchrow("SELECT description, owner_instance FROM squirrel.pv WHERE id = $1", arm1_ids["pv"])
    assert (pv["description"], pv["owner_instance"]) == ("forwarded", arm1.name)
    assert await head.conn.fetchval(
        "SELECT deleted_at IS NOT NULL FROM squirrel.snapshot WHERE id = $1", arm1_ids["snapshot"]
    )
    assert lsn_value(await applied_lsn(head.admin, arm1_sub)) >= lsn_value(commit_lsn)

    # arm2's subscription is its own: it never carried the write, and still streams.
    assert await arm2.admin.fetchval("SELECT count(*) FROM squirrel.pv WHERE description = 'forwarded'") == 0
    assert await applied_lsn(head.admin, arm2_sub) is not None


async def test_the_heads_own_rows_stay_on_the_head(octopus: Octopus, head_app):
    arm1, arm2 = octopus.arms.values()
    await _subscribed(octopus, arm1.name)
    head = octopus.head
    group_id = str(uuid4())

    async with db.session_scope() as session:
        await session.execute(
            text(
                "INSERT INTO squirrel.tag_group (id, name, owner_instance) VALUES (CAST(:id AS uuid), 'Head', :owner)"
            ),
            {"id": group_id, "owner": head.name},
        )
        await session.commit()

    async def landed():
        return await head.conn.fetchval("SELECT owner_instance FROM squirrel.tag_group WHERE id = $1", group_id)

    assert await eventually(landed) == head.name
    for arm in (arm1, arm2):
        assert await arm.admin.fetchval("SELECT count(*) FROM squirrel.tag_group WHERE id = $1", group_id) == 0


async def _captured_import(
    octopus: Octopus, arm: str, bundle: SnapshotBundleDTO, monkeypatch, *, strict: bool = False
) -> tuple[bool, str]:
    """Save ``bundle`` on ``arm`` the way its captured import does, as a forwarded request.

    Returns whether it saved anything, and the position the arm would answer with. A
    refusal is raised as the service raises it, with nothing saved.
    """
    monkeypatch.setenv("CANOPY_INSTANCE", arm)
    get_settings.cache_clear()
    engine = create_async_engine(
        make_url(octopus.arms[arm].dsn).set(drivername="postgresql+asyncpg").render_as_string(hide_password=False),
        poolclass=NullPool,
    )
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False, autoflush=False)
    write = ForwardedWrite(on_behalf_of="operator@head", report_position=True)
    token = forwarded_write.set(write)
    original = db.get_session_maker
    db.get_session_maker = lambda: maker
    try:
        sessions = db.get_session()
        session = await anext(sessions)
        try:
            result = await SnapshotService(session, None).import_bundle(
                bundle, captured=True, created_by="operator@head", strict=strict
            )
        except Exception:
            await sessions.aclose()  # rolls the arm's transaction back
            raise
        with pytest.raises(StopAsyncIteration):
            await anext(sessions)
    finally:
        db.get_session_maker = original
        forwarded_write.reset(token)
        await engine.dispose()
        monkeypatch.setenv("CANOPY_INSTANCE", octopus.head.name)
        get_settings.cache_clear()
    return result.created, write.commit_lsn


async def test_a_snapshot_taken_on_the_head_comes_back_from_its_arm(octopus: Octopus, head_app, monkeypatch):
    arm1, _ = octopus.arms.values()
    ids = await _subscribed(octopus, arm1.name)
    head = octopus.head
    await head.admin.execute(f'GRANT SELECT ON pg_replication_origin_status TO "{head.app}"')
    await head.admin.execute(f'GRANT EXECUTE ON FUNCTION pg_show_replication_origin_status() TO "{head.app}"')
    snapshot_id = str(uuid4())
    bundle = SnapshotBundleDTO(
        format="canopy.squirrel.snapshot-bundle",
        version=1,
        source_instance=head.name,
        id=snapshot_id,
        title="Taken on the head",
        created_date=datetime(2026, 11, 6, 14, 2, tzinfo=UTC),
        facility=f"fac_{arm1.name}",
        values=[BundleValueDTO(pv_id=ids["pv"], pv_name="SIM:PV:1:SETPOINT", setpoint_value={"value": 2.0})],
    )
    subscription = subscription_name(arm1.name)

    async def is_here(conn) -> bool:
        return bool(await conn.fetchval("SELECT 1 FROM squirrel.snapshot WHERE id = $1::uuid", snapshot_id))

    created, position = await _captured_import(octopus, arm1.name, bundle, monkeypatch)
    assert created and position
    assert await wait_until_applied(position, 30.0, subscription=subscription, until=is_here) is True
    row = await head.conn.fetchrow(
        "SELECT owner_instance, created_by, created_at FROM squirrel.snapshot WHERE id = $1", snapshot_id
    )
    assert (row["owner_instance"], row["created_by"]) == (arm1.name, "operator@head")
    assert row["created_at"] == bundle.created_date

    # A resend saves nothing, and still answers with a position: the arm's current one.
    again, replay_position = await _captured_import(octopus, arm1.name, bundle, monkeypatch)
    assert not again
    assert lsn_value(replay_position) >= lsn_value(position)
    # The snapshot is already here, so the wait ends at once even if unpublished WAL
    # keeps the position ahead of anything that replicates.
    assert await wait_until_applied(replay_position, 5.0, subscription=subscription, until=is_here) is True
    assert await arm1.admin.fetchval("SELECT count(*) FROM squirrel.snapshot WHERE id = $1", snapshot_id) == 1
