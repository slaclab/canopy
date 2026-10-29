"""The switch to the split layout, the heartbeat and pruning, on a primary and its mirror.

Opt-in like ``test_replication_e2e.py``, whose ``pair`` fixture this reuses. The
mirror-side ``switch`` and ``heartbeat`` reach the primary as an ordinary login role,
the way a deployment's heartbeat role would, so the test also shows that role can
read what ``switch`` needs.
"""

from uuid import uuid4
from datetime import UTC, datetime, timedelta

import pytest
import asyncpg
import pytest_asyncio
from pydantic import SecretStr
from sqlalchemy.pool import NullPool
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from canopy.squirrel.prune import PRUNED_TABLES, prune
from canopy.core.replication.sql import Refused
from canopy.core.replication.gate import check_prune_gate
from canopy.core.replication.config import PruneConfig
from canopy.core.replication.publisher import publish, publisher_status
from canopy.core.replication.subscriber import (
    grants,
    switch,
    heartbeat,
    subscribe,
    subscriber_status,
)

from .conftest import drop_roles, server_dsn
from .test_replication_e2e import (  # noqa: F401 — pair is a fixture
    PUBLISHER_ADMIN,
    Pair,
    pair,
    _copied,
    eventually,
    pytestmark,
)

WINDOW_DAYS = 30
CONFIG = PruneConfig(dry_run=False)


@pytest_asyncio.fixture
async def split_pair(pair: Pair):  # noqa: F811 — the imported pair fixture, by name
    """``pair``, plus a heartbeat login role on the primary that the mirror-side commands use."""
    role, password = f"rt_heartbeat_{uuid4().hex[:8]}", uuid4().hex
    database = await pair.pub.fetchval("SELECT current_database()")
    await pair.pub.execute(f"CREATE ROLE \"{role}\" LOGIN PASSWORD '{password}'")
    pair.config = pair.config.model_copy(
        update={
            "heartbeat_role": role,
            "heartbeat_dsn": SecretStr(server_dsn(PUBLISHER_ADMIN, database=database, user=role, password=password)),
        }
    )
    try:
        yield pair
    finally:
        # The role's grants live in the primary's test database, which pair drops later.
        await pair.pub.execute(f'DROP OWNED BY "{role}"')
        server = await asyncpg.connect(PUBLISHER_ADMIN)
        try:
            await drop_roles(server, [role])
        finally:
            await server.close()


async def _snapshot(conn, title: str, *, age_days: int = 0, retain: bool = False) -> str:
    snapshot_id = str(uuid4())
    await conn.execute(
        "INSERT INTO squirrel.snapshot (id, title, created_at, retain) VALUES ($1, $2, $3, $4)",
        snapshot_id,
        title,
        datetime.now(UTC) - timedelta(days=age_days),
        retain,
    )
    await conn.execute(
        "INSERT INTO squirrel.snapshot_value (id, snapshot_id, pv_id, pv_name) "
        "SELECT gen_random_uuid(), $1, id, setpoint_address FROM squirrel.pv",
        snapshot_id,
    )
    return snapshot_id


async def _has(conn, table: str, column: str, value: str) -> bool:
    return await conn.fetchval(f"SELECT EXISTS (SELECT 1 FROM {table} WHERE {column} = $1)", value)


async def _gate(p: Pair):
    return await check_prune_gate(
        p.pub, instance=p.instance, window_days=WINDOW_DAYS, tables=PRUNED_TABLES, config=CONFIG, replication=p.config
    )


async def _after_split(p: Pair) -> dict | None:
    """``publish --split`` once the mirror's new walsender is visible, else ``None`` to retry."""
    try:
        return await publish(p.pub, role="primary", config=p.config, split=True)
    except Refused:
        return None


async def test_split_switch_heartbeat_and_prune(split_pair: Pair):
    p = split_pair
    await publish(p.pub, role="primary", config=p.config)
    pv, tag_group, tag = str(uuid4()), str(uuid4()), str(uuid4())
    await p.pub.execute("INSERT INTO squirrel.pv (id, setpoint_address) VALUES ($1, 'SIM:PV:1:SETPOINT')", pv)
    await p.pub.execute("INSERT INTO squirrel.tag_group (id, name) VALUES ($1, 'Area')", tag_group)
    await p.pub.execute("INSERT INTO squirrel.tag (id, name, group_id) VALUES ($1, 'Injector', $2)", tag, tag_group)
    await p.pub.execute("INSERT INTO squirrel.pv_tag (pv_id, tag_id) VALUES ($1, $2)", pv, tag)
    old = await _snapshot(p.pub, "old", age_days=40)
    kept = await _snapshot(p.pub, "old but retained", age_days=50, retain=True)
    recent = await _snapshot(p.pub, "recent", age_days=1)
    await grants(p.sub, role="mirror", config=p.config)
    await subscribe(p.sub, role="mirror", instance=p.instance, config=p.config)
    await eventually(lambda: _copied(p))

    # Single layout: canopy_history publishes deletes of the pruned tables, so the
    # gate refuses whatever else holds.
    assert "no_delete_publication" in (await _gate(p)).failures

    # Step 1: the split pair appears beside canopy_history; the mirror hasn't moved yet.
    first = await publish(p.pub, role="primary", config=p.config, split=True)
    assert first["layout"] == "switching"
    with pytest.raises(Refused, match="still streams canopy_history"):
        await publish(p.pub, role="primary", config=p.config, split=True)
    assert "no_delete_publication" in (await _gate(p)).failures

    # Step 2: the mirror switches, reading its slot's position as the heartbeat role.
    switched = await switch(p.sub, role="mirror", instance=p.instance, config=p.config, timeout=30)
    assert switched["switched"] is True
    assert (await switch(p.sub, role="mirror", instance=p.instance, config=p.config))["switched"] is False

    # Step 3: once the mirror's new walsender is up, canopy_history goes.
    done = await eventually(lambda: _after_split(p))
    assert done["layout"] == "split" and done["publications"]["canopy_history"] == {"dropped": True}
    mirror = (await subscriber_status(p.sub_admin, instance=p.instance))["subscription"]
    assert mirror["layout"] == "split" and mirror["expected_publications"] == ["canopy_ref", "canopy_ts"]

    # Replication carries on: a snapshot DELETE no longer reaches history, a link-row
    # delete still does, and an insert behind them shows when both have been applied.
    probe = await _snapshot(p.pub, "probe")
    await eventually(lambda: _has(p.sub, "squirrel.snapshot", "id", probe))
    await p.pub.execute("DELETE FROM squirrel.snapshot WHERE id = $1", probe)
    await p.pub.execute("DELETE FROM squirrel.pv_tag WHERE pv_id = $1", pv)
    marker = await _snapshot(p.pub, "marker")
    await eventually(lambda: _has(p.sub, "squirrel.snapshot", "id", marker))
    assert await _has(p.sub, "squirrel.snapshot", "id", probe)
    assert not await _has(p.sub, "squirrel.pv_tag", "pv_id", pv)

    # Without a heartbeat the gate still refuses; a failed check is named.
    assert set((await _gate(p)).failures) == {"heartbeat_fresh"}
    await heartbeat(
        p.sub, role="mirror", instance=p.instance, config=p.config, backup_ok=True, wal_archive_ok=True, drift_ok=False
    )
    assert set((await _gate(p)).failures) == {"heartbeat_ok"}
    beat = await heartbeat(
        p.sub, role="mirror", instance=p.instance, config=p.config, backup_ok=True, wal_archive_ok=True, drift_ok=True
    )
    assert beat["name"] == f"canopy_history_{p.instance}"
    assert await eventually(lambda: _passes(p))

    # Prune on the primary: a dry run first, then the real thing.
    url = make_url(server_dsn(PUBLISHER_ADMIN, database=await p.pub.fetchval("SELECT current_database()")))
    engine = create_async_engine(url.set(drivername="postgresql+asyncpg"), poolclass=NullPool)
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        dry = await prune(maker, window_days=WINDOW_DAYS, config=PruneConfig(), check_gate=lambda: _gate(p))
        assert (dry["snapshots"], dry["values"], dry["gate"]) == (1, 1, {})
        assert await _has(p.pub, "squirrel.snapshot", "id", old)

        pruned = await prune(maker, window_days=WINDOW_DAYS, config=CONFIG, check_gate=lambda: _gate(p))
        assert (pruned["snapshots"], pruned["values"], pruned["complete"]) == (1, 1, True)
    finally:
        await engine.dispose()

    assert not await _has(p.pub, "squirrel.snapshot", "id", old)
    assert not await _has(p.pub, "squirrel.snapshot_value", "snapshot_id", old)
    assert await _has(p.pub, "squirrel.snapshot", "id", kept) and await _has(p.pub, "squirrel.snapshot", "id", recent)
    after = await _snapshot(p.pub, "after prune")
    await eventually(lambda: _has(p.sub, "squirrel.snapshot", "id", after))
    assert await _has(p.sub, "squirrel.snapshot", "id", old)
    assert await _has(p.sub, "squirrel.snapshot_value", "snapshot_id", old)

    status = await publisher_status(p.pub, config=p.config)
    assert status["layout"] == "split"
    slot = next(s for s in status["slots"] if s["slot_name"] == f"canopy_history_{p.instance}")
    assert slot["walsender_publications"] == ["canopy_ref", "canopy_ts"]
    mirror = (await subscriber_status(p.sub_admin, instance=p.instance))["subscription"]
    assert mirror["apply_error_count"] == 0


async def _passes(p: Pair) -> bool:
    # Right after the walsender restarts its reply age is briefly unknown.
    return (await _gate(p)).ok


async def test_switch_refuses_before_the_primary_has_the_split_pair(split_pair: Pair):
    p = split_pair
    await publish(p.pub, role="primary", config=p.config)
    await grants(p.sub, role="mirror", config=p.config)
    await subscribe(p.sub, role="mirror", instance=p.instance, config=p.config)

    with pytest.raises(Refused, match="has no canopy_ref, canopy_ts yet; run `publish --split` there first"):
        await switch(p.sub, role="mirror", instance=p.instance, config=p.config, timeout=5)

    await p.sub_admin.execute(f'ALTER SUBSCRIPTION "canopy_history_{p.instance}" DISABLE')
    with pytest.raises(Refused, match="is disabled"):
        await switch(p.sub, role="mirror", instance=p.instance, config=p.config, timeout=5)
