"""The switch to the split layout, the heartbeat and pruning, on one arm of a head with two.

Opt-in like ``test_replication_e2e.py``: the ``octopus`` fixture skips unless the
``CANOPY_TEST_REPLICATION_*`` variables point at the servers. The head-side ``switch``
and ``heartbeat`` reach an arm as an ordinary login role, the way a deployment's
heartbeat role would, so the tests also show that role can read what ``switch`` needs.
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

from canopy.core.arms import Arm, ArmsConfig, subscription_name
from canopy.squirrel.prune import PRUNED_TABLES, prune
from canopy.core.replication.sql import Refused
from canopy.core.replication.gate import check_prune_gate
from canopy.core.replication.drift import SESSION
from canopy.core.replication.config import PruneConfig
from canopy.core.replication.reseed import reseed
from canopy.core.replication.publisher import publish, publisher_status
from canopy.core.replication.subscriber import (
    grants,
    switch,
    heartbeat,
    subscribe,
    subscriber_status,
)

from .conftest import (
    HEAD_SERVER,
    ArmSide,
    Octopus,
    copied,
    seed_rows,
    drop_roles,
    eventually,
    server_dsn,
    _reader_dsn,
)

WINDOW_DAYS = 30
CONFIG = PruneConfig(dry_run=False)
one_arm = pytest.mark.parametrize("octopus", [1], indirect=True)


@pytest_asyncio.fixture
async def split_octopus(octopus: Octopus):
    """``octopus``, plus a heartbeat login role on every arm that the head-side commands use."""
    role, password = f"rt_heartbeat_{uuid4().hex[:8]}", uuid4().hex
    arms = {}
    for name, side in octopus.arms.items():
        await side.admin.execute(f"CREATE ROLE \"{role}\" LOGIN PASSWORD '{password}'")
        database = make_url(side.dsn).database
        arms[name] = Arm(
            publisher_dsn=SecretStr(side.publisher_dsn),
            heartbeat_dsn=SecretStr(server_dsn(side.dsn, database=database, user=role, password=password)),
        )
    octopus.config = octopus.config.model_copy(update={"heartbeat_role": role})
    octopus.arms_config = ArmsConfig(arms=arms)
    try:
        yield octopus
    finally:
        for side in octopus.arms.values():
            # The role's grants live in the arm's test database, which octopus drops later.
            await side.admin.execute(f'DROP OWNED BY "{role}"')
            server = await asyncpg.connect(server_dsn(side.dsn, database="postgres"))
            try:
                await drop_roles(server, [role])
            finally:
                await server.close()


async def _snapshot(side: ArmSide, title: str, *, age_days: int = 0, retain: bool = False) -> str:
    snapshot_id = str(uuid4())
    await side.admin.execute(
        "INSERT INTO squirrel.snapshot (id, title, created_at, retain, owner_instance) VALUES ($1, $2, $3, $4, $5)",
        snapshot_id,
        title,
        datetime.now(UTC) - timedelta(days=age_days),
        retain,
        side.name,
    )
    await side.admin.execute(
        "INSERT INTO squirrel.snapshot_value (id, snapshot_id, pv_id, pv_name) "
        "SELECT gen_random_uuid(), $1, id, setpoint_address FROM squirrel.pv",
        snapshot_id,
    )
    return snapshot_id


async def _has(conn, table: str, column: str, value: str) -> bool:
    return await conn.fetchval(f"SELECT EXISTS (SELECT 1 FROM {table} WHERE {column} = $1)", value)


async def _gate(o: Octopus, side: ArmSide):
    return await check_prune_gate(
        side.admin,
        instance=side.name,
        window_days=WINDOW_DAYS,
        tables=PRUNED_TABLES,
        config=CONFIG,
        replication=o.config,
    )


async def _after_split(o: Octopus, side: ArmSide) -> dict | None:
    """``publish --split`` on ``side`` once the head's new walsender is visible, else ``None`` to retry."""
    try:
        return await publish(side.admin, role="primary", config=o.config, split=True)
    except Refused:
        return None


async def _status(o: Octopus, arm: str) -> dict:
    return (await subscriber_status(o.head.admin, arms=o.arms_config.arms, only=arm))["subscriptions"][0]


async def _beat(o: Octopus, arm: str, *, drift_ok: bool = True) -> dict:
    return await heartbeat(
        o.head.conn,
        role="mirror",
        arm=arm,
        arms=o.arms_config.arms,
        backup_ok=True,
        wal_archive_ok=True,
        drift_ok=drift_ok,
    )


async def _subscribe_every_arm(o: Octopus) -> None:
    await grants(o.head.conn, role="mirror", head=o.head.name, config=o.config)
    await subscribe(o.head.conn, role="mirror", head=o.head.name, arms=o.arms_config.arms, connect_arm=o.connect_arm)
    for name in o.arms:
        await eventually(lambda name=name: copied(o, name))


async def test_one_arm_splits_and_prunes_while_the_other_stays_single(split_octopus: Octopus):
    o = split_octopus
    arm1, arm2 = o.arms.values()
    for side in (arm1, arm2):
        await publish(side.admin, role="primary", config=o.config)
    other = await seed_rows(arm2.admin, arm2.name)
    pv, tag_group, tag = str(uuid4()), str(uuid4()), str(uuid4())
    await arm1.admin.execute(
        "INSERT INTO squirrel.pv (id, setpoint_address, owner_instance) VALUES ($1, 'SIM:PV:1:SETPOINT', $2)",
        pv,
        arm1.name,
    )
    await arm1.admin.execute(
        "INSERT INTO squirrel.tag_group (id, name, owner_instance) VALUES ($1, 'Area', $2)", tag_group, arm1.name
    )
    await arm1.admin.execute(
        "INSERT INTO squirrel.tag (id, name, group_id) VALUES ($1, 'Injector', $2)", tag, tag_group
    )
    await arm1.admin.execute("INSERT INTO squirrel.pv_tag (pv_id, tag_id) VALUES ($1, $2)", pv, tag)
    old = await _snapshot(arm1, "old", age_days=40)
    kept = await _snapshot(arm1, "old but retained", age_days=50, retain=True)
    recent = await _snapshot(arm1, "recent", age_days=1)
    await _subscribe_every_arm(o)

    # Single layout: canopy_history publishes deletes of the pruned tables, so arm1's
    # gate refuses whatever else holds.
    assert "no_delete_publication" in (await _gate(o, arm1)).failures

    # Step 1: arm1's split pair appears beside canopy_history; the head hasn't moved yet.
    first = await publish(arm1.admin, role="primary", config=o.config, split=True)
    assert first["layout"] == "switching"
    with pytest.raises(Refused, match="streams canopy_history, not exactly canopy_ref, canopy_ts"):
        await publish(arm1.admin, role="primary", config=o.config, split=True)

    # Step 2: the head switches its subscription to arm1 only, reading the slot's
    # position as arm1's heartbeat role.
    switched = await switch(o.head.conn, role="mirror", arm=arm1.name, arms=o.arms_config.arms, timeout=30)
    assert switched["switched"] is True
    again = await switch(o.head.conn, role="mirror", arm=arm1.name, arms=o.arms_config.arms)
    assert again["switched"] is False

    # Step 3: once the head's new walsender is up on arm1, canopy_history goes there.
    done = await eventually(lambda: _after_split(o, arm1))
    assert done["layout"] == "split" and done["publications"]["canopy_history"] == {"dropped": True}
    split, single = await _status(o, arm1.name), await _status(o, arm2.name)
    assert split["layout"] == "split" and split["expected_publications"] == ["canopy_ref", "canopy_ts"]
    assert single["layout"] == "single" and single["publications"] == ["canopy_history"]

    # Replication from arm1 carries on: a snapshot DELETE no longer reaches the head, a
    # link-row delete still does, and an insert behind them shows when both have been applied.
    probe = await _snapshot(arm1, "probe")
    await eventually(lambda: _has(o.head.conn, "squirrel.snapshot", "id", probe))
    await arm1.admin.execute("DELETE FROM squirrel.snapshot WHERE id = $1", probe)
    await arm1.admin.execute("DELETE FROM squirrel.pv_tag WHERE pv_id = $1", pv)
    marker = await _snapshot(arm1, "marker")
    await eventually(lambda: _has(o.head.conn, "squirrel.snapshot", "id", marker))
    assert await _has(o.head.conn, "squirrel.snapshot", "id", probe)
    assert not await _has(o.head.conn, "squirrel.pv_tag", "pv_id", pv)

    # Without a heartbeat for arm1 the gate still refuses; a failed check is named.
    # arm2's heartbeat goes to arm2 and does nothing for arm1.
    assert set((await _gate(o, arm1)).failures) == {"heartbeat_fresh"}
    assert (await _beat(o, arm2.name))["name"] == subscription_name(arm2.name)
    assert set((await _gate(o, arm1)).failures) == {"heartbeat_fresh"}
    await _beat(o, arm1.name, drift_ok=False)
    assert set((await _gate(o, arm1)).failures) == {"heartbeat_ok"}
    assert (await _beat(o, arm1.name))["name"] == subscription_name(arm1.name)
    assert await eventually(lambda: _passes(o, arm1))

    # A reconnected stream has a new walsender, and the heartbeat from before it no
    # longer vouches for arm1 until the head reports again.
    name = subscription_name(arm1.name)
    await o.head.admin.execute(f'ALTER SUBSCRIPTION "{name}" DISABLE')
    await eventually(lambda: _slot_inactive(arm1, name))
    await o.head.admin.execute(f'ALTER SUBSCRIPTION "{name}" ENABLE')
    assert await eventually(lambda: _refused_only_for(o, arm1, "heartbeat_fresh"))
    await _beat(o, arm1.name)
    assert await eventually(lambda: _passes(o, arm1))

    # Prune on arm1: a dry run first, then the real thing.
    url = make_url(arm1.dsn).set(drivername="postgresql+asyncpg")
    engine = create_async_engine(url, poolclass=NullPool)
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        dry = await prune(maker, window_days=WINDOW_DAYS, config=PruneConfig(), check_gate=lambda: _gate(o, arm1))
        assert (dry["snapshots"], dry["values"], dry["gate"]) == (1, 1, {})
        assert await _has(arm1.admin, "squirrel.snapshot", "id", old)

        pruned = await prune(maker, window_days=WINDOW_DAYS, config=CONFIG, check_gate=lambda: _gate(o, arm1))
        assert (pruned["snapshots"], pruned["values"], pruned["complete"]) == (1, 1, True)
    finally:
        await engine.dispose()

    assert not await _has(arm1.admin, "squirrel.snapshot", "id", old)
    assert not await _has(arm1.admin, "squirrel.snapshot_value", "snapshot_id", old)
    assert await _has(arm1.admin, "squirrel.snapshot", "id", kept)
    assert await _has(arm1.admin, "squirrel.snapshot", "id", recent)
    after = await _snapshot(arm1, "after prune")
    await eventually(lambda: _has(o.head.conn, "squirrel.snapshot", "id", after))
    assert await _has(o.head.conn, "squirrel.snapshot", "id", old)
    assert await _has(o.head.conn, "squirrel.snapshot_value", "snapshot_id", old)

    # arm2 never moved: its rows on the head are untouched and its single layout still streams.
    assert await _has(o.head.conn, "squirrel.snapshot", "id", other["snapshot"])
    late = await _snapshot(arm2, "arm2 after arm1's prune")
    await eventually(lambda: _has(o.head.conn, "squirrel.snapshot", "id", late))

    status = await publisher_status(arm1.admin, config=o.config)
    assert status["layout"] == "split"
    slot = next(s for s in status["slots"] if s["slot_name"] == name)
    assert slot["walsender_publications"] == ["canopy_ref", "canopy_ts"]
    assert (await _status(o, arm1.name))["apply_error_count"] == 0
    assert (await _status(o, arm2.name))["apply_error_count"] == 0


async def _slot_inactive(side: ArmSide, name: str) -> bool:
    return not await side.admin.fetchval("SELECT active FROM pg_replication_slots WHERE slot_name = $1", name)


async def _refused_only_for(o: Octopus, side: ArmSide, check: str) -> bool:
    return set((await _gate(o, side)).failures) == {check}


async def _passes(o: Octopus, side: ArmSide) -> bool:
    # Right after the walsender restarts its reply age is briefly unknown.
    return (await _gate(o, side)).ok


@one_arm
async def test_switch_refuses_before_the_arm_has_the_split_pair(split_octopus: Octopus):
    o, arm = split_octopus, split_octopus.arm
    await publish(arm.admin, role="primary", config=o.config)
    await _subscribe_every_arm(o)

    with pytest.raises(Refused, match="has no canopy_ref, canopy_ts yet; run `publish --split` there first"):
        await switch(o.head.conn, role="mirror", arm=arm.name, arms=o.arms_config.arms, timeout=5)

    await o.head.admin.execute(f'ALTER SUBSCRIPTION "{subscription_name(arm.name)}" DISABLE')
    with pytest.raises(Refused, match="is disabled"):
        await switch(o.head.conn, role="mirror", arm=arm.name, arms=o.arms_config.arms, timeout=5)


@one_arm
async def test_reseed_subscribes_the_head_to_a_split_arms_pair(split_octopus: Octopus):
    o, arm = split_octopus, split_octopus.arm
    # An arm created after its switch, as when the head's copy is rebuilt for it: the pair only.
    await publish(arm.admin, role="primary", config=o.config, split=True)
    await grants(o.head.conn, role="mirror", head=o.head.name, config=o.config)
    await arm.admin.execute(
        "INSERT INTO squirrel.pv (id, setpoint_address, owner_instance) VALUES ($1, 'SIM:PV:1:SETPOINT', $2)",
        str(uuid4()),
        arm.name,
    )
    before = await _snapshot(arm, "before the re-seed")

    # Re-seed runs as a superuser on the head, since it advances the subscription's origin.
    primary = await asyncpg.connect(await _reader_dsn(o, arm.name), server_settings=SESSION)
    head = await asyncpg.connect(
        server_dsn(HEAD_SERVER, database=make_url(o.head.dsn).database), server_settings=SESSION
    )
    try:
        report = await reseed(
            primary,
            head,
            role="mirror",
            arm=arm.name,
            arms=o.arms_config.arms,
            head=o.head.name,
            since=datetime(2000, 1, 1, tzinfo=UTC),
        )
    finally:
        await primary.close()
        await head.close()

    assert report["created"] is True and report["publications"] == ["canopy_ref", "canopy_ts"]
    assert await _has(o.head.conn, "squirrel.snapshot", "id", before)
    status = await _status(o, arm.name)
    assert status["enabled"] and status["layout"] == "split"
    # The pair's rule holds from the first transaction after the cut.
    await arm.admin.execute("DELETE FROM squirrel.snapshot WHERE id = $1", before)
    marker = await _snapshot(arm, "marker")
    await eventually(lambda: _has(o.head.conn, "squirrel.snapshot", "id", marker))
    assert await _has(o.head.conn, "squirrel.snapshot", "id", before)
