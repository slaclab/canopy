"""``reseed`` end to end: one arm's gap refilled on the head, and the hand-over back to replication.

Opt-in like ``test_replication_e2e.py``: the ``octopus`` fixture skips unless the
``CANOPY_TEST_REPLICATION_*`` variables point at the servers.
"""

import os
import sys
import json
import asyncio
import subprocess
from uuid import uuid4
from datetime import datetime, timedelta

import pytest
import asyncpg
from sqlalchemy.engine import make_url

import canopy.core.replication.reseed as reseed_module
from canopy.core.arms import subscription_name
from canopy.core.replication.sql import Refused
from canopy.core.replication.drift import SESSION, drift_check
from canopy.core.replication.reseed import reseed
from canopy.core.replication.registry import REGISTRY
from canopy.core.replication.subscriber import subscriber_status

from .conftest import (
    HEAD_SERVER,
    Octopus,
    eventually,
    server_dsn,
    _reader_dsn,
    _subscribed,
)
from .test_replication_e2e import _rows, _in_step

PRESENT = sorted(REGISTRY.replicated)
one_arm = pytest.mark.parametrize("octopus", [1], indirect=True)


def _head_admin_dsn(o: Octopus) -> str:
    """The head's database as a superuser, which ``reseed`` runs as (it advances an origin)."""
    return server_dsn(HEAD_SERVER, database=make_url(o.head.dsn).database)


async def _connections(o: Octopus) -> tuple[asyncpg.Connection, asyncpg.Connection]:
    """``reseed``'s connections: the arm as its reader role, and the head as a superuser."""
    primary = await asyncpg.connect(await _reader_dsn(o, o.arm.name), server_settings=SESSION)
    mirror = await asyncpg.connect(_head_admin_dsn(o), server_settings=SESSION)
    return primary, mirror


async def _reseed(o: Octopus, primary, mirror, **kwargs) -> dict:
    return await reseed(
        primary,
        mirror,
        **{"role": "mirror", "arm": o.arm.name, "arms": o.arms_config.arms, "head": o.head.name} | kwargs,
    )


async def _status(o: Octopus) -> dict:
    return (await subscriber_status(o.head.admin, arms=o.arms_config.arms, only=o.arm.name))["subscriptions"][0]


async def _lose_subscription(o: Octopus) -> None:
    """What a lost slot leaves behind once cleaned up: the head's rows, no subscription, no slot."""
    name = subscription_name(o.arm.name)
    await o.head.admin.execute(f'ALTER SUBSCRIPTION "{name}" DISABLE')

    async def worker_gone():
        return not await o.head.admin.fetchval(
            "SELECT count(*) FROM pg_stat_subscription s JOIN pg_subscription u ON u.oid = s.subid "
            "WHERE u.subname = $1 AND s.pid IS NOT NULL",
            name,
        )

    await eventually(worker_gone)
    await o.head.admin.execute(f'ALTER SUBSCRIPTION "{name}" SET (slot_name = NONE)')
    await o.head.admin.execute(f'DROP SUBSCRIPTION "{name}"')
    await o.arm.admin.execute("SELECT pg_drop_replication_slot($1)", name)


async def _newest_on_head(o: Octopus) -> datetime:
    return await o.head.conn.fetchval(
        "SELECT max(created_at) FROM squirrel.snapshot WHERE owner_instance = $1", o.arm.name
    )


async def _apply_error_total(o: Octopus) -> int:
    status = await _status(o)
    return status["apply_error_count"] + status["sync_error_count"]


@one_arm
async def test_after_a_lost_slot(octopus: Octopus):
    o = octopus
    arm = o.arm.admin
    ids = await _subscribed(o)
    since = await _newest_on_head(o)
    await _lose_subscription(o)

    # The gap: changes the head never received.
    gap_snapshot = str(uuid4())
    async with arm.transaction():
        await arm.execute(
            f"INSERT INTO squirrel.snapshot (id, title, owner_instance) VALUES ($1, 'In the gap', '{o.arm.name}')",
            gap_snapshot,
        )
        await arm.execute(
            "INSERT INTO squirrel.snapshot_value (id, snapshot_id, pv_id, pv_name) "
            "VALUES (gen_random_uuid(), $1, $2, 'SIM:PV:1:SETPOINT')",
            gap_snapshot,
            ids["pv"],
        )
    await arm.execute("UPDATE squirrel.pv SET description = 'edited in the gap'")
    await arm.execute("UPDATE squirrel.snapshot SET title = 'Renamed in the gap' WHERE id = $1", ids["snapshot"])
    async with arm.transaction():
        await arm.execute("DELETE FROM squirrel.pv_tag WHERE pv_id = $1", ids["pv"])
        await arm.execute("INSERT INTO squirrel.pv_tag (pv_id, tag_id) VALUES ($1, $2)", ids["pv"], ids["tag_b"])
    await arm.execute("DELETE FROM squirrel.tag WHERE id = $1", ids["tag_a"])

    primary, mirror = await _connections(o)
    try:
        report = await _reseed(o, primary, mirror, since=since)
        assert report["arm"] == o.arm.name and report["created"] is True and report["skipped"] == {}
        assert report["tables"]["squirrel.snapshot"]["read"] == 2
        assert report["tables"]["squirrel.tag"]["deleted"] == 1

        # Replication carries on from the cut.
        await arm.execute("UPDATE squirrel.pv SET description = 'after the re-seed'")
        await eventually(lambda: _in_step(o, o.arm.name))
        status = await _status(o)
        assert status["enabled"] and status["apply_error_count"] == 0
        assert (await drift_check(primary, mirror, arm=o.arm.name))["in_step"]
    finally:
        await primary.close()
        await mirror.close()


@one_arm
async def test_copies_a_facility_tree(octopus: Octopus):
    """``core.facility`` references itself, so a parent and its child go in the same statement."""
    o = octopus
    arm = o.arm.admin
    ids = await _subscribed(o)
    gone, gone_child = str(uuid4()), str(uuid4())
    async with arm.transaction():
        await arm.execute(
            f"INSERT INTO core.facility (id, slug, name, owner_instance) VALUES ($1, 'gone', 'Gone', '{o.arm.name}')",
            gone,
        )
        await arm.execute(
            f"INSERT INTO core.facility (id, slug, name, parent_id, owner_instance) VALUES ($1, 'gone-a', 'Gone A', $2, '{o.arm.name}')",
            gone_child,
            gone,
        )
    await eventually(lambda: _in_step(o, o.arm.name))
    since = await _newest_on_head(o)
    await _lose_subscription(o)

    # The gap: the old tree removed child first, as the parent's RESTRICT makes the arm
    # do, and a new tree with a PV assigned to its child.
    await arm.execute("DELETE FROM core.facility WHERE id = $1", gone_child)
    await arm.execute("DELETE FROM core.facility WHERE id = $1", gone)
    site, site_a = str(uuid4()), str(uuid4())
    async with arm.transaction():
        await arm.execute(
            f"INSERT INTO core.facility (id, slug, name, owner_instance) VALUES ($1, 'site', 'Site', '{o.arm.name}')",
            site,
        )
        await arm.execute(
            f"INSERT INTO core.facility (id, slug, name, parent_id, owner_instance) VALUES ($1, 'site-a', 'Site A', $2, '{o.arm.name}')",
            site_a,
            site,
        )
        await arm.execute("INSERT INTO squirrel.pv_facility (pv_id, facility_id) VALUES ($1, $2)", ids["pv"], site_a)

    primary, mirror = await _connections(o)
    try:
        report = await _reseed(o, primary, mirror, since=since)

        assert report["tables"]["core.facility"]["deleted"] == 2
        assert await o.head.conn.fetch(
            "SELECT f.slug, parent.slug AS parent FROM core.facility f "
            "LEFT JOIN core.facility parent ON parent.id = f.parent_id ORDER BY f.slug"
        ) == [(f"fac_{o.arm.name}", None), ("site", None), ("site-a", "site")]
        links = await o.head.conn.fetch(
            "SELECT facility_id::text AS id FROM squirrel.pv_facility WHERE pv_id = $1", ids["pv"]
        )
        assert {row["id"] for row in links} == {ids["facility"], site_a}
        assert (await drift_check(primary, mirror, arm=o.arm.name))["in_step"]
    finally:
        await primary.close()
        await mirror.close()


@one_arm
async def test_hands_over_to_replication_at_the_cut(octopus: Octopus):
    """Committed before the cut: copied, not replayed. Committed after the snapshot: streamed, not copied."""
    o = octopus
    await _subscribed(o)
    since = await _newest_on_head(o)
    await o.head.admin.execute(f'ALTER SUBSCRIPTION "{subscription_name(o.arm.name)}" DISABLE')
    before, after = str(uuid4()), str(uuid4())
    # Still in the slot, since the subscription is disabled: replaying it after the copy would stop apply.
    await o.arm.admin.execute(
        f"INSERT INTO squirrel.snapshot (id, title, owner_instance) VALUES ($1, 'Before the cut', '{o.arm.name}')",
        before,
    )

    async def write_after_the_snapshot():
        await o.arm.admin.execute(
            f"INSERT INTO squirrel.snapshot (id, title, owner_instance) VALUES ($1, 'After the snapshot', '{o.arm.name}')",
            after,
        )

    primary, mirror = await _connections(o)
    try:
        report = await _reseed(o, primary, mirror, since=since, hooks={"after_snapshot": write_after_the_snapshot})
        assert report["created"] is False
        assert report["tables"]["squirrel.snapshot"]["read"] == 2  # the seeded one and `before`

        await eventually(lambda: _in_step(o, o.arm.name))
        titles = {row["title"] for row in await o.head.conn.fetch("SELECT title FROM squirrel.snapshot")}
        assert {"Before the cut", "After the snapshot"} <= titles
        assert (await _status(o))["apply_error_count"] == 0
    finally:
        await primary.close()
        await mirror.close()


@one_arm
async def test_refusals(octopus: Octopus):
    o = octopus
    await _subscribed(o)
    newest = await _newest_on_head(o)
    primary, mirror = await _connections(o)
    try:
        with pytest.raises(Refused, match="is enabled"):
            await _reseed(o, primary, mirror, since=newest)
        await o.head.admin.execute(f'ALTER SUBSCRIPTION "{subscription_name(o.arm.name)}" DISABLE')
        with pytest.raises(Refused, match="later than the newest snapshot"):
            await _reseed(o, primary, mirror, since=newest + timedelta(seconds=1))
        # The tables' owner can't advance a replication origin without a grant.
        with pytest.raises(Refused, match="pg_replication_origin_advance"):
            await _reseed(o, primary, o.head.conn, since=newest)
        with pytest.raises(Refused, match="runs on a mirror"):
            await _reseed(o, primary, mirror, role="primary", since=newest)
        with pytest.raises(Refused, match="no arm named 'arm9'"):
            await _reseed(o, primary, mirror, arm="arm9", since=newest)
        assert not (await _status(o))["enabled"]
    finally:
        await primary.close()
        await mirror.close()


@one_arm
async def test_command(octopus: Octopus):
    o = octopus
    await _subscribed(o)
    name = subscription_name(o.arm.name)
    since = (await _newest_on_head(o)).isoformat()
    env = {
        "PATH": os.environ.get("PATH", ""),
        "CANOPY_ROLE": "mirror",
        "CANOPY_INSTANCE": o.head.name,
        "CANOPY_DATABASE_URL": _head_admin_dsn(o).replace("postgresql://", "postgresql+asyncpg://", 1),
        f"CANOPY_ARMS__{o.arm.name.upper()}__READER_DSN": await _reader_dsn(o, o.arm.name),
    }

    def reseed_command(*arm: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, "-m", "canopy.core.replication", "reseed", *arm, "--since", since],
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
        )

    assert reseed_command().returncode == 2  # --arm is required
    refused = reseed_command("--arm", o.arm.name)
    assert refused.returncode == 2 and "is enabled" in refused.stderr

    await o.head.admin.execute(f'ALTER SUBSCRIPTION "{name}" DISABLE')
    done = reseed_command("--arm", o.arm.name)
    assert done.returncode == 0, done.stderr
    assert json.loads(done.stdout)["subscription"] == name
    await o.arm.admin.execute("UPDATE squirrel.pv SET description = 'after the command'")
    await eventually(lambda: _in_step(o, o.arm.name))


@one_arm
async def test_cut_waits_out_a_write_left_open(octopus: Octopus):
    """A write open at the cut is waited for, copied, and not replayed: no apply error."""
    o = octopus
    await _subscribed(o)
    since = await _newest_on_head(o)
    await o.head.admin.execute(f'ALTER SUBSCRIPTION "{subscription_name(o.arm.name)}" DISABLE')
    primary, mirror = await _connections(o)
    writer = await asyncpg.connect(o.arm.dsn)
    try:
        open_write = writer.transaction()
        await open_write.start()
        await writer.execute(
            f"INSERT INTO squirrel.snapshot (id, title, owner_instance) VALUES (gen_random_uuid(), 'Open at the cut', '{o.arm.name}')"
        )
        with pytest.raises(Refused, match="stayed open past --cut-wait"):
            await _reseed(o, primary, mirror, since=since, cut_wait_s=0.5)
        assert not (await _status(o))["enabled"]

        task = asyncio.create_task(_reseed(o, primary, mirror, since=since))
        await asyncio.sleep(0.5)
        assert not task.done()
        await open_write.commit()
        await task
        await eventually(lambda: _in_step(o, o.arm.name))
        assert await o.head.conn.fetchval("SELECT count(*) FROM squirrel.snapshot WHERE title = 'Open at the cut'") == 1
        assert await _apply_error_total(o) == 0
    finally:
        await writer.close()
        await primary.close()
        await mirror.close()


@one_arm
async def test_cut_retries_until_it_is_exact(octopus: Octopus):
    """A write between the bound and the snapshot spoils that attempt; one that never stops is refused."""
    o = octopus
    await _subscribed(o)
    since = await _newest_on_head(o)
    await o.head.admin.execute(f'ALTER SUBSCRIPTION "{subscription_name(o.arm.name)}" DISABLE')
    primary, mirror = await _connections(o)
    try:
        calls = 0

        async def write(always: bool):
            nonlocal calls
            calls += 1
            if always or calls == 1:
                await o.arm.admin.execute(
                    f"INSERT INTO squirrel.snapshot (id, title, owner_instance) VALUES (gen_random_uuid(), 'Mid-cut', '{o.arm.name}')"
                )

        with pytest.raises(Refused, match="kept writing"):
            await _reseed(
                o, primary, mirror, since=since, cut_wait_s=0.5, hooks={"before_snapshot": lambda: write(always=True)}
            )
        assert not (await _status(o))["enabled"]

        calls = 0
        report = await _reseed(o, primary, mirror, since=since, hooks={"before_snapshot": lambda: write(always=False)})
        assert report["cut_attempts"] == 2
        await eventually(lambda: _in_step(o, o.arm.name))
        assert await _apply_error_total(o) == 0
    finally:
        await primary.close()
        await mirror.close()


@one_arm
async def test_moves_the_origin_only_after_the_copy_commits(octopus: Octopus):
    """A failure before or after the copy commits leaves the subscription disabled; a rerun converges."""
    o = octopus
    ids = await _subscribed(o)
    since = await _newest_on_head(o)
    await _lose_subscription(o)
    gap = str(uuid4())
    await o.arm.admin.execute(
        f"INSERT INTO squirrel.snapshot (id, title, owner_instance) VALUES ($1, 'In the gap', '{o.arm.name}')", gap
    )
    name = subscription_name(o.arm.name)

    async def origin() -> str | None:
        return await o.head.admin.fetchval(
            "SELECT o.remote_lsn::text FROM pg_replication_origin_status o "
            "JOIN pg_subscription s ON o.external_id = 'pg_' || s.oid WHERE s.subname = $1",
            name,
        )

    async def fail():
        raise RuntimeError("injected")

    primary, mirror = await _connections(o)
    try:
        with pytest.raises(RuntimeError, match="injected"):
            await _reseed(o, primary, mirror, since=since, hooks={"before_commit": fail})
        assert await o.head.conn.fetchval("SELECT count(*) FROM squirrel.snapshot WHERE id = $1", gap) == 0
        assert await origin() in (None, "0/0")
        assert not (await _status(o))["enabled"]

        with pytest.raises(RuntimeError, match="injected"):
            await _reseed(o, primary, mirror, since=since, hooks={"after_commit": fail})
        assert await o.head.conn.fetchval("SELECT count(*) FROM squirrel.snapshot WHERE id = $1", gap) == 1
        assert await origin() in (None, "0/0")
        assert not (await _status(o))["enabled"]

        await _reseed(o, primary, mirror, since=since)
        await o.arm.admin.execute("UPDATE squirrel.pv SET description = 'after the rerun' WHERE id = $1", ids["pv"])
        await eventually(lambda: _in_step(o, o.arm.name))
        assert await _apply_error_total(o) == 0
    finally:
        await primary.close()
        await mirror.close()


@one_arm
async def test_finds_a_snapshot_by_id_whatever_its_time(octopus: Octopus):
    """A snapshot that started before --since but committed in the gap still comes across, with its values."""
    o = octopus
    arm = o.arm.admin
    ids = await _subscribed(o)
    since = await _newest_on_head(o)
    await _lose_subscription(o)
    late = str(uuid4())
    async with arm.transaction():
        await arm.execute(
            "INSERT INTO squirrel.snapshot (id, title, created_at, updated_at, owner_instance) "
            "VALUES ($1, 'Started early', $2::timestamptz - interval '1 hour', $2::timestamptz - interval '1 hour', $3)",
            late,
            since,
            o.arm.name,
        )
        await arm.execute(
            "INSERT INTO squirrel.snapshot_value (id, snapshot_id, pv_id, pv_name) "
            "VALUES (gen_random_uuid(), $1, $2, 'SIM:PV:1:SETPOINT')",
            late,
            ids["pv"],
        )
    primary, mirror = await _connections(o)
    try:
        await _reseed(o, primary, mirror, since=since)
        assert (
            await o.head.conn.fetchval("SELECT count(*) FROM squirrel.snapshot_value WHERE snapshot_id = $1", late) == 1
        )
        await eventually(lambda: _in_step(o, o.arm.name))
    finally:
        await primary.close()
        await mirror.close()


@one_arm
async def test_follows_swapped_names_and_a_moved_child(octopus: Octopus):
    """Unique names swapped through a temporary one, and a child moved off a parent that is then deleted."""
    o = octopus
    arm = o.arm.admin
    ids = await _subscribed(o)
    zone, old_parent, new_parent, child = (str(uuid4()) for _ in range(4))
    async with arm.transaction():
        await arm.execute(
            f"INSERT INTO squirrel.tag_group (id, name, owner_instance) VALUES ($1, 'Zone', '{o.arm.name}')", zone
        )
        await arm.execute(
            f"INSERT INTO core.facility (id, slug, name, owner_instance) VALUES ($1, 'old', 'Old', '{o.arm.name}')",
            old_parent,
        )
        await arm.execute(
            f"INSERT INTO core.facility (id, slug, name, owner_instance) VALUES ($1, 'new', 'New', '{o.arm.name}')",
            new_parent,
        )
        await arm.execute(
            f"INSERT INTO core.facility (id, slug, name, parent_id, owner_instance) VALUES ($1, 'child', 'Child', $2, '{o.arm.name}')",
            child,
            old_parent,
        )
    await eventually(lambda: _in_step(o, o.arm.name))
    since = await _newest_on_head(o)
    await _lose_subscription(o)

    async with arm.transaction():
        await arm.execute("UPDATE squirrel.tag_group SET name = 'tmp' WHERE id = $1", ids["group"])
        await arm.execute("UPDATE squirrel.tag_group SET name = 'Area' WHERE id = $1", zone)
        await arm.execute("UPDATE squirrel.tag_group SET name = 'Zone' WHERE id = $1", ids["group"])
    await arm.execute("UPDATE core.facility SET parent_id = $1 WHERE id = $2", new_parent, child)
    await arm.execute("DELETE FROM core.facility WHERE id = $1", old_parent)

    primary, mirror = await _connections(o)
    try:
        report = await _reseed(o, primary, mirror, since=since)
        assert report["tables"]["squirrel.tag_group"]["set_aside"] == 2
        assert report["tables"]["core.facility"]["deleted"] == 1
        assert await o.head.conn.fetchval("SELECT name FROM squirrel.tag_group WHERE id = $1", zone) == "Area"
        assert (
            await o.head.conn.fetchval("SELECT parent_id::text FROM core.facility WHERE id = $1", child) == new_parent
        )
        assert (await drift_check(primary, mirror, arm=o.arm.name))["in_step"]
    finally:
        await primary.close()
        await mirror.close()


@one_arm
async def test_a_replaced_pv_keeps_its_owner_and_is_deleted(octopus: Octopus):
    """A PV deleted and created again at the same address: the old row is set aside, then deleted, never re-owned."""
    o = octopus
    arm = o.arm.name
    await _subscribed(o)
    old_pv, new_pv = str(uuid4()), str(uuid4())
    await o.arm.admin.execute(
        "INSERT INTO squirrel.pv (id, setpoint_address, owner_instance) VALUES ($1, 'SIM:PV:2:SETPOINT', $2)",
        old_pv,
        arm,
    )
    await eventually(lambda: _in_step(o, arm))
    since = await _newest_on_head(o)
    await _lose_subscription(o)
    await o.arm.admin.execute("DELETE FROM squirrel.pv WHERE id = $1", old_pv)
    await o.arm.admin.execute(
        "INSERT INTO squirrel.pv (id, setpoint_address, owner_instance) VALUES ($1, 'SIM:PV:2:SETPOINT', $2)",
        new_pv,
        arm,
    )

    primary, mirror = await _connections(o)
    try:
        report = await _reseed(o, primary, mirror, since=since)
        assert report["tables"]["squirrel.pv"]["set_aside"] == 1 and report["tables"]["squirrel.pv"]["deleted"] == 1
        rows = await o.head.conn.fetch(
            "SELECT id::text, owner_instance FROM squirrel.pv WHERE setpoint_address = 'SIM:PV:2:SETPOINT'"
        )
        assert [tuple(row.values()) for row in rows] == [(new_pv, arm)]
        assert not await o.head.conn.fetchval(
            "SELECT count(*) FROM squirrel.pv WHERE owner_instance <> ALL($1::text[])", [arm, o.head.name]
        )
        await eventually(lambda: _in_step(o, arm))
    finally:
        await primary.close()
        await mirror.close()


@one_arm
async def test_waits_for_the_apply_worker_to_stop(octopus: Octopus, monkeypatch):
    o = octopus
    await _subscribed(o)
    name = subscription_name(o.arm.name)
    oid = await o.head.admin.fetchval("SELECT oid FROM pg_subscription WHERE subname = $1", name)
    monkeypatch.setattr(reseed_module, "WORKER_WAIT_S", 0.5)
    with pytest.raises(Refused, match="apply worker is still running"):
        await reseed_module._wait_for_no_worker(o.head.admin, oid)
    await o.head.admin.execute(f'ALTER SUBSCRIPTION "{name}" DISABLE')
    monkeypatch.setattr(reseed_module, "WORKER_WAIT_S", 10.0)
    await reseed_module._wait_for_no_worker(o.head.admin, oid)


async def _everyone_else(o: Octopus, arm: str) -> dict:
    """Every row on the head owned by anyone but ``arm``, by table and owner."""
    owners = [name for name in o.arms if name != arm] + [o.head.name]
    return {(table, owner): await _rows(o.head.conn, table, owner) for table in PRESENT for owner in owners}


async def _two_arms_and_the_heads_rows(o: Octopus) -> dict[str, dict[str, str]]:
    """Both arms subscribed and copied (the same PV and tag group on each), plus a facility and PV of the head's own."""
    ids = {name: await _subscribed(o, name) for name in o.arms}
    for name in o.arms:
        await eventually(lambda name=name: _in_step(o, name))
    await o.head.admin.execute(
        "INSERT INTO core.facility (id, slug, name, owner_instance) VALUES (gen_random_uuid(), 'head-only', 'Head', $1)",
        o.head.name,
    )
    await o.head.admin.execute(
        "INSERT INTO squirrel.pv (id, setpoint_address, owner_instance) VALUES (gen_random_uuid(), 'SIM:PV:1:SETPOINT', $1)",
        o.head.name,
    )
    return ids


async def _lose(o: Octopus, arm: str) -> None:
    """``_lose_subscription`` for any arm."""
    name = subscription_name(arm)
    await o.head.admin.execute(f'ALTER SUBSCRIPTION "{name}" DISABLE')

    async def worker_gone():
        return not await o.head.admin.fetchval(
            "SELECT count(*) FROM pg_stat_subscription s JOIN pg_subscription u ON u.oid = s.subid "
            "WHERE u.subname = $1 AND s.pid IS NOT NULL",
            name,
        )

    await eventually(worker_gone)
    await o.head.admin.execute(f'ALTER SUBSCRIPTION "{name}" SET (slot_name = NONE)')
    await o.head.admin.execute(f'DROP SUBSCRIPTION "{name}"')
    await o.arms[arm].admin.execute("SELECT pg_drop_replication_slot($1)", name)


async def _reseed_arm(o: Octopus, arm: str, **kwargs) -> dict:
    primary = await asyncpg.connect(await _reader_dsn(o, arm), server_settings=SESSION)
    mirror = await asyncpg.connect(_head_admin_dsn(o), server_settings=SESSION)
    try:
        return await reseed(
            primary, mirror, **{"role": "mirror", "arm": arm, "arms": o.arms_config.arms, "head": o.head.name} | kwargs
        )
    finally:
        await primary.close()
        await mirror.close()


async def test_reseeds_one_arm_and_leaves_every_other_owners_rows_alone(octopus: Octopus):
    o = octopus
    first, second = o.arms
    ids = await _two_arms_and_the_heads_rows(o)
    newest = await o.head.conn.fetchval(
        "SELECT max(created_at) FROM squirrel.snapshot WHERE owner_instance = $1", first
    )
    await _lose(o, first)
    # The second arm has a newer snapshot than anything the head holds from the first:
    # the floor is per arm.
    await o.arms[second].admin.execute(
        "INSERT INTO squirrel.snapshot (id, title, owner_instance) VALUES (gen_random_uuid(), 'Newer', $1)", second
    )
    await eventually(lambda: _in_step(o, second))
    newer = await o.head.conn.fetchval(
        "SELECT max(created_at) FROM squirrel.snapshot WHERE owner_instance = $1", second
    )
    with pytest.raises(Refused, match=f"newest snapshot here from {first}"):
        await _reseed_arm(o, first, since=newer)

    # The gap on the first arm: a tag deleted, the shared PV address edited, a new snapshot.
    arm = o.arms[first].admin
    await arm.execute("DELETE FROM squirrel.tag WHERE id = $1", ids[first]["tag_a"])
    await arm.execute("UPDATE squirrel.pv SET description = 'edited in the gap' WHERE id = $1", ids[first]["pv"])
    await arm.execute(
        "INSERT INTO squirrel.snapshot (id, title, owner_instance) VALUES (gen_random_uuid(), 'In the gap', $1)", first
    )
    before = await _everyone_else(o, first)

    report = await _reseed_arm(o, first, since=newest)
    assert report["tables"]["squirrel.tag"]["deleted"] == 1 and report["created"] is True
    assert await _everyone_else(o, first) == before  # the second arm's rows and the head's, untouched
    await eventually(lambda: _in_step(o, first))

    # The second arm streamed on throughout, and still does.
    await o.arms[second].admin.execute("UPDATE squirrel.pv SET description = 'still streaming'")
    await eventually(lambda: _in_step(o, second))
    for name in o.arms:
        primary = await asyncpg.connect(await _reader_dsn(o, name), server_settings=SESSION)
        try:
            assert (await drift_check(primary, o.head.conn, arm=name))["in_step"]
        finally:
            await primary.close()


async def test_refuses_a_slug_another_owner_holds(octopus: Octopus):
    o = octopus
    first, second = o.arms
    ids = await _two_arms_and_the_heads_rows(o)
    since = await o.head.conn.fetchval("SELECT max(created_at) FROM squirrel.snapshot WHERE owner_instance = $1", first)
    await _lose(o, first)
    # Facility slugs are unique across owners; the first arm can't see the second's.
    await o.arms[first].admin.execute(
        "UPDATE core.facility SET slug = $1 WHERE id = $2", f"fac_{second}", ids[first]["facility"]
    )
    before = await _everyone_else(o, first)
    with pytest.raises(Refused, match=f"held here by a row of {second}"):
        await _reseed_arm(o, first, since=since)
    assert await _everyone_else(o, first) == before
    status = (await subscriber_status(o.head.admin, arms=o.arms_config.arms, only=first))["subscriptions"][0]
    assert not status["enabled"]


async def test_refuses_a_key_another_owner_holds(octopus: Octopus):
    o = octopus
    first, second = o.arms
    ids = await _two_arms_and_the_heads_rows(o)
    since = await o.head.conn.fetchval("SELECT max(created_at) FROM squirrel.snapshot WHERE owner_instance = $1", first)
    await _lose(o, first)
    # A row on the first arm whose id the head already holds for the second arm's tag group.
    await o.arms[first].admin.execute(
        "INSERT INTO squirrel.tag_group (id, name, owner_instance) VALUES ($1, 'Collides', $2)",
        ids[second]["group"],
        first,
    )
    before = await _everyone_else(o, first)
    with pytest.raises(Refused, match=f"held here under the same key by {second}"):
        await _reseed_arm(o, first, since=since)
    assert await _everyone_else(o, first) == before
