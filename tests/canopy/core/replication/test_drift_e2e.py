"""``drift-check`` end to end: each arm's rows against the head's copy of them, and the head's own references.

Opt-in like ``test_replication_e2e.py``: the ``octopus`` fixture skips unless the
``CANOPY_TEST_REPLICATION_*`` variables point at the servers.
"""

import os
import sys
import json
import asyncio
import subprocess
from uuid import uuid4
from datetime import UTC, datetime

import pytest
import asyncpg
from pydantic import SecretStr

from canopy.core.arms import Arm, subscription_name
from canopy.core.replication.drift import dangling, check_arms, drift_check
from canopy.core.replication.registry import REGISTRY, TableClass, ReplicationRegistry
from canopy.core.replication.publisher import publish

from .conftest import Octopus, eventually, _reader_dsn, _subscribed, _arm_connections
from .test_replication_e2e import _in_step

PRESENT = sorted(REGISTRY.replicated)
one_arm = pytest.mark.parametrize("octopus", [1], indirect=True)


async def _as_apply(conn: asyncpg.Connection, sql: str, *args) -> None:
    """Run ``sql`` the way apply does: foreign-key triggers don't fire."""
    async with conn.transaction():
        await conn.execute("SET LOCAL session_replication_role = replica")
        await conn.execute(sql, *args)


async def _readers(o: Octopus) -> dict[str, Arm]:
    """Every arm as ``drift-check`` sees it: only its reader URI."""
    return {name: Arm(reader_dsn=SecretStr(await _reader_dsn(o, name))) for name in o.arms}


@one_arm
async def test_finds_what_the_head_lost(octopus: Octopus):
    o = octopus
    arm = o.arm.name
    ids = await _subscribed(o)
    old = str(uuid4())
    await o.arm.admin.execute(
        "INSERT INTO squirrel.snapshot (id, title, created_at, owner_instance) "
        "VALUES ($1, 'Old', now() - interval '10 days', $2)",
        old,
        arm,
    )
    await eventually(lambda: _in_step(o, arm))
    old_day = await o.arm.admin.fetchval(
        "SELECT (created_at AT TIME ZONE 'UTC')::date::text FROM squirrel.snapshot WHERE id = $1", old
    )
    today = datetime.now(UTC).date().isoformat()
    primary, mirror = await _arm_connections(o, arm)
    try:
        report = await drift_check(primary, mirror, arm=arm)
        assert report["in_step"] and report["compared"] == PRESENT and report["skipped"] == {}

        # A column only the head has yet (it migrates first) isn't drift.
        await o.head.admin.execute("ALTER TABLE squirrel.pv ADD COLUMN zz_history_only text DEFAULT 'x'")
        # What replication never does: an edit made on the head, and deletes there that
        # skip the foreign keys, as apply does, leaving a snapshot value without its snapshot.
        await o.head.admin.execute("UPDATE squirrel.tag SET name = 'Edited on the head' WHERE id = $1", ids["tag_a"])
        await _as_apply(
            o.head.admin, "DELETE FROM squirrel.snapshot WHERE id = ANY($1::uuid[])", [ids["snapshot"], old]
        )

        report = await drift_check(primary, mirror, arm=arm)
        assert not report["in_step"]
        assert sorted(report["drift"]) == ["squirrel.snapshot", "squirrel.snapshot_value", "squirrel.tag"]
        tag = report["drift"]["squirrel.tag"]
        assert tag["primary"]["rows"] == tag["mirror"]["rows"] and tag["primary"]["md5"] != tag["mirror"]["md5"]
        snapshot = report["drift"]["squirrel.snapshot"]
        assert [d["day"] for d in snapshot["days"]] == [old_day, today] and snapshot["missing_rows"] == 2
        # The value left behind has no snapshot to date it or name its owner on the head;
        # the head check reports it, owned through its PV.
        assert report["drift"]["squirrel.snapshot_value"]["days"] == [{"day": today, "primary": 1, "mirror": 0}]
        assert await dangling(mirror) == [
            {
                "table": "squirrel.snapshot_value",
                "column": "snapshot_id",
                "parent": "squirrel.snapshot",
                "owner": arm,
                "rows": 1,
            }
        ]

        recent = await drift_check(primary, mirror, arm=arm, days=3)
        assert [d["day"] for d in recent["drift"]["squirrel.snapshot"]["days"]] == [today]
    finally:
        await primary.close()
        await mirror.close()


@one_arm
async def test_waits_for_an_arm_that_is_only_behind(octopus: Octopus):
    """Lag is never drift: not checked while the slot can't confirm, in step once the head catches up."""
    o = octopus
    arm = o.arm.name
    await _subscribed(o)
    name = subscription_name(arm)
    primary, mirror = await _arm_connections(o, arm)
    try:
        await o.head.admin.execute(f'ALTER SUBSCRIPTION "{name}" DISABLE')
        await o.arm.admin.execute(
            "INSERT INTO squirrel.snapshot (id, title, owner_instance) VALUES (gen_random_uuid(), 'Not applied yet', $1)",
            arm,
        )
        behind = await drift_check(primary, mirror, arm=arm, settle_s=1)
        assert behind["status"] == "not_checked" and not behind["drift"]
        assert list(behind["inconclusive"]) == ["squirrel.snapshot"]

        async def enable_soon():
            await asyncio.sleep(1)
            await o.head.admin.execute(f'ALTER SUBSCRIPTION "{name}" ENABLE')

        enabling = asyncio.create_task(enable_soon())
        caught_up = await drift_check(primary, mirror, arm=arm, settle_s=30)
        await enabling
        assert caught_up["status"] == "in_step", caught_up
    finally:
        await primary.close()
        await mirror.close()


@one_arm
async def test_retries_when_the_arm_changes_mid_comparison(octopus: Octopus):
    """A write between the reads makes one comparison inconclusive; real drift still shows on the next."""
    o = octopus
    arm = o.arm.name
    ids = await _subscribed(o)
    await o.head.admin.execute("DELETE FROM squirrel.tag WHERE id = $1", ids["tag_b"])  # the head loses a row

    async def new_tag():
        await o.arm.admin.execute(
            "INSERT INTO squirrel.tag (id, name, group_id) VALUES (gen_random_uuid(), $1, $2)",
            uuid4().hex,
            ids["group"],
        )

    primary, mirror = await _arm_connections(o, arm)
    try:
        hooked = 0

        async def write_once():
            nonlocal hooked
            hooked += 1
            if hooked == 1:
                await new_tag()

        report = await drift_check(primary, mirror, arm=arm, after_mirror_read=write_once)
        assert report["status"] == "drift" and report["attempts"] == 2
        assert list(report["drift"]) == ["squirrel.tag"]

        churned = await drift_check(primary, mirror, arm=arm, after_mirror_read=new_tag)
        assert churned["status"] == "not_checked" and not churned["drift"]
        assert churned["attempts"] == 3 and list(churned["inconclusive"]) == ["squirrel.tag"]
    finally:
        await primary.close()
        await mirror.close()


@one_arm
async def test_reports_a_published_table_not_yet_subscribed(octopus: Octopus):
    o = octopus
    await _subscribed(o)
    for conn in (o.head.conn, o.arm.admin):
        await conn.execute("CREATE TABLE squirrel.zz_probe (id int PRIMARY KEY)")
    probe = ReplicationRegistry(
        tables={**REGISTRY.tables, "squirrel.zz_probe": TableClass.REFERENCE},
        publications=REGISTRY.publications,
        owner_paths=REGISTRY.owner_paths,
        shared_keys=REGISTRY.shared_keys,
    )
    await publish(o.arm.admin, role="primary", config=o.config, registry=probe)
    primary, mirror = await _arm_connections(o, o.arm.name)
    try:
        report = await drift_check(primary, mirror, arm=o.arm.name, registry=probe)
        assert report["status"] == "not_checked" and not report["drift"]
        assert report["skipped"] == {"squirrel.zz_probe": "published but not subscribed yet (run refresh)"}
    finally:
        await primary.close()
        await mirror.close()


async def test_each_arm_is_compared_on_its_own_rows(octopus: Octopus):
    """Two arms with the same PV and tag group, and rows of the head's own: only an arm's rows count for it."""
    o = octopus
    first, second = o.arms
    ids = {name: await _subscribed(o, name) for name in o.arms}
    for name in o.arms:
        await eventually(lambda name=name: _in_step(o, name))
    # Rows the head writes for itself (a superuser here, past the head's own row-level security).
    head_facility = str(uuid4())
    await o.head.admin.execute(
        "INSERT INTO core.facility (id, slug, name, owner_instance) VALUES ($1, 'head-only', 'Head', $2)",
        head_facility,
        o.head.name,
    )
    await o.head.admin.execute(
        "INSERT INTO squirrel.pv (id, setpoint_address, owner_instance) VALUES (gen_random_uuid(), 'SIM:PV:1:SETPOINT', $1)",
        o.head.name,
    )
    readers = await _readers(o)

    report = await check_arms(o.head.conn, readers, [first, second], settle_s=5)
    assert report["status"] == "in_step", report
    assert [report["arms"][name]["compared"] for name in (first, second)] == [PRESENT, PRESENT]

    # The head loses one of the first arm's tags: only that arm drifts.
    await o.head.admin.execute("DELETE FROM squirrel.tag WHERE id = $1", ids[first]["tag_b"])
    report = await check_arms(o.head.conn, readers, [first, second], settle_s=5)
    assert report["status"] == "drift"
    assert list(report["arms"][first]["drift"]) == ["squirrel.tag"]
    assert report["arms"][second]["status"] == "in_step"
    assert report["head"] == {"status": "in_step", "dangling": []}


@one_arm
async def test_the_head_finds_references_whose_parent_is_gone(octopus: Octopus):
    o = octopus
    arm = o.arm.name
    ids = await _subscribed(o)
    root, child, filed, unfiled = (str(uuid4()) for _ in range(4))
    # A root facility (no parent) with a child, a snapshot filed under the child and one
    # filed nowhere. NULL references are never dangling.
    await o.arm.admin.execute(
        "INSERT INTO core.facility (id, slug, name, parent_id, owner_instance) "
        "VALUES ($1, 'site', 'Site', NULL, $3), ($2, 'site-area', 'Area', $1, $3)",
        root,
        child,
        arm,
    )
    await o.arm.admin.execute(
        "INSERT INTO squirrel.snapshot (id, title, facility_id, owner_instance) "
        "VALUES ($1, 'Filed', $2, $4), ($3, 'Unfiled', NULL, $4)",
        filed,
        child,
        unfiled,
        arm,
    )
    await o.arm.admin.execute("INSERT INTO squirrel.pv_facility (pv_id, facility_id) VALUES ($1, $2)", ids["pv"], child)
    await eventually(lambda: _in_step(o, arm))
    assert await dangling(o.head.conn) == []

    await _as_apply(o.head.admin, "DELETE FROM core.facility WHERE id = $1", child)
    await _as_apply(o.head.admin, "DELETE FROM squirrel.tag WHERE id = $1", ids["tag_a"])
    # Each is the arm's: a snapshot is its own owner, and the links follow their surviving PV.
    assert await dangling(o.head.conn) == [
        {"table": "squirrel.pv_facility", "column": "facility_id", "parent": "core.facility", "owner": arm, "rows": 1},
        {"table": "squirrel.pv_tag", "column": "tag_id", "parent": "squirrel.tag", "owner": arm, "rows": 1},
        {"table": "squirrel.snapshot", "column": "facility_id", "parent": "core.facility", "owner": arm, "rows": 1},
    ]

    report = await check_arms(o.head.conn, await _readers(o), [arm], settle_s=5)
    assert report["status"] == "drift" and report["arms"][arm]["status"] == "drift"
    assert len(report["arms"][arm]["dangling"]) == 3
    assert report["head"] == {"status": "in_step", "dangling": []}


@one_arm
async def test_a_dangling_row_is_owned_through_any_parent_that_survives(octopus: Octopus):
    """A PV gone from the head: its tag link, facility link and value find an owner elsewhere; its meta row can't."""
    o = octopus
    arm = o.arm.name
    ids = await _subscribed(o)
    await _as_apply(o.head.admin, "DELETE FROM squirrel.pv WHERE id = $1", ids["pv"])
    assert await dangling(o.head.conn) == [
        {"table": "squirrel.meta_pv", "column": "pv_id", "parent": "squirrel.pv", "owner": None, "rows": 1},
        {"table": "squirrel.pv_facility", "column": "pv_id", "parent": "squirrel.pv", "owner": arm, "rows": 1},
        {"table": "squirrel.pv_tag", "column": "pv_id", "parent": "squirrel.pv", "owner": arm, "rows": 1},
        {"table": "squirrel.snapshot_value", "column": "pv_id", "parent": "squirrel.pv", "owner": arm, "rows": 1},
    ]
    report = await check_arms(o.head.conn, await _readers(o), [arm], settle_s=5)
    assert [d["table"] for d in report["arms"][arm]["dangling"]] == [
        "squirrel.pv_facility",
        "squirrel.pv_tag",
        "squirrel.snapshot_value",
    ]
    # Nobody can be found to own the meta row, so it fails every arm's run.
    assert report["head"]["status"] == "drift"
    assert [d["table"] for d in report["head"]["dangling"]] == ["squirrel.meta_pv"]


async def test_an_arm_that_cant_be_checked_doesnt_stop_the_others(octopus: Octopus):
    o = octopus
    first, second = o.arms
    await publish(o.arms[second].admin, role="primary", config=o.config)
    await _subscribed(o, first)  # the second arm is published but not subscribed yet
    readers = await _readers(o)

    report = await check_arms(o.head.conn, readers, [first, second], settle_s=5)
    assert report["status"] == "not_checked"
    assert report["arms"][first]["status"] == "in_step"
    assert report["arms"][second]["status"] == "not_checked"
    assert "no subscription" in report["arms"][second]["error"]

    # One arm whose server is out of reach is that arm's "couldn't check", not the run's failure.
    unreachable = {**readers, second: Arm(reader_dsn=SecretStr("postgresql://reader:pw@127.0.0.1:1/canopy"))}
    report = await check_arms(o.head.conn, unreachable, [first, second], settle_s=5)
    assert [report["arms"][name]["status"] for name in (first, second)] == ["in_step", "not_checked"]


@one_arm
async def test_command_exit_status(octopus: Octopus):
    o = octopus
    await _subscribed(o)
    reader = await _reader_dsn(o, o.arm.name)
    variable = f"CANOPY_ARMS__{o.arm.name.upper()}__READER_DSN"
    env = {
        "PATH": os.environ.get("PATH", ""),
        "CANOPY_ROLE": "mirror",
        "CANOPY_INSTANCE": o.head.name,
        "CANOPY_DATABASE_URL": o.head.dsn.replace("postgresql://", "postgresql+asyncpg://", 1),
        variable: reader,
    }

    def drift_check_command(*options: str, settle: str = "10", **overrides: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, "-m", "canopy.core.replication", "drift-check", "--settle", settle, *options],
            env=env | overrides,
            capture_output=True,
            text=True,
            timeout=120,
        )

    in_step = drift_check_command(settle="0")
    assert in_step.returncode == 0, in_step.stderr
    assert json.loads(in_step.stdout)["arms"][o.arm.name]["in_step"] is True
    assert drift_check_command("--arm", o.arm.name, settle="0").returncode == 0

    await o.head.admin.execute("DELETE FROM squirrel.pv_tag")
    drifted = drift_check_command("--arm", o.arm.name)
    assert drifted.returncode == 1
    assert "squirrel.pv_tag" in json.loads(drifted.stdout)["arms"][o.arm.name]["drift"]
    assert "Drift in squirrel.pv_tag" in drifted.stderr

    unknown = drift_check_command("--arm", "arm9")
    assert unknown.returncode == 2 and "no arm named 'arm9'" in unknown.stderr

    wrong = "wrong-" + uuid4().hex
    password = reader.split(":")[2].split("@")[0]
    failed = drift_check_command(**{variable: reader.replace(password, wrong)})
    assert failed.returncode == 2
    assert "password authentication failed" in json.loads(failed.stdout)["arms"][o.arm.name]["error"]
    assert wrong not in failed.stderr + failed.stdout
