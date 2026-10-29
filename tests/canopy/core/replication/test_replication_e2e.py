"""Replication end to end: a primary and its mirror on two Postgres 16 servers.

Opt-in: skipped unless the three ``CANOPY_TEST_REPLICATION_*`` variables point at a
pair of servers, as ``docker-compose.yml`` in this directory starts them.

Set up the way the deploy repos will: the primary's tables belong to a superuser,
while the mirror's tables belong to an ordinary owner role granted
``pg_create_subscription``, which runs ``grants``, ``subscribe`` and ``refresh``.
Each test gets its own databases and roles on both servers.
"""

import os
import sys
import json
import asyncio
import subprocess
from uuid import uuid4
from datetime import UTC, datetime, timedelta
from dataclasses import dataclass

import pytest
import asyncpg
import pytest_asyncio
from pydantic import SecretStr

import canopy.core.replication.reseed as reseed_module
from canopy.core.replication.sql import Refused, ReplicationError
from canopy.core.replication.drift import SESSION, drift_check
from canopy.core.replication.config import ReplicationConfig
from canopy.core.replication.reseed import reseed
from canopy.core.replication.metrics import read_status
from canopy.core.replication.registry import REGISTRY, TableClass, ReplicationRegistry
from canopy.core.replication.publisher import publish, publisher_status
from canopy.core.replication.subscriber import (
    grants,
    refresh,
    subscribe,
    subscriber_status,
)

from .conftest import (
    PUBLISHER_ADMIN,
    SUBSCRIBER_ADMIN,
    PUBLISHER_CONNINFO,
    drop_roles,
    eventually,
    server_dsn,
    build_schema,
    requires_pair,
    metric_samples,
)

pytestmark = requires_pair

PRESENT = sorted(REGISTRY.replicated)


@dataclass
class Pair:
    instance: str
    owner: str  # the mirror's table owner, which runs the mirror-side commands
    config: ReplicationConfig
    pub: asyncpg.Connection  # primary, as a superuser
    sub: asyncpg.Connection  # mirror, as the tables' owner
    sub_admin: asyncpg.Connection  # mirror, as a superuser


@pytest_asyncio.fixture
async def pair():
    suffix = uuid4().hex[:8]
    pub_db, sub_db = f"rt_pub_{suffix}", f"rt_sub_{suffix}"
    replicator, reader, owner, app = (f"rt_{name}_{suffix}" for name in ("replicator", "reader", "owner", "app"))
    replicator_pw, owner_pw = uuid4().hex, uuid4().hex

    pub_server = await asyncpg.connect(PUBLISHER_ADMIN)
    sub_server = await asyncpg.connect(SUBSCRIBER_ADMIN)
    await pub_server.execute(f'CREATE DATABASE "{pub_db}"')
    await pub_server.execute(f"CREATE ROLE \"{replicator}\" REPLICATION LOGIN PASSWORD '{replicator_pw}'")
    await pub_server.execute(f'CREATE ROLE "{reader}" LOGIN')
    await sub_server.execute(f"CREATE ROLE \"{owner}\" LOGIN PASSWORD '{owner_pw}'")
    await sub_server.execute(f'GRANT pg_create_subscription TO "{owner}"')
    await sub_server.execute(f'CREATE ROLE "{app}" LOGIN')
    await sub_server.execute(f'CREATE DATABASE "{sub_db}" OWNER "{owner}"')
    await build_schema(server_dsn(PUBLISHER_ADMIN, database=pub_db))
    await build_schema(server_dsn(SUBSCRIBER_ADMIN, database=sub_db, user=owner, password=owner_pw))

    config = ReplicationConfig(
        replicator_role=replicator,
        reader_role=reader,
        app_role=app,
        publisher_dsn=SecretStr(f"{PUBLISHER_CONNINFO} dbname={pub_db} user={replicator} password={replicator_pw}"),
    )
    pair = Pair(
        instance=f"e2e_{suffix}",
        owner=owner,
        config=config,
        pub=await asyncpg.connect(server_dsn(PUBLISHER_ADMIN, database=pub_db)),
        sub=await asyncpg.connect(server_dsn(SUBSCRIBER_ADMIN, database=sub_db, user=owner, password=owner_pw)),
        sub_admin=await asyncpg.connect(server_dsn(SUBSCRIBER_ADMIN, database=sub_db)),
    )
    try:
        yield pair
    finally:
        # Dropping the subscription also drops its slot on the primary.
        await pair.sub_admin.execute(f'DROP SUBSCRIPTION IF EXISTS "canopy_history_{pair.instance}"')
        for conn in (pair.pub, pair.sub, pair.sub_admin):
            await conn.close()
        await pub_server.execute(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE database = $1", pub_db
        )
        await pub_server.execute(f'DROP DATABASE "{pub_db}" WITH (FORCE)')
        await sub_server.execute(f'DROP DATABASE "{sub_db}" WITH (FORCE)')
        await drop_roles(pub_server, [replicator, reader])
        await drop_roles(sub_server, [owner, app])
        await pub_server.close()
        await sub_server.close()


async def _copied(p: Pair) -> bool:
    sub = (await subscriber_status(p.sub_admin, instance=p.instance))["subscription"]
    return sub["exists"] and sub["tables_ready"] == len(PRESENT) and not sub["tables_not_ready"]


async def _rows(conn: asyncpg.Connection, table: str) -> list:
    return [tuple(row.values()) for row in await conn.fetch(f"SELECT * FROM {table} ORDER BY 1, 2")]


async def _in_step(p: Pair) -> bool:
    for table in PRESENT:
        if await _rows(p.pub, table) != await _rows(p.sub, table):
            return False
    return True


async def _seed(conn: asyncpg.Connection) -> dict[str, str]:
    ids = {name: str(uuid4()) for name in ("group", "tag_a", "tag_b", "pv", "snapshot", "value", "config", "key")}
    await conn.execute("INSERT INTO squirrel.tag_group (id, name) VALUES ($1, 'Area')", ids["group"])
    await conn.execute(
        "INSERT INTO squirrel.tag (id, name, group_id) VALUES ($1, 'Injector', $3), ($2, 'Linac', $3)",
        ids["tag_a"],
        ids["tag_b"],
        ids["group"],
    )
    await conn.execute(
        "INSERT INTO squirrel.pv (id, setpoint_address, device) VALUES ($1, 'SIM:PV:1:SETPOINT', 'SIM:DEV:1')",
        ids["pv"],
    )
    await conn.execute("INSERT INTO squirrel.pv_tag (pv_id, tag_id) VALUES ($1, $2)", ids["pv"], ids["tag_a"])
    await conn.execute("INSERT INTO squirrel.meta_pv (pv_id, display_name) VALUES ($1, 'Meta')", ids["pv"])
    await conn.execute("INSERT INTO squirrel.snapshot (id, title) VALUES ($1, 'Before')", ids["snapshot"])
    await conn.execute(
        "INSERT INTO squirrel.snapshot_value (id, snapshot_id, pv_id, pv_name, setpoint_value) "
        """VALUES ($1, $2, $3, 'SIM:PV:1:SETPOINT', '{"value": 1.0}')""",
        ids["value"],
        ids["snapshot"],
        ids["pv"],
    )
    await conn.execute(
        "INSERT INTO magpie.config (id, slug, app, data) VALUES ($1, 'cfg0000000000001', 'trace', '{}')", ids["config"]
    )
    await conn.execute(
        "INSERT INTO core.api_key (id, app_name, token_hash, is_active, read_access, write_access, admin_access) "
        "VALUES ($1, 'primary-only', repeat('a', 64), true, true, false, false)",
        ids["key"],
    )
    return ids


async def test_copy_then_stream_changes(pair: Pair):
    p = pair
    published = await publish(p.pub, role="primary", config=p.config)
    assert published["roles"] == {p.config.replicator_role: "granted", p.config.reader_role: "granted"}
    ids = await _seed(p.pub)
    await grants(p.sub, role="mirror", config=p.config)

    created = await subscribe(p.sub, role="mirror", instance=p.instance, config=p.config)
    assert created["created"] is True

    await eventually(lambda: _copied(p))
    assert await _in_step(p)
    assert await p.sub.fetchval("SELECT count(*) FROM core.api_key") == 0

    # The mirror's app role reads the copy and can't write it.
    await p.sub_admin.execute(f'SET ROLE "{p.config.app_role}"')
    assert await p.sub_admin.fetchval("SELECT count(*) FROM squirrel.snapshot_value") == 1
    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        await p.sub_admin.execute("DELETE FROM squirrel.snapshot")
    await p.sub_admin.execute("RESET ROLE")

    # Routine edits: a new snapshot, a PV edit, replacing a PV's tags (delete then
    # insert of link rows, which stalls an insert/update-only publication) and
    # deleting a snapshot with its values.
    new_snapshot = str(uuid4())
    async with p.pub.transaction():
        await p.pub.execute("INSERT INTO squirrel.snapshot (id, title) VALUES ($1, 'After')", new_snapshot)
        await p.pub.execute(
            "INSERT INTO squirrel.snapshot_value (id, snapshot_id, pv_id, pv_name) "
            "VALUES (gen_random_uuid(), $1, $2, 'SIM:PV:1:SETPOINT')",
            new_snapshot,
            ids["pv"],
        )
    await p.pub.execute("UPDATE squirrel.pv SET description = 'edited' WHERE id = $1", ids["pv"])
    async with p.pub.transaction():
        await p.pub.execute("DELETE FROM squirrel.pv_tag WHERE pv_id = $1", ids["pv"])
        await p.pub.execute(
            "INSERT INTO squirrel.pv_tag (pv_id, tag_id) VALUES ($1, $2), ($1, $3)",
            ids["pv"],
            ids["tag_a"],
            ids["tag_b"],
        )
    await p.pub.execute("DELETE FROM squirrel.snapshot WHERE id = $1", ids["snapshot"])

    await eventually(lambda: _in_step(p))
    assert await p.sub.fetchval("SELECT count(*) FROM squirrel.pv_tag") == 2

    primary = await publisher_status(p.pub, config=p.config)
    slot = next(s for s in primary["slots"] if s["slot_name"] == f"canopy_history_{p.instance}")
    assert slot["kind"] == "subscription" and slot["active"] is True
    assert slot["wal_status"] == "reserved"

    mirror = (await subscriber_status(p.sub_admin, instance=p.instance))["subscription"]
    assert mirror["applied_lsn"] not in (None, "0/0")
    assert mirror["apply_error_count"] == 0 and mirror["sync_error_count"] == 0
    assert mirror["streaming"] == "p" and mirror["owner"] == p.owner
    # The owner role, like a health check without a grant, can't read the applied position.
    as_owner = (await subscriber_status(p.sub, instance=p.instance))["subscription"]
    assert as_owner["applied_lsn"] is None and "superuser" in as_owner["applied_lsn_error"]

    again = await subscribe(p.sub, role="mirror", instance=p.instance, config=p.config)
    assert again == {"name": f"canopy_history_{p.instance}", "created": False}


async def test_refresh_copies_a_newly_published_table(pair: Pair):
    p = pair
    await publish(p.pub, role="primary", config=p.config)
    await subscribe(p.sub, role="mirror", instance=p.instance, config=p.config)
    await eventually(lambda: _copied(p))

    # A migration adds a table on both sides (mirror first), and the registry classifies it.
    for conn in (p.sub, p.pub):
        await conn.execute("CREATE TABLE squirrel.zz_probe (id int PRIMARY KEY, note text)")
    await p.pub.execute("INSERT INTO squirrel.zz_probe VALUES (1, 'one'), (2, 'two')")
    probe = ReplicationRegistry(
        tables={**REGISTRY.tables, "squirrel.zz_probe": TableClass.REFERENCE}, publications=REGISTRY.publications
    )
    assert (await publish(p.pub, role="primary", config=p.config, registry=probe))["publications"]["canopy_history"][
        "added"
    ] == ["squirrel.zz_probe"]

    await p.sub.execute("INSERT INTO squirrel.zz_probe VALUES (1, 'stray')")
    with pytest.raises(Refused, match="already have rows: squirrel.zz_probe"):
        await refresh(p.sub, role="mirror", instance=p.instance, registry=probe)
    await p.sub.execute("DELETE FROM squirrel.zz_probe")

    result = await refresh(p.sub, role="mirror", instance=p.instance, registry=probe)
    assert result["added"] == ["squirrel.zz_probe"]

    async def probe_copied():
        return await _rows(p.sub, "squirrel.zz_probe") == [(1, "one"), (2, "two")]

    await eventually(probe_copied)
    assert (await refresh(p.sub, role="mirror", instance=p.instance, registry=probe))["added"] == []


async def test_a_failed_subscribe_never_shows_the_password(pair: Pair, caplog):
    p = pair
    await publish(p.pub, role="primary", config=p.config)
    wrong = "wrong-" + uuid4().hex
    p.config.publisher_dsn = SecretStr(
        p.config.publisher_dsn.get_secret_value().rsplit("password=", 1)[0] + f"password={wrong}"
    )

    with pytest.raises(ReplicationError) as caught:
        await subscribe(p.sub, role="mirror", instance=p.instance, config=p.config)

    assert "password authentication failed" in str(caught.value)
    assert wrong not in str(caught.value) and wrong not in caplog.text
    assert not (await subscriber_status(p.sub_admin, instance=p.instance))["subscription"]["exists"]
    assert not await p.pub.fetchval("SELECT count(*) FROM pg_replication_slots WHERE database = current_database()")


async def test_gauges_on_both_sides(pair: Pair):
    p = pair
    await publish(p.pub, role="primary", config=p.config)
    await _seed(p.pub)
    await grants(p.sub, role="mirror", config=p.config)
    await subscribe(p.sub, role="mirror", instance=p.instance, config=p.config)
    await eventually(lambda: _copied(p))
    # One streamed change, so the applied position moves past the initial copy's 0/0.
    await p.pub.execute("UPDATE squirrel.pv SET description = 'streamed'")
    await eventually(lambda: _in_step(p))
    name = f"canopy_history_{p.instance}"
    slot = frozenset({"slot": name, "kind": "subscription"}.items())

    primary = metric_samples(await read_status(p.pub, role="primary", instance=None))
    assert primary[("canopy_replication_slot_active", slot)] == 1
    assert primary[("canopy_replication_slot_wal_status", slot | {("wal_status", "reserved")})] == 1
    assert primary[("canopy_replication_slot_retained_bytes", slot)] >= 0
    assert ("canopy_replication_slot_lag_bytes", slot) in primary

    # The mirror's API reads its gauges as the app role, which can't see the applied position.
    subscription = frozenset({"subscription": name}.items())
    await p.sub_admin.execute(f'SET ROLE "{p.config.app_role}"')
    mirror = metric_samples(await read_status(p.sub_admin, role="mirror", instance=p.instance))
    await p.sub_admin.execute("RESET ROLE")
    assert mirror[("canopy_replication_subscription_worker_up", subscription)] == 1
    assert mirror[("canopy_replication_subscription_receive_age_seconds", subscription)] < 60
    assert mirror[("canopy_replication_subscription_apply_errors_total", subscription)] == 0
    assert mirror[("canopy_replication_subscription_tables_not_ready", subscription)] == 0
    assert ("canopy_replication_subscription_applied_lsn_bytes", subscription) not in mirror

    as_superuser = metric_samples(await read_status(p.sub_admin, role="mirror", instance=p.instance))
    assert as_superuser[("canopy_replication_subscription_applied_lsn_bytes", subscription)] > 0

    # A disabled subscription keeps its pg_stat_subscription row, without a worker.
    await p.sub_admin.execute(f'ALTER SUBSCRIPTION "{name}" DISABLE')

    async def worker_stopped():
        samples = metric_samples(await read_status(p.sub_admin, role="mirror", instance=p.instance))
        return samples[("canopy_replication_subscription_worker_up", subscription)] == 0

    await eventually(worker_stopped)


async def _subscribed(p: Pair) -> dict[str, str]:
    """Publish, seed, grant, subscribe, and wait for the copy; returns the seeded ids."""
    await publish(p.pub, role="primary", config=p.config)
    ids = await _seed(p.pub)
    await grants(p.sub, role="mirror", config=p.config)
    await subscribe(p.sub, role="mirror", instance=p.instance, config=p.config)
    await eventually(lambda: _copied(p))
    return ids


async def _reader_dsn(p: Pair) -> str:
    """The primary as its read-only reader role, which ``publish`` granted SELECT on the published tables."""
    password = uuid4().hex
    await p.pub.execute(f"ALTER ROLE \"{p.config.reader_role}\" PASSWORD '{password}'")
    database = await p.pub.fetchval("SELECT current_database()")
    return server_dsn(PUBLISHER_ADMIN, database=database, user=p.config.reader_role, password=password)


async def _drift_connections(p: Pair) -> tuple[asyncpg.Connection, asyncpg.Connection]:
    database = await p.sub.fetchval("SELECT current_database()")
    primary = await asyncpg.connect(await _reader_dsn(p), server_settings=SESSION)
    mirror = await asyncpg.connect(server_dsn(SUBSCRIBER_ADMIN, database=database), server_settings=SESSION)
    return primary, mirror


async def test_drift_check_finds_what_the_mirror_lost(pair: Pair):
    p = pair
    ids = await _subscribed(p)
    old = str(uuid4())
    await p.pub.execute(
        "INSERT INTO squirrel.snapshot (id, title, created_at) VALUES ($1, 'Old', now() - interval '10 days')", old
    )
    await eventually(lambda: _in_step(p))
    old_day = await p.pub.fetchval(
        "SELECT (created_at AT TIME ZONE 'UTC')::date::text FROM squirrel.snapshot WHERE id = $1", old
    )
    today = datetime.now(UTC).date().isoformat()
    primary, mirror = await _drift_connections(p)
    try:
        report = await drift_check(primary, mirror, instance=p.instance)
        assert report["in_step"] and report["compared"] == PRESENT and report["skipped"] == {}

        # A column only the mirror has yet (it migrates first) isn't drift.
        await p.sub_admin.execute("ALTER TABLE squirrel.pv ADD COLUMN zz_history_only text DEFAULT 'x'")
        # What replication never does: an edit made on the mirror, and deletes there that
        # skip the foreign keys, as apply does, leaving a snapshot value without its snapshot.
        await p.sub_admin.execute("UPDATE squirrel.tag SET name = 'Edited on the mirror' WHERE id = $1", ids["tag_a"])
        await p.sub_admin.execute("SET session_replication_role = replica")
        await p.sub_admin.execute("DELETE FROM squirrel.snapshot WHERE id = ANY($1::uuid[])", [ids["snapshot"], old])
        await p.sub_admin.execute("RESET session_replication_role")

        report = await drift_check(primary, mirror, instance=p.instance)
        assert not report["in_step"]
        assert sorted(report["drift"]) == ["squirrel.snapshot", "squirrel.snapshot_value", "squirrel.tag"]
        tag = report["drift"]["squirrel.tag"]
        assert tag["primary"]["rows"] == tag["mirror"]["rows"] and tag["primary"]["md5"] != tag["mirror"]["md5"]
        snapshot = report["drift"]["squirrel.snapshot"]
        assert [d["day"] for d in snapshot["days"]] == [old_day, today] and snapshot["missing_rows"] == 2
        values = report["drift"]["squirrel.snapshot_value"]
        assert values["days"] == [{"day": today, "primary": 1, "mirror": 0}]
        assert values["orphans"] == {"primary": 0, "mirror": 1}

        recent = await drift_check(primary, mirror, instance=p.instance, days=3)
        assert [d["day"] for d in recent["drift"]["squirrel.snapshot"]["days"]] == [today]
    finally:
        await primary.close()
        await mirror.close()


async def test_drift_check_rereads_what_differed_after_settling(pair: Pair):
    """The re-check runs in new transactions, so a mirror that was only behind comes out in step."""
    p = pair
    await _subscribed(p)
    name = f"canopy_history_{p.instance}"
    primary, mirror = await _drift_connections(p)
    try:
        await p.sub_admin.execute(f'ALTER SUBSCRIPTION "{name}" DISABLE')
        await p.pub.execute("INSERT INTO squirrel.snapshot (id, title) VALUES (gen_random_uuid(), 'Not applied yet')")
        behind = await drift_check(primary, mirror, instance=p.instance)
        assert list(behind["drift"]) == ["squirrel.snapshot"]

        async def catch_up():
            await p.sub_admin.execute(f'ALTER SUBSCRIPTION "{name}" ENABLE')
            await eventually(lambda: _in_step(p))

        settled = await drift_check(primary, mirror, instance=p.instance, settle=catch_up)
        assert settled["in_step"] and settled["rechecked"] == ["squirrel.snapshot"]
    finally:
        await primary.close()
        await mirror.close()


async def test_drift_check_command_exit_status(pair: Pair):
    p = pair
    await _subscribed(p)
    database = await p.sub.fetchval("SELECT current_database()")
    reader_dsn = await _reader_dsn(p)
    env = {
        "PATH": os.environ.get("PATH", ""),
        "CANOPY_ROLE": "mirror",
        "CANOPY_INSTANCE": p.instance,
        "CANOPY_DATABASE_URL": server_dsn(SUBSCRIBER_ADMIN, database=database).replace(
            "postgresql://", "postgresql+asyncpg://", 1
        ),
        "CANOPY_REPLICATION__READER_DSN": reader_dsn,
    }

    def drift_check_command(**overrides: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, "-m", "canopy.core.replication", "drift-check", "--settle", "0"],
            env=env | overrides,
            capture_output=True,
            text=True,
            timeout=120,
        )

    in_step = drift_check_command()
    assert in_step.returncode == 0, in_step.stderr
    assert json.loads(in_step.stdout)["in_step"] is True

    await p.sub_admin.execute("DELETE FROM squirrel.pv_tag")
    drifted = drift_check_command()
    assert drifted.returncode == 1
    assert "squirrel.pv_tag" in json.loads(drifted.stdout)["drift"]
    assert "Drift in squirrel.pv_tag" in drifted.stderr

    wrong = "wrong-" + uuid4().hex
    password = reader_dsn.split(":")[2].split("@")[0]
    failed = drift_check_command(CANOPY_REPLICATION__READER_DSN=reader_dsn.replace(password, wrong))
    assert failed.returncode == 2
    assert "password authentication failed" in failed.stderr
    assert wrong not in failed.stderr + failed.stdout


async def _lose_subscription(p: Pair) -> None:
    """What a lost slot leaves behind once cleaned up: the mirror's rows, no subscription, no slot."""
    name = f"canopy_history_{p.instance}"
    await p.sub_admin.execute(f'ALTER SUBSCRIPTION "{name}" DISABLE')

    async def worker_gone():
        return not await p.sub_admin.fetchval(
            "SELECT count(*) FROM pg_stat_subscription s JOIN pg_subscription u ON u.oid = s.subid "
            "WHERE u.subname = $1 AND s.pid IS NOT NULL",
            name,
        )

    await eventually(worker_gone)
    await p.sub_admin.execute(f'ALTER SUBSCRIPTION "{name}" SET (slot_name = NONE)')
    await p.sub_admin.execute(f'DROP SUBSCRIPTION "{name}"')
    await p.pub.execute("SELECT pg_drop_replication_slot($1)", name)


async def _newest_on_mirror(p: Pair) -> datetime:
    return await p.sub.fetchval("SELECT max(created_at) FROM squirrel.snapshot")


async def test_reseed_after_a_lost_slot(pair: Pair):
    p = pair
    ids = await _subscribed(p)
    since = await _newest_on_mirror(p)
    await _lose_subscription(p)

    # The gap: changes the mirror never received.
    gap_snapshot = str(uuid4())
    async with p.pub.transaction():
        await p.pub.execute("INSERT INTO squirrel.snapshot (id, title) VALUES ($1, 'In the gap')", gap_snapshot)
        await p.pub.execute(
            "INSERT INTO squirrel.snapshot_value (id, snapshot_id, pv_id, pv_name) "
            "VALUES (gen_random_uuid(), $1, $2, 'SIM:PV:1:SETPOINT')",
            gap_snapshot,
            ids["pv"],
        )
    await p.pub.execute("UPDATE squirrel.pv SET description = 'edited in the gap'")
    await p.pub.execute("UPDATE squirrel.snapshot SET title = 'Renamed in the gap' WHERE id = $1", ids["snapshot"])
    async with p.pub.transaction():
        await p.pub.execute("DELETE FROM squirrel.pv_tag WHERE pv_id = $1", ids["pv"])
        await p.pub.execute("INSERT INTO squirrel.pv_tag (pv_id, tag_id) VALUES ($1, $2)", ids["pv"], ids["tag_b"])
    await p.pub.execute("DELETE FROM squirrel.tag WHERE id = $1", ids["tag_a"])

    primary, mirror = await _drift_connections(p)
    try:
        report = await reseed(primary, mirror, role="mirror", instance=p.instance, config=p.config, since=since)
        assert report["created"] is True and report["skipped"] == {}
        assert report["tables"]["squirrel.snapshot"]["read"] == 2
        assert report["tables"]["squirrel.tag"]["deleted"] == 1

        # Replication carries on from the cut.
        await p.pub.execute("UPDATE squirrel.pv SET description = 'after the re-seed'")
        await eventually(lambda: _in_step(p))
        status = (await subscriber_status(p.sub_admin, instance=p.instance))["subscription"]
        assert status["enabled"] and status["apply_error_count"] == 0
        assert (await drift_check(primary, mirror, instance=p.instance))["in_step"]
    finally:
        await primary.close()
        await mirror.close()


async def test_reseed_copies_a_facility_tree(pair: Pair):
    """``core.facility`` references itself, so a parent and its child go in the same statement."""
    p = pair
    ids = await _subscribed(p)
    gone, gone_child = str(uuid4()), str(uuid4())
    async with p.pub.transaction():
        await p.pub.execute("INSERT INTO core.facility (id, slug, name) VALUES ($1, 'gone', 'Gone')", gone)
        await p.pub.execute(
            "INSERT INTO core.facility (id, slug, name, parent_id) VALUES ($1, 'gone-a', 'Gone A', $2)",
            gone_child,
            gone,
        )
    await eventually(lambda: _in_step(p))
    since = await _newest_on_mirror(p)
    await _lose_subscription(p)

    # The gap: the old tree removed child first, as the parent's RESTRICT makes the primary
    # do, and a new tree with a PV assigned to its child.
    await p.pub.execute("DELETE FROM core.facility WHERE id = $1", gone_child)
    await p.pub.execute("DELETE FROM core.facility WHERE id = $1", gone)
    site, site_a = str(uuid4()), str(uuid4())
    async with p.pub.transaction():
        await p.pub.execute("INSERT INTO core.facility (id, slug, name) VALUES ($1, 'site', 'Site')", site)
        await p.pub.execute(
            "INSERT INTO core.facility (id, slug, name, parent_id) VALUES ($1, 'site-a', 'Site A', $2)", site_a, site
        )
        await p.pub.execute("INSERT INTO squirrel.pv_facility (pv_id, facility_id) VALUES ($1, $2)", ids["pv"], site_a)

    primary, mirror = await _drift_connections(p)
    try:
        report = await reseed(primary, mirror, role="mirror", instance=p.instance, config=p.config, since=since)

        assert report["tables"]["core.facility"]["deleted"] == 2
        assert await p.sub.fetch(
            "SELECT f.slug, parent.slug AS parent FROM core.facility f "
            "LEFT JOIN core.facility parent ON parent.id = f.parent_id ORDER BY f.slug"
        ) == [("site", None), ("site-a", "site")]
        assert (
            await p.sub.fetchval("SELECT facility_id::text FROM squirrel.pv_facility WHERE pv_id = $1", ids["pv"])
            == site_a
        )
        assert (await drift_check(primary, mirror, instance=p.instance))["in_step"]
    finally:
        await primary.close()
        await mirror.close()


async def test_reseed_hands_over_to_replication_at_the_cut(pair: Pair):
    """Committed before the cut: copied, not replayed. Committed after the snapshot: streamed, not copied."""
    p = pair
    await _subscribed(p)
    name = f"canopy_history_{p.instance}"
    since = await _newest_on_mirror(p)
    await p.sub_admin.execute(f'ALTER SUBSCRIPTION "{name}" DISABLE')
    before, after = str(uuid4()), str(uuid4())
    # Still in the slot, since the mirror is disabled: replaying it after the copy would stop apply.
    await p.pub.execute("INSERT INTO squirrel.snapshot (id, title) VALUES ($1, 'Before the cut')", before)

    async def write_after_the_snapshot():
        await p.pub.execute("INSERT INTO squirrel.snapshot (id, title) VALUES ($1, 'After the snapshot')", after)

    primary, mirror = await _drift_connections(p)
    try:
        report = await reseed(
            primary,
            mirror,
            role="mirror",
            instance=p.instance,
            config=p.config,
            since=since,
            after_snapshot=write_after_the_snapshot,
        )
        assert report["created"] is False
        assert report["tables"]["squirrel.snapshot"]["read"] == 2  # the seeded one and `before`

        await eventually(lambda: _in_step(p))
        titles = {row["title"] for row in await p.sub.fetch("SELECT title FROM squirrel.snapshot")}
        assert {"Before the cut", "After the snapshot"} <= titles
        status = (await subscriber_status(p.sub_admin, instance=p.instance))["subscription"]
        assert status["apply_error_count"] == 0
    finally:
        await primary.close()
        await mirror.close()


async def test_reseed_refusals(pair: Pair):
    p = pair
    await _subscribed(p)
    name = f"canopy_history_{p.instance}"
    newest = await _newest_on_mirror(p)
    primary, mirror = await _drift_connections(p)
    try:
        with pytest.raises(Refused, match="is enabled"):
            await reseed(primary, mirror, role="mirror", instance=p.instance, config=p.config, since=newest)
        await p.sub_admin.execute(f'ALTER SUBSCRIPTION "{name}" DISABLE')
        with pytest.raises(Refused, match="later than the newest snapshot"):
            await reseed(
                primary,
                mirror,
                role="mirror",
                instance=p.instance,
                config=p.config,
                since=newest + timedelta(seconds=1),
            )
        # The tables' owner can't advance a replication origin without a grant.
        with pytest.raises(Refused, match="pg_replication_origin_advance"):
            await reseed(primary, p.sub, role="mirror", instance=p.instance, config=p.config, since=newest)
        with pytest.raises(Refused, match="runs on a mirror"):
            await reseed(primary, mirror, role="primary", instance=p.instance, config=p.config, since=newest)
        assert not (await subscriber_status(p.sub_admin, instance=p.instance))["subscription"]["enabled"]
    finally:
        await primary.close()
        await mirror.close()


async def test_reseed_command(pair: Pair):
    p = pair
    await _subscribed(p)
    name = f"canopy_history_{p.instance}"
    since = (await _newest_on_mirror(p)).isoformat()
    database = await p.sub.fetchval("SELECT current_database()")
    env = {
        "PATH": os.environ.get("PATH", ""),
        "CANOPY_ROLE": "mirror",
        "CANOPY_INSTANCE": p.instance,
        "CANOPY_DATABASE_URL": server_dsn(SUBSCRIBER_ADMIN, database=database).replace(
            "postgresql://", "postgresql+asyncpg://", 1
        ),
        "CANOPY_REPLICATION__READER_DSN": await _reader_dsn(p),
    }

    def reseed_command() -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, "-m", "canopy.core.replication", "reseed", "--since", since],
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
        )

    refused = reseed_command()
    assert refused.returncode == 2 and "is enabled" in refused.stderr

    await p.sub_admin.execute(f'ALTER SUBSCRIPTION "{name}" DISABLE')
    done = reseed_command()
    assert done.returncode == 0, done.stderr
    assert json.loads(done.stdout)["subscription"] == name
    await p.pub.execute("UPDATE squirrel.pv SET description = 'after the command'")
    await eventually(lambda: _in_step(p))


async def test_reseed_cut_waits_for_open_write_transactions(pair: Pair, monkeypatch):
    p = pair
    await _subscribed(p)
    name = f"canopy_history_{p.instance}"
    since = await _newest_on_mirror(p)
    await p.sub_admin.execute(f'ALTER SUBSCRIPTION "{name}" DISABLE')
    primary, mirror = await _drift_connections(p)
    writer = await asyncpg.connect(
        server_dsn(PUBLISHER_ADMIN, database=await p.pub.fetchval("SELECT current_database()"))
    )
    try:
        # Too long: refused before the mirror is touched.
        monkeypatch.setattr(reseed_module, "CUT_WAIT_S", 0.5)
        open_write = writer.transaction()
        await open_write.start()
        await writer.execute("INSERT INTO squirrel.snapshot (id, title) VALUES (gen_random_uuid(), 'Open')")
        with pytest.raises(Refused, match="has been open"):
            await reseed(primary, mirror, role="mirror", instance=p.instance, config=p.config, since=since)
        assert not (await subscriber_status(p.sub_admin, instance=p.instance))["subscription"]["enabled"]

        # Long enough: the cut waits until the transaction ends (here, rolled back).
        monkeypatch.setattr(reseed_module, "CUT_WAIT_S", 30.0)
        task = asyncio.create_task(
            reseed(primary, mirror, role="mirror", instance=p.instance, config=p.config, since=since)
        )
        await asyncio.sleep(0.5)
        assert not task.done()
        await open_write.rollback()
        report = await task
        assert report["open_at_cut"] >= 1  # the writer, plus anything else running on the server
        await eventually(lambda: _in_step(p))
    finally:
        await writer.close()
        await primary.close()
        await mirror.close()


async def test_reseed_waits_for_the_apply_worker_to_stop(pair: Pair, monkeypatch):
    p = pair
    await _subscribed(p)
    oid = await p.sub_admin.fetchval(
        "SELECT oid FROM pg_subscription WHERE subname = $1", f"canopy_history_{p.instance}"
    )
    monkeypatch.setattr(reseed_module, "WORKER_WAIT_S", 0.5)
    with pytest.raises(Refused, match="apply worker is still running"):
        await reseed_module._wait_for_no_worker(p.sub_admin, oid)
    await p.sub_admin.execute(f'ALTER SUBSCRIPTION "canopy_history_{p.instance}" DISABLE')
    monkeypatch.setattr(reseed_module, "WORKER_WAIT_S", 10.0)
    await reseed_module._wait_for_no_worker(p.sub_admin, oid)
