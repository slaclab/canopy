"""Replication end to end: a primary and its mirror on two Postgres 16 servers.

Opt-in: skipped unless the three ``CANOPY_TEST_REPLICATION_*`` variables point at a
pair of servers, as ``docker-compose.yml`` in this directory starts them.

Set up the way the deploy repos will: the primary's tables belong to a superuser,
while the mirror's tables belong to an ordinary owner role granted
``pg_create_subscription``, which runs ``grants``, ``subscribe`` and ``refresh``.
Each test gets its own databases and roles on both servers.
"""

from uuid import uuid4
from dataclasses import dataclass

import pytest
import asyncpg
import pytest_asyncio
from pydantic import SecretStr

from canopy.core.replication.sql import Refused, ReplicationError
from canopy.core.replication.config import ReplicationConfig
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
