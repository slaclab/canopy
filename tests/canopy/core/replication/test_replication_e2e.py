"""Replication end to end: arms and a head on their own Postgres 16 servers.

Opt-in: the ``octopus`` fixture skips unless the ``CANOPY_TEST_REPLICATION_*``
variables point at the servers, as ``docker-compose.yml`` in this directory starts
them. Set up the way the deploy repos will: an arm's tables belong to a superuser
(as prod's do today), while the head's belong to an ordinary owner role granted
``pg_create_subscription``, which runs ``grants``, ``subscribe`` and ``refresh``.
"""

from uuid import uuid4

import pytest
import asyncpg
from pydantic import SecretStr

from canopy.core.arms import Arm, ArmsConfig, subscription_name
from canopy.core.replication.sql import Refused, ReplicationError
from canopy.core.replication.registry import REGISTRY, TableClass, ReplicationRegistry
from canopy.core.replication.publisher import publish, publisher_status
from canopy.core.replication.subscriber import (
    grants,
    refresh,
    subscribe,
    subscriber_status,
)

from .conftest import Octopus, copied, eventually

PRESENT = sorted(REGISTRY.replicated)
one_arm = pytest.mark.parametrize("octopus", [1], indirect=True)


async def _rows(conn: asyncpg.Connection, table: str) -> list:
    return [tuple(row.values()) for row in await conn.fetch(f"SELECT * FROM {table} ORDER BY 1, 2")]


async def _in_step(o: Octopus) -> bool:
    for table in PRESENT:
        if await _rows(o.arm.admin, table) != await _rows(o.head.conn, table):
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
        "VALUES ($1, 'arm-only', repeat('a', 64), true, true, false, false)",
        ids["key"],
    )
    return ids


async def _subscribe(o: Octopus, **kwargs) -> dict:
    return await subscribe(o.head.conn, role="mirror", head=o.head.name, arms=o.arms_config.arms, **kwargs)


@one_arm
async def test_copy_then_stream_changes(octopus: Octopus):
    o, arm = octopus, octopus.arm
    published = await publish(arm.admin, role="primary", config=o.config)
    assert published["roles"] == {o.config.replicator_role: "granted", o.config.reader_role: "granted"}
    ids = await _seed(arm.admin)
    await grants(o.head.conn, role="mirror", config=o.config)

    created = await _subscribe(o)
    assert [(sub["arm"], sub["created"]) for sub in created["subscriptions"]] == [(arm.name, True)]

    await eventually(lambda: copied(o, arm.name))
    assert await _in_step(o)
    assert await o.head.conn.fetchval("SELECT count(*) FROM core.api_key") == 0

    # The head's app role reads the copy and can't write it.
    await o.head.admin.execute(f'SET ROLE "{o.config.app_role}"')
    assert await o.head.admin.fetchval("SELECT count(*) FROM squirrel.snapshot_value") == 1
    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        await o.head.admin.execute("DELETE FROM squirrel.snapshot")
    await o.head.admin.execute("RESET ROLE")

    # Routine edits: a new snapshot, a PV edit, replacing a PV's tags (delete then
    # insert of link rows, which stalls an insert/update-only publication) and
    # deleting a snapshot with its values.
    new_snapshot = str(uuid4())
    async with arm.admin.transaction():
        await arm.admin.execute("INSERT INTO squirrel.snapshot (id, title) VALUES ($1, 'After')", new_snapshot)
        await arm.admin.execute(
            "INSERT INTO squirrel.snapshot_value (id, snapshot_id, pv_id, pv_name) "
            "VALUES (gen_random_uuid(), $1, $2, 'SIM:PV:1:SETPOINT')",
            new_snapshot,
            ids["pv"],
        )
    await arm.admin.execute("UPDATE squirrel.pv SET description = 'edited' WHERE id = $1", ids["pv"])
    async with arm.admin.transaction():
        await arm.admin.execute("DELETE FROM squirrel.pv_tag WHERE pv_id = $1", ids["pv"])
        await arm.admin.execute(
            "INSERT INTO squirrel.pv_tag (pv_id, tag_id) VALUES ($1, $2), ($1, $3)",
            ids["pv"],
            ids["tag_a"],
            ids["tag_b"],
        )
    await arm.admin.execute("DELETE FROM squirrel.snapshot WHERE id = $1", ids["snapshot"])

    await eventually(lambda: _in_step(o))
    assert await o.head.conn.fetchval("SELECT count(*) FROM squirrel.pv_tag") == 2

    primary = await publisher_status(arm.admin, config=o.config)
    slot = next(s for s in primary["slots"] if s["slot_name"] == subscription_name(arm.name))
    assert slot["kind"] == "subscription" and slot["active"] is True
    assert slot["wal_status"] == "reserved"

    [head] = (await subscriber_status(o.head.admin, arms=o.arms_config.arms))["subscriptions"]
    assert head["arm"] == arm.name and head["applied_lsn"] not in (None, "0/0")
    assert head["apply_error_count"] == 0 and head["sync_error_count"] == 0
    assert head["streaming"] == "p" and head["owner"] == o.head.owner
    # The owner role, like a health check without a grant, can't read the applied position.
    [as_owner] = (await subscriber_status(o.head.conn, arms=o.arms_config.arms))["subscriptions"]
    assert as_owner["applied_lsn"] is None and "superuser" in as_owner["applied_lsn_error"]

    again = await _subscribe(o)
    assert again == {"subscriptions": [{"arm": arm.name, "name": subscription_name(arm.name), "created": False}]}


@one_arm
async def test_refresh_copies_a_newly_published_table(octopus: Octopus):
    o, arm = octopus, octopus.arm
    await publish(arm.admin, role="primary", config=o.config)
    await _subscribe(o)
    await eventually(lambda: copied(o, arm.name))

    # A migration adds a table on both sides (head first), and the registry classifies it.
    for conn in (o.head.conn, arm.admin):
        await conn.execute("CREATE TABLE squirrel.zz_probe (id int PRIMARY KEY, note text)")
    await arm.admin.execute("INSERT INTO squirrel.zz_probe VALUES (1, 'one'), (2, 'two')")
    probe = ReplicationRegistry(
        tables={**REGISTRY.tables, "squirrel.zz_probe": TableClass.REFERENCE}, publications=REGISTRY.publications
    )
    assert (await publish(arm.admin, role="primary", config=o.config, registry=probe))["publications"][
        "canopy_history"
    ]["added"] == ["squirrel.zz_probe"]

    await o.head.conn.execute("INSERT INTO squirrel.zz_probe VALUES (1, 'stray')")
    with pytest.raises(Refused, match="already have rows: squirrel.zz_probe"):
        await refresh(o.head.conn, role="mirror", arms=o.arms_config.arms, registry=probe)
    await o.head.conn.execute("DELETE FROM squirrel.zz_probe")

    [result] = (await refresh(o.head.conn, role="mirror", arms=o.arms_config.arms, registry=probe))["subscriptions"]
    assert result["added"] == ["squirrel.zz_probe"]

    async def probe_copied():
        return await _rows(o.head.conn, "squirrel.zz_probe") == [(1, "one"), (2, "two")]

    await eventually(probe_copied)
    [again] = (await refresh(o.head.conn, role="mirror", arms=o.arms_config.arms, registry=probe))["subscriptions"]
    assert again["added"] == []


@one_arm
async def test_a_failed_subscribe_never_shows_the_password(octopus: Octopus, caplog):
    o, arm = octopus, octopus.arm
    await publish(arm.admin, role="primary", config=o.config)
    wrong = "wrong-" + uuid4().hex
    arms = {arm.name: Arm(publisher_dsn=SecretStr(arm.publisher_dsn.rsplit("password=", 1)[0] + f"password={wrong}"))}

    with pytest.raises(ReplicationError, match=f"failed for {arm.name}") as caught:
        await subscribe(o.head.conn, role="mirror", head=o.head.name, arms=arms)

    assert "password authentication failed" in str(caught.value)
    assert wrong not in str(caught.value) and wrong not in caplog.text
    assert not (await subscriber_status(o.head.admin, arms=arms))["subscriptions"][0]["exists"]
    assert not await arm.admin.fetchval("SELECT count(*) FROM pg_replication_slots WHERE database = current_database()")


async def test_a_head_subscribes_one_arm_until_rows_carry_their_owner(octopus: Octopus):
    o = octopus
    first, second = o.arms
    for arm in o.arms.values():
        await publish(arm.admin, role="primary", config=o.config)

    with pytest.raises(Refused, match="subscribes to one arm"):
        await _subscribe(o)
    assert [
        sub["exists"] for sub in (await subscriber_status(o.head.admin, arms=o.arms_config.arms))["subscriptions"]
    ] == [
        False,
        False,
    ]

    await _subscribe(o, only=first)
    with pytest.raises(Refused, match="subscribes to one arm"):
        await _subscribe(o, only=second)

    status = await subscriber_status(o.head.admin, arms=o.arms_config.arms)
    assert [(sub["arm"], sub["exists"]) for sub in status["subscriptions"]] == [(first, True), (second, False)]
    # Refreshing every arm skips the one without a subscription.
    refreshed = await refresh(o.head.conn, role="mirror", arms=o.arms_config.arms)
    assert [(sub["arm"], sub["refreshed"]) for sub in refreshed["subscriptions"]] == [(first, True), (second, False)]
    # An arm taken out of the settings leaves its subscription behind, reported as unexpected.
    remaining = ArmsConfig(arms={second: o.arms_config.arms[second]}).arms
    assert (await subscriber_status(o.head.admin, arms=remaining))["unexpected"] == [subscription_name(first)]
