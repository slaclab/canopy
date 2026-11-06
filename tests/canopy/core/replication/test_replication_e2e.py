"""Replication end to end: arms and a head on their own Postgres 16 servers.

Opt-in: the ``octopus`` fixture skips unless the ``CANOPY_TEST_REPLICATION_*``
variables point at the servers, as ``docker-compose.yml`` in this directory starts
them. Set up the way the deploy repos will: an arm's tables belong to a superuser
(as prod's do today), while the head's belong to an ordinary owner role granted
``pg_create_subscription``, which runs ``grants``, ``subscribe`` and ``refresh``.
"""

from uuid import uuid4
from datetime import UTC, datetime

import pytest
import asyncpg
from pydantic import SecretStr
from sqlalchemy.engine import make_url

from canopy.core.arms import Arm, subscription_name
from canopy.core.replication.sql import Refused, ReplicationError, owner_join
from canopy.core.replication.metrics import read_status
from canopy.core.replication.registry import (
    REGISTRY,
    OwnerPath,
    TableClass,
    ReplicationRegistry,
)
from canopy.core.replication.publisher import publish, publisher_status
from canopy.core.replication.subscriber import (
    grants,
    refresh,
    subscribe,
    subscriber_status,
)

from .conftest import (
    Octopus,
    copied,
    seed_rows,
    eventually,
    build_schema,
    metric_samples,
)

PRESENT = sorted(REGISTRY.replicated)
one_arm = pytest.mark.parametrize("octopus", [1], indirect=True)


async def _rows(conn: asyncpg.Connection, table: str, owner: str, registry: ReplicationRegistry = REGISTRY) -> list:
    """``table``'s rows owned by ``owner``."""
    join_sql, owner_sql = owner_join(registry, table, alias="t")
    rows = await conn.fetch(f"SELECT t.* FROM {table} t {join_sql} WHERE {owner_sql} = $1 ORDER BY 1, 2", owner)
    return [tuple(row.values()) for row in rows]


async def _in_step(o: Octopus, arm: str) -> bool:
    """Whether the head holds exactly ``arm``'s rows, as the arm has them."""
    for table in PRESENT:
        if await _rows(o.arms[arm].admin, table, arm) != await _rows(o.head.conn, table, arm):
            return False
    return True


async def _subscribe(o: Octopus, **kwargs) -> dict:
    return await subscribe(
        o.head.conn, role="mirror", head=o.head.name, arms=o.arms_config.arms, connect_arm=o.connect_arm, **kwargs
    )


async def _refresh(o: Octopus, **kwargs) -> dict:
    return await refresh(o.head.conn, role="mirror", arms=o.arms_config.arms, connect_arm=o.connect_arm, **kwargs)


async def _grants(o: Octopus) -> dict:
    return await grants(o.head.conn, role="mirror", head=o.head.name, config=o.config)


@one_arm
async def test_copy_then_stream_changes(octopus: Octopus):
    o, arm = octopus, octopus.arm
    published = await publish(arm.admin, role="primary", config=o.config)
    assert published["roles"] == {o.config.replicator_role: "granted", o.config.reader_role: "granted"}
    ids = await seed_rows(arm.admin, arm.name)
    await arm.admin.execute(
        "INSERT INTO core.api_key (id, app_name, token_hash, is_active, read_access, write_access, admin_access) "
        "VALUES (gen_random_uuid(), 'arm-only', repeat('a', 64), true, true, false, false)"
    )
    await _grants(o)

    created = await _subscribe(o)
    assert [(sub["arm"], sub["created"]) for sub in created["subscriptions"]] == [(arm.name, True)]

    await eventually(lambda: copied(o, arm.name))
    assert await _in_step(o, arm.name)
    assert await o.head.conn.fetchval("SELECT count(*) FROM core.api_key") == 0

    # Routine edits: a new snapshot, a PV edit, replacing a PV's tags (delete then
    # insert of link rows, which stalls an insert/update-only publication) and
    # deleting a snapshot with its values.
    new_snapshot = str(uuid4())
    async with arm.admin.transaction():
        await arm.admin.execute(
            "INSERT INTO squirrel.snapshot (id, title, owner_instance) VALUES ($1, 'After', $2)", new_snapshot, arm.name
        )
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

    await eventually(lambda: _in_step(o, arm.name))
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

    # A migration adds a root table on both sides (head first), and the registry classifies it.
    for conn in (o.head.conn, arm.admin):
        await conn.execute(
            "CREATE TABLE squirrel.zz_probe (id int PRIMARY KEY, note text, owner_instance text NOT NULL)"
        )
    await arm.admin.execute("INSERT INTO squirrel.zz_probe VALUES (1, 'one', $1), (2, 'two', $1)", arm.name)
    probe = ReplicationRegistry(
        tables={**REGISTRY.tables, "squirrel.zz_probe": TableClass.REFERENCE},
        publications=REGISTRY.publications,
        owner_paths={**REGISTRY.owner_paths, "squirrel.zz_probe": OwnerPath(column="owner_instance")},
        shared_keys=REGISTRY.shared_keys,
    )
    assert (await publish(arm.admin, role="primary", config=o.config, registry=probe))["publications"][
        "canopy_history"
    ]["added"] == ["squirrel.zz_probe"]

    # The head's own rows in the new table are expected; a row of the arm's isn't.
    await o.head.conn.execute("INSERT INTO squirrel.zz_probe VALUES (10, 'mine', $1)", o.head.name)
    await o.head.conn.execute("INSERT INTO squirrel.zz_probe VALUES (1, 'stray', $1)", arm.name)
    with pytest.raises(Refused, match="already hold that arm's rows: squirrel.zz_probe"):
        await _refresh(o, registry=probe)
    await o.head.conn.execute("DELETE FROM squirrel.zz_probe WHERE id = 1")

    [result] = (await _refresh(o, registry=probe))["subscriptions"]
    assert result["added"] == ["squirrel.zz_probe"]

    async def probe_copied():
        return await _rows(o.head.conn, "squirrel.zz_probe", arm.name, probe) == [
            (1, "one", arm.name),
            (2, "two", arm.name),
        ]

    await eventually(probe_copied)
    assert await _rows(o.head.conn, "squirrel.zz_probe", o.head.name, probe) == [(10, "mine", o.head.name)]
    [again] = (await _refresh(o, registry=probe))["subscriptions"]
    assert again["added"] == []


@one_arm
async def test_a_failed_subscribe_never_shows_the_password(octopus: Octopus, caplog):
    o, arm = octopus, octopus.arm
    await publish(arm.admin, role="primary", config=o.config)
    wrong = "wrong-" + uuid4().hex
    arms = {arm.name: Arm(publisher_dsn=SecretStr(arm.publisher_dsn.rsplit("password=", 1)[0] + f"password={wrong}"))}

    with pytest.raises(ReplicationError, match=f"failed for {arm.name}") as caught:
        await subscribe(o.head.conn, role="mirror", head=o.head.name, arms=arms, connect_arm=o.connect_arm)

    assert "password authentication failed" in str(caught.value)
    assert wrong not in str(caught.value) and wrong not in caplog.text
    assert not (await subscriber_status(o.head.admin, arms=arms))["subscriptions"][0]["exists"]
    assert not await arm.admin.fetchval("SELECT count(*) FROM pg_replication_slots WHERE database = current_database()")


async def test_two_arms_and_the_heads_own_rows_share_the_tables(octopus: Octopus):
    o = octopus
    first, second = o.arms
    for name, arm in o.arms.items():
        await publish(arm.admin, role="primary", config=o.config)
        # The same setpoint address and tag-group name on both arms: unique per owner.
        await seed_rows(arm.admin, name)
    await _grants(o)
    # The head's API writes rows of its own, through its app role and row-level security.
    await o.head.admin.execute(f'SET ROLE "{o.config.app_role}"')
    own = await seed_rows(o.head.admin, o.head.name)
    await o.head.admin.execute("RESET ROLE")

    created = await _subscribe(o)
    assert [(sub["arm"], sub["created"]) for sub in created["subscriptions"]] == [(first, True), (second, True)]
    for name in o.arms:
        await eventually(lambda name=name: copied(o, name))
        assert await _in_step(o, name)
    assert (
        await o.head.conn.fetchval("SELECT count(*) FROM squirrel.pv WHERE setpoint_address = 'SIM:PV:1:SETPOINT'") == 3
    )

    # A change on one arm reaches only that arm's rows on the head.
    await o.arms[second].admin.execute("UPDATE squirrel.pv SET description = 'edited on arm2'")
    await eventually(lambda: _in_step(o, second))
    assert await o.head.conn.fetchval(
        "SELECT count(*) FROM squirrel.pv WHERE description = 'edited on arm2'"
    ) == 1 and await _in_step(o, first)

    [only_second] = (await _refresh(o, only=second))["subscriptions"]
    assert only_second["arm"] == second
    status = await subscriber_status(o.head.admin, arms=o.arms_config.arms)
    assert [sub["arm"] for sub in status["subscriptions"]] == [first, second] and status["unexpected"] == []

    # The app role writes the head's rows and no arm's. Values arrive by COPY, as the
    # head's own captures write them; only root tables have row-level security.
    await o.head.admin.execute(f'SET ROLE "{o.config.app_role}"')
    try:
        assert await o.head.admin.execute("UPDATE squirrel.pv SET description = 'from the head'") == "UPDATE 1"
        assert await o.head.admin.execute("DELETE FROM squirrel.snapshot WHERE owner_instance <> $1", o.head.name) == (
            "DELETE 0"
        )
        with pytest.raises(asyncpg.InsufficientPrivilegeError, match="row-level security"):
            await o.head.admin.execute(
                "INSERT INTO squirrel.snapshot (id, title, owner_instance) VALUES (gen_random_uuid(), 'x', $1)", first
            )
        snapshot = str(uuid4())
        await o.head.admin.execute(
            "INSERT INTO squirrel.snapshot (id, title, owner_instance) VALUES ($1, 'Captured here', $2)",
            snapshot,
            o.head.name,
        )
        await o.head.admin.copy_records_to_table(
            "snapshot_value",
            schema_name="squirrel",
            columns=["id", "snapshot_id", "pv_id", "pv_name", "timestamp"],
            records=[(str(uuid4()), snapshot, own["pv"], "SIM:PV:1:SETPOINT", datetime.now(UTC))],
        )
    finally:
        await o.head.admin.execute("RESET ROLE")
    assert (
        await o.head.conn.fetchval("SELECT count(*) FROM squirrel.snapshot_value WHERE snapshot_id = $1", snapshot) == 1
    )
    for name in o.arms:
        assert await _in_step(o, name)


@pytest.mark.parametrize("octopus", [2], indirect=True)
async def test_subscribe_refuses_rows_the_head_already_holds_for_that_arm(octopus: Octopus):
    o = octopus
    first, second = o.arms
    for arm in o.arms.values():
        await publish(arm.admin, role="primary", config=o.config)
    await o.head.conn.execute(
        "INSERT INTO squirrel.tag_group (id, name, owner_instance) VALUES (gen_random_uuid(), 'Leftover', $1)", second
    )

    with pytest.raises(Refused, match=f"arm {second} would copy into tables that already hold that arm's rows"):
        await _subscribe(o)

    # Checked before anything was created, for either arm.
    status = await subscriber_status(o.head.admin, arms=o.arms_config.arms)
    assert [sub["exists"] for sub in status["subscriptions"]] == [False, False]
    assert (await _subscribe(o, only=first))["subscriptions"][0]["created"] is True


async def test_subscribe_refuses_a_slug_another_owner_already_has(octopus: Octopus):
    o = octopus
    first, second = o.arms
    for name, arm in o.arms.items():
        await publish(arm.admin, role="primary", config=o.config)
        await seed_rows(arm.admin, name, slug="same")
    await _subscribe(o, only=first)
    await eventually(lambda: copied(o, first))

    with pytest.raises(Refused, match=f"core.facility.slug 'fac_same' \\(here, owned by {first}\\)") as caught:
        await _subscribe(o, only=second)

    assert f"magpie.config.slug 'cfg_same' (here, owned by {first})" in str(caught.value)
    assert not (await subscriber_status(o.head.admin, arms=o.arms_config.arms, only=second))["subscriptions"][0][
        "exists"
    ]
    assert not await o.arms[second].admin.fetchval(
        "SELECT count(*) FROM pg_replication_slots WHERE database = current_database()"
    )


@one_arm
async def test_a_second_head_cant_take_an_arms_slot(octopus: Octopus):
    o, arm = octopus, octopus.arm
    await publish(arm.admin, role="primary", config=o.config)
    await _subscribe(o)
    other_db = f"{make_url(o.head.dsn).database}_second"
    await o.head.admin.execute(f'CREATE DATABASE "{other_db}" OWNER "{o.head.owner}"')
    other_dsn = make_url(o.head.dsn).set(database=other_db).render_as_string(hide_password=False)
    try:
        await build_schema(other_dsn)
        other = await asyncpg.connect(other_dsn)
        try:
            with pytest.raises(
                ReplicationError, match=f'replication slot "{subscription_name(arm.name)}" already exists'
            ):
                await subscribe(
                    other, role="mirror", head="other_head", arms=o.arms_config.arms, connect_arm=o.connect_arm
                )
        finally:
            await other.close()
    finally:
        await o.head.admin.execute(f'DROP DATABASE "{other_db}" WITH (FORCE)')


@pytest.mark.parametrize("bare_octopus", [2], indirect=True)
async def test_bare_octopus_has_its_roles_and_no_tables(bare_octopus: Octopus):
    o = bare_octopus
    for conn in (o.head.conn, *(arm.admin for arm in o.arms.values())):
        assert not await conn.fetchval(
            "SELECT count(*) FROM pg_tables WHERE schemaname = ANY($1::text[])", REGISTRY.schemas
        )
    # The head's owner can create the schema itself and subscribe, as in production.
    assert await o.head.conn.fetchval("SELECT pg_has_role(current_user, 'pg_create_subscription', 'MEMBER')")
    assert await o.head.conn.fetchval("SELECT has_database_privilege(current_database(), 'CREATE')")


async def test_gauges_on_the_arms_and_per_arm_on_the_head(octopus: Octopus, monkeypatch):
    o = octopus
    first, second = o.arms.values()
    monkeypatch.setattr("canopy.core.replication.metrics.get_arms_config", lambda: o.arms_config)
    for arm in o.arms.values():
        await publish(arm.admin, role="primary", config=o.config)
    await seed_rows(first.admin, first.name)
    await grants(o.head.conn, role="mirror", head=o.head.name, config=o.config)
    await _subscribe(o, only=first.name)  # the second arm stays listed without a subscription
    await eventually(lambda: copied(o, first.name))
    # One streamed change, so the applied position moves past the initial copy's 0/0.
    await first.admin.execute("UPDATE squirrel.pv SET description = 'streamed'")
    await eventually(lambda: _in_step(o, first.name))

    slot = frozenset({"slot": subscription_name(first.name), "kind": "subscription"}.items())
    on_first = metric_samples(await read_status(first.admin, role="primary"))
    assert on_first[("canopy_replication_slot_active", slot)] == 1
    assert on_first[("canopy_replication_slot_wal_status", slot | {("wal_status", "reserved")})] == 1
    assert on_first[("canopy_replication_slot_retained_bytes", slot)] >= 0
    assert ("canopy_replication_slot_lag_bytes", slot) in on_first
    assert metric_samples(await read_status(second.admin, role="primary")) == {
        ("canopy_replication_status_up", frozenset()): 1
    }

    # The head's API reads its gauges as the app role, which can't see the applied position.
    arm1, arm2 = (frozenset({"arm": arm.name}.items()) for arm in (first, second))
    await o.head.admin.execute(f'SET ROLE "{o.config.app_role}"')
    head = metric_samples(await read_status(o.head.admin, role="mirror"))
    await o.head.admin.execute("RESET ROLE")
    assert head[("canopy_replication_subscription_exists", arm1)] == 1
    assert head[("canopy_replication_subscription_worker_up", arm1)] == 1
    assert head[("canopy_replication_subscription_receive_age_seconds", arm1)] < 60
    assert head[("canopy_replication_subscription_apply_errors_total", arm1)] == 0
    assert head[("canopy_replication_subscription_tables_not_ready", arm1)] == 0
    assert ("canopy_replication_subscription_applied_lsn_bytes", arm1) not in head
    assert head[("canopy_replication_subscription_exists", arm2)] == 0
    assert not [name for name, labels in head if labels == arm2 and not name.endswith("_exists")]
    assert head[("canopy_replication_subscriptions_unexpected", frozenset())] == 0

    as_superuser = metric_samples(await read_status(o.head.admin, role="mirror"))
    assert as_superuser[("canopy_replication_subscription_applied_lsn_bytes", arm1)] > 0

    # A disabled subscription keeps its pg_stat_subscription row, without a worker.
    await o.head.admin.execute(f'ALTER SUBSCRIPTION "{subscription_name(first.name)}" DISABLE')

    async def worker_stopped():
        samples = metric_samples(await read_status(o.head.admin, role="mirror"))
        return samples[("canopy_replication_subscription_worker_up", arm1)] == 0

    await eventually(worker_stopped)
