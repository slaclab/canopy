"""The head's commands without an arm: ``grants``, what ``subscribe``, ``refresh`` and
``status`` do before any subscription exists, and the catalog reads behind
``/v1/instance``'s arms. Subscribing for real is in ``test_replication_e2e.py``."""

from uuid import uuid4

import pytest
import asyncpg
import pytest_asyncio
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.pool import NullPool
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from canopy.core.arms import Arm
from canopy.core.instance import arm_states
from canopy.core.replication.sql import Refused, ReplicationError
from canopy.core.replication.registry import REGISTRY
from canopy.core.replication.subscriber import (
    grants,
    refresh,
    subscribe,
    subscriber_status,
)

from .conftest import DB_DSN, privileges, direct_grants

PRESENT = sorted(REGISTRY.replicated)
DSN = "host=arm1.example.org dbname=canopy user=replicator password=s3cret-pw"
ARMS = {"arm1": Arm(publisher_dsn=SecretStr(DSN))}


async def test_grants_reads_on_replicated_tables_and_writes_on_api_keys(db, config):
    await db.execute(f'CREATE ROLE "{config.app_role}"')
    # Anything the app role could write before must go.
    await db.execute(f'GRANT INSERT, DELETE ON squirrel.pv TO "{config.app_role}"')

    result = await grants(db, role="mirror", config=config)

    assert result["replicated"] == PRESENT
    assert result["local"] == ["core.api_key"]
    for table in PRESENT:
        assert await privileges(db, config.app_role, table) == {"SELECT"}, table
    assert await privileges(db, config.app_role, "core.api_key") == {"SELECT", "INSERT", "UPDATE", "DELETE"}
    for schema in REGISTRY.schemas:
        assert await db.fetchval("SELECT has_schema_privilege($1, $2, 'USAGE')", config.app_role, schema)


async def test_grants_makes_tables_created_later_readable(db, config):
    await db.execute(f'CREATE ROLE "{config.app_role}"')
    await grants(db, role="mirror", config=config)

    # The next migration, run by the tables' owner.
    await db.execute("CREATE TABLE squirrel.added_later (id int PRIMARY KEY)")

    assert await privileges(db, config.app_role, "squirrel.added_later") == {"SELECT"}


async def test_grants_twice_is_harmless(db, config):
    await db.execute(f'CREATE ROLE "{config.app_role}"')
    first = await grants(db, role="mirror", config=config)
    assert await grants(db, role="mirror", config=config) == first
    assert await privileges(db, config.app_role, "squirrel.snapshot") == {"SELECT"}


async def test_grants_refusals(db, config):
    with pytest.raises(Refused, match="runs on a mirror"):
        await grants(db, role="primary", config=config)
    with pytest.raises(Refused, match="doesn't exist"):
        await grants(db, role="mirror", config=config)

    await db.execute(f'CREATE ROLE "{config.app_role}" SUPERUSER')
    with pytest.raises(Refused, match="is a superuser"):
        await grants(db, role="mirror", config=config)

    await db.execute(f'ALTER ROLE "{config.app_role}" NOSUPERUSER')
    await db.execute(f'ALTER TABLE squirrel.tag OWNER TO "{config.app_role}"')
    with pytest.raises(Refused, match="owns squirrel.tag"):
        await grants(db, role="mirror", config=config)


@pytest.mark.parametrize(
    ("setup", "refusal"),
    [
        pytest.param(
            ['CREATE ROLE "{helper}"', 'GRANT INSERT ON squirrel.pv TO "{helper}"', 'GRANT "{helper}" TO "{app}"'],
            r"INSERT on squirrel\.pv(?! \(granted)",
            id="inherited-from-another-role",
        ),
        pytest.param(
            ["GRANT UPDATE ON squirrel.snapshot TO PUBLIC"],
            r"UPDATE on squirrel\.snapshot \(granted to PUBLIC\)",
            id="granted-to-public",
        ),
        pytest.param(
            [
                'CREATE ROLE "{helper}"',
                'GRANT UPDATE (title) ON squirrel.snapshot TO "{helper}"',
                'GRANT "{helper}" TO "{app}"',
            ],
            r"UPDATE on squirrel\.snapshot",
            id="column-grant-through-another-role",
        ),
        pytest.param(
            ['GRANT pg_write_all_data TO "{app}"'],
            r"INSERT on magpie\.config",
            id="pg-write-all-data",
        ),
        pytest.param(
            [
                'CREATE ROLE "{helper}"',
                'ALTER TABLE squirrel.tag OWNER TO "{helper}"',
                'GRANT "{helper}" TO "{app}" WITH INHERIT FALSE',
            ],
            r"belongs to rt_reader_\w+, which owns registered tables",
            id="member-of-a-table-owner",
        ),
    ],
)
async def test_grants_refuses_an_app_role_that_can_still_write(db, config, setup, refusal):
    # The reader role isn't used on a mirror, so it doubles as the helper role (and is dropped after).
    await db.execute(f'CREATE ROLE "{config.app_role}"')
    for statement in setup:
        await db.execute(statement.format(app=config.app_role, helper=config.reader_role))

    with pytest.raises(Refused, match=refusal):
        await grants(db, role="mirror", config=config)

    # Rolled back: the app role got none of this run's grants.
    assert await direct_grants(db, config.app_role, "squirrel.snapshot") == set()


async def test_grants_leaves_default_privilege_reads_on_unclassified_tables_alone(db, config):
    await db.execute(f'CREATE ROLE "{config.app_role}"')
    await grants(db, role="mirror", config=config)
    await db.execute("CREATE TABLE squirrel.not_yet_classified (id int PRIMARY KEY)")

    # Its default-privilege SELECT is within policy, so the next run (which warns about it) still passes.
    await grants(db, role="mirror", config=config)
    assert await privileges(db, config.app_role, "squirrel.not_yet_classified") == {"SELECT"}


async def _subscriptions_here(conn) -> int:
    return await conn.fetchval(
        "SELECT count(*) FROM pg_subscription WHERE subdbid = (SELECT oid FROM pg_database WHERE datname = current_database())"
    )


async def test_subscribe_refuses_before_touching_anything(db):
    with pytest.raises(Refused, match="runs on a mirror"):
        await subscribe(db, role="primary", head="head", arms=ARMS)
    with pytest.raises(Refused, match="no arms are configured"):
        await subscribe(db, role="mirror", head="head", arms={})
    with pytest.raises(Refused, match="no arm named 'arm9'"):
        await subscribe(db, role="mirror", head="head", arms=ARMS, only="arm9")
    with pytest.raises(Refused, match="this head's own name"):
        await subscribe(db, role="mirror", head="arm1", arms=ARMS)
    with pytest.raises(Refused, match="PUBLISHER_DSN.* for arm1"):
        await subscribe(db, role="mirror", head="head", arms={"arm1": Arm()})
    assert await _subscriptions_here(db) == 0


async def test_subscribe_refuses_when_a_replicated_table_has_rows(db):
    await db.execute("INSERT INTO squirrel.tag_group (id, name) VALUES (gen_random_uuid(), 'Area')")

    with pytest.raises(Refused, match="already have rows: squirrel.tag_group"):
        await subscribe(db, role="mirror", head="head", arms=ARMS)

    assert await _subscriptions_here(db) == 0


async def test_refresh_without_a_subscription_is_a_no_op(db):
    assert await refresh(db, role="mirror", arms=ARMS) == {
        "subscriptions": [{"arm": "arm1", "name": "canopy_history_arm1", "refreshed": False}]
    }
    with pytest.raises(Refused, match="runs on a mirror"):
        await refresh(db, role="primary", arms=ARMS)
    with pytest.raises(Refused, match="no arms are configured"):
        await refresh(db, role="mirror", arms={})


async def test_status_without_a_subscription(db):
    assert await subscriber_status(db, arms=ARMS) == {
        "side": "subscriber",
        "subscriptions": [{"arm": "arm1", "name": "canopy_history_arm1", "exists": False}],
        "unexpected": [],
    }


@pytest_asyncio.fixture
async def stray_subscription(db):
    """A history subscription for an arm nobody configured, made without connecting anywhere."""
    name = f"canopy_history_gone_{uuid4().hex[:8]}"
    await db.execute(
        f"CREATE SUBSCRIPTION {name} CONNECTION 'host=gone.example.org dbname=canopy' "
        "PUBLICATION canopy_history WITH (connect = false)"
    )
    try:
        yield name
    finally:
        await db.execute(f"ALTER SUBSCRIPTION {name} SET (slot_name = NONE)")
        await db.execute(f"DROP SUBSCRIPTION {name}")


async def test_a_subscription_no_arm_claims_is_reported(db, stray_subscription, caplog):
    assert (await subscriber_status(db, arms=ARMS))["unexpected"] == [stray_subscription]

    await refresh(db, role="mirror", arms=ARMS)
    assert f"Subscription {stray_subscription} belongs to no configured arm" in caplog.text


async def test_status_with_no_arm_still_reports_what_is_left(db, stray_subscription):
    assert await subscriber_status(db, arms={}) == {
        "side": "subscriber",
        "subscriptions": [],
        "unexpected": [stray_subscription],
    }
    with pytest.raises(Refused, match="no arms are configured"):
        await subscriber_status(db, arms={}, only="arm1")


async def test_arm_states_reads_the_catalogs_as_the_app_role(db, config, stray_subscription):
    gone = stray_subscription.removeprefix("canopy_history_")
    await db.execute(f'CREATE ROLE "{config.app_role}"')
    engine = create_async_engine(
        make_url(DB_DSN).set(drivername="postgresql+asyncpg").render_as_string(hide_password=False), poolclass=NullPool
    )
    try:
        async with AsyncSession(engine) as session:
            # The API's role is no superuser; every column read here must be open to it.
            await session.execute(text(f'SET ROLE "{config.app_role}"'))
            assert await arm_states(session, ["arm1", gone]) == {"arm1": "missing", gone: "disabled"}
    finally:
        await engine.dispose()


# --------------------------------------------------------------------------- #
# subscribe's handling of the connection string, against a scripted connection
# --------------------------------------------------------------------------- #


class ScriptedConnection:
    """Answers just the queries ``subscribe`` makes on an empty, migrated head."""

    def __init__(self, *, superuser: bool, create_error: Exception | None = None):
        self.superuser = superuser
        self.create_error = create_error
        self.executed: list[str] = []
        self.created = False

    async def fetchval(self, sql, *args):
        if "server_version_num" in sql:
            return "160015"
        if "rolsuper" in sql:
            return self.superuser
        if "pg_create_subscription" in sql:
            return True
        if "standard_conforming_strings" in sql:
            return "on"
        if "SELECT EXISTS (SELECT 1 FROM" in sql:
            return False
        raise AssertionError(f"unexpected query: {sql}")

    async def fetchrow(self, sql, *args):
        assert "FROM pg_subscription" in sql
        return {"oid": 1, "subname": "canopy_history_arm1"} if self.created else None

    async def fetch(self, sql, *args):
        if "pg_subscription_rel" in sql or "FROM pg_subscription" in sql:
            return []
        return [{"name": name} for name in REGISTRY.replicated]

    async def execute(self, sql):
        self.executed.append(sql)
        if sql.startswith("CREATE SUBSCRIPTION"):
            if self.create_error is not None:
                raise self.create_error
            self.created = True


@pytest.mark.parametrize("superuser", [True, False])
async def test_subscribe_keeps_failed_statements_out_of_the_server_log_when_it_can(superuser):
    conn = ScriptedConnection(superuser=superuser)

    result = await subscribe(conn, role="mirror", head="head", arms=ARMS)

    assert [(sub["arm"], sub["created"]) for sub in result["subscriptions"]] == [("arm1", True)]
    assert ("SET log_min_error_statement = panic" in conn.executed) is superuser
    create = next(sql for sql in conn.executed if sql.startswith("CREATE SUBSCRIPTION"))
    assert create == (
        f"CREATE SUBSCRIPTION \"canopy_history_arm1\" CONNECTION '{DSN}' "
        'PUBLICATION "canopy_history" WITH (copy_data = true, streaming = parallel)'
    )


async def test_subscribe_failure_is_redacted_and_unchained(caplog):
    leaky = f'invalid connection string syntax near "{DSN}" (password s3cret-pw)'
    conn = ScriptedConnection(superuser=True, create_error=asyncpg.PostgresError(leaky))

    with pytest.raises(ReplicationError, match="failed for arm1") as caught:
        await subscribe(conn, role="mirror", head="head", arms=ARMS)

    for text_out in (str(caught.value), caplog.text):
        assert "s3cret-pw" not in text_out and DSN not in text_out
    assert caught.value.__cause__ is None and caught.value.__context__ is None
