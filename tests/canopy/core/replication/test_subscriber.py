"""The mirror's commands without a primary: ``grants``, and what ``subscribe``, ``refresh``
and ``status`` do before any subscription exists. Subscribing for real is in
``test_replication_e2e.py``."""

import pytest
import asyncpg
from pydantic import SecretStr

from canopy.core.replication.sql import Refused, ReplicationError
from canopy.core.replication.registry import REGISTRY
from canopy.core.replication.subscriber import (
    grants,
    refresh,
    subscribe,
    subscriber_status,
)

from .conftest import privileges

PRESENT = sorted(REGISTRY.replicated)
DSN = "host=primary.example.org dbname=canopy user=replicator password=s3cret-pw"


async def test_grants_reads_on_replicated_tables_and_writes_on_api_keys(db, config):
    await db.execute(f'CREATE ROLE "{config.app_role}"')
    # Anything the app role could write before must go.
    await db.execute(f'GRANT INSERT, DELETE ON squirrel.pv TO "{config.app_role}"')

    result = await grants(db, role="mirror", config=config)

    assert result["replicated"] == PRESENT
    assert result["local"] == ["core.api_key", "core.history_heartbeat"]
    for table in PRESENT:
        assert await privileges(db, config.app_role, table) == {"SELECT"}, table
    for table in result["local"]:
        assert await privileges(db, config.app_role, table) == {"SELECT", "INSERT", "UPDATE", "DELETE"}, table
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


async def test_subscribe_refuses_before_touching_anything(db, config):
    with pytest.raises(Refused, match="runs on a mirror"):
        await subscribe(db, role="primary", instance="lab", config=config)
    with pytest.raises(Refused, match="CANOPY_INSTANCE"):
        await subscribe(db, role="mirror", instance=None, config=config)
    with pytest.raises(Refused, match="PUBLISHER_DSN"):
        await subscribe(db, role="mirror", instance="lab", config=config)


async def test_subscribe_refuses_when_a_replicated_table_has_rows(db, config):
    config.publisher_dsn = SecretStr(DSN)
    await db.execute("INSERT INTO squirrel.tag_group (id, name) VALUES (gen_random_uuid(), 'Area')")

    with pytest.raises(Refused, match="already have rows: squirrel.tag_group"):
        await subscribe(db, role="mirror", instance="lab", config=config)

    assert await db.fetchval("SELECT count(*) FROM pg_subscription") == 0


async def test_refresh_without_a_subscription_is_a_no_op(db):
    assert await refresh(db, role="mirror", instance="lab") == {"name": "canopy_history_lab", "refreshed": False}
    with pytest.raises(Refused, match="runs on a mirror"):
        await refresh(db, role="primary", instance="lab")


async def test_status_without_a_subscription(db):
    assert await subscriber_status(db, instance="lab") == {
        "side": "subscriber",
        "subscription": {"name": "canopy_history_lab", "exists": False},
    }


# --------------------------------------------------------------------------- #
# subscribe's handling of the connection string, against a scripted connection
# --------------------------------------------------------------------------- #


class ScriptedConnection:
    """Answers just the queries ``subscribe`` makes on an empty, migrated mirror."""

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
        return {"oid": 1, "subname": "canopy_history_lab"} if self.created else None

    async def fetch(self, sql, *args):
        if "pg_subscription_rel" in sql:
            return []
        return [{"name": name} for name in REGISTRY.replicated]

    async def execute(self, sql):
        self.executed.append(sql)
        if sql.startswith("CREATE SUBSCRIPTION"):
            if self.create_error is not None:
                raise self.create_error
            self.created = True


@pytest.mark.parametrize("superuser", [True, False])
async def test_subscribe_keeps_failed_statements_out_of_the_server_log_when_it_can(config, superuser):
    config.publisher_dsn = SecretStr(DSN)
    conn = ScriptedConnection(superuser=superuser)

    result = await subscribe(conn, role="mirror", instance="lab", config=config)

    assert result["created"] is True
    assert ("SET log_min_error_statement = panic" in conn.executed) is superuser
    create = next(sql for sql in conn.executed if sql.startswith("CREATE SUBSCRIPTION"))
    assert create == (
        f"CREATE SUBSCRIPTION \"canopy_history_lab\" CONNECTION '{DSN}' "
        'PUBLICATION "canopy_history" WITH (copy_data = true, streaming = parallel)'
    )


async def test_subscribe_failure_is_redacted_and_unchained(config):
    leaky = f'invalid connection string syntax near "{DSN}" (password s3cret-pw)'
    config.publisher_dsn = SecretStr(DSN)
    conn = ScriptedConnection(superuser=True, create_error=asyncpg.PostgresError(leaky))

    with pytest.raises(ReplicationError) as caught:
        await subscribe(conn, role="mirror", instance="lab", config=config)

    assert "s3cret-pw" not in str(caught.value)
    assert DSN not in str(caught.value)
    assert caught.value.__cause__ is None and caught.value.__suppress_context__
