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
from canopy.core.replication.sql import Refused, ReplicationError, tables_with_rows_of
from canopy.core.replication.registry import REGISTRY
from canopy.core.replication.subscriber import (
    grants,
    refresh,
    subscribe,
    subscriber_status,
)

from .conftest import DB_DSN, privileges, build_schema, direct_grants

PRESENT = sorted(REGISTRY.replicated)
DSN = "host=arm1.example.org dbname=canopy user=replicator password=s3cret-pw"
ARMS = {"arm1": Arm(publisher_dsn=SecretStr(DSN))}


HEAD = "head"
ROOTS = sorted(t for t in REGISTRY.replicated if not REGISTRY.owner_chain(t))
RW = {"SELECT", "INSERT", "UPDATE", "DELETE"}


async def _row_security(conn, table: str) -> tuple[bool, bool]:
    """``(enabled, forced)`` for ``table``'s row-level security."""
    row = await conn.fetchrow(
        "SELECT relrowsecurity, relforcerowsecurity FROM pg_class WHERE oid = $1::regclass", table
    )
    return row["relrowsecurity"], row["relforcerowsecurity"]


async def test_grants_reads_every_row_and_writes_only_the_heads(db, config):
    await db.execute(f'CREATE ROLE "{config.app_role}"')
    # Anything else the app role held must go.
    await db.execute(f'GRANT TRUNCATE ON squirrel.pv TO "{config.app_role}"')

    result = await grants(db, role="mirror", head=HEAD, config=config)

    assert result["replicated"] == PRESENT and result["row_security"] == ROOTS
    assert result["local"] == ["core.api_key"]
    for table in PRESENT + ["core.api_key"]:
        assert await privileges(db, config.app_role, table) == RW, table
    for table in PRESENT:
        assert await _row_security(db, table) == ((True, False) if table in ROOTS else (False, False)), table
    for schema in REGISTRY.schemas:
        assert await db.fetchval("SELECT has_schema_privilege($1, $2, 'USAGE')", config.app_role, schema)

    # As the app role: every row is visible, and only the head's own can be written.
    await db.execute(
        "INSERT INTO squirrel.tag_group (id, name, owner_instance) VALUES (gen_random_uuid(), 'A', 'arm1')"
    )
    await db.execute(f'SET ROLE "{config.app_role}"')
    try:
        await db.execute(
            "INSERT INTO squirrel.tag_group (id, name, owner_instance) VALUES (gen_random_uuid(), 'A', $1)", HEAD
        )
        assert await db.fetchval("SELECT count(*) FROM squirrel.tag_group") == 2
        assert await db.execute("UPDATE squirrel.tag_group SET description = 'x'") == "UPDATE 1"
        assert await db.execute("DELETE FROM squirrel.tag_group WHERE owner_instance = 'arm1'") == "DELETE 0"
        with pytest.raises(asyncpg.InsufficientPrivilegeError, match="row-level security"):
            await db.execute(
                "INSERT INTO squirrel.tag_group (id, name, owner_instance) VALUES (gen_random_uuid(), 'B', 'arm1')"
            )
        with pytest.raises(asyncpg.InsufficientPrivilegeError, match="row-level security"):
            await db.execute("UPDATE squirrel.tag_group SET owner_instance = 'arm1' WHERE owner_instance = $1", HEAD)
    finally:
        await db.execute("RESET ROLE")


async def test_grants_makes_tables_created_later_readable(db, config):
    await db.execute(f'CREATE ROLE "{config.app_role}"')
    await grants(db, role="mirror", head=HEAD, config=config)

    # The next migration, run by the tables' owner.
    await db.execute("CREATE TABLE squirrel.added_later (id int PRIMARY KEY)")

    assert await privileges(db, config.app_role, "squirrel.added_later") == {"SELECT"}


async def test_grants_twice_is_harmless_and_follows_a_new_head_name(db, config):
    await db.execute(f'CREATE ROLE "{config.app_role}"')
    first = await grants(db, role="mirror", head=HEAD, config=config)
    assert await grants(db, role="mirror", head=HEAD, config=config) == first
    assert await db.fetchval("SELECT count(*) FROM pg_policies WHERE tablename = 'pv' AND schemaname = 'squirrel'") == 4

    await grants(db, role="mirror", head="renamed", config=config)
    checks = await db.fetch(
        "SELECT coalesce(with_check, qual) AS rule FROM pg_policies "
        "WHERE schemaname = 'squirrel' AND tablename = 'pv' AND policyname <> 'canopy_read'"
    )
    assert len(checks) == 3 and all("'renamed'" in row["rule"] for row in checks)


async def test_grants_refusals(db, config):
    with pytest.raises(Refused, match="runs on a mirror"):
        await grants(db, role="primary", head=HEAD, config=config)
    with pytest.raises(Refused, match="doesn't exist"):
        await grants(db, role="mirror", head=HEAD, config=config)

    await db.execute(f'CREATE ROLE "{config.app_role}" SUPERUSER')
    with pytest.raises(Refused, match="is a superuser"):
        await grants(db, role="mirror", head=HEAD, config=config)

    await db.execute(f'ALTER ROLE "{config.app_role}" NOSUPERUSER BYPASSRLS')
    with pytest.raises(Refused, match="has BYPASSRLS"):
        await grants(db, role="mirror", head=HEAD, config=config)

    await db.execute(f'ALTER ROLE "{config.app_role}" NOBYPASSRLS')
    await db.execute(f'ALTER TABLE squirrel.tag OWNER TO "{config.app_role}"')
    with pytest.raises(Refused, match="owns squirrel.tag"):
        await grants(db, role="mirror", head=HEAD, config=config)


@pytest.mark.parametrize(
    ("setup", "refusal"),
    [
        pytest.param(
            # TRUNCATE ignores row-level security, so it must never reach the app role.
            [
                'CREATE ROLE "{helper}"',
                'GRANT TRUNCATE ON squirrel.snapshot TO "{helper}"',
                'GRANT "{helper}" TO "{app}"',
            ],
            r"TRUNCATE on squirrel\.snapshot(?! \(granted)",
            id="inherited-from-another-role",
        ),
        pytest.param(
            ["GRANT TRIGGER ON squirrel.pv TO PUBLIC"],
            r"TRIGGER on squirrel\.pv \(granted to PUBLIC\)",
            id="granted-to-public",
        ),
        pytest.param(
            [
                'CREATE ROLE "{helper}"',
                'GRANT REFERENCES (title) ON squirrel.snapshot TO "{helper}"',
                'GRANT "{helper}" TO "{app}"',
            ],
            r"REFERENCES on squirrel\.snapshot",
            id="column-grant-through-another-role",
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
async def test_grants_refuses_an_app_role_that_holds_more(db, config, setup, refusal):
    # The reader role isn't used on a head, so it doubles as the helper role (and is dropped after).
    await db.execute(f'CREATE ROLE "{config.app_role}"')
    for statement in setup:
        await db.execute(statement.format(app=config.app_role, helper=config.reader_role))

    with pytest.raises(Refused, match=refusal):
        await grants(db, role="mirror", head=HEAD, config=config)

    # Rolled back: the app role got none of this run's grants, and no table got its policies.
    assert await direct_grants(db, config.app_role, "squirrel.snapshot") == set()
    assert await _row_security(db, "squirrel.pv") == (False, False)


async def test_grants_leaves_default_privilege_reads_on_unclassified_tables_alone(db, config):
    await db.execute(f'CREATE ROLE "{config.app_role}"')
    await grants(db, role="mirror", head=HEAD, config=config)
    await db.execute("CREATE TABLE squirrel.not_yet_classified (id int PRIMARY KEY)")

    # Its default-privilege SELECT is within policy, so the next run (which warns about it) still passes.
    await grants(db, role="mirror", head=HEAD, config=config)
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


async def test_subscribe_refuses_rows_the_head_holds_for_that_arm(db):
    await db.execute(
        "INSERT INTO squirrel.tag_group (id, name, owner_instance) VALUES (gen_random_uuid(), 'Area', 'arm1')"
    )

    with pytest.raises(Refused, match="already hold that arm's rows: squirrel.tag_group"):
        await subscribe(db, role="mirror", head="head", arms=ARMS)

    assert await _subscriptions_here(db) == 0


async def test_only_that_arms_rows_count(db):
    pv, group, tag = str(uuid4()), str(uuid4()), str(uuid4())
    await db.execute("INSERT INTO squirrel.pv (id, setpoint_address, owner_instance) VALUES ($1, 'SIM:A', 'head')", pv)
    await db.execute("INSERT INTO squirrel.tag_group (id, name, owner_instance) VALUES ($1, 'Area', 'head')", group)
    await db.execute("INSERT INTO squirrel.tag (id, name, group_id) VALUES ($1, 'Injector', $2)", tag, group)
    await db.execute("INSERT INTO squirrel.pv_tag (pv_id, tag_id) VALUES ($1, $2)", pv, tag)

    # The head's rows (and children following them) don't block an arm; that arm's would.
    assert await tables_with_rows_of(db, REGISTRY, PRESENT, "arm1") == []
    assert await tables_with_rows_of(db, REGISTRY, PRESENT, "head") == [
        "squirrel.pv",
        "squirrel.pv_tag",
        "squirrel.tag",
        "squirrel.tag_group",
    ]


@pytest_asyncio.fixture
async def arm_db(db):
    """A second database on the same server, standing in for an arm the CLI reads.

    Only the checks before ``CREATE SUBSCRIPTION`` can run against it: a
    subscription to a database on its own server waits forever for its slot.
    """
    name = f"rt_arm_{uuid4().hex[:8]}"
    await db.execute(f'CREATE DATABASE "{name}"')
    dsn = make_url(DB_DSN).set(database=name).render_as_string(hide_password=False)
    try:
        await build_schema(dsn)
        conn = await asyncpg.connect(dsn)
        url = make_url(dsn)
        # As an arm's setting would hold it: key=value, which the CLI turns into a URI to read the arm.
        conninfo = f"host={url.host} port={url.port} dbname={name} user={url.username} password='{url.password}'"
        try:
            yield conn, {"arm1": Arm(publisher_dsn=SecretStr(conninfo))}
        finally:
            await conn.close()
    finally:
        await db.execute(f'DROP DATABASE "{name}" WITH (FORCE)')


async def test_subscribe_reads_the_arm_and_refuses_a_slug_clash(db, arm_db):
    arm, arms = arm_db
    await arm.execute(
        "INSERT INTO core.facility (id, slug, name, owner_instance) VALUES (gen_random_uuid(), 'lab', 'L', 'arm1')"
    )
    await db.execute(
        "INSERT INTO core.facility (id, slug, name, owner_instance) VALUES (gen_random_uuid(), 'lab', 'L', 'head')"
    )

    with pytest.raises(Refused, match=r"core.facility.slug 'lab' \(here, owned by head\)"):
        await subscribe(db, role="mirror", head="head", arms=arms)
    assert await _subscriptions_here(db) == 0


async def test_refresh_checks_the_slugs_of_tables_about_to_be_copied(db, arm_db, stray_subscription_for_arm1):
    arm, arms = arm_db
    await arm.execute(
        "INSERT INTO magpie.config (id, slug, app, data, owner_instance) VALUES (gen_random_uuid(), 'shared', 't', '{}', 'arm1')"
    )
    await db.execute(
        "INSERT INTO magpie.config (id, slug, app, data, owner_instance) VALUES (gen_random_uuid(), 'shared', 't', '{}', 'arm2')"
    )

    with pytest.raises(Refused, match=r"magpie.config.slug 'shared' \(here, owned by arm2\)"):
        await refresh(db, role="mirror", arms=arms)


async def test_an_unreachable_arm_is_reported_not_refused(db):
    async def unreachable(arm):
        raise OSError("connection refused")

    with pytest.raises(ReplicationError, match="arm1: can't read it to check its unique keys: connection refused"):
        await subscribe(db, role="mirror", head="head", arms=ARMS, connect_arm=unreachable)
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


async def _unconnected_subscription(db, name: str):
    """A subscription made without connecting anywhere (disabled, no slot), dropped after."""
    await db.execute(
        f"CREATE SUBSCRIPTION {name} CONNECTION 'host=gone.example.org dbname=canopy' "
        "PUBLICATION canopy_history WITH (connect = false)"
    )
    try:
        yield name
    finally:
        await db.execute(f"ALTER SUBSCRIPTION {name} SET (slot_name = NONE)")
        await db.execute(f"DROP SUBSCRIPTION {name}")


@pytest_asyncio.fixture
async def stray_subscription(db):
    """A history subscription for an arm nobody configured."""
    async for name in _unconnected_subscription(db, f"canopy_history_gone_{uuid4().hex[:8]}"):
        yield name


@pytest_asyncio.fixture
async def stray_subscription_for_arm1(db):
    """arm1's subscription, holding no table yet: a refresh would copy every one."""
    async for name in _unconnected_subscription(db, "canopy_history_arm1"):
        yield name


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
        if "pg_class" in sql:
            return [{"name": name} for name in REGISTRY.replicated]
        return []  # the arm's slugs, and any here they clash with: none

    async def close(self):
        pass

    async def execute(self, sql):
        self.executed.append(sql)
        if sql.startswith("CREATE SUBSCRIPTION"):
            if self.create_error is not None:
                raise self.create_error
            self.created = True


def _same(conn):
    async def connect(arm):
        return conn

    return connect


@pytest.mark.parametrize("superuser", [True, False])
async def test_subscribe_keeps_failed_statements_out_of_the_server_log_when_it_can(superuser):
    conn = ScriptedConnection(superuser=superuser)

    result = await subscribe(conn, role="mirror", head="head", arms=ARMS, connect_arm=_same(conn))

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
        await subscribe(conn, role="mirror", head="head", arms=ARMS, connect_arm=_same(conn))

    for text_out in (str(caught.value), caplog.text):
        assert "s3cret-pw" not in text_out and DSN not in text_out
    assert caught.value.__cause__ is None and caught.value.__context__ is None
