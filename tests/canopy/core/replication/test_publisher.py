"""``publish`` and the primary's ``status`` against a real Postgres."""

import logging

import pytest

from canopy.core.replication.sql import Refused
from canopy.core.replication.registry import REGISTRY
from canopy.core.replication.publisher import (
    publish,
    _not_switched,
    publisher_status,
    publication_state,
    publication_layout,
)

from .conftest import privileges, direct_grants

PRESENT = sorted(REGISTRY.replicated)


async def _published(conn) -> list[str]:
    rows = await conn.fetch(
        "SELECT schemaname || '.' || tablename AS name FROM pg_publication_tables WHERE pubname = 'canopy_history'"
    )
    return sorted(row["name"] for row in rows)


async def test_publish_creates_the_publication_from_the_registry(db, config, caplog):
    caplog.set_level(logging.INFO, logger="canopy.core.replication")
    result = await publish(db, role="primary", config=config)

    assert await _published(db) == PRESENT
    assert "core.api_key" not in await _published(db)
    row = await db.fetchrow("SELECT pubinsert, pubupdate, pubdelete, pubtruncate FROM pg_publication")
    assert tuple(row.values()) == (True, True, True, False)
    assert result["publications"]["canopy_history"]["created"] is True
    assert result["missing"] == []
    assert result["roles"] == {
        config.replicator_role: "absent",
        config.reader_role: "absent",
        config.heartbeat_role: "absent",
    }
    assert "Not in this database yet" not in caplog.text


async def test_publish_twice_changes_nothing(db, config, caplog):
    await publish(db, role="primary", config=config)
    caplog.clear()
    caplog.set_level(logging.INFO, logger="canopy.core.replication")

    result = await publish(db, role="primary", config=config)

    assert result["publications"]["canopy_history"] == {
        "created": False,
        "added": [],
        "removed": [],
        "cleared": [],
        "schemas_removed": [],
        "operations_changed": False,
    }
    assert "Publication canopy_history: no change" in caplog.text


async def test_publish_converges_a_stale_publication(db, config):
    await db.execute(
        "CREATE PUBLICATION canopy_history FOR TABLE core.api_key, squirrel.pv WITH (publish = 'insert, truncate')"
    )

    result = await publish(db, role="primary", config=config)

    assert await _published(db) == PRESENT
    sync = result["publications"]["canopy_history"]
    assert sync["removed"] == ["core.api_key"]
    assert "squirrel.pv" not in sync["added"] and "squirrel.snapshot" in sync["added"]
    assert sync["operations_changed"] is True
    row = await db.fetchrow("SELECT pubinsert, pubupdate, pubdelete, pubtruncate FROM pg_publication")
    assert tuple(row.values()) == (True, True, True, False)


async def test_a_retired_table_already_dropped_is_not_reported_missing(db, config, caplog, monkeypatch):
    # Retired tables stay registered for databases that still hold them; one that has
    # dropped the table shouldn't log it as "not here yet" on every deploy.
    monkeypatch.setattr("canopy.core.replication.publisher.RETIRED", frozenset({"squirrel.pv_facility"}))
    await db.execute("DROP TABLE squirrel.pv_facility")
    caplog.set_level(logging.INFO, logger="canopy.core.replication")
    result = await publish(db, role="primary", config=config)
    assert result["missing"] == []
    assert "Not in this database yet" not in caplog.text


async def test_publish_picks_up_a_registered_table_once_it_exists(db, config, caplog):
    # A mirror migrates a release ahead of its primary, so a primary can lack a table
    # the registry already names.
    await db.execute("DROP TABLE squirrel.pv_facility")
    caplog.set_level(logging.INFO, logger="canopy.core.replication")
    skipped = await publish(db, role="primary", config=config)
    assert skipped["missing"] == ["squirrel.pv_facility"]
    assert "Not in this database yet, skipped: squirrel.pv_facility" in caplog.text
    await db.execute(
        "CREATE TABLE squirrel.pv_facility (pv_id uuid, facility_id uuid, PRIMARY KEY (pv_id, facility_id))"
    )

    result = await publish(db, role="primary", config=config)

    assert result["publications"]["canopy_history"]["added"] == ["squirrel.pv_facility"]
    assert "squirrel.pv_facility" in await _published(db)


async def test_publish_grants_existing_roles_select_on_published_tables_only(db, config):
    for role in (config.replicator_role, config.reader_role):
        await db.execute(f'CREATE ROLE "{role}"')
    # A stray grant on the token hashes, which publish must take back.
    await db.execute(f'GRANT SELECT, UPDATE ON core.api_key TO "{config.reader_role}"')

    result = await publish(db, role="primary", config=config)

    assert result["roles"] == {
        config.replicator_role: "granted",
        config.reader_role: "granted",
        config.heartbeat_role: "absent",
    }
    for role in (config.replicator_role, config.reader_role):
        for table in PRESENT:
            assert await privileges(db, role, table) == {"SELECT"}, (role, table)
        assert await privileges(db, role, "core.api_key") == set()


async def test_publish_refuses_an_unclassified_table_and_changes_nothing(db, config):
    await db.execute("CREATE TABLE squirrel.mystery (id int PRIMARY KEY)")

    with pytest.raises(Refused, match="unclassified tables: squirrel.mystery"):
        await publish(db, role="primary", config=config)

    assert await db.fetchval("SELECT count(*) FROM pg_publication") == 0


async def test_publish_refuses_on_a_mirror(db, config):
    with pytest.raises(Refused, match="runs on a primary"):
        await publish(db, role="mirror", config=config)
    assert await db.fetchval("SELECT count(*) FROM pg_publication") == 0


async def test_publisher_status(db, config):
    before = await publisher_status(db, config=config)
    assert before["side"] == "publisher"
    assert before["publications"][0]["exists"] is False
    assert before["publications"][0]["missing"] == PRESENT

    await db.execute(f'CREATE ROLE "{config.replicator_role}"')
    await publish(db, role="primary", config=config)
    after = await publisher_status(db, config=config)

    assert after["publications"] == [
        {
            "name": "canopy_history",
            "exists": True,
            "operations": ["insert", "update", "delete"],
            "tables": PRESENT,
            "missing": [],
            "unexpected": [],
            "restricted": {},
            "schemas": [],
            "all_tables": False,
        }
    ]
    assert after["roles"] == {config.replicator_role: True, config.reader_role: False, config.heartbeat_role: False}
    assert after["slots"] == []
    assert after["wal_level"] in {"replica", "logical"}
    assert after["current_lsn"]


# --- the split layout --------------------------------------------------------- #

REFERENCE = sorted(t for t in PRESENT if REGISTRY.tables[t].value == "reference")
TIME_SERIES = sorted(t for t in PRESENT if REGISTRY.tables[t].value == "time_series")


async def _publications(conn) -> dict[str, tuple[list[str], tuple]]:
    rows = await conn.fetch("SELECT pubname, pubinsert, pubupdate, pubdelete, pubtruncate FROM pg_publication")
    tables = await conn.fetch("SELECT pubname, schemaname || '.' || tablename AS name FROM pg_publication_tables")
    return {
        row["pubname"]: (
            sorted(t["name"] for t in tables if t["pubname"] == row["pubname"]),
            tuple(row.values())[1:],
        )
        for row in rows
    }


async def test_split_creates_the_pair_beside_canopy_history(db, config):
    await publish(db, role="primary", config=config)
    assert await publication_layout(db) == "single"

    result = await publish(db, role="primary", config=config, split=True)

    assert result["layout"] == "switching"
    assert await _publications(db) == {
        "canopy_history": (PRESENT, (True, True, True, False)),
        "canopy_ref": (REFERENCE, (True, True, True, False)),
        "canopy_ts": (TIME_SERIES, (True, True, False, False)),
    }
    # A deploy's plain publish keeps all three in step and switches nothing.
    again = await publish(db, role="primary", config=config)
    assert again["layout"] == "switching"
    assert set(again["publications"]) == {"canopy_history", "canopy_ref", "canopy_ts"}


async def test_split_keeps_canopy_history_until_a_mirror_is_seen_streaming_the_pair(db, config):
    await publish(db, role="primary", config=config, split=True)
    await db.execute("CREATE PUBLICATION canopy_history FOR TABLE squirrel.pv")

    with pytest.raises(Refused, match="canopy_history is still needed: no history mirror's slot exists here"):
        await publish(db, role="primary", config=config, split=True)

    assert await publication_layout(db) == "switching"


async def test_split_on_a_new_primary_creates_only_the_pair(db, config):
    result = await publish(db, role="primary", config=config, split=True)

    assert result["layout"] == "split"
    assert set(await _publications(db)) == {"canopy_ref", "canopy_ts"}
    status = await publisher_status(db, config=config)
    assert status["layout"] == "split"
    assert [p["name"] for p in status["publications"]] == ["canopy_ref", "canopy_ts"]
    assert all(p["exists"] and not p["missing"] for p in status["publications"])


async def test_status_reports_the_layout_in_use(db, config):
    assert (await publisher_status(db, config=config))["layout"] == "none"
    await publish(db, role="primary", config=config)
    single = await publisher_status(db, config=config)
    await publish(db, role="primary", config=config, split=True)
    switching = await publisher_status(db, config=config)

    assert (single["layout"], [p["name"] for p in single["publications"]]) == ("single", ["canopy_history"])
    assert switching["layout"] == "switching"
    assert [p["name"] for p in switching["publications"]] == ["canopy_history", "canopy_ref", "canopy_ts"]


async def test_publish_lets_the_heartbeat_role_write_its_table_only(db, config):
    await db.execute(f'CREATE ROLE "{config.heartbeat_role}"')
    await db.execute(f'GRANT SELECT ON squirrel.pv TO "{config.heartbeat_role}"')

    result = await publish(db, role="primary", config=config)

    assert result["roles"][config.heartbeat_role] == "granted"
    assert await privileges(db, config.heartbeat_role, "core.history_heartbeat") == {"SELECT", "INSERT", "UPDATE"}
    for table in PRESENT + ["core.api_key"]:
        assert await privileges(db, config.heartbeat_role, table) == set(), table


async def test_publish_refuses_a_heartbeat_role_that_inherits_more(db, config):
    extra = f"{config.heartbeat_role}_extra"
    await db.execute(f'CREATE ROLE "{config.heartbeat_role}"')
    await db.execute(f'CREATE ROLE "{extra}"')
    try:
        await db.execute(f'GRANT SELECT ON squirrel.pv TO "{extra}"')
        await db.execute(f'GRANT "{extra}" TO "{config.heartbeat_role}"')

        with pytest.raises(Refused, match=f"role {config.heartbeat_role} must have no more than read-write on core"):
            await publish(db, role="primary", config=config)

        # The whole publish rolled back, grants and publication alike.
        assert await db.fetchval("SELECT count(*) FROM pg_publication") == 0
        assert await privileges(db, config.heartbeat_role, "core.history_heartbeat") == set()
    finally:
        await db.execute(f'REVOKE ALL ON squirrel.pv FROM "{extra}"')
        await db.execute(f'DROP ROLE "{extra}"')


@pytest.mark.parametrize(
    ("streams", "switched"),
    [
        (["canopy_ref", "canopy_ts"], True),
        (["canopy_ts"], False),
        (["canopy_ref"], False),
        (["canopy_history"], False),
        (["canopy_history", "canopy_ref", "canopy_ts"], False),
    ],
    ids=["pair", "ts-only", "ref-only", "single", "pair-plus-single"],
)
def test_a_mirror_has_switched_only_when_it_streams_exactly_the_pair(streams, switched):
    reasons = _not_switched([{"slot": "canopy_history_site_a", "active": True, "publications": streams}], REGISTRY)

    assert (reasons == []) is switched, reasons


async def test_publish_clears_row_filters_and_column_lists(db, config, caplog):
    await db.execute(
        "CREATE PUBLICATION canopy_history FOR TABLE squirrel.pv WHERE (id IS NOT NULL), squirrel.snapshot (id, title) "
        "WITH (publish = 'insert, update, delete')"
    )
    before = (await publisher_status(db, config=config))["publications"][0]
    assert set(before["restricted"]) == {"squirrel.pv", "squirrel.snapshot"}
    assert "id" in before["restricted"]["squirrel.pv"]["row_filter"]
    assert before["restricted"]["squirrel.pv"]["columns"] is None
    assert before["restricted"]["squirrel.snapshot"]["row_filter"] is None
    assert sorted(before["restricted"]["squirrel.snapshot"]["columns"]) == ["id", "title"]
    caplog.set_level(logging.INFO, logger="canopy.core.replication")

    result = await publish(db, role="primary", config=config)

    assert result["publications"]["canopy_history"]["cleared"] == ["squirrel.pv", "squirrel.snapshot"]
    assert "re-seed those tables" in caplog.text
    assert await _published(db) == PRESENT
    assert (await publisher_status(db, config=config))["publications"][0]["restricted"] == {}
    assert (await publish(db, role="primary", config=config))["publications"]["canopy_history"]["cleared"] == []


async def test_publish_lists_tables_instead_of_publishing_schemas_whole(db, config):
    await db.execute("CREATE PUBLICATION canopy_history FOR TABLE squirrel.pv, TABLES IN SCHEMA magpie")
    assert (await publisher_status(db, config=config))["publications"][0]["schemas"] == ["magpie"]

    result = await publish(db, role="primary", config=config)

    assert result["publications"]["canopy_history"]["schemas_removed"] == ["magpie"]
    assert (await publication_state(db, "canopy_history")).schemas == frozenset()
    assert await _published(db) == PRESENT


async def test_publish_refuses_a_for_all_tables_publication(db, config):
    await db.execute("CREATE PUBLICATION canopy_history FOR ALL TABLES")

    with pytest.raises(Refused, match="FOR ALL TABLES"):
        await publish(db, role="primary", config=config)

    assert (await publication_state(db, "canopy_history")).all_tables
    assert (await publisher_status(db, config=config))["publications"][0]["all_tables"] is True


async def test_publication_state_of_a_missing_publication(db):
    state = await publication_state(db, "canopy_history")
    assert not state.exists and not state.all_tables
    assert state.tables == state.schemas == state.operations == frozenset() and state.restricted == {}


@pytest.mark.parametrize(
    ("setup", "refusal"),
    [
        pytest.param(
            ['CREATE ROLE "{helper}"', 'GRANT SELECT ON core.api_key TO "{helper}"', 'GRANT "{helper}" TO "{reader}"'],
            r"SELECT on core\.api_key(?! \(granted)",
            id="inherited-from-another-role",
        ),
        pytest.param(
            ["GRANT SELECT ON core.api_key TO PUBLIC"],
            r"SELECT on core\.api_key \(granted to PUBLIC\)",
            id="granted-to-public",
        ),
        pytest.param(
            [
                'CREATE ROLE "{helper}"',
                'GRANT SELECT (token_hash) ON core.api_key TO "{helper}"',
                'GRANT "{helper}" TO "{reader}"',
            ],
            r"SELECT on core\.api_key",
            id="column-grant-through-another-role",
        ),
        pytest.param(
            [
                'CREATE ROLE "{helper}"',
                'ALTER TABLE squirrel.tag OWNER TO "{helper}"',
                'GRANT "{helper}" TO "{reader}" WITH INHERIT FALSE',
            ],
            r"belongs to rt_app_\w+, which owns Canopy tables",
            id="member-of-a-table-owner",
        ),
        pytest.param(
            ['GRANT pg_write_all_data TO "{reader}"'],
            r"INSERT on magpie\.config",
            id="writes-on-published-tables",
        ),
        pytest.param(
            ['ALTER ROLE "{reader}" SUPERUSER'],
            r"it is a superuser",
            id="superuser",
        ),
    ],
)
async def test_publish_refuses_a_role_with_access_outside_its_grants(db, config, setup, refusal):
    # The app role isn't used on a primary, so it doubles as the helper role (and is dropped after).
    await db.execute(f'CREATE ROLE "{config.reader_role}"')
    for statement in setup:
        await db.execute(statement.format(reader=config.reader_role, helper=config.app_role))

    with pytest.raises(Refused, match=refusal) as caught:
        await publish(db, role="primary", config=config)

    assert "doesn't change shared grants" in str(caught.value)
    # The whole run rolled back: no publication, and none of the reader's own grants.
    assert await db.fetchval("SELECT count(*) FROM pg_publication") == 0
    assert await direct_grants(db, config.reader_role, "squirrel.pv") == set()
