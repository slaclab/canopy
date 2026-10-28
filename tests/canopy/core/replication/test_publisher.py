"""``publish`` and the primary's ``status`` against a real Postgres."""

import logging

import pytest

from canopy.core.replication.sql import Refused
from canopy.core.replication.registry import REGISTRY
from canopy.core.replication.publisher import publish, publisher_status

from .conftest import privileges

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
    assert result["roles"] == {config.replicator_role: "absent", config.reader_role: "absent"}
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

    assert result["roles"] == {config.replicator_role: "granted", config.reader_role: "granted"}
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
        }
    ]
    assert after["roles"] == {config.replicator_role: True, config.reader_role: False}
    assert after["slots"] == []
    assert after["wal_level"] in {"replica", "logical"}
    assert after["current_lsn"]
