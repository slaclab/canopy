"""The ``snapshot (created_at, id)`` index, against Postgres."""

from datetime import UTC, datetime, timedelta

from sqlalchemy import text, event

from canopy.squirrel.data.models import Snapshot
from canopy.squirrel.data.repositories import SnapshotRepository

INDEX = "ix_squirrel_snapshot_created_at_id"


async def test_index_covers_created_at_then_id(test_engine):
    async with test_engine.connect() as conn:
        indexdef = (
            await conn.execute(
                text("SELECT indexdef FROM pg_indexes WHERE schemaname = 'squirrel' AND indexname = :name"),
                {"name": INDEX},
            )
        ).scalar_one()

    assert indexdef.endswith("ON squirrel.snapshot USING btree (created_at, id)")


async def test_keyset_page_reads_the_index_without_sorting(test_engine, db_session):
    start = datetime(2026, 1, 1, tzinfo=UTC)
    db_session.add_all(Snapshot(title=f"s{i}", created_at=start + timedelta(minutes=i)) for i in range(20))
    await db_session.commit()

    pages: list[tuple[str, tuple]] = []

    def capture(_conn, _cursor, statement, parameters, _context, _executemany):
        if "ORDER BY" in statement and "squirrel.snapshot.created_at DESC" in statement:
            pages.append((statement, parameters))

    event.listen(test_engine.sync_engine, "before_cursor_execute", capture)
    try:
        repo = SnapshotRepository(db_session)
        _, cursor, _ = await repo.search(limit=5)
        await repo.search(limit=5, after=cursor)
    finally:
        event.remove(test_engine.sync_engine, "before_cursor_execute", capture)

    assert len(pages) == 2
    async with test_engine.connect() as conn:
        # On 20 rows a sequential or bitmap scan plus a sort is cheaper, so rule those out
        # and check what matters: the index reads the page in order, with no sort.
        await conn.execute(text("SET enable_seqscan = off"))
        await conn.execute(text("SET enable_bitmapscan = off"))
        for statement, parameters in pages:
            plan = "\n".join((await conn.exec_driver_sql(f"EXPLAIN {statement}", parameters)).scalars())
            assert f"Index Scan Backward using {INDEX}" in plan, plan
            assert "Sort" not in plan, plan
