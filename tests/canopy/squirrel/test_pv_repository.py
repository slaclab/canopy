"""Repository tests for ``PvRepository.bulk_create`` and ``get_by_ids`` against the test Postgres."""

import uuid
import random

import pytest
from httpx import AsyncClient
from sqlalchemy import event, inspect
from sqlalchemy.ext.asyncio import AsyncSession

from canopy.squirrel.data import repositories
from canopy.squirrel.data.models import Pv, Tag, TagGroup
from canopy.core.facilities.models import Facility
from tests.canopy.squirrel.conftest import WRITE_HEADERS
from canopy.squirrel.data.repositories import PvRepository


async def _seed_tags(session: AsyncSession, count: int) -> list[Tag]:
    group = TagGroup(name="Sim group")
    tags = [Tag(name=f"sim-tag-{i}", description=f"Simulated tag {i}", group=group) for i in range(count)]
    session.add_all([group, *tags])
    await session.commit()
    return tags


def _new_pvs(count: int, tags: list[Tag]) -> list[Pv]:
    """PVs whose addresses are deliberately not in insertion or sort order."""
    numbers = list(range(1, count + 1))
    random.Random(254).shuffle(numbers)
    return [
        Pv(
            setpoint_address=f"SIM:DEV:{n:05d}:SET",
            readback_address=f"SIM:DEV:{n:05d}:RBV",
            device=f"SIM:DEV:{n:05d}",
            tags=[tags[n % len(tags)], tags[(n + 1) % len(tags)]],
        )
        for n in numbers
    ]


def _assert_loaded(pv: Pv) -> None:
    """Every attribute ``PvService._to_dto`` reads is loaded, so reading it never lazy-loads."""
    state = inspect(pv)
    read_by_to_dto = {
        "id",
        "setpoint_address",
        "readback_address",
        "config_address",
        "device",
        "description",
        "abs_tolerance",
        "rel_tolerance",
        "tags",
        "created_at",
        "updated_at",
    }
    assert not (read_by_to_dto & (state.unloaded | state.expired_attributes))
    assert pv.created_at is not None and pv.updated_at is not None
    for tag in pv.tags:
        tag_state = inspect(tag)
        assert not ({"id", "name", "description"} & (tag_state.unloaded | tag_state.expired_attributes))


async def test_bulk_create_returns_input_order_with_attributes_loaded(db_session: AsyncSession, monkeypatch):
    tags = await _seed_tags(db_session, 3)
    pvs = _new_pvs(12, tags)
    expected = [pv.setpoint_address for pv in pvs]

    # The reload refreshes the given objects in place, so the order the database returns
    # rows in can't change the result's order.
    repo = PvRepository(db_session)
    real_get_by_ids = repo.get_by_ids

    async def reversed_get_by_ids(ids, **kwargs):
        return list(reversed(await real_get_by_ids(ids, **kwargs)))

    monkeypatch.setattr(repo, "get_by_ids", reversed_get_by_ids)
    created = await repo.bulk_create(pvs)

    assert [pv.setpoint_address for pv in created] == expected
    assert all(got is given for got, given in zip(created, pvs, strict=True))
    for pv in created:
        _assert_loaded(pv)
        assert len(pv.tags) == 2
        assert {t.name for t in pv.tags} <= {"sim-tag-0", "sim-tag-1", "sim-tag-2"}


async def test_bulk_create_fails_clearly_if_the_reload_misses_a_row(db_session: AsyncSession, monkeypatch):
    """A row missing from the reload is a clear error, not a ``KeyError`` or a half-loaded PV."""
    tags = await _seed_tags(db_session, 3)
    repo = PvRepository(db_session)
    real_get_by_ids = repo.get_by_ids

    async def short_get_by_ids(ids, **kwargs):
        return (await real_get_by_ids(ids, **kwargs))[1:]

    monkeypatch.setattr(repo, "get_by_ids", short_get_by_ids)
    with pytest.raises(RuntimeError, match="Reloaded 2 of 3 created PVs"):
        await repo.bulk_create(_new_pvs(3, tags))


async def test_get_by_ids_sends_any_number_of_ids_as_one_parameter(db_session: AsyncSession):
    """The ids go as one ``uuid[]`` parameter, so the SQL text is the same for every size."""
    tags = await _seed_tags(db_session, 3)
    repo = PvRepository(db_session)
    ids = [pv.id for pv in await repo.bulk_create(_new_pvs(11, tags))]

    statements: list[tuple[str, object]] = []

    def record(conn, cursor, statement, parameters, context, executemany):
        statements.append((statement, parameters))

    sync_engine = db_session.bind.sync_engine
    event.listen(sync_engine, "before_cursor_execute", record)
    try:
        few = await repo.get_by_ids(ids[:2])
        few_statement = statements[0]
        statements.clear()
        many = await repo.get_by_ids(ids)
        many_statement = statements[0]
    finally:
        event.remove(sync_engine, "before_cursor_execute", record)

    assert {pv.id for pv in few} == set(ids[:2])
    assert {pv.id for pv in many} == set(ids)
    assert few_statement[0] == many_statement[0]
    assert len(few_statement[1]) == len(many_statement[1]) == 1


async def test_bulk_create_of_nothing_is_a_no_op(db_session: AsyncSession):
    assert await PvRepository(db_session).bulk_create([]) == []


async def test_multi_create_reports_tags_and_timestamps_in_input_order(client: AsyncClient, sample_tag_group: dict):
    gid = sample_tag_group["id"]
    tag_ids = []
    for name in ("first", "second"):
        r = await client.post(f"/v1/tags/{gid}/tags", headers=WRITE_HEADERS, json={"name": name})
        assert r.status_code == 200, r.text
        tag_ids = [t["id"] for t in r.json()["group"]["tags"]]
    rows = [
        {"setpointAddress": "SIM:DEV:00003:SET", "tags": tag_ids},
        {"setpointAddress": "SIM:DEV:00001:SET", "tags": [tag_ids[0]]},
        {"setpointAddress": "SIM:DEV:00002:SET"},
    ]

    r = await client.post("/v1/pvs/multi", headers=WRITE_HEADERS, json=rows)

    assert r.status_code == 200, r.text
    body = r.json()
    assert [pv["setpointAddress"] for pv in body] == [row["setpointAddress"] for row in rows]
    assert [sorted(t["id"] for t in pv["tags"]) for pv in body] == [sorted(tag_ids), [tag_ids[0]], []]
    assert all(t["name"] in {"first", "second"} for pv in body for t in pv["tags"])
    assert all(pv["createdDate"] and pv["lastModifiedDate"] for pv in body)


async def test_lock_by_identity_locks_every_address_with_one_ordered_statement(db_session: AsyncSession, monkeypatch):
    """One ``text[]`` parameter and one id order, so imports with overlapping PVs can't deadlock.

    A batch per ``PARAM_BATCH_SIZE`` addresses would order each batch on its own; with
    the limit at 2 that would show here as several statements.
    """
    monkeypatch.setattr(repositories, "PARAM_BATCH_SIZE", 2)
    tags = await _seed_tags(db_session, 3)
    repo = PvRepository(db_session)
    pvs = await repo.bulk_create(_new_pvs(11, tags))
    owner = pvs[0].owner_instance
    addresses = [pv.setpoint_address for pv in pvs]

    statements: list[tuple[str, object]] = []

    def record(conn, cursor, statement, parameters, context, executemany):
        if statement.lstrip().upper().startswith("SELECT") and "FOR UPDATE" in statement.upper():
            statements.append((statement, parameters))

    sync_engine = db_session.bind.sync_engine
    event.listen(sync_engine, "before_cursor_execute", record)
    try:
        few = await repo.lock_by_identity("setpoint", addresses[:2], owner=owner)
        few_statements = list(statements)
        statements.clear()
        many = await repo.lock_by_identity("setpoint", addresses, owner=owner)
        many_statements = list(statements)
    finally:
        event.remove(sync_engine, "before_cursor_execute", record)

    assert set(few) == set(addresses[:2]) and set(many) == set(addresses)
    assert len(few_statements) == len(many_statements) == 1
    assert few_statements[0][0] == many_statements[0][0]
    assert "ORDER BY squirrel.pv.id" in many_statements[0][0]


async def test_facilities_of_owned_reads_every_id_and_its_facilities_in_one_statement(
    db_session: AsyncSession, monkeypatch, as_instance
):
    """One ``uuid[]`` parameter however many ids: a batch per ``PARAM_BATCH_SIZE`` would show as several."""
    monkeypatch.setattr(repositories, "PARAM_BATCH_SIZE", 2)
    repo = PvRepository(db_session)
    pvs = await repo.bulk_create(_new_pvs(5, await _seed_tags(db_session, 3)))
    one, two = Facility(slug="one", name="One"), Facility(slug="two", name="Two")
    db_session.add_all([one, two])
    await db_session.flush()
    await repo.add_facilities_many([(pvs[0].id, {one.id, two.id}), (pvs[1].id, {one.id}), (pvs[3].id, {two.id})])
    assert await repo.soft_delete(pvs[3].id)
    with as_instance("arm2"):
        theirs = Pv(setpoint_address="THEIRS:SP")
        db_session.add(theirs)
        await db_session.flush()
    ids = [pv.id for pv in pvs] + [theirs.id, str(uuid.uuid4())]

    statements: list[str] = []

    def record(conn, cursor, statement, parameters, context, executemany):
        if statement.lstrip().upper().startswith("SELECT"):
            statements.append(statement)

    sync_engine = db_session.bind.sync_engine
    event.listen(sync_engine, "before_cursor_execute", record)
    try:
        found = await repo.facilities_of_owned(ids, owner=pvs[0].owner_instance)
    finally:
        event.remove(sync_engine, "before_cursor_execute", record)

    # Deleted PVs count; another owner's PV and an unknown id don't; no facility is an empty set.
    assert found == {
        pvs[0].id: {one.id, two.id},
        pvs[1].id: {one.id},
        pvs[2].id: set(),
        pvs[3].id: {two.id},
        pvs[4].id: set(),
    }
    assert len(statements) == 1
