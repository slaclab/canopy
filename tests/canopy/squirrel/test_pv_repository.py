"""Repository tests for ``PvRepository.bulk_create`` against the test Postgres."""

import random

from httpx import AsyncClient
from sqlalchemy import inspect
from sqlalchemy.ext.asyncio import AsyncSession

from canopy.squirrel.data import repositories
from canopy.squirrel.data.models import Pv, Tag, TagGroup
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

    # The reload must map rows back by id, not trust the database's row order.
    repo = PvRepository(db_session)
    real_get_by_ids = repo.get_by_ids

    async def reversed_get_by_ids(ids, **kwargs):
        return list(reversed(await real_get_by_ids(ids, **kwargs)))

    monkeypatch.setattr(repo, "get_by_ids", reversed_get_by_ids)
    created = await repo.bulk_create(pvs)

    assert [pv.setpoint_address for pv in created] == expected
    for pv in created:
        _assert_loaded(pv)
        assert len(pv.tags) == 2
        assert {t.name for t in pv.tags} <= {"sim-tag-0", "sim-tag-1", "sim-tag-2"}


async def test_bulk_create_reloads_across_several_batches(db_session: AsyncSession, monkeypatch):
    """A create larger than one reload batch still returns every row, in input order."""
    monkeypatch.setattr(repositories, "PARAM_BATCH_SIZE", 4)
    tags = await _seed_tags(db_session, 3)
    pvs = _new_pvs(11, tags)
    expected = [pv.setpoint_address for pv in pvs]

    created = await PvRepository(db_session).bulk_create(pvs)

    assert [pv.setpoint_address for pv in created] == expected
    assert len({pv.id for pv in created}) == 11
    for pv in created:
        _assert_loaded(pv)


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
