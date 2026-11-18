"""Row owners in Squirrel: PVs, snapshots and tag groups are written only by their owner.

The suite runs as ``arm1``. Rows another instance owns are created inside
``as_instance("arm2")``, the way a mirror receives an arm's rows through
replication, and then written as ``arm1``.
"""

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from canopy.core.errors import NotOwner
from canopy.squirrel.data.models import Pv, Tag, Snapshot
from canopy.core.facilities.models import Facility
from canopy.squirrel.schemas.snapshot import NewSnapshotDTO
from canopy.squirrel.services.snapshot_service import SnapshotService

from .conftest import WRITE_HEADERS
from .test_services_snapshot import FakeEpicsIO, _ev, _NoHook


async def _facility(db_session: AsyncSession, as_instance, slug: str, owner: str) -> Facility:
    with as_instance(owner):
        facility = Facility(slug=slug, name=slug.upper())
        db_session.add(facility)
        await db_session.commit()
    return facility


async def _pv(client: AsyncClient, address: str, *, params: str = "", **fields) -> dict:
    r = await client.post(f"/v1/pvs{params}", headers=WRITE_HEADERS, json={"setpointAddress": address, **fields})
    assert r.status_code == 200, r.text
    return r.json()


def _refused(r, code: str, status: int) -> None:
    assert r.status_code == status, r.text
    assert r.json()["code"] == code


# --- PVs ---------------------------------------------------------------------- #


async def test_another_owners_pv_cant_be_edited_or_deleted(client, as_instance):
    with as_instance("arm2"):
        remote = await _pv(client, "REMOTE:SP")

    assert remote["ownerInstance"] == "arm2"
    _refused(await client.put(f"/v1/pvs/{remote['id']}", headers=WRITE_HEADERS, json={"device": "X"}), "not_owner", 403)
    _refused(await client.delete(f"/v1/pvs/{remote['id']}", headers=WRITE_HEADERS), "not_owner", 403)


async def test_a_setpoint_address_is_unique_per_owner(client, as_instance):
    with as_instance("arm2"):
        await _pv(client, "SHARED:SP")

    local = await _pv(client, "SHARED:SP")
    assert local["ownerInstance"] == "arm1"
    duplicate = await client.post("/v1/pvs", headers=WRITE_HEADERS, json={"setpointAddress": "SHARED:SP"})
    assert duplicate.status_code == 409, duplicate.text
    assert duplicate.json()["code"] != "owner_mismatch"


async def test_a_pv_in_another_owners_facility_is_refused(client, db_session, as_instance):
    await _facility(db_session, as_instance, "remote", "arm2")

    r = await client.post("/v1/pvs", headers=WRITE_HEADERS, json={"setpointAddress": "A:SP", "facilities": ["remote"]})

    _refused(r, "not_owner", 403)


async def test_facilities_of_two_owners_are_a_mismatch(client, db_session, as_instance):
    await _facility(db_session, as_instance, "remote", "arm2")
    await _facility(db_session, as_instance, "local", "arm1")

    r = await client.post(
        "/v1/pvs", headers=WRITE_HEADERS, json={"setpointAddress": "A:SP", "facilities": ["remote", "local"]}
    )

    _refused(r, "owner_mismatch", 409)


async def test_a_pv_cant_take_another_owners_tag(client, as_instance):
    with as_instance("arm2"):
        group = (await client.post("/v1/tags", headers=WRITE_HEADERS, json={"name": "Area"})).json()
        added = await client.post(f"/v1/tags/{group['id']}/tags", headers=WRITE_HEADERS, json={"name": "north"})
        tag_id = added.json()["group"]["tags"][0]["id"]
    local = await _pv(client, "LOCAL:SP")

    _refused(
        await client.post("/v1/pvs", headers=WRITE_HEADERS, json={"setpointAddress": "B:SP", "tags": [tag_id]}),
        "owner_mismatch",
        409,
    )
    _refused(
        await client.put(f"/v1/pvs/{local['id']}", headers=WRITE_HEADERS, json={"tags": [tag_id]}),
        "owner_mismatch",
        409,
    )


async def test_the_owner_parameter_on_a_pv_with_no_facility(client):
    assert (await _pv(client, "MINE:SP", params="?owner=arm1"))["ownerInstance"] == "arm1"
    _refused(
        await client.post("/v1/pvs?owner=arm2", headers=WRITE_HEADERS, json={"setpointAddress": "THEIRS:SP"}),
        "not_owner",
        403,
    )
    bad = await client.post("/v1/pvs?owner=Arm-2", headers=WRITE_HEADERS, json={"setpointAddress": "X:SP"})
    assert bad.status_code == 422


async def test_an_import_has_one_owner(client, db_session, as_instance):
    await _facility(db_session, as_instance, "remote", "arm2")
    await _facility(db_session, as_instance, "local", "arm1")

    remote_rows = [{"setpointAddress": "R1:SP", "facilities": ["remote"]}]
    _refused(await client.post("/v1/pvs/multi", headers=WRITE_HEADERS, json=remote_rows), "not_owner", 403)
    mixed = [
        {"setpointAddress": "M1:SP", "facilities": ["remote"]},
        {"setpointAddress": "M2:SP", "facilities": ["local"]},
    ]
    _refused(await client.post("/v1/pvs/multi", headers=WRITE_HEADERS, json=mixed), "owner_mismatch", 409)


async def test_the_owner_parameter_on_an_import(client):
    rows = [{"setpointAddress": "I1:SP"}, {"setpointAddress": "I2:SP"}]

    _refused(await client.post("/v1/pvs/multi?owner=arm2", headers=WRITE_HEADERS, json=rows), "not_owner", 403)
    mine = await client.post("/v1/pvs/multi?owner=arm1", headers=WRITE_HEADERS, json=rows)
    assert mine.status_code == 200, mine.text
    assert {pv["ownerInstance"] for pv in mine.json()} == {"arm1"}


async def test_an_import_checks_duplicates_against_its_own_owner(client, as_instance):
    with as_instance("arm2"):
        await _pv(client, "IMPORTED:SP")

    r = await client.post("/v1/pvs/multi", headers=WRITE_HEADERS, json=[{"setpointAddress": "IMPORTED:SP"}])

    assert r.status_code == 200, r.text
    assert r.json()[0]["ownerInstance"] == "arm1"


async def test_a_meta_pv_follows_its_pv(client, as_instance):
    with as_instance("arm2"):
        remote = await _pv(client, "REMOTE:SP")
        assert (
            await client.post("/v1/pvs/meta", headers=WRITE_HEADERS, json={"pvName": "REMOTE:SP"})
        ).status_code == 200
        await _pv(client, "SHARED:SP")
    await _pv(client, "SHARED:SP")

    # Only arm2 registered this address, so the marker belongs on arm2.
    _refused(await client.post("/v1/pvs/meta", headers=WRITE_HEADERS, json={"pvName": "REMOTE:SP"}), "not_owner", 403)
    # Naming another owner is refused, not dropped.
    _refused(
        await client.post("/v1/pvs/meta?owner=arm2", headers=WRITE_HEADERS, json={"pvName": "SHARED:SP"}),
        "not_owner",
        403,
    )
    # Both did: this instance marks its own.
    mine = await client.post("/v1/pvs/meta?owner=arm1", headers=WRITE_HEADERS, json={"pvName": "SHARED:SP"})
    assert mine.status_code == 200, mine.text
    listed = (await client.get("/v1/pvs/meta", headers=WRITE_HEADERS)).json()
    assert {(m["pvName"], m["ownerInstance"]) for m in listed} == {("REMOTE:SP", "arm2"), ("SHARED:SP", "arm1")}

    _refused(
        await client.patch(f"/v1/pvs/meta/{remote['id']}", headers=WRITE_HEADERS, json={"active": False}),
        "not_owner",
        403,
    )
    _refused(await client.delete(f"/v1/pvs/meta/{remote['id']}", headers=WRITE_HEADERS), "not_owner", 403)


# --- snapshots ---------------------------------------------------------------- #


async def test_a_snapshot_in_another_owners_facility_is_refused_before_enqueue(
    client, db_session, as_instance, enqueued
):
    await _facility(db_session, as_instance, "remote", "arm2")

    r = await client.post("/v1/snapshots", headers=WRITE_HEADERS, json={"title": "t", "facility": "remote"})

    _refused(r, "not_owner", 403)
    assert enqueued == []


async def test_the_owner_parameter_on_a_snapshot(client, db_session, as_instance, enqueued):
    await _facility(db_session, as_instance, "local", "arm1")

    # A name other than this instance's is refused, not dropped: with or without a facility.
    _refused(
        await client.post("/v1/snapshots?owner=arm2", headers=WRITE_HEADERS, json={"title": "t"}), "not_owner", 403
    )
    _refused(
        await client.post("/v1/snapshots?owner=arm2", headers=WRITE_HEADERS, json={"title": "t", "facility": "local"}),
        "not_owner",
        403,
    )
    assert enqueued == []
    assert (
        await client.post("/v1/snapshots?owner=arm1", headers=WRITE_HEADERS, json={"title": "t"})
    ).status_code == 200
    assert len(enqueued) == 1


async def test_another_owners_snapshot_cant_be_edited_or_deleted(client, db_session, as_instance):
    with as_instance("arm2"):
        snapshot = Snapshot(title="theirs")
        db_session.add(snapshot)
        await db_session.commit()

    listed = (await client.get("/v1/snapshots", headers=WRITE_HEADERS)).json()["results"]
    assert [(s["title"], s["ownerInstance"]) for s in listed] == [("theirs", "arm2")]
    _refused(
        await client.put(f"/v1/snapshots/{snapshot.id}", headers=WRITE_HEADERS, json={"title": "mine"}),
        "not_owner",
        403,
    )
    _refused(await client.delete(f"/v1/snapshots/{snapshot.id}", headers=WRITE_HEADERS), "not_owner", 403)


async def test_a_capture_with_no_facility_takes_only_this_instances_pvs(db_session: AsyncSession, as_instance):
    with as_instance("arm2"):
        db_session.add(Pv(setpoint_address="THEIRS:SP"))
        await db_session.flush()
    db_session.add(Pv(setpoint_address="MINE:SP"))
    await db_session.flush()
    svc = SnapshotService(
        db_session, FakeEpicsIO({"THEIRS:SP": _ev(1.0), "MINE:SP": _ev(2.0)}), restore_hooks=[_NoHook()]
    )

    summary = await svc.create_snapshot(NewSnapshotDTO(title="local"))

    assert summary.owner_instance == "arm1"
    full = await svc.get_by_id(summary.id)
    assert [v.pv_name for v in full.pv_values] == ["MINE:SP"]


async def test_a_capture_in_another_owners_facility_is_refused_by_the_save(db_session: AsyncSession, as_instance):
    # The route refuses first; this is the service's own check, on the write.
    with as_instance("arm2"):
        facility = Facility(slug="remote", name="Remote")
        db_session.add(facility)
        await db_session.commit()
    svc = SnapshotService(db_session, FakeEpicsIO({}), restore_hooks=[_NoHook()])

    with pytest.raises(NotOwner):
        await svc.create_snapshot(NewSnapshotDTO(title="t"), facility_id=facility.id)


# --- tag groups --------------------------------------------------------------- #


async def test_a_tag_group_name_is_unique_per_owner(client, as_instance):
    with as_instance("arm2"):
        assert (await client.post("/v1/tags", headers=WRITE_HEADERS, json={"name": "Area"})).status_code == 200

    mine = await client.post("/v1/tags", headers=WRITE_HEADERS, json={"name": "Area"})
    assert mine.status_code == 200, mine.text
    assert mine.json()["ownerInstance"] == "arm1"
    duplicate = await client.post("/v1/tags", headers=WRITE_HEADERS, json={"name": "area"})
    assert duplicate.status_code == 409
    owners = sorted(g["ownerInstance"] for g in (await client.get("/v1/tags", headers=WRITE_HEADERS)).json())
    assert owners == ["arm1", "arm2"]


async def test_another_owners_tag_group_is_read_only_here(client, as_instance):
    with as_instance("arm2"):
        group = (await client.post("/v1/tags", headers=WRITE_HEADERS, json={"name": "Area"})).json()
        added = await client.post(f"/v1/tags/{group['id']}/tags", headers=WRITE_HEADERS, json={"name": "north"})
        tag_id = added.json()["group"]["tags"][0]["id"]
    base = f"/v1/tags/{group['id']}"

    _refused(await client.put(base, headers=WRITE_HEADERS, json={"name": "Zone"}), "not_owner", 403)
    _refused(await client.post(f"{base}/tags", headers=WRITE_HEADERS, json={"name": "south"}), "not_owner", 403)
    _refused(await client.put(f"{base}/tags/{tag_id}", headers=WRITE_HEADERS, json={"name": "n"}), "not_owner", 403)
    _refused(await client.delete(f"{base}/tags/{tag_id}", headers=WRITE_HEADERS), "not_owner", 403)
    _refused(await client.delete(base, headers=WRITE_HEADERS), "not_owner", 403)
    _refused(await client.post("/v1/tags?owner=arm2", headers=WRITE_HEADERS, json={"name": "Zone"}), "not_owner", 403)


async def test_a_bulk_import_never_adds_to_another_owners_group(client, as_instance):
    with as_instance("arm2"):
        await client.post("/v1/tags", headers=WRITE_HEADERS, json={"name": "Area"})

    r = await client.post("/v1/tags/bulk", headers=WRITE_HEADERS, json={"groups": {"Area": ["north"]}})

    assert r.status_code == 200, r.text
    assert r.json()["groupsCreated"] == 1 and r.json()["tagsCreated"] == 1
    _refused(
        await client.post("/v1/tags/bulk?owner=arm2", headers=WRITE_HEADERS, json={"groups": {"Area": ["south"]}}),
        "not_owner",
        403,
    )


# --- the backstop --------------------------------------------------------------- #


async def test_the_backstop_answers_403_when_a_service_check_is_missing(client, as_instance, monkeypatch):
    with as_instance("arm2"):
        remote = await _pv(client, "REMOTE:SP")
    monkeypatch.setattr("canopy.squirrel.services.pv_service.require_owned", lambda owner, what: None)

    r = await client.put(f"/v1/pvs/{remote['id']}", headers=WRITE_HEADERS, json={"device": "X"})

    _refused(r, "not_owner", 403)
    assert "squirrel.pv" in r.json()["detail"]


async def test_the_backstop_covers_a_tag_through_its_group(db_session: AsyncSession, as_instance):
    with as_instance("arm2"):
        from canopy.squirrel.data.models import TagGroup

        group = TagGroup(name="Area")
        db_session.add(group)
        await db_session.flush()

    db_session.add(Tag(name="north", group_id=group.id))
    with pytest.raises(NotOwner, match="squirrel.tag"):
        await db_session.flush()
