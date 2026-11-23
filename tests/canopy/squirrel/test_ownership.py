"""Row owners in Squirrel: PVs, snapshots and tag groups are written only by their owner.

The suite runs as ``arm1``. Rows another instance owns are created inside
``as_instance("arm2")``, the way a mirror receives an arm's rows through
replication, and then written as ``arm1``.
"""

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from canopy.core.errors import NotOwner, InsufficientFacilityAccess
from canopy.squirrel.errors import SnapshotNotFound
from canopy.squirrel.schemas.pv import UpdatePVElementDTO
from canopy.squirrel.data.models import (
    Pv,
    Tag,
    MetaPv,
    Snapshot,
    TagGroup,
    SnapshotValue,
)
from canopy.core.facilities.models import Facility
from canopy.squirrel.schemas.snapshot import NewSnapshotDTO
from canopy.squirrel.data.repositories import PvRepository
from canopy.squirrel.services.pv_service import PvService
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


async def test_a_caller_outside_the_facility_isnt_told_the_owner(db_session: AsyncSession, as_instance):
    # The facility gate runs first, so a guessed id gets what GET would give it.
    with as_instance("arm2"):
        facility = Facility(slug="remote", name="Remote")
        db_session.add(facility)
        await db_session.flush()
        snapshot = Snapshot(title="theirs", facility_id=facility.id)
        pv = Pv(setpoint_address="REMOTE:SP")
        db_session.add_all([snapshot, pv])
        await db_session.flush()
        await PvRepository(db_session).set_facilities(pv.id, {facility.id})
    snapshots = SnapshotService(db_session, FakeEpicsIO({}), restore_hooks=[_NoHook()], facility_scope={"elsewhere"})
    pvs = PvService(db_session, facility_scope={"elsewhere"})

    with pytest.raises(SnapshotNotFound):
        await snapshots.update_snapshot_metadata(snapshot.id, title="mine")
    with pytest.raises(SnapshotNotFound):
        await snapshots.delete_snapshot(snapshot.id)
    with pytest.raises(InsufficientFacilityAccess):
        await pvs.update(pv.id, UpdatePVElementDTO(device="X"))
    with pytest.raises(InsufficientFacilityAccess):
        await pvs.delete(pv.id)


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
        group = TagGroup(name="Area")
        db_session.add(group)
        await db_session.flush()

    db_session.add(Tag(name="north", group_id=group.id))
    with pytest.raises(NotOwner, match="squirrel.tag"):
        await db_session.flush()


async def _theirs_and_mine(db_session: AsyncSession, as_instance) -> tuple[Snapshot, Pv, Snapshot, Pv]:
    with as_instance("arm2"):
        theirs = (Snapshot(title="theirs"), Pv(setpoint_address="THEIRS:SP"))
        db_session.add_all(theirs)
        await db_session.flush()
    mine = (Snapshot(title="mine"), Pv(setpoint_address="MINE:SP"))
    db_session.add_all(mine)
    await db_session.flush()
    return *theirs, *mine


@pytest.mark.parametrize("stored", ["arm2", "arm1"])
async def test_the_backstop_refuses_a_change_to_a_stored_owner(db_session: AsyncSession, as_instance, stored):
    # Taking another instance's row, or giving one away.
    with as_instance(stored):
        pv = Pv(setpoint_address="A:SP")
        db_session.add(pv)
        await db_session.flush()

    pv.owner_instance = "arm1" if stored == "arm2" else "arm2"
    with pytest.raises(NotOwner, match=f"owned by '{stored}', and its owner_instance never changes"):
        await db_session.flush()


async def test_the_backstop_judges_a_moved_tag_by_its_old_group_too(db_session: AsyncSession, as_instance):
    with as_instance("arm2"):
        theirs = TagGroup(name="Theirs")
        db_session.add(theirs)
        await db_session.flush()
        tag = Tag(name="north", group_id=theirs.id)
        db_session.add(tag)
        await db_session.flush()
    mine = TagGroup(name="Mine")
    db_session.add(mine)
    await db_session.flush()

    tag.group_id = mine.id
    with pytest.raises(NotOwner, match="'arm2'"):
        await db_session.flush()


async def test_the_backstop_reads_a_moved_tags_old_group_when_it_wasnt_loaded(db_session: AsyncSession, as_instance):
    with as_instance("arm2"):
        theirs = TagGroup(name="Theirs")
        db_session.add(theirs)
        await db_session.flush()
        tag = Tag(name="north", group_id=theirs.id)
        db_session.add(tag)
        await db_session.flush()
    mine = TagGroup(name="Mine")
    db_session.add(mine)
    await db_session.flush()

    db_session.expire(tag)  # the old group_id isn't in the history now
    tag.group_id = mine.id
    with pytest.raises(NotOwner, match="squirrel.tag row is owned by 'arm2'"):
        await db_session.flush()


async def test_the_backstop_covers_a_tag_attached_to_another_owners_group(db_session: AsyncSession, as_instance):
    # Through the relationship: the flush hasn't copied the group's id into group_id yet.
    with as_instance("arm2"):
        theirs = TagGroup(name="Theirs")
        db_session.add(theirs)
        await db_session.flush()

    db_session.add(Tag(name="north", group=theirs))
    with pytest.raises(NotOwner, match="squirrel.tag row is owned by 'arm2'"):
        await db_session.flush()


@pytest.mark.parametrize("foreign", ["snapshot", "pv"])
async def test_the_backstop_covers_a_new_snapshot_value(db_session: AsyncSession, as_instance, foreign):
    their_snapshot, their_pv, my_snapshot, my_pv = await _theirs_and_mine(db_session, as_instance)
    snapshot, pv = (their_snapshot, my_pv) if foreign == "snapshot" else (my_snapshot, their_pv)

    db_session.add(SnapshotValue(snapshot_id=snapshot.id, pv_id=pv.id, pv_name=pv.setpoint_address))
    with pytest.raises(NotOwner, match="squirrel.snapshot_value row is owned by 'arm2'"):
        await db_session.flush()


@pytest.mark.parametrize("write", ["update", "delete"])
async def test_the_backstop_covers_another_owners_snapshot_value(db_session: AsyncSession, as_instance, write):
    their_snapshot, their_pv, _, _ = await _theirs_and_mine(db_session, as_instance)
    with as_instance("arm2"):
        value = SnapshotValue(snapshot_id=their_snapshot.id, pv_id=their_pv.id, pv_name="THEIRS:SP")
        db_session.add(value)
        await db_session.flush()

    if write == "update":
        value.pv_name = "MINE:SP"
    else:
        await db_session.delete(value)
    with pytest.raises(NotOwner, match="squirrel.snapshot_value"):
        await db_session.flush()


async def test_the_backstop_covers_a_meta_pv_marker(db_session: AsyncSession, as_instance):
    _, their_pv, _, my_pv = await _theirs_and_mine(db_session, as_instance)
    db_session.add(MetaPv(pv_id=my_pv.id, display_name="Mine"))
    await db_session.flush()

    db_session.add(MetaPv(pv_id=their_pv.id, display_name="Theirs"))
    with pytest.raises(NotOwner, match="squirrel.meta_pv row is owned by 'arm2'"):
        await db_session.flush()


async def test_the_backstop_passes_a_pv_delete_that_cascades_to_its_values(db_session: AsyncSession, as_instance):
    _, _, my_snapshot, my_pv = await _theirs_and_mine(db_session, as_instance)
    db_session.add(SnapshotValue(snapshot_id=my_snapshot.id, pv_id=my_pv.id, pv_name="MINE:SP"))
    await db_session.flush()
    await db_session.refresh(my_pv, ["snapshot_values"])

    await db_session.delete(my_pv)
    await db_session.flush()


async def test_the_backstop_passes_this_instances_values(db_session: AsyncSession, as_instance):
    _, _, my_snapshot, my_pv = await _theirs_and_mine(db_session, as_instance)
    value = SnapshotValue(snapshot_id=my_snapshot.id, pv_id=my_pv.id, pv_name="MINE:SP")
    db_session.add(value)
    await db_session.flush()

    value.pv_name = "MINE:SP2"
    await db_session.flush()
    await db_session.delete(value)
    await db_session.flush()
