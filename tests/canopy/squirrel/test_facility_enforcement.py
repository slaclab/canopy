"""Facility-scoped writes: a key's ``facilities`` grant constrains what it can mutate.

Auth is injected by rebinding the process-global key registry (the seam
``tests/canopy/core/test_module_scoping.py`` already uses) — but multi-key here, since
every case contrasts a facility-restricted key against an unrestricted one in the same
request flow. The registry is restored in teardown; it leaks across the suite otherwise.
"""

import pytest
from httpx import AsyncClient, ASGITransport
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncSession

from canopy.core import security
from canopy.core.db import get_session
from canopy.core.errors import register_canopy_error_handler
from canopy.core.modules import WILDCARD, tag_module
from canopy.squirrel.app import _facility_in_use
from canopy.core.security import APIKey
from canopy.core.facilities import gateway as facilities_gateway
from canopy.core.facilities import service as facilities_service
from canopy.squirrel.data.models import Snapshot, SnapshotValue
from canopy.core.facilities.models import Facility
from canopy.core.facilities.routes import router as facilities_router

SC_HEADERS = {"X-API-Key": "sc-key"}
NC_HEADERS = {"X-API-Key": "nc-key"}
LCLS_HEADERS = {"X-API-Key": "lcls-key"}
NONE_HEADERS = {"X-API-Key": "none-key"}
SC_FACET_HEADERS = {"X-API-Key": "sc-facet-key"}
WILD_HEADERS = {"X-API-Key": "wild-key"}
ADMIN_HEADERS = {"X-API-Key": "admin-key"}


class MultiKeyRegistry:
    def __init__(self, keys: list[APIKey]) -> None:
        self.by_token = {k.token: k for k in keys}

    async def get(self, token: str) -> APIKey | None:
        return self.by_token.get(token)


@pytest.fixture(autouse=True)
def scoped_keys():
    """Five write-capable keys differing only in facility grant, plus an admin key for
    the facility definition routes."""
    previous = security._registry
    security.bind_key_registry(
        MultiKeyRegistry(
            [
                APIKey(
                    token="sc-key",
                    read=True,
                    write=True,
                    modules=frozenset({WILDCARD}),
                    facilities=frozenset({"lcls-sc"}),
                ),
                APIKey(
                    token="nc-key",
                    read=True,
                    write=True,
                    modules=frozenset({WILDCARD}),
                    facilities=frozenset({"lcls-nc"}),
                ),
                APIKey(
                    token="lcls-key",
                    read=True,
                    write=True,
                    modules=frozenset({WILDCARD}),
                    facilities=frozenset({"lcls"}),
                ),
                APIKey(token="none-key", read=True, write=True, modules=frozenset({WILDCARD}), facilities=frozenset()),
                APIKey(
                    token="sc-facet-key",
                    read=True,
                    write=True,
                    modules=frozenset({WILDCARD}),
                    facilities=frozenset({"lcls-sc", "facet"}),
                ),
                APIKey(
                    token="wild-key",
                    read=True,
                    write=True,
                    modules=frozenset({WILDCARD}),
                    facilities=frozenset({WILDCARD}),
                ),
                APIKey(
                    token="admin-key",
                    read=True,
                    write=True,
                    admin=True,
                    modules=frozenset({WILDCARD}),
                    facilities=frozenset({WILDCARD}),
                ),
            ]
        )
    )
    yield
    security._registry = previous


async def _seed_hierarchy(session: AsyncSession) -> dict[str, Facility]:
    """``lcls`` with children ``lcls-sc``/``lcls-nc``, plus an unrelated ``facet``."""
    lcls = Facility(slug="lcls", name="LCLS")
    facet = Facility(slug="facet", name="FACET")
    session.add_all([lcls, facet])
    await session.commit()
    sc = Facility(slug="lcls-sc", name="SC", parent_id=lcls.id)
    nc = Facility(slug="lcls-nc", name="NC", parent_id=lcls.id)
    session.add_all([sc, nc])
    await session.commit()
    return {"lcls": lcls, "lcls-sc": sc, "lcls-nc": nc, "facet": facet}


async def _create_pv(client: AsyncClient, headers: dict, address: str, facilities: list[str]) -> str:
    r = await client.post("/v1/pvs", headers=headers, json={"setpointAddress": address, "facilities": facilities})
    assert r.status_code == 200, r.text
    return r.json()["id"]


# --- PV writes --------------------------------------------------------------- #


async def test_scoped_key_writes_in_its_facility_but_not_a_sibling(client: AsyncClient, db_session: AsyncSession):
    await _seed_hierarchy(db_session)
    pv_id = await _create_pv(client, SC_HEADERS, "SC:PV:SP", ["lcls-sc"])

    r = await client.put(f"/v1/pvs/{pv_id}", headers=SC_HEADERS, json={"device": "mine"})
    assert r.status_code == 200, r.text

    r = await client.put(f"/v1/pvs/{pv_id}", headers=NC_HEADERS, json={"device": "theirs"})
    assert r.status_code == 403
    assert r.json()["code"] == "insufficient_facility_access"


async def test_parent_grant_covers_a_child_facility(client: AsyncClient, db_session: AsyncSession):
    await _seed_hierarchy(db_session)
    pv_id = await _create_pv(client, WILD_HEADERS, "SC:PV:SP", ["lcls-sc"])

    # `lcls-key` holds the parent; descendant expansion is what makes this a 200.
    r = await client.put(f"/v1/pvs/{pv_id}", headers=LCLS_HEADERS, json={"device": "d"})
    assert r.status_code == 200, r.text


async def test_holding_only_one_of_a_pvs_several_facilities_is_not_enough(
    client: AsyncClient, db_session: AsyncSession
):
    """Every write to a multi-facility PV — not just a change to its facility list —
    requires holding *all* of its facilities, not just one. Otherwise a key scoped to
    `lcls-sc` could edit, or (far worse) delete, a PV also assigned to `facet` on the
    strength of a single shared facility, reaching data and destroying a row it was
    never granted any access to via `facet`."""
    facilities = await _seed_hierarchy(db_session)
    pv_id = await _create_pv(client, WILD_HEADERS, "SHARED:PV:SP", ["lcls-sc", "facet"])

    r = await client.put(f"/v1/pvs/{pv_id}", headers=SC_HEADERS, json={"device": "d"})
    assert r.status_code == 403, r.text
    assert r.json()["code"] == "insufficient_facility_access"

    r = await client.delete(f"/v1/pvs/{pv_id}", headers=SC_HEADERS)
    assert r.status_code == 403, r.text

    # A key holding every one of the PV's facilities — not just the wildcard — can
    # still write it.
    r = await client.put(f"/v1/pvs/{pv_id}", headers=SC_FACET_HEADERS, json={"device": "d"})
    assert r.status_code == 200, r.text
    assert set(r.json()["facilityIds"]) == {facilities["lcls-sc"].id, facilities["facet"].id}


async def test_an_update_cannot_silently_drop_a_facility_the_key_cant_reach(
    client: AsyncClient, db_session: AsyncSession
):
    """Overlap with the *new* set isn't enough: every facility being added or removed
    must be in the grant, or an SC-only key could rewrite a shared PV's facilities to
    quietly strip a facility (`facet`) it was never scoped to touch."""
    facilities = await _seed_hierarchy(db_session)
    pv_id = await _create_pv(client, WILD_HEADERS, "SHARED:PV:SP", ["lcls-sc", "facet"])

    # Rewriting to just the facility the key holds would silently drop `facet`.
    r = await client.put(f"/v1/pvs/{pv_id}", headers=SC_HEADERS, json={"facilities": ["lcls-sc"]})
    assert r.status_code == 403
    assert r.json()["code"] == "insufficient_facility_access"

    # The refused write did not land.
    r = await client.get("/v1/pvs/paged", headers=WILD_HEADERS)
    pv = next(p for p in r.json()["results"] if p["id"] == pv_id)
    assert set(pv["facilityIds"]) == {facilities["lcls-sc"].id, facilities["facet"].id}

    # Symmetric case: adding a facility the key doesn't hold is refused the same way,
    # even though the PV's sole existing facility overlaps the grant.
    other_pv = await _create_pv(client, SC_HEADERS, "SC:PV:SP", ["lcls-sc"])
    r = await client.put(f"/v1/pvs/{other_pv}", headers=SC_HEADERS, json={"facilities": ["lcls-sc", "facet"]})
    assert r.status_code == 403


async def test_unassigned_pv_is_writable_only_by_an_unrestricted_key(client: AsyncClient, db_session: AsyncSession):
    """An unassigned row isn't covered by any grant, so a restricted key — even one
    holding every facility it names elsewhere — can't write it; only an unrestricted
    key can. This is the write-side mirror of list filtering hiding the row from a
    restricted key's reads."""
    await _seed_hierarchy(db_session)
    pv_id = await _create_pv(client, WILD_HEADERS, "ORPHAN:PV:SP", [])

    r = await client.put(f"/v1/pvs/{pv_id}", headers=NC_HEADERS, json={"device": "d"})
    assert r.status_code == 403, r.text
    assert r.json()["code"] == "insufficient_facility_access"
    r = await client.delete(f"/v1/pvs/{pv_id}", headers=NONE_HEADERS)
    assert r.status_code == 403, r.text

    r = await client.put(f"/v1/pvs/{pv_id}", headers=WILD_HEADERS, json={"device": "d"})
    assert r.status_code == 200, r.text
    r = await client.delete(f"/v1/pvs/{pv_id}", headers=WILD_HEADERS)
    assert r.status_code == 200, r.text


async def test_assigning_a_pv_outside_the_grant_is_refused(client: AsyncClient, db_session: AsyncSession):
    await _seed_hierarchy(db_session)

    r = await client.post(
        "/v1/pvs", headers=SC_HEADERS, json={"setpointAddress": "NC:PV:SP", "facilities": ["lcls-nc"]}
    )
    assert r.status_code == 403
    assert r.json()["code"] == "insufficient_facility_access"

    pv_id = await _create_pv(client, SC_HEADERS, "SC:PV:SP", ["lcls-sc"])
    r = await client.put(f"/v1/pvs/{pv_id}", headers=SC_HEADERS, json={"facilities": ["lcls-nc"]})
    assert r.status_code == 403


async def test_creating_a_pv_in_several_facilities_needs_one_of_them_held(
    client: AsyncClient, db_session: AsyncSession
):
    """A create is allowed when at least one requested facility is in the key's grant."""
    facilities = await _seed_hierarchy(db_session)

    r = await client.post(
        "/v1/pvs",
        headers=SC_HEADERS,
        json={"setpointAddress": "SHARED:PV:SP", "facilities": ["lcls-sc", "facet"]},
    )
    assert r.status_code == 200, r.text
    assert set(r.json()["facilityIds"]) == {facilities["lcls-sc"].id, facilities["facet"].id}

    r = await client.get("/v1/pvs/paged", headers=WILD_HEADERS)
    assert len(r.json()["results"]) == 1

    r = await client.post(
        "/v1/pvs",
        headers=SC_FACET_HEADERS,
        json={"setpointAddress": "SHARED-2:PV:SP", "facilities": ["lcls-sc", "facet"]},
    )
    assert r.status_code == 200, r.text


async def test_create_does_not_leak_an_out_of_scope_pv_s_existence_via_409(
    client: AsyncClient, db_session: AsyncSession
):
    """The facility gate must run before the address-uniqueness check: otherwise a
    restricted key probing guessed addresses could learn an out-of-scope PV exists
    from a ``PvAlreadyExists`` 409 before ever being told it lacks facility access."""
    await _seed_hierarchy(db_session)
    await _create_pv(client, NC_HEADERS, "NC:PV:SP", ["lcls-nc"])

    r = await client.post(
        "/v1/pvs", headers=SC_HEADERS, json={"setpointAddress": "NC:PV:SP", "facilities": ["lcls-nc"]}
    )
    assert r.status_code == 403
    assert r.json()["code"] == "insufficient_facility_access"

    r = await client.post(
        "/v1/pvs/multi", headers=SC_HEADERS, json=[{"setpointAddress": "NC:PV:SP", "facilities": ["lcls-nc"]}]
    )
    assert r.status_code == 403
    assert r.json()["code"] == "insufficient_facility_access"


async def test_multi_create_gates_each_row_not_the_import_s_union(client: AsyncClient, db_session: AsyncSession):
    """Every row must independently overlap the grant; a sibling row cannot authorize it."""
    await _seed_hierarchy(db_session)

    r = await client.post(
        "/v1/pvs/multi",
        headers=SC_HEADERS,
        json=[
            {"setpointAddress": "SC:PV:SP", "facilities": ["lcls-sc"]},
            {"setpointAddress": "NC:PV:SP", "facilities": ["lcls-nc"]},
        ],
    )
    assert r.status_code == 403
    assert r.json()["code"] == "insufficient_facility_access"

    # And the refused batch did not partially land.
    r = await client.get("/v1/pvs/paged", headers=WILD_HEADERS)
    assert r.json()["results"] == []

    # A row may also name out-of-scope facilities if it names at least one in scope.
    r = await client.post(
        "/v1/pvs/multi",
        headers=SC_HEADERS,
        json=[{"setpointAddress": "SHARED:PV:SP", "facilities": ["lcls-sc", "facet"]}],
    )
    assert r.status_code == 200, r.text
    assert len(r.json()) == 1


async def test_unknown_facility_slug_on_a_write_is_404_not_a_silent_no_op(
    client: AsyncClient, db_session: AsyncSession
):
    await _seed_hierarchy(db_session)
    r = await client.post("/v1/pvs", headers=WILD_HEADERS, json={"setpointAddress": "X:PV:SP", "facilities": ["nope"]})
    assert r.status_code == 404
    assert r.json()["code"] == "facility_not_found"


async def test_multiple_unknown_facility_slugs_are_all_named_at_once(client: AsyncClient, db_session: AsyncSession):
    await _seed_hierarchy(db_session)
    r = await client.post(
        "/v1/pvs", headers=WILD_HEADERS, json={"setpointAddress": "X:PV:SP", "facilities": ["nope", "also-nope"]}
    )
    assert r.status_code == 404
    assert r.json()["code"] == "facility_not_found"
    assert "nope" in r.json()["detail"]
    assert "also-nope" in r.json()["detail"]


# --- reads ------------------------------------------------------------------- #


async def test_read_is_intersected_with_the_grant_in_both_directions(client: AsyncClient, db_session: AsyncSession):
    await _seed_hierarchy(db_session)
    await _create_pv(client, WILD_HEADERS, "SC:PV:SP", ["lcls-sc"])
    await _create_pv(client, WILD_HEADERS, "NC:PV:SP", ["lcls-nc"])

    # Key narrower than the query: `?facility=lcls` covers both children, the grant one.
    r = await client.get("/v1/pvs/paged?facility=lcls", headers=SC_HEADERS)
    assert {p["setpointAddress"] for p in r.json()["results"]} == {"SC:PV:SP"}

    # Query narrower than the key.
    r = await client.get("/v1/pvs/paged?facility=lcls-sc", headers=LCLS_HEADERS)
    assert {p["setpointAddress"] for p in r.json()["results"]} == {"SC:PV:SP"}

    # Disjoint: the query asks for a facility the key isn't granted -> nothing, not
    # everything.
    r = await client.get("/v1/pvs/paged?facility=lcls-nc", headers=SC_HEADERS)
    assert r.json()["results"] == []

    # No query at all still respects the grant.
    r = await client.get("/v1/pvs/paged", headers=SC_HEADERS)
    assert {p["setpointAddress"] for p in r.json()["results"]} == {"SC:PV:SP"}


async def test_empty_grant_reads_nothing(client: AsyncClient, db_session: AsyncSession):
    await _seed_hierarchy(db_session)
    await _create_pv(client, WILD_HEADERS, "SC:PV:SP", ["lcls-sc"])

    r = await client.get("/v1/pvs/paged", headers=NONE_HEADERS)
    assert r.status_code == 200, r.text
    assert r.json()["results"] == []


# --- the async write paths --------------------------------------------------- #


async def test_scoped_key_must_name_a_facility_to_create_a_snapshot(
    client: AsyncClient, db_session: AsyncSession, enqueued: list
):
    await _seed_hierarchy(db_session)

    r = await client.post("/v1/snapshots", headers=SC_HEADERS, json={"title": "T"})
    assert r.status_code == 403
    assert r.json()["code"] == "insufficient_facility_access"
    assert enqueued == []  # a refused write must not become a queued job

    r = await client.post("/v1/snapshots", headers=SC_HEADERS, json={"title": "T", "facility": "lcls-nc"})
    assert r.status_code == 403
    assert enqueued == []

    r = await client.post("/v1/snapshots", headers=SC_HEADERS, json={"title": "T", "facility": "lcls-sc"})
    assert r.status_code == 200, r.text
    assert len(enqueued) == 1


async def test_unrestricted_key_may_create_an_unassigned_snapshot(
    client: AsyncClient, db_session: AsyncSession, enqueued: list
):
    r = await client.post("/v1/snapshots", headers=WILD_HEADERS, json={"title": "T"})
    assert r.status_code == 200, r.text
    assert enqueued[-1][2]["facility_id"] is None


async def test_restore_is_gated_on_the_snapshot_s_own_facility(
    client: AsyncClient, db_session: AsyncSession, enqueued: list
):
    """A key that can't view a snapshot can't restore it either — even when the snapshot
    holds PVs in the key's own facility. `lcls-sc` is below `lcls`, never above it."""
    facilities = await _seed_hierarchy(db_session)
    sc_pv = await _create_pv(client, WILD_HEADERS, "SC:PV:SP", ["lcls-sc"])
    snapshot = Snapshot(title="lcls", facility_id=facilities["lcls"].id)
    db_session.add(snapshot)
    await db_session.commit()
    db_session.add(
        SnapshotValue(snapshot_id=snapshot.id, pv_id=sc_pv, pv_name="SC:PV:SP", setpoint_value={"value": 1.0})
    )
    await db_session.commit()
    enqueued.clear()

    assert (await client.get(f"/v1/snapshots/{snapshot.id}", headers=SC_HEADERS)).status_code == 404
    r = await client.post(f"/v1/snapshots/{snapshot.id}/restore", headers=SC_HEADERS, json={})
    assert r.status_code == 404, r.text
    assert r.json()["code"] == "snapshot_not_found"
    assert enqueued == []

    r = await client.post(f"/v1/snapshots/{snapshot.id}/restore", headers=LCLS_HEADERS, json={})
    assert r.status_code == 200, r.text
    assert enqueued[-1][2]["pv_ids"] == [sc_pv]


async def test_restore_skips_a_pv_reassigned_out_of_scope_after_capture(
    client: AsyncClient, db_session: AsyncSession, enqueued: list
):
    """Capture only takes the snapshot's facility's PVs, but a PV's assignment can change
    afterwards; restore must not drive a setpoint the key can no longer reach."""
    facilities = await _seed_hierarchy(db_session)
    kept_pv = await _create_pv(client, WILD_HEADERS, "SC:KEPT:SP", ["lcls-sc"])
    moved_pv = await _create_pv(client, WILD_HEADERS, "SC:MOVED:SP", ["lcls-sc"])
    snapshot = Snapshot(title="sc", facility_id=facilities["lcls-sc"].id)
    db_session.add(snapshot)
    await db_session.commit()
    db_session.add_all(
        [
            SnapshotValue(snapshot_id=snapshot.id, pv_id=pv_id, pv_name=name, setpoint_value={"value": 1.0})
            for pv_id, name in [(kept_pv, "SC:KEPT:SP"), (moved_pv, "SC:MOVED:SP")]
        ]
    )
    await db_session.commit()
    r = await client.put(f"/v1/pvs/{moved_pv}", headers=WILD_HEADERS, json={"facilities": ["lcls-nc"]})
    assert r.status_code == 200, r.text

    r = await client.post(f"/v1/snapshots/{snapshot.id}/restore", headers=SC_HEADERS, json={})
    assert r.status_code == 200, r.text
    kwargs = enqueued[-1][2]
    assert kwargs["pv_ids"] == [kept_pv]
    assert kwargs["skipped_pv_ids"] == [moved_pv]


async def test_restore_includes_a_multi_facility_pv_on_one_shared_facility(
    client: AsyncClient, db_session: AsyncSession, enqueued: list
):
    """One shared facility is enough to restore a multi-facility PV, matching capture
    and listing (both include a PV assigned to *any* facility in scope). Restore only
    writes the value captured under this snapshot's own facility — it never touches the
    PV's other facilities — so it must not apply the full-containment rule that guards
    update/delete, which do mutate the shared row itself. Requiring every facility here
    would skip a PV the key can see in this very snapshot and is legitimately restoring
    a value scoped to the facility it does hold."""
    facilities = await _seed_hierarchy(db_session)
    shared_pv = await _create_pv(client, WILD_HEADERS, "SHARED:PV:SP", ["lcls-sc", "facet"])
    snapshot = Snapshot(title="sc", facility_id=facilities["lcls-sc"].id)
    db_session.add(snapshot)
    await db_session.commit()
    db_session.add(
        SnapshotValue(snapshot_id=snapshot.id, pv_id=shared_pv, pv_name="SHARED:PV:SP", setpoint_value={"value": 1.0})
    )
    await db_session.commit()
    enqueued.clear()

    r = await client.post(f"/v1/snapshots/{snapshot.id}/restore", headers=SC_HEADERS, json={})
    assert r.status_code == 200, r.text
    assert enqueued[-1][2]["pv_ids"] == [shared_pv]


async def test_restore_with_every_pv_out_of_scope_is_403(client: AsyncClient, db_session: AsyncSession, enqueued: list):
    """A visible snapshot whose PVs have all been reassigned out of the key's reach: the
    key may see the snapshot but write none of it, which is a refusal, not a no-op job."""
    facilities = await _seed_hierarchy(db_session)
    pv = await _create_pv(client, WILD_HEADERS, "SC:PV:SP", ["lcls-sc"])
    snapshot = Snapshot(title="sc", facility_id=facilities["lcls-sc"].id)
    db_session.add(snapshot)
    await db_session.commit()
    db_session.add(SnapshotValue(snapshot_id=snapshot.id, pv_id=pv, pv_name="SC:PV:SP", setpoint_value={"value": 1.0}))
    await db_session.commit()
    r = await client.put(f"/v1/pvs/{pv}", headers=WILD_HEADERS, json={"facilities": ["lcls-nc"]})
    assert r.status_code == 200, r.text
    enqueued.clear()

    r = await client.post(f"/v1/snapshots/{snapshot.id}/restore", headers=SC_HEADERS, json={})
    assert r.status_code == 403, r.text
    assert r.json()["code"] == "insufficient_facility_access"
    assert enqueued == []


async def test_restore_naming_pvs_outside_the_snapshot_is_refused_not_widened(
    client: AsyncClient, db_session: AsyncSession, enqueued: list
):
    """Regression: `pvIds` that match nothing in the snapshot narrowed to `[]`, which the
    route enqueued and the worker read as "restore everything" — every facility's PVs."""
    facilities = await _seed_hierarchy(db_session)
    sc_pv = await _create_pv(client, WILD_HEADERS, "SC:PV:SP", ["lcls-sc"])
    nc_pv = await _create_pv(client, WILD_HEADERS, "NC:PV:SP", ["lcls-nc"])
    snapshot = Snapshot(title="all", facility_id=facilities["lcls"].id)
    db_session.add(snapshot)
    await db_session.commit()
    db_session.add_all(
        [
            SnapshotValue(snapshot_id=snapshot.id, pv_id=pv_id, pv_name=name, setpoint_value={"value": 1.0})
            for pv_id, name in [(sc_pv, "SC:PV:SP"), (nc_pv, "NC:PV:SP")]
        ]
    )
    await db_session.commit()
    enqueued.clear()

    r = await client.post(
        f"/v1/snapshots/{snapshot.id}/restore",
        headers=LCLS_HEADERS,
        json={"pvIds": ["00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002"]},
    )
    assert r.status_code == 400, r.text
    assert r.json()["code"] == "bad_request"
    assert enqueued == []


async def test_restore_with_an_empty_pv_list_restores_nothing(
    client: AsyncClient, db_session: AsyncSession, enqueued: list
):
    facilities = await _seed_hierarchy(db_session)
    sc_pv = await _create_pv(client, WILD_HEADERS, "SC:PV:SP", ["lcls-sc"])
    snapshot = Snapshot(title="sc", facility_id=facilities["lcls-sc"].id)
    db_session.add(snapshot)
    await db_session.commit()
    db_session.add(
        SnapshotValue(snapshot_id=snapshot.id, pv_id=sc_pv, pv_name="SC:PV:SP", setpoint_value={"value": 1.0})
    )
    await db_session.commit()
    enqueued.clear()

    for headers in (SC_HEADERS, WILD_HEADERS):
        r = await client.post(f"/v1/snapshots/{snapshot.id}/restore", headers=headers, json={"pvIds": []})
        assert r.status_code == 400, r.text
    assert enqueued == []


async def test_snapshot_metadata_and_delete_are_facility_gated(client: AsyncClient, db_session: AsyncSession):
    facilities = await _seed_hierarchy(db_session)
    snapshot = Snapshot(title="sc-snap", facility_id=facilities["lcls-sc"].id)
    db_session.add(snapshot)
    await db_session.commit()

    r = await client.put(f"/v1/snapshots/{snapshot.id}", headers=NC_HEADERS, json={"title": "hijacked"})
    assert r.status_code == 403
    r = await client.delete(f"/v1/snapshots/{snapshot.id}", headers=NC_HEADERS)
    assert r.status_code == 403

    # And the refused update did not land.
    r = await client.get(f"/v1/snapshots/{snapshot.id}", headers=WILD_HEADERS)
    assert r.json()["title"] == "sc-snap"

    r = await client.put(f"/v1/snapshots/{snapshot.id}", headers=SC_HEADERS, json={"title": "renamed"})
    assert r.status_code == 200, r.text


# --- out-of-scope snapshots must not leak via get/compare/restore ------------ #


async def test_get_snapshot_outside_the_grant_is_404_not_403(client: AsyncClient, db_session: AsyncSession):
    """Existence itself must not leak: an out-of-scope snapshot reports the same as a
    snapshot that doesn't exist, not a 403 that confirms something is there."""
    facilities = await _seed_hierarchy(db_session)
    snapshot = Snapshot(title="nc-snap", facility_id=facilities["lcls-nc"].id)
    db_session.add(snapshot)
    await db_session.commit()

    r = await client.get(f"/v1/snapshots/{snapshot.id}", headers=SC_HEADERS)
    assert r.status_code == 404, r.text
    assert r.json()["code"] == "snapshot_not_found"

    r = await client.get(f"/v1/snapshots/{snapshot.id}", headers=WILD_HEADERS)
    assert r.status_code == 200, r.text


async def test_compare_snapshots_outside_the_grant_is_404(client: AsyncClient, db_session: AsyncSession):
    facilities = await _seed_hierarchy(db_session)
    in_scope = Snapshot(title="sc-snap", facility_id=facilities["lcls-sc"].id)
    out_of_scope = Snapshot(title="nc-snap", facility_id=facilities["lcls-nc"].id)
    db_session.add_all([in_scope, out_of_scope])
    await db_session.commit()

    r = await client.get(f"/v1/snapshots/{in_scope.id}/compare/{out_of_scope.id}", headers=SC_HEADERS)
    assert r.status_code == 404, r.text
    assert r.json()["code"] == "snapshot_not_found"


async def test_unassigned_snapshot_is_hidden_from_a_restricted_key_by_id_too(
    client: AsyncClient, db_session: AsyncSession, enqueued: list
):
    """An unassigned snapshot (``facility_id IS NULL``) is already hidden from a
    restricted key's list; this pins that getting it directly by id, or comparing it,
    doesn't leak it either — a snapshot captured before facility scoping existed
    (or taken with no facility) must not expose every PV's values to a key that can
    list none of them."""
    await _seed_hierarchy(db_session)
    sc_pv = await _create_pv(client, WILD_HEADERS, "SC:PV:SP", ["lcls-sc"])
    snapshot = Snapshot(title="unassigned")
    db_session.add(snapshot)
    await db_session.commit()
    db_session.add(
        SnapshotValue(snapshot_id=snapshot.id, pv_id=sc_pv, pv_name="SC:PV:SP", setpoint_value={"value": 1.0})
    )
    await db_session.commit()

    for headers in (SC_HEADERS, NONE_HEADERS):
        r = await client.get(f"/v1/snapshots/{snapshot.id}", headers=headers)
        assert r.status_code == 404, r.text
        assert r.json()["code"] == "snapshot_not_found"

    r = await client.get(f"/v1/snapshots/{snapshot.id}", headers=WILD_HEADERS)
    assert r.status_code == 200, r.text

    r = await client.get(f"/v1/snapshots/{snapshot.id}/compare/{snapshot.id}", headers=SC_HEADERS)
    assert r.status_code == 404, r.text


async def test_unassigned_snapshot_is_not_writable_or_restorable_by_a_restricted_key(
    client: AsyncClient, db_session: AsyncSession, enqueued: list
):
    """Mirrors the unassigned-PV write gate: an unassigned snapshot has no facility a
    grant can cover, so update/delete/restore are refused for any restricted key —
    including one holding every facility that exists — not just an empty-grant one."""
    await _seed_hierarchy(db_session)
    sc_pv = await _create_pv(client, WILD_HEADERS, "SC:PV:SP", ["lcls-sc"])
    snapshot = Snapshot(title="unassigned")
    db_session.add(snapshot)
    await db_session.commit()
    db_session.add(
        SnapshotValue(snapshot_id=snapshot.id, pv_id=sc_pv, pv_name="SC:PV:SP", setpoint_value={"value": 1.0})
    )
    await db_session.commit()
    enqueued.clear()

    # update/delete go through `_require_facility_write`, which refuses an unassigned
    # row with 403 (it's known to exist, just not writable); restore goes through
    # `snapshot_exists`, which uses the read-side masking rule, so it's 404.
    r = await client.put(f"/v1/snapshots/{snapshot.id}", headers=SC_HEADERS, json={"title": "renamed"})
    assert r.status_code == 403, r.text
    r = await client.delete(f"/v1/snapshots/{snapshot.id}", headers=SC_HEADERS)
    assert r.status_code == 403, r.text
    r = await client.post(f"/v1/snapshots/{snapshot.id}/restore", headers=SC_HEADERS, json={})
    assert r.status_code == 404, r.text
    assert enqueued == []

    r = await client.post(f"/v1/snapshots/{snapshot.id}/restore", headers=WILD_HEADERS, json={})
    assert r.status_code == 200, r.text
    # Unrestricted: `plan_restore` passes the original `pv_ids=None` straight through,
    # keeping its "restore everything" meaning rather than expanding it to a list.
    assert enqueued[-1][2]["pv_ids"] is None


async def test_a_restricted_key_cannot_unassign_a_pv_it_fully_holds(client: AsyncClient, db_session: AsyncSession):
    """Holding every one of a PV's current facilities is enough to *edit* it, but not
    enough to strip it down to unassigned: that would hand write access to every other
    write-scoped key and hide it from every facility-filtered read, including the key
    doing the stripping once it moves on to another facility."""
    await _seed_hierarchy(db_session)
    pv_id = await _create_pv(client, SC_HEADERS, "SC:PV:SP", ["lcls-sc"])

    r = await client.put(f"/v1/pvs/{pv_id}", headers=SC_HEADERS, json={"facilities": []})
    assert r.status_code == 403
    assert r.json()["code"] == "insufficient_facility_access"

    r = await client.get("/v1/pvs/paged", headers=WILD_HEADERS)
    pv = next(p for p in r.json()["results"] if p["id"] == pv_id)
    assert pv["facilityIds"] != []

    r = await client.put(f"/v1/pvs/{pv_id}", headers=WILD_HEADERS, json={"facilities": []})
    assert r.status_code == 200, r.text
    assert r.json()["facilityIds"] == []


async def test_a_restricted_key_cannot_create_an_unassigned_pv(client: AsyncClient, db_session: AsyncSession):
    """The create-time counterpart: a restricted key must name a facility it holds when
    minting a PV, the same rule ``POST /v1/snapshots`` already applies. Allowing an
    unassigned create would hand the new row to every write-scoped key, including ones
    with no overlap with the key that made it."""
    await _seed_hierarchy(db_session)

    r = await client.post("/v1/pvs", headers=SC_HEADERS, json={"setpointAddress": "ORPHAN:PV:SP", "facilities": []})
    assert r.status_code == 403
    assert r.json()["code"] == "insufficient_facility_access"


# --- facility deletion ------------------------------------------------------- #


@pytest.fixture
async def facilities_client(test_engine, db_session):
    """The parent app's ``/v1/facilities`` routes, with Squirrel's in-use checker
    registered exactly as ``squirrel_lifespan`` does."""
    previous = list(facilities_service._in_use_checkers)
    facilities_service._in_use_checkers.clear()
    facilities_gateway.register_in_use_checker(_facility_in_use, "PVs or snapshots")

    app = FastAPI()
    register_canopy_error_handler(app)
    tag_module(app, "core")
    app.include_router(facilities_router)

    async def override_get_session():
        async with AsyncSession(test_engine, expire_on_commit=False) as s:
            try:
                yield s
                await s.commit()
            except Exception:
                await s.rollback()
                raise

    app.dependency_overrides[get_session] = override_get_session
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            yield c
    finally:
        facilities_service._in_use_checkers[:] = previous


async def test_a_facility_with_squirrel_pvs_or_snapshots_cannot_be_deleted(
    client: AsyncClient, facilities_client: AsyncClient, db_session: AsyncSession
):
    facilities = await _seed_hierarchy(db_session)
    await _create_pv(client, WILD_HEADERS, "SC:PV:SP", ["lcls-sc"])
    db_session.add(Snapshot(title="nc", facility_id=facilities["lcls-nc"].id))
    await db_session.commit()

    for slug in ("lcls-sc", "lcls-nc"):
        r = await facilities_client.delete(f"/v1/facilities/{slug}", headers=ADMIN_HEADERS)
        assert r.status_code == 400, r.text
        assert r.json()["code"] == "facility_in_use"
        assert "PVs or snapshots" in r.json()["detail"]

    r = await facilities_client.delete("/v1/facilities/facet", headers=ADMIN_HEADERS)
    assert r.status_code == 200, r.text
