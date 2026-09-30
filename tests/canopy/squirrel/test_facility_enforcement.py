"""Facility-scoped writes: a key's ``facilities`` grant constrains what it can mutate.

Auth is injected by rebinding the process-global key registry (the seam
``tests/canopy/core/test_module_scoping.py`` already uses) — but multi-key here, since
every case contrasts a facility-restricted key against an unrestricted one in the same
request flow. The registry is restored in teardown; it leaks across the suite otherwise.
"""

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from canopy.core import security
from canopy.core.modules import WILDCARD
from canopy.core.security import APIKey
from canopy.squirrel.data.models import Snapshot, SnapshotValue
from canopy.core.facilities.models import Facility

SC_HEADERS = {"X-API-Key": "sc-key"}
NC_HEADERS = {"X-API-Key": "nc-key"}
LCLS_HEADERS = {"X-API-Key": "lcls-key"}
NONE_HEADERS = {"X-API-Key": "none-key"}
WILD_HEADERS = {"X-API-Key": "wild-key"}


class MultiKeyRegistry:
    def __init__(self, keys: list[APIKey]) -> None:
        self.by_token = {k.token: k for k in keys}

    async def get(self, token: str) -> APIKey | None:
        return self.by_token.get(token)


@pytest.fixture(autouse=True)
def scoped_keys():
    """Five write-capable keys differing only in facility grant."""
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
                    token="wild-key",
                    read=True,
                    write=True,
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


async def test_holding_one_of_a_pvs_facilities_is_enough(client: AsyncClient, db_session: AsyncSession):
    facilities = await _seed_hierarchy(db_session)
    pv_id = await _create_pv(client, WILD_HEADERS, "SHARED:PV:SP", ["lcls-sc", "facet"])

    r = await client.put(f"/v1/pvs/{pv_id}", headers=SC_HEADERS, json={"device": "d"})
    assert r.status_code == 200, r.text
    # The write didn't quietly drop the facility the key can't reach.
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


async def test_unassigned_pv_is_writable_by_any_write_scoped_key(client: AsyncClient, db_session: AsyncSession):
    await _seed_hierarchy(db_session)
    pv_id = await _create_pv(client, WILD_HEADERS, "ORPHAN:PV:SP", [])

    r = await client.put(f"/v1/pvs/{pv_id}", headers=NC_HEADERS, json={"device": "d"})
    assert r.status_code == 200, r.text
    r = await client.delete(f"/v1/pvs/{pv_id}", headers=NONE_HEADERS)
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


async def test_multi_create_gates_each_row_not_the_import_s_union(client: AsyncClient, db_session: AsyncSession):
    """A row assigned solely to an out-of-scope facility must not ride in on the back
    of a sibling row the key can reach — gating the import's union of facilities
    instead of each row would let it through, making ``/pvs/multi`` a way around the
    single-create gate."""
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


async def test_restore_writes_only_in_scope_pvs_and_reports_the_rest(
    client: AsyncClient, db_session: AsyncSession, enqueued: list
):
    facilities = await _seed_hierarchy(db_session)
    sc_pv = await _create_pv(client, WILD_HEADERS, "SC:PV:SP", ["lcls-sc"])
    nc_pv = await _create_pv(client, WILD_HEADERS, "NC:PV:SP", ["lcls-nc"])
    orphan_pv = await _create_pv(client, WILD_HEADERS, "ORPHAN:PV:SP", [])

    # A snapshot in `lcls` captures every registered PV, across both children.
    snapshot = Snapshot(title="all", facility_id=facilities["lcls"].id)
    db_session.add(snapshot)
    await db_session.commit()
    db_session.add_all(
        [
            SnapshotValue(snapshot_id=snapshot.id, pv_id=pv_id, pv_name=name, setpoint_value={"value": 1.0})
            for pv_id, name in [(sc_pv, "SC:PV:SP"), (nc_pv, "NC:PV:SP"), (orphan_pv, "ORPHAN:PV:SP")]
        ]
    )
    await db_session.commit()

    r = await client.post(f"/v1/snapshots/{snapshot.id}/restore", headers=SC_HEADERS, json={})
    assert r.status_code == 200, r.text
    kwargs = enqueued[-1][2]
    # The snapshot's own facility (`lcls`) is in scope, but the write set is still
    # narrowed: the NC setpoint must not be driven by an SC-only key.
    assert set(kwargs["pv_ids"]) == {sc_pv, orphan_pv}
    assert kwargs["skipped_pv_ids"] == [nc_pv]


async def test_restore_with_nothing_in_scope_is_404_not_403(
    client: AsyncClient, db_session: AsyncSession, enqueued: list
):
    """A snapshot with zero PVs the key can reach must not be distinguishable from one
    that doesn't exist — the restore precondition (`snapshot_exists`) is per-PV, same
    as the write gate itself, not the snapshot's own (possibly-parent) facility tag."""
    facilities = await _seed_hierarchy(db_session)
    nc_pv = await _create_pv(client, WILD_HEADERS, "NC:PV:SP", ["lcls-nc"])
    snapshot = Snapshot(title="nc-only", facility_id=facilities["lcls"].id)
    db_session.add(snapshot)
    await db_session.commit()
    db_session.add(
        SnapshotValue(snapshot_id=snapshot.id, pv_id=nc_pv, pv_name="NC:PV:SP", setpoint_value={"value": 1.0})
    )
    await db_session.commit()
    enqueued.clear()

    r = await client.post(f"/v1/snapshots/{snapshot.id}/restore", headers=SC_HEADERS, json={})
    assert r.status_code == 404, r.text
    assert r.json()["code"] == "snapshot_not_found"
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

    r = await client.get(f"/v1/snapshots/{out_of_scope.id}/compare/{in_scope.id}", headers=SC_HEADERS)
    assert r.status_code == 404, r.text
