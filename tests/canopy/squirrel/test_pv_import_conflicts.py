"""``POST /v1/pvs/import?onConflict=…``: skipping or updating rows whose PV is already registered.

Facility-restricted keys are injected by rebinding the key registry, as
``test_facility_enforcement.py`` does, with simulated facility slugs. A key's grant
holds facility ids, so the seeded facilities have fixed ones.
"""

import pytest
from httpx import AsyncClient
from sqlalchemy import event, delete
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from canopy.core import security
from canopy.core.modules import WILDCARD
from canopy.squirrel.app import app as squirrel_app
from canopy.core.security import APIKey
from canopy.squirrel.data.models import Pv
from canopy.core.facilities.models import Facility

ALL = {"X-API-Key": "all-key"}
ALPHA = {"X-API-Key": "alpha-key"}
ALPHA_BETA = {"X-API-Key": "alpha-beta-key"}


class _KeyRegistry:
    def __init__(self, keys: list[APIKey]) -> None:
        self.by_token = {k.token: k for k in keys}

    async def get(self, token: str) -> APIKey | None:
        return self.by_token.get(token)


# The ids the ``facilities`` fixture seeds ``alpha`` and ``beta`` with.
FACILITY_IDS = {"alpha": "00000000-0000-4000-8000-0000000000a1", "beta": "00000000-0000-4000-8000-0000000000b2"}


def _key(token: str, facilities: set[str]) -> APIKey:
    """A key minted granting ``facilities`` (slugs, or ``*``): it holds their ids."""
    grant = frozenset(slug if slug == WILDCARD else FACILITY_IDS[slug] for slug in facilities)
    return APIKey(token=token, read=True, write=True, modules=frozenset({WILDCARD}), facility_ids=grant)


@pytest.fixture(autouse=True)
def keys():
    previous = security._registry
    security.bind_key_registry(
        _KeyRegistry(
            [
                _key("all-key", {WILDCARD}),
                _key("alpha-key", {"alpha"}),
                _key("alpha-beta-key", {"alpha", "beta"}),
            ]
        )
    )
    yield
    security._registry = previous


@pytest.fixture
async def facilities(db_session: AsyncSession) -> None:
    db_session.add_all([Facility(id=FACILITY_IDS[slug], slug=slug, name=slug.title()) for slug in ("alpha", "beta")])
    await db_session.commit()


async def _create(client: AsyncClient, **fields) -> dict:
    r = await client.post("/v1/pvs", headers=ALL, json=fields)
    assert r.status_code == 200, r.text
    return r.json()


async def _tags(client: AsyncClient, *names: str) -> list[str]:
    r = await client.post("/v1/tags", headers=ALL, json={"name": "Sim group"})
    assert r.status_code == 200, r.text
    gid = r.json()["id"]
    for name in names:
        r = await client.post(f"/v1/tags/{gid}/tags", headers=ALL, json={"name": name})
        assert r.status_code == 200, r.text
    by_name = {t["name"]: t["id"] for t in r.json()["group"]["tags"]}
    return [by_name[name] for name in names]


async def _all_pvs(client: AsyncClient) -> dict[str, dict]:
    """Every PV by identifying address, read with the unrestricted key."""
    r = await client.get("/v1/pvs/paged", headers=ALL, params={"pageSize": 1000})
    assert r.status_code == 200, r.text
    return {pv["setpointAddress"] or pv["readbackAddress"] or pv["configAddress"]: pv for pv in r.json()["results"]}


def _import(client: AsyncClient, rows: list[dict], mode: str | None, headers: dict = ALL):
    """``POST /v1/pvs/import`` with ``onConflict=mode``, or ``POST /v1/pvs/multi`` for ``None``."""
    if mode is None:
        return client.post("/v1/pvs/multi", headers=headers, json=rows)
    return client.post("/v1/pvs/import", headers=headers, params={"onConflict": mode}, json=rows)


# --- request contract -------------------------------------------------------- #


@pytest.mark.parametrize("params", [{}, {"onConflict": "error"}, {"onConflict": "replace"}])
async def test_import_needs_on_conflict_skip_or_update(client: AsyncClient, params: dict):
    r = await client.post("/v1/pvs/import", headers=ALL, params=params, json=[{"setpointAddress": "SIM:DEV:00001:SET"}])
    assert r.status_code == 422
    assert await _all_pvs(client) == {}


async def test_multi_still_refuses_a_registered_row_and_returns_a_list(client: AsyncClient):
    await _create(client, setpointAddress="SIM:DEV:00001:SET")

    r = await _import(client, [{"setpointAddress": "SIM:DEV:00002:SET"}], None)
    assert r.status_code == 200, r.text
    assert isinstance(r.json(), list) and r.json()[0]["setpointAddress"] == "SIM:DEV:00002:SET"

    r = await _import(client, [{"setpointAddress": "SIM:DEV:00001:SET"}], None)
    assert r.status_code == 409
    assert r.json() == {
        "code": "pv_already_exists",
        "detail": "Setpoint addresses already exist: ['SIM:DEV:00001:SET']",
    }


def test_each_bulk_route_has_one_response_schema():
    """Typed clients get a plain list from ``/multi`` and the result object from ``/import``,
    with no ``anyOf`` to narrow."""
    paths = squirrel_app.openapi()["paths"]

    def ok_schema(path: str) -> dict:
        return paths[path]["post"]["responses"]["200"]["content"]["application/json"]["schema"]

    multi = ok_schema("/v1/pvs/multi")
    assert multi["type"] == "array"
    assert multi["items"] == {"$ref": "#/components/schemas/PVElementDTO"}
    assert ok_schema("/v1/pvs/import") == {"$ref": "#/components/schemas/PVImportResultDTO"}
    assert "onConflict" not in {p["name"] for p in paths["/v1/pvs/multi"]["post"].get("parameters", [])}


@pytest.mark.parametrize("mode", ["skip", "update"])
async def test_duplicates_within_the_request_are_still_400(client: AsyncClient, mode: str):
    rows = [{"readbackAddress": "SIM:DEV:00001:RBV"}, {"readbackAddress": "SIM:DEV:00001:RBV"}]
    r = await _import(client, rows, mode)
    assert r.status_code == 400
    assert "readback" in r.json()["detail"]
    assert await _all_pvs(client) == {}


# --- skip -------------------------------------------------------------------- #


async def test_skip_creates_new_rows_and_leaves_registered_ones_alone(client: AsyncClient):
    await _create(client, setpointAddress="SIM:DEV:00001:SET", readbackAddress="SIM:DEV:00001:RBV", device="kept")
    await _create(client, readbackAddress="SIM:DEV:00002:RBV", description="kept")
    await _create(client, configAddress="SIM:DEV:00003:CFG", absTolerance=0.5)
    before = await _all_pvs(client)

    rows = [
        {"setpointAddress": "SIM:DEV:00004:SET"},
        {"setpointAddress": "SIM:DEV:00001:SET", "device": "ignored"},
        # A setpoint-less row whose readback a setpoint PV also has is a new identity.
        {"readbackAddress": "SIM:DEV:00001:RBV"},
        {"readbackAddress": "SIM:DEV:00002:RBV", "description": "ignored"},
        {"readbackAddress": "SIM:DEV:00005:RBV"},
        {"configAddress": "SIM:DEV:00003:CFG", "absTolerance": 9.0},
        {"configAddress": "SIM:DEV:00006:CFG"},
    ]
    r = await _import(client, rows, "skip")

    assert r.status_code == 200, r.text
    body = r.json()
    assert set(body) == {"created", "updated", "skipped", "addressMismatches"}
    assert [pv["setpointAddress"] or pv["readbackAddress"] or pv["configAddress"] for pv in body["created"]] == [
        "SIM:DEV:00004:SET",
        "SIM:DEV:00001:RBV",
        "SIM:DEV:00005:RBV",
        "SIM:DEV:00006:CFG",
    ]
    assert body["skipped"] == [
        {"row": 2, "kind": "setpoint", "address": "SIM:DEV:00001:SET"},
        {"row": 4, "kind": "readback", "address": "SIM:DEV:00002:RBV"},
        {"row": 6, "kind": "config", "address": "SIM:DEV:00003:CFG"},
    ]
    assert body["updated"] == [] and body["addressMismatches"] == []

    after = await _all_pvs(client)
    assert len(after) == len(before) + 4
    for address, pv in before.items():
        assert after[address] == pv


# --- update ------------------------------------------------------------------ #


async def test_update_applies_only_the_fields_a_row_provides(client: AsyncClient, facilities):
    old_tag, new_tag = await _tags(client, "old", "new")
    stored = await _create(
        client,
        setpointAddress="SIM:DEV:00001:SET",
        readbackAddress="SIM:DEV:00001:RBV",
        device="SIM:DEV:00001",
        description="before",
        absTolerance=0.1,
        relTolerance=0.01,
        tags=[old_tag],
        facilities=["alpha"],
    )
    untouched = await _create(
        client, readbackAddress="SIM:DEV:00002:RBV", description="before", absTolerance=0.2, tags=[old_tag]
    )

    rows = [
        # `device: null` is "leave alone", as in `PUT /v1/pvs/{id}`; tolerances and
        # facilities are omitted, so their stored values stay despite the DTO defaults.
        {"setpointAddress": "SIM:DEV:00001:SET", "description": "after", "device": None, "tags": [new_tag]},
        {"configAddress": "SIM:DEV:00003:CFG", "description": "new"},
        # Provides nothing to change: listed as updated, stored values intact.
        {"readbackAddress": "SIM:DEV:00002:RBV"},
    ]
    r = await _import(client, rows, "update")

    assert r.status_code == 200, r.text
    body = r.json()
    assert [pv["configAddress"] for pv in body["created"]] == ["SIM:DEV:00003:CFG"]
    assert body["skipped"] == [] and body["addressMismatches"] == []
    first, second = body["updated"]
    assert first["id"] == stored["id"]
    assert first["description"] == "after"
    assert first["device"] == "SIM:DEV:00001"
    assert (first["absTolerance"], first["relTolerance"]) == (0.1, 0.01)
    assert [t["id"] for t in first["tags"]] == [new_tag]
    assert first["facilities"] == ["alpha"]
    assert first["lastModifiedDate"] >= stored["lastModifiedDate"]
    assert second["id"] == untouched["id"]
    assert {k: second[k] for k in ("description", "absTolerance", "tags")} == {
        k: untouched[k] for k in ("description", "absTolerance", "tags")
    }

    after = await _all_pvs(client)
    assert after["SIM:DEV:00001:SET"] == first


async def test_update_replaces_tags_and_tolerances_when_given(client: AsyncClient):
    tag_a, tag_b = await _tags(client, "a", "b")
    await _create(client, setpointAddress="SIM:DEV:00001:SET", absTolerance=0.1, tags=[tag_a, tag_b])

    rows = [{"setpointAddress": "SIM:DEV:00001:SET", "absTolerance": 0.0, "tags": []}]
    r = await _import(client, rows, "update")

    assert r.status_code == 200, r.text
    [pv] = r.json()["updated"]
    assert pv["absTolerance"] == 0.0
    assert pv["tags"] == []


async def test_update_reports_a_differing_address_and_keeps_the_stored_one(client: AsyncClient):
    await _create(client, setpointAddress="SIM:DEV:00001:SET", readbackAddress="SIM:DEV:00001:RBV")
    await _create(client, readbackAddress="SIM:DEV:00002:RBV", configAddress="SIM:DEV:00002:CFG")

    rows = [
        {"setpointAddress": "SIM:DEV:00001:SET", "readbackAddress": "SIM:DEV:00099:RBV", "configAddress": "SIM:CFG"},
        # Omitting an address is not a mismatch; repeating the stored one isn't either.
        {"readbackAddress": "SIM:DEV:00002:RBV", "configAddress": "SIM:DEV:00002:CFG", "description": "d"},
    ]
    r = await _import(client, rows, "update")

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["addressMismatches"] == [
        {"row": 1, "kind": "readback", "requested": "SIM:DEV:00099:RBV", "stored": "SIM:DEV:00001:RBV"},
        {"row": 1, "kind": "config", "requested": "SIM:CFG", "stored": None},
    ]
    first, second = body["updated"]
    assert (first["readbackAddress"], first["configAddress"]) == ("SIM:DEV:00001:RBV", None)
    assert second["description"] == "d"
    stored = (await _all_pvs(client))["SIM:DEV:00001:SET"]
    assert (stored["readbackAddress"], stored["configAddress"]) == ("SIM:DEV:00001:RBV", None)


async def test_an_unknown_tag_refuses_an_update_row_but_is_dropped_from_a_new_row(client: AsyncClient):
    [tag] = await _tags(client, "kept")
    await _create(client, setpointAddress="SIM:DEV:00001:SET", tags=[tag])
    unknown = "00000000-0000-4000-8000-000000000000"

    r = await _import(
        client,
        [{"setpointAddress": "SIM:DEV:00002:SET"}, {"setpointAddress": "SIM:DEV:00001:SET", "tags": [unknown]}],
        "update",
    )
    assert r.status_code == 400
    assert r.json()["detail"] == (
        f"Unknown tag IDs on rows that update an existing PV: row 2 (SIM:DEV:00001:SET): {unknown}"
    )
    after = await _all_pvs(client)
    assert set(after) == {"SIM:DEV:00001:SET"}
    assert [t["id"] for t in after["SIM:DEV:00001:SET"]["tags"]] == [tag]

    r = await _import(client, [{"setpointAddress": "SIM:DEV:00002:SET", "tags": [unknown]}], "update")
    assert r.status_code == 200, r.text
    assert r.json()["created"][0]["tags"] == []


async def test_update_matches_config_only_and_setpoint_less_pvs(client: AsyncClient):
    config_only = await _create(client, configAddress="SIM:DEV:00001:CFG")
    readback_only = await _create(client, readbackAddress="SIM:DEV:00002:RBV")

    rows = [
        {"configAddress": "SIM:DEV:00001:CFG", "device": "cfg"},
        {"readbackAddress": "SIM:DEV:00002:RBV", "device": "rbv"},
    ]
    r = await _import(client, rows, "update")

    assert r.status_code == 200, r.text
    assert [(pv["id"], pv["device"]) for pv in r.json()["updated"]] == [
        (config_only["id"], "cfg"),
        (readback_only["id"], "rbv"),
    ]
    assert r.json()["created"] == []


async def test_update_picks_the_lowest_id_when_registered_pvs_share_an_identity(
    client: AsyncClient, db_session: AsyncSession
):
    """Only setpoints are unique in the database, so older data can hold two setpoint-less
    PVs with one readback; the import updates one of them, the same one every time."""
    ids = ["00000000-0000-4000-8000-000000000002", "00000000-0000-4000-8000-000000000001"]
    db_session.add_all([Pv(id=pv_id, readback_address="SIM:DEV:00001:RBV") for pv_id in ids])
    await db_session.commit()

    r = await _import(client, [{"readbackAddress": "SIM:DEV:00001:RBV", "device": "d"}], "update")

    assert r.status_code == 200, r.text
    assert [pv["id"] for pv in r.json()["updated"]] == [min(ids)]


# --- facility-restricted keys ------------------------------------------------ #


@pytest.mark.parametrize("mode", ["skip", "update"])
async def test_every_row_is_gated_as_new_before_existence_is_checked(client: AsyncClient, facilities, mode: str):
    """A row naming no facility the key holds gets the same 403 whether or not its address
    is registered, so neither mode tells a restricted key which addresses exist."""
    await _create(client, setpointAddress="SIM:DEV:00001:SET", facilities=["beta"])

    responses = [
        await _import(client, [{"setpointAddress": address, "facilities": ["beta"]}], mode, ALPHA)
        for address in ("SIM:DEV:00001:SET", "SIM:DEV:00002:SET")
    ]
    assert [r.status_code for r in responses] == [403, 403]
    assert responses[0].json()["detail"].replace("00001", "00002") == responses[1].json()["detail"]
    assert set(await _all_pvs(client)) == {"SIM:DEV:00001:SET"}


async def test_skip_with_a_restricted_key_leaves_an_out_of_scope_pv_untouched(client: AsyncClient, facilities):
    before = await _create(client, setpointAddress="SIM:DEV:00001:SET", device="beta-owned", facilities=["beta"])

    rows = [
        {"setpointAddress": "SIM:DEV:00001:SET", "device": "ignored", "facilities": ["alpha"]},
        {"setpointAddress": "SIM:DEV:00002:SET", "facilities": ["alpha"]},
    ]
    r = await _import(client, rows, "skip", ALPHA)

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["skipped"] == [{"row": 1, "kind": "setpoint", "address": "SIM:DEV:00001:SET"}]
    assert [pv["setpointAddress"] for pv in body["created"]] == ["SIM:DEV:00002:SET"]
    assert (await _all_pvs(client))["SIM:DEV:00001:SET"] == before


async def test_update_with_a_restricted_key_needs_every_facility_of_the_pv(client: AsyncClient, facilities):
    await _create(client, setpointAddress="SIM:DEV:00001:SET", facilities=["alpha"])
    shared = await _create(client, setpointAddress="SIM:DEV:00002:SET", facilities=["alpha", "beta"])
    unassigned = await _create(client, setpointAddress="SIM:DEV:00003:SET")

    rows = [
        {"setpointAddress": "SIM:DEV:00001:SET", "description": "mine", "facilities": ["alpha"]},
        {"setpointAddress": "SIM:DEV:00002:SET", "description": "shared", "facilities": ["alpha"]},
        {"setpointAddress": "SIM:DEV:00003:SET", "description": "orphan", "facilities": ["alpha"]},
    ]
    r = await _import(client, rows, "update", ALPHA)

    assert r.status_code == 403
    detail = r.json()["detail"]
    assert r.json()["code"] == "insufficient_facility_access"
    assert detail == (
        "API key may not update 2 of the 3 registered PVs this import matches: "
        "row 2 (SIM:DEV:00002:SET): the registered PV is outside the key's facilities; "
        "row 3 (SIM:DEV:00003:SET): the registered PV is outside the key's facilities"
    )
    # Where the out-of-scope PVs are assigned stays hidden.
    assert "beta" not in detail
    after = await _all_pvs(client)
    assert after["SIM:DEV:00001:SET"]["description"] is None
    assert after["SIM:DEV:00002:SET"] == shared
    assert after["SIM:DEV:00003:SET"] == unassigned

    # A key holding both of the shared PV's facilities may update it.
    r = await _import(client, rows[1:2], "update", ALPHA_BETA)
    assert r.status_code == 200, r.text
    [pv] = r.json()["updated"]
    assert pv["description"] == "shared"
    assert pv["facilities"] == ["alpha", "beta"]


@pytest.mark.parametrize(
    ("headers", "stored", "named"),
    [
        # A key holding both of the PV's facilities naming one of them, as it must.
        (ALPHA_BETA, ["alpha", "beta"], ["alpha"]),
        # A restricted key naming one facility outside its grant alongside one it holds.
        (ALPHA, ["alpha"], ["alpha", "beta"]),
        # An unrestricted key naming a different facility, as a CSV import's default does.
        (ALL, ["alpha"], ["beta"]),
        # An unrestricted key naming none at all.
        (ALL, ["alpha", "beta"], []),
    ],
)
async def test_update_never_changes_a_registered_pvs_facilities(
    client: AsyncClient, facilities, headers: dict, stored: list[str], named: list[str]
):
    """A matched row's ``facilities`` only passes the create gate; changing a registered
    PV's facilities is ``PUT /v1/pvs/{id}``'s job."""
    await _create(client, setpointAddress="SIM:DEV:00001:SET", facilities=stored)

    rows = [{"setpointAddress": "SIM:DEV:00001:SET", "description": "changed", "facilities": named}]
    r = await _import(client, rows, "update", headers)

    assert r.status_code == 200, r.text
    [pv] = r.json()["updated"]
    assert (pv["description"], pv["facilities"]) == ("changed", stored)
    assert (await _all_pvs(client))["SIM:DEV:00001:SET"]["facilities"] == stored


async def test_update_with_a_restricted_key_may_leave_facilities_unchanged(client: AsyncClient, facilities):
    await _create(client, setpointAddress="SIM:DEV:00001:SET", facilities=["alpha"])

    r = await _import(
        client, [{"setpointAddress": "SIM:DEV:00001:SET", "facilities": ["alpha"], "device": "d"}], "update", ALPHA
    )

    assert r.status_code == 200, r.text
    assert r.json()["updated"][0]["device"] == "d"


# --- atomicity --------------------------------------------------------------- #


async def test_a_refused_update_row_late_in_the_import_writes_nothing(client: AsyncClient, facilities):
    await _create(client, setpointAddress="SIM:DEV:00001:SET", description="before", facilities=["alpha"])
    await _create(client, setpointAddress="SIM:DEV:00009:SET", facilities=["beta"])
    before = await _all_pvs(client)

    rows = [
        {"setpointAddress": "SIM:DEV:00002:SET", "facilities": ["alpha"]},
        {"setpointAddress": "SIM:DEV:00001:SET", "description": "after", "facilities": ["alpha"]},
        {"readbackAddress": "SIM:DEV:00003:RBV", "facilities": ["alpha"]},
        {"setpointAddress": "SIM:DEV:00009:SET", "description": "refused", "facilities": ["alpha"]},
    ]
    r = await _import(client, rows, "update", ALPHA)

    assert r.status_code == 403
    assert await _all_pvs(client) == before


# --- query count ------------------------------------------------------------- #


@pytest.mark.parametrize("mode", ["skip", "update"])
async def test_statement_count_does_not_grow_with_the_row_count(
    client: AsyncClient, test_engine: AsyncEngine, facilities, mode: str
):
    tag_ids = await _tags(client, "a", "b", "c")

    async def run(start: int, count: int) -> int:
        existing = [f"SIM:DEV:{start + i:05d}:SET" for i in range(count)]
        r = await _import(client, [{"setpointAddress": a, "facilities": ["alpha"]} for a in existing], None)
        assert r.status_code == 200, r.text
        rows = [
            {"setpointAddress": a, "description": "d", "tags": tag_ids, "facilities": ["alpha", "beta"]}
            for a in existing
        ] + [
            {"setpointAddress": f"SIM:DEV:{start + count + i:05d}:SET", "tags": tag_ids, "facilities": ["alpha"]}
            for i in range(count)
        ]
        statements = 0

        def count_statement(*_args):
            nonlocal statements
            statements += 1

        event.listen(test_engine.sync_engine, "before_cursor_execute", count_statement)
        try:
            r = await _import(client, rows, mode)
        finally:
            event.remove(test_engine.sync_engine, "before_cursor_execute", count_statement)
        assert r.status_code == 200, r.text
        assert len(r.json()["created"]) == count
        return statements

    assert await run(1, 5) == await run(1000, 50)


# --- tag ids ----------------------------------------------------------------- #


@pytest.mark.parametrize("mode", [None, "skip", "update"])
async def test_a_new_row_matches_tag_ids_in_any_case(client: AsyncClient, mode: str | None):
    """Postgres returns tag ids in lowercase; a row may send them in uppercase, as ``POST
    /v1/pvs`` accepts."""
    [tag] = await _tags(client, "kept")

    r = await _import(client, [{"setpointAddress": "SIM:DEV:00001:SET", "tags": [tag.upper()]}], mode)

    assert r.status_code == 200, r.text
    [pv] = r.json() if mode is None else r.json()["created"]
    assert [t["id"] for t in pv["tags"]] == [tag]


async def test_an_update_row_matches_tag_ids_in_any_case(client: AsyncClient):
    first, second = await _tags(client, "first", "second")
    await _create(client, setpointAddress="SIM:DEV:00001:SET", tags=[first])

    rows = [{"setpointAddress": "SIM:DEV:00001:SET", "tags": [first.upper(), second.upper()]}]
    r = await _import(client, rows, "update")

    assert r.status_code == 200, r.text
    assert sorted(t["id"] for t in r.json()["updated"][0]["tags"]) == sorted([first, second])


@pytest.mark.parametrize("mode", [None, "skip", "update"])
async def test_a_tag_id_repeated_on_a_new_row_is_attached_once(client: AsyncClient, mode: str | None):
    [tag] = await _tags(client, "kept")

    rows = [{"setpointAddress": "SIM:DEV:00001:SET", "tags": [tag, tag.upper(), tag]}]
    r = await _import(client, rows, mode)

    assert r.status_code == 200, r.text
    [pv] = r.json() if mode is None else r.json()["created"]
    assert [t["id"] for t in pv["tags"]] == [tag]


async def test_a_malformed_tag_id_is_an_unknown_tag(client: AsyncClient):
    """Not a UUID at all: dropped from a new row and refused on an update row, like any
    other unknown tag, rather than reaching the database as a uuid it can't parse."""
    [tag] = await _tags(client, "kept")
    await _create(client, setpointAddress="SIM:DEV:00001:SET", tags=[tag])

    r = await _import(client, [{"setpointAddress": "SIM:DEV:00002:SET", "tags": ["not-a-uuid", tag]}], None)
    assert r.status_code == 200, r.text
    assert [t["id"] for t in r.json()[0]["tags"]] == [tag]

    r = await _import(client, [{"setpointAddress": "SIM:DEV:00001:SET", "tags": ["not-a-uuid"]}], "update")
    assert r.status_code == 400
    assert r.json()["detail"] == (
        "Unknown tag IDs on rows that update an existing PV: row 1 (SIM:DEV:00001:SET): not-a-uuid"
    )
    assert [t["id"] for t in (await _all_pvs(client))["SIM:DEV:00001:SET"]["tags"]] == [tag]


# --- updated_at ---------------------------------------------------------------- #


async def test_update_moves_updated_at_only_for_rows_that_change_a_pv_column(client: AsyncClient, facilities):
    """A tags-only change issues no UPDATE on ``pv``, so its ``updated_at`` stays, as with
    ``PUT /v1/pvs/{id}``. Nor does a row naming only facilities, which an import never
    changes on a registered PV."""
    [tag] = await _tags(client, "kept")
    before = [await _create(client, setpointAddress=f"SIM:DEV:{n:05d}:SET", facilities=["alpha"]) for n in range(1, 5)]

    rows = [
        {"setpointAddress": "SIM:DEV:00001:SET", "description": "changed"},
        {"setpointAddress": "SIM:DEV:00002:SET", "tags": [tag]},
        {"setpointAddress": "SIM:DEV:00003:SET", "facilities": ["beta"]},
        {"setpointAddress": "SIM:DEV:00004:SET"},
    ]
    r = await _import(client, rows, "update")

    assert r.status_code == 200, r.text
    updated = r.json()["updated"]
    assert updated[0]["lastModifiedDate"] > before[0]["lastModifiedDate"]
    assert [pv["lastModifiedDate"] for pv in updated[1:]] == [pv["lastModifiedDate"] for pv in before[1:]]
    assert [t["id"] for t in updated[1]["tags"]] == [tag]
    assert updated[2]["facilities"] == ["alpha"]
    after = await _all_pvs(client)
    assert [after[pv["setpointAddress"]]["lastModifiedDate"] for pv in updated] == [
        pv["lastModifiedDate"] for pv in updated
    ]


# --- grants by facility id ---------------------------------------------------- #


@pytest.mark.parametrize("mode", ["skip", "update"])
async def test_a_facility_recreated_under_a_granted_slug_isnt_reached(
    client: AsyncClient, db_session: AsyncSession, facilities, mode: str
):
    """``alpha-key`` was granted ``alpha``. Once ``alpha`` is deleted (as a history head
    applies its arm's delete, with no check of the head's keys) and a new ``alpha`` is
    created, the key's grant names a facility that no longer exists: the import refuses
    a new row in the new ``alpha`` and leaves its registered PV alone."""
    await db_session.execute(delete(Facility).where(Facility.id == FACILITY_IDS["alpha"]))
    await db_session.commit()
    db_session.add(Facility(slug="alpha", name="Alpha again"))
    await db_session.commit()
    registered = await _create(client, setpointAddress="SIM:DEV:00001:SET", description="before", facilities=["alpha"])

    rows = [
        {"setpointAddress": "SIM:DEV:00001:SET", "description": "after", "facilities": ["alpha"]},
        {"setpointAddress": "SIM:DEV:00002:SET", "facilities": ["alpha"]},
    ]
    r = await _import(client, rows, mode, headers=ALPHA)

    assert (r.status_code, r.json()["code"]) == (403, "insufficient_facility_access")
    pvs = await _all_pvs(client)
    assert set(pvs) == {"SIM:DEV:00001:SET"}
    assert pvs["SIM:DEV:00001:SET"]["description"] == registered["description"] == "before"


@pytest.mark.parametrize("mode", ["skip", "update"])
async def test_a_facility_the_key_cant_reach_is_refused_before_its_owner_is_named(
    client: AsyncClient, db_session: AsyncSession, facilities, as_instance, mode: str
):
    """A row in ``remote``, owned by another instance, from a key granted only ``alpha``:
    the facility refusal comes first, so the caller isn't told who owns ``remote``."""
    with as_instance("arm2"):
        db_session.add(Facility(slug="remote", name="Remote"))
        await db_session.commit()

    r = await _import(client, [{"setpointAddress": "SIM:DEV:00003:SET", "facilities": ["remote"]}], mode, headers=ALPHA)

    assert (r.status_code, r.json()["code"]) == (403, "insufficient_facility_access")
    assert "arm2" not in r.json()["detail"]
    assert await _all_pvs(client) == {}
