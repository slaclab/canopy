"""API key `facilities` scoping — mirrors `modules` except empty-list semantics."""

from canopy.core.modules import WILDCARD
from tests.canopy.core.keys.conftest import db_backed_registry

ADMIN_HEADERS = {"X-API-Key": "admin-env-key"}


async def test_omitted_facilities_defaults_to_empty_not_wildcard(session, session_maker, keys_client):
    with db_backed_registry(env="admin-env-key:admin@*"):
        resp = keys_client.post("/v1/api-keys", json={"appName": "trace", "read": True}, headers=ADMIN_HEADERS)

    assert resp.status_code == 200, resp.text
    assert resp.json()["facilities"] == []


async def test_empty_facility_list_is_accepted_not_rejected(session, session_maker, keys_client):
    with db_backed_registry(env="admin-env-key:admin@*"):
        resp = keys_client.post(
            "/v1/api-keys",
            json={"appName": "trace", "read": True, "facilities": []},
            headers=ADMIN_HEADERS,
        )

    assert resp.status_code == 200, resp.text
    assert resp.json()["facilities"] == []


async def test_facilities_round_trip_through_create_and_list(session, session_maker, keys_client):
    with db_backed_registry(env="admin-env-key:admin@*"):
        create = keys_client.post(
            "/v1/api-keys",
            json={"appName": "trace", "read": True, "facilities": ["lcls", "lcls-sc"]},
            headers=ADMIN_HEADERS,
        )
        listed = keys_client.get("/v1/api-keys", headers=ADMIN_HEADERS)

    assert create.status_code == 200, create.text
    assert create.json()["facilities"] == ["lcls", "lcls-sc"]

    entry = next(k for k in listed.json() if k["appName"] == "trace")
    assert entry["facilities"] == ["lcls", "lcls-sc"]


async def test_wildcard_facility_collapses_the_list(session, session_maker, keys_client):
    with db_backed_registry(env="admin-env-key:admin@*"):
        resp = keys_client.post(
            "/v1/api-keys",
            json={"appName": "trace", "read": True, "facilities": ["lcls", WILDCARD]},
            headers=ADMIN_HEADERS,
        )

    assert resp.status_code == 200, resp.text
    assert resp.json()["facilities"] == [WILDCARD]
