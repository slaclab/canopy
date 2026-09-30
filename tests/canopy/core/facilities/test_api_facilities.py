"""API tests for the facility hierarchy endpoint."""

from canopy.core.facilities.models import Facility

READ_HEADERS = {"X-API-Key": "test-read"}


async def _seed_facility(session, slug: str, parent_id: str | None = None) -> Facility:
    facility = Facility(slug=slug, name=slug.upper(), parent_id=parent_id)
    session.add(facility)
    await session.commit()
    return facility


async def test_list_facilities(facilities_client, session, session_maker, monkeypatch):
    from canopy.core import security
    from canopy.core.security import APIKey

    class FakeRegistry:
        async def get(self, token: str) -> APIKey | None:
            return APIKey(token=token, read=True, write=False, name="read") if token == "test-read" else None

    previous = security._registry
    security.bind_key_registry(FakeRegistry())
    try:
        lcls = await _seed_facility(session, "lcls")
        await _seed_facility(session, "lcls-sc", parent_id=lcls.id)

        r = facilities_client.get("/v1/facilities", headers=READ_HEADERS)
        assert r.status_code == 200, r.text
        slugs = {f["slug"] for f in r.json()}
        assert {"lcls", "lcls-sc"} <= slugs
        sc = next(f for f in r.json() if f["slug"] == "lcls-sc")
        assert sc["parentId"] == lcls.id
    finally:
        security._registry = previous


async def test_facilities_require_read_auth(facilities_client):
    assert facilities_client.get("/v1/facilities").status_code == 401
