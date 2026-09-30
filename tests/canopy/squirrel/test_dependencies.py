"""Tests for Squirrel's dependency aliases and service factory composition.

The facility-scope test uses a real Postgres *test database* (shared fixtures with
``test_facility_scoping.py`` / ``test_key_store.py`` via ``conftest.py``), skipped when
unreachable.
"""

from canopy.core import db
from canopy.squirrel import dependencies

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from canopy.core.db import get_session
from canopy.core.modules import tag_module
from canopy.core.facilities import gateway as facilities_gateway
from canopy.squirrel.dependencies import PvServiceDep, SnapshotServiceDep

WRITE_HEADERS = {"X-API-Key": "test-write"}


def test_session_dep_is_the_shared_core_alias():
    """Squirrel takes its request session through ``canopy.core.db.SessionDep``.

    A module-local alias could drop the function scope. It would then commit after the
    response and, next to the core alias, give the request a second session.
    """
    assert dependencies.SessionDep is db.SessionDep


def test_facility_scope_is_resolved_once_per_request_across_services(test_engine, monkeypatch):
    """PvServiceDep and SnapshotServiceDep both depend on get_facility_scope. FastAPI
    caches a dependency's result per request by default, so a route pulling in both
    services must still call resolve_key_facility_scope once, not once per service —
    the module docstring's "once per request" claim, verified rather than assumed."""
    calls = []
    original = facilities_gateway.resolve_key_facility_scope

    async def counting(*args, **kwargs):
        calls.append(1)
        return await original(*args, **kwargs)

    monkeypatch.setattr(facilities_gateway, "resolve_key_facility_scope", counting)

    app = FastAPI()
    tag_module(app, "squirrel")

    @app.get("/both")
    async def both(pv_service: PvServiceDep, snapshot_service: SnapshotServiceDep) -> dict:
        return {"ok": True}

    maker = async_sessionmaker(test_engine, class_=AsyncSession, expire_on_commit=False)

    async def override_get_session():
        async with maker() as s:
            yield s

    app.dependency_overrides[get_session] = override_get_session

    with TestClient(app) as client:
        r = client.get("/both", headers=WRITE_HEADERS)

    assert r.status_code == 200, r.text
    assert len(calls) == 1
