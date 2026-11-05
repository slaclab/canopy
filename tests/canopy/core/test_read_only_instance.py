"""The read-only guard on a mirror (``CANOPY_ROLE=mirror``), checked against the real route table.

Walks ``canopy.main``'s own routes and every mounted sub-app's, at any depth, so a
route added later is classified here the moment it exists: a write- or admin-gated
route must be refused with ``read_only_instance``, any other non-GET route and every
WebSocket must be gated to refuse or named in an allowlist below, and a route of a
type the walk doesn't know fails it. Nothing can reach a mirror's database or the
machine without someone deciding it should.
"""

import re

import pytest
from fastapi import FastAPI, Security, WebSocket
from fastapi.routing import APIRoute, APIWebSocketRoute
from starlette.routing import Mount, Route, BaseRoute
from fastapi.exceptions import WebSocketException
from fastapi.testclient import TestClient
from starlette.responses import PlainTextResponse

from canopy.core import security
from canopy.main import app
from canopy.core.errors import register_canopy_error_handler
from canopy.core.modules import WILDCARD, tag_module
from canopy.core.security import (
    APIKey,
    require_read_access,
    require_admin_access,
    require_write_access,
    ws_require_read_access,
    ws_require_write_access,
    require_local_admin_access,
    require_machine_write_access,
    require_external_write_access,
)
from canopy.core.forwarding import routes as forwarding_routes

MIRROR_REFUSED_GATES = {
    require_write_access,
    require_admin_access,
    require_machine_write_access,
    require_external_write_access,
}

# Non-GET routes that a mirror keeps serving. Read-scoped: they compute, validate
# or subscribe, and write nothing replicated or on the machine.
READ_SCOPED_NON_GET = {
    ("POST", "/formula-engine/v1/evaluate"),
    ("POST", "/formula-engine/v1/validate"),
    ("POST", "/vole/v1/pvs/subscribe"),
    ("POST", "/vole/v1/pvs/subscribe/{pv:path}"),
    ("POST", "/raccoon/v1/pvs/validate"),
    ("POST", "/converter/v1/conversions"),
    ("POST", "/converter/v1/conversions/validate"),
    ("POST", "/converter/v1/conversions/batch"),
    # Anonymous by design; events go to the log pipeline, not Postgres.
    ("POST", "/analytics/v1/events"),
}

# WebSockets a mirror keeps serving: they only subscribe. A message type that writes
# needs ws_require_write_access on its route, or a role check of its own.
READ_SCOPED_WEBSOCKETS = {
    "/hare/v1/ws/pvs",
    "/formula-engine/v1/ws/formulas",
}

# Admin routes over instance-local state (``core.api_key`` is never replicated).
LOCAL_ADMIN_PREFIX = "/v1/api-keys"

ALL_SCOPES = APIKey(token="all", read=True, write=True, admin=True, modules=frozenset({WILDCARD}), name="all")
HEADERS = {"X-API-Key": "all"}


class FakeRegistry:
    def __init__(self, *keys: APIKey) -> None:
        self.keys = {key.token: key for key in keys}

    async def get(self, token: str) -> APIKey | None:
        return self.keys.get(token)


@pytest.fixture
def bind_keys():
    previous = security._registry

    def _bind(*keys: APIKey) -> None:
        security.bind_key_registry(FakeRegistry(*keys))

    yield _bind
    security._registry = previous


def _calls(dependant) -> set:
    calls = set()
    for sub in dependant.dependencies:
        calls.add(sub.call)
        calls |= _calls(sub)
    return calls


def _is_gate(call) -> bool:
    """Part of the key and scope chain: the gates themselves and the header they read."""
    return getattr(call, "__module__", "").startswith(("canopy.core.security", "fastapi.security"))


def _runtime_dependencies(dependant) -> set:
    """Dependencies that lead to no gate, such as a module's service.

    The request test overrides these, so a route that declares its gate after its
    service (``create_entry(..., service, api_key=Security(require_write_access))``)
    still reaches the gate without the app's lifespan having bound that service.
    """
    found = set()
    for sub in dependant.dependencies:
        if _is_gate(sub.call):
            continue
        if any(_is_gate(call) for call in _calls(sub)):
            found |= _runtime_dependencies(sub)
        else:
            found.add(sub.call)
    return found


def _walk(routes: list[BaseRoute], prefix: str = "") -> list[tuple[str, BaseRoute]]:
    """``(full path, route)`` for every route, descending into mounts at any depth.

    A mount whose app has no routes to walk (static files, a bare ASGI app) is
    returned as the ``Mount`` itself, for the classifier to reject.
    """
    found = []
    for route in routes:
        path = prefix + getattr(route, "path", "")
        if isinstance(route, Mount) and getattr(route.app, "routes", None) is not None:
            found += _walk(route.app.routes, path)
        else:
            found.append((path, route))
    return found


def _unclassified(root: FastAPI) -> list[str]:
    """Every route under ``root`` a mirror would serve that nobody has classified."""
    problems = []
    for path, route in _walk(root.routes):
        if isinstance(route, APIRoute):
            calls = _calls(route.dependant)
            if calls & MIRROR_REFUSED_GATES or require_local_admin_access in calls:
                continue
            problems += [
                f"{method} {path}"
                for method in sorted(route.methods - {"GET", "HEAD"})
                if (method, path) not in READ_SCOPED_NON_GET
            ]
        elif isinstance(route, APIWebSocketRoute):
            if ws_require_write_access not in _calls(route.dependant) and path not in READ_SCOPED_WEBSOCKETS:
                problems.append(f"WS {path}")
        elif type(route) is Route and route.methods is not None and route.methods <= {"GET", "HEAD"}:
            continue  # FastAPI's own docs and OpenAPI pages
        else:
            problems.append(f"{type(route).__name__} {path}")
    return problems


ROUTES = [
    (method, path, route)
    for path, route in _walk(app.routes)
    if isinstance(route, APIRoute)
    for method in sorted(route.methods)
]
ROUTE_BY_KEY = {(method, path): route for method, path, route in ROUTES}
REFUSED = [(method, path) for method, path, route in ROUTES if _calls(route.dependant) & MIRROR_REFUSED_GATES]


def _concrete(path: str) -> str:
    # Gates run before path, query and body validation, so any value will do.
    return re.sub(r"\{[^}]+\}", "x", path)


@pytest.mark.parametrize(("method", "path"), REFUSED, ids=[f"{m} {p}" for m, p in REFUSED])
def test_write_and_admin_routes_are_refused_on_a_mirror(method, path, mirror_role, bind_keys, monkeypatch):
    bind_keys(ALL_SCOPES)
    route = ROUTE_BY_KEY[(method, path)]
    overrides = {call: lambda: None for call in _runtime_dependencies(route.dependant)}
    monkeypatch.setattr(route.dependency_overrides_provider, "dependency_overrides", overrides)
    client = TestClient(app, raise_server_exceptions=False)

    resp = client.request(method, _concrete(path), headers=HEADERS)

    assert resp.status_code == 403, resp.text
    assert resp.json()["code"] == "read_only_instance"


# On a forward instance the middleware answers forwardable routes before any gate
# runs, machine writes depend on CANOPY_MACHINE_ACCESS (both in
# tests/canopy/core/forwarding), and writes to an outside service (the e-log) run there
# as on a primary (tests/canopy/elog); every other write or admin route is refused there
# exactly as on a mirror. Shared-table writes count as refused: they are forwarded only
# with CANOPY_FORWARD__ALLOW_SHARED.
FORWARD_REFUSED = [
    (method, path)
    for method, path, route in ROUTES
    if (method, path) in REFUSED
    and forwarding_routes.match(method, _concrete(path), allow_shared=False) is None
    and require_machine_write_access not in _calls(route.dependant)
    and require_external_write_access not in _calls(route.dependant)
]


@pytest.mark.parametrize(("method", "path"), FORWARD_REFUSED, ids=[f"{m} {p}" for m, p in FORWARD_REFUSED])
def test_routes_not_forwarded_are_refused_on_a_forward_instance(method, path, forward_role, bind_keys, monkeypatch):
    bind_keys(ALL_SCOPES)
    route = ROUTE_BY_KEY[(method, path)]
    overrides = {call: lambda: None for call in _runtime_dependencies(route.dependant)}
    monkeypatch.setattr(route.dependency_overrides_provider, "dependency_overrides", overrides)
    client = TestClient(app, raise_server_exceptions=False)

    resp = client.request(method, _concrete(path), headers=HEADERS)

    assert resp.status_code == 403, resp.text
    assert resp.json() == {"code": "read_only_instance", "detail": security.NOT_FORWARDED_DETAIL}


def test_a_forward_instance_still_refuses_shared_tables_and_debug_routes():
    assert {
        ("POST", "/squirrel/v1/tags"),
        ("POST", "/magpie/v1/configs/{app}"),
        ("POST", "/vole/v1/_debug/evict/{pv:path}"),
    } <= set(FORWARD_REFUSED)
    assert ("POST", "/squirrel/v1/snapshots") not in FORWARD_REFUSED
    assert ("PUT", "/hare/v1/pvs/{pv:path}") not in FORWARD_REFUSED
    assert ("POST", "/elog/v1/apps/{app}/entries") not in FORWARD_REFUSED


def test_the_refused_set_covers_the_machine_and_data_writes():
    # Guards the walk itself: if it stopped finding routes, the parametrized test
    # above would pass by checking nothing.
    assert {
        ("POST", "/squirrel/v1/snapshots/{snapshot_id}/restore"),
        ("POST", "/squirrel/v1/snapshots"),
        ("PUT", "/hare/v1/pvs/{pv:path}"),
        ("POST", "/vole/v1/_debug/evict/{pv:path}"),
        ("POST", "/magpie/v1/configs/{app}"),
        ("POST", "/squirrel/v1/tags"),
    } <= set(REFUSED)


def test_every_non_get_route_is_refused_or_explicitly_allowed():
    unclassified = _unclassified(app)

    assert not unclassified, (
        "New route(s) a mirror would serve. Gate them with require_write_access, require_admin_access "
        "or ws_require_write_access, or add them to READ_SCOPED_NON_GET or READ_SCOPED_WEBSOCKETS if "
        f"they write nothing: {unclassified}"
    )


def test_the_walk_flags_what_it_cannot_classify():
    # Guards the classifier: a nested mount is walked, and an unknown route type fails.
    async def raw(request):
        return PlainTextResponse("")

    async def asgi_app(scope, receive, send):
        return None

    inner = FastAPI()

    @inner.post("/ungated")
    async def ungated() -> None:
        return None

    @inner.post("/gated", dependencies=[Security(require_write_access)])
    async def gated() -> None:
        return None

    @inner.websocket("/ws", dependencies=[Security(ws_require_read_access)])
    async def read_socket(websocket: WebSocket) -> None:
        return None

    @inner.websocket("/ws-write", dependencies=[Security(ws_require_write_access)])
    async def write_socket(websocket: WebSocket) -> None:
        return None

    outer = FastAPI()
    outer.mount("/inner", inner)
    root = FastAPI()
    root.mount("/outer", outer)
    root.mount("/static", asgi_app)
    root.router.routes.append(Route("/raw", raw, methods=["POST"]))

    assert sorted(_unclassified(root)) == [
        "Mount /static",
        "POST /outer/inner/ungated",
        "Route /raw",
        "WS /outer/inner/ws",
    ]


def test_allowlisted_routes_still_exist_and_are_not_refused():
    served = {(method, path): route for method, path, route in ROUTES}
    sockets = {path: route for path, route in _walk(app.routes) if isinstance(route, APIWebSocketRoute)}

    for method, path in READ_SCOPED_NON_GET:
        assert (method, path) in served, f"stale allowlist entry: {method} {path}"
        assert not _calls(served[(method, path)].dependant) & MIRROR_REFUSED_GATES
    for path in READ_SCOPED_WEBSOCKETS:
        assert path in sockets, f"stale allowlist entry: WS {path}"
        assert ws_require_write_access not in _calls(sockets[path].dependant)


def test_key_management_is_the_only_local_admin_surface():
    local_admin = {
        (method, path) for method, path, route in ROUTES if require_local_admin_access in _calls(route.dependant)
    }

    assert local_admin
    assert all(path.startswith(LOCAL_ADMIN_PREFIX) for _, path in local_admin)
    assert not set(REFUSED) & local_admin


class TestGates:
    """The gates themselves, on a throwaway app: ordering against auth and scope, and primary unchanged."""

    @pytest.fixture
    def client(self, bind_keys) -> TestClient:
        bind_keys(
            ALL_SCOPES,
            APIKey(token="ro", read=True, write=False, modules=frozenset({WILDCARD}), name="ro"),
            APIKey(token="admin", read=False, write=False, admin=True, modules=frozenset({WILDCARD}), name="admin"),
        )
        gated = FastAPI()
        register_canopy_error_handler(gated)
        tag_module(gated, "core")

        @gated.get("/read")
        async def read_route(key: APIKey = Security(require_read_access)) -> dict[str, str]:
            return {"name": key.name}

        @gated.post("/write")
        async def write_route(key: APIKey = Security(require_write_access)) -> dict[str, str]:
            return {"name": key.name}

        @gated.post("/admin")
        async def admin_route(key: APIKey = Security(require_admin_access)) -> dict[str, str]:
            return {"name": key.name}

        @gated.post("/local-admin")
        async def local_admin_route(key: APIKey = Security(require_local_admin_access)) -> dict[str, str]:
            return {"name": key.name}

        return TestClient(gated)

    def test_primary_serves_writes_and_admin(self, client):
        assert client.post("/write", headers=HEADERS).status_code == 200
        assert client.post("/admin", headers=HEADERS).status_code == 200
        assert client.post("/write", headers={"X-API-Key": "ro"}).json()["code"] == "insufficient_write_access"

    def test_mirror_refuses_before_the_scope_check(self, client, mirror_role):
        # A read-only key learns the instance is read-only, not that it needs a write key.
        resp = client.post("/write", headers={"X-API-Key": "ro"})

        assert resp.status_code == 403
        assert resp.json()["code"] == "read_only_instance"

    def test_mirror_still_authenticates_first(self, client, mirror_role):
        assert client.post("/write").json()["code"] == "invalid_api_key"
        assert client.post("/write", headers={"X-API-Key": "nope"}).status_code == 401

    def test_mirror_serves_reads_and_local_admin(self, client, mirror_role):
        assert client.get("/read", headers=HEADERS).status_code == 200
        assert client.post("/local-admin", headers={"X-API-Key": "admin"}).status_code == 200
        assert client.post("/admin", headers={"X-API-Key": "admin"}).json()["code"] == "read_only_instance"

    @pytest.mark.parametrize("role", ["mirror_role", "forward_role"])
    def test_websocket_write_gate_refuses_on_a_replica(self, request, role):
        # A forward instance forwards HTTP writes only; a socket that writes is refused as on a mirror.
        request.getfixturevalue(role)
        with pytest.raises(WebSocketException) as exc_info:
            ws_require_write_access(ALL_SCOPES)

        assert exc_info.value.reason.startswith("read_only_instance:")
