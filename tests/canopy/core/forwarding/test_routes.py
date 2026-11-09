"""The head's routing table against the real route table.

Walks ``canopy.main``'s own routes and every mounted sub-app's, at any depth, so a
route added later is classified the moment it exists: a non-GET route or WebSocket
gated by anything but a read scope must be in exactly one of ``ROUTES`` (with the
rule that finds its owner) and ``LOCAL`` (with the reason it runs here), and a route
of a type the walk doesn't know fails it. Nothing is forwarded to an arm, or left on
the head, without someone deciding it.
"""

import re

import pytest
from fastapi import FastAPI, Security, WebSocket
from fastapi.routing import APIRoute, APIWebSocketRoute
from starlette.routing import Mount, Route, BaseRoute, compile_path
from starlette.responses import PlainTextResponse

from canopy.main import app
from canopy.core.security import (
    require_read_access,
    require_admin_access,
    require_write_access,
    ws_require_read_access,
    ws_require_write_access,
    require_read_or_admin_access,
    require_wildcard_facility_access,
)
from canopy.core.forwarding.routes import LOCAL, ROUTES, Row, match
from canopy.core.replication.registry import REGISTRY

GATES = {require_write_access, require_admin_access, require_wildcard_facility_access}
READ_GATES = {require_read_access, require_read_or_admin_access, ws_require_read_access}
ROUTED = {(route.method, route.path): route for route in ROUTES}


def _calls(dependant) -> set:
    calls = set()
    for sub in dependant.dependencies:
        calls.add(sub.call)
        calls |= _calls(sub)
    return calls


def _is_read(dependant) -> bool:
    """Gated by a read scope and nothing more: it computes, validates or subscribes."""
    calls = _calls(dependant)
    return bool(calls & READ_GATES) and not calls & (GATES | {ws_require_write_access})


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


def _unclassified(root: FastAPI, classified: set[tuple[str, str]]) -> list[str]:
    """Every route under ``root`` a head would serve that nobody has classified."""
    problems = []
    for path, route in _walk(root.routes):
        if isinstance(route, APIRoute):
            if _is_read(route.dependant):
                continue
            problems += [
                f"{method} {path}"
                for method in sorted(route.methods - {"GET", "HEAD"})
                if (method, path) not in classified
            ]
        elif isinstance(route, APIWebSocketRoute):
            if not _is_read(route.dependant) and ("WS", path) not in classified:
                problems.append(f"WS {path}")
        elif type(route) is Route and route.methods is not None and route.methods <= {"GET", "HEAD"}:
            continue  # FastAPI's own docs and OpenAPI pages
        else:
            problems.append(f"{type(route).__name__} {path}")
    return problems


SERVED = {
    (method, path): route
    for path, route in _walk(app.routes)
    if isinstance(route, APIRoute)
    for method in sorted(route.methods)
}


def _concrete(path: str) -> str:
    return re.sub(r"\{[^}]+\}", "x", path)


def test_every_write_is_routed_or_local_once():
    assert len(ROUTED) == len(ROUTES)
    assert not set(ROUTED) & set(LOCAL)
    unclassified = _unclassified(app, set(ROUTED) | set(LOCAL))
    assert not unclassified, (
        "Write route(s) a head hasn't been told about. Add each to ROUTES (with its owner rule) or LOCAL "
        f"(with the reason) in canopy.core.forwarding.routes: {unclassified}"
    )
    stale = (set(ROUTED) | set(LOCAL)) - set(SERVED)
    assert not stale, f"entries naming no route: {sorted(stale)}"


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

    @inner.post("/read", dependencies=[Security(require_read_access)])
    async def read() -> None:
        return None

    @inner.post("/write", dependencies=[Security(require_write_access)])
    async def write() -> None:
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

    assert sorted(_unclassified(root, {("POST", "/outer/inner/write")})) == [
        "Mount /static",
        "POST /outer/inner/ungated",
        "Route /raw",
        "WS /outer/inner/ws-write",
    ]


@pytest.mark.parametrize("route", ROUTES, ids=lambda r: f"{r.method} {r.path}")
def test_each_route_repeats_the_apps_gates_and_resolves_to_itself(route):
    served = SERVED[(route.method, route.path)]
    assert set(route.gates) == _calls(served.dependant) & GATES

    concrete = _concrete(route.path)
    same_method = [path for method, path in SERVED if method == route.method]
    assert [path for path in same_method if compile_path(path)[0].match(concrete)] == [route.path]
    assert match(route.method, concrete) == (route, route.regex.match(concrete).groupdict())


@pytest.mark.parametrize("route", ROUTES, ids=lambda r: f"{r.method} {r.path}")
def test_owner_param_matches_the_apps_query(route):
    query = {param.alias for param in SERVED[(route.method, route.path)].dependant.query_params}
    assert route.owner_param == ("owner" in query)


def test_the_owner_param_creates():
    # A capture takes ?owner= too, but is never routed: it is taken here (canopy.squirrel.capture).
    assert ("POST", "/squirrel/v1/snapshots") in LOCAL
    assert {(r.method, r.path) for r in ROUTES if r.owner_param} == {
        ("POST", "/squirrel/v1/pvs"),
        ("POST", "/squirrel/v1/pvs/multi"),
        ("POST", "/squirrel/v1/pvs/meta"),
        ("POST", "/squirrel/v1/tags"),
        ("POST", "/squirrel/v1/tags/bulk"),
        ("POST", "/magpie/v1/configs/{app}"),
        ("POST", "/v1/facilities"),
    }


@pytest.mark.parametrize("route", [r for r in ROUTES if isinstance(r.owner, Row)], ids=lambda r: r.path)
def test_each_row_rule_names_an_owned_table_and_a_path_parameter(route):
    assert route.owner.table in REGISTRY.owner_paths
    assert route.owner.param in route.regex.groupindex


def test_unique_keys_are_the_registrys():
    for route in ROUTES:
        if route.unique is not None:
            table, column = route.unique
            assert column in REGISTRY.shared_keys[table]
    assert ROUTED[("POST", "/v1/facilities")].unique == ("core.facility", "slug")


def test_only_the_captured_import_stays_by_mode():
    assert {(r.path, r.local_when) for r in ROUTES if r.local_when} == {
        ("/squirrel/v1/snapshots/import", ("mode", "captured"))
    }


def test_only_a_restore_writes_the_machine():
    assert {(r.method, r.path) for r in ROUTES if r.machine} == {
        ("POST", "/squirrel/v1/snapshots/{snapshot_id}/restore")
    }


def test_reads_captures_and_unknown_paths_are_never_routed():
    assert match("GET", "/squirrel/v1/snapshots") is None
    assert match("POST", "/squirrel/v1/snapshots") is None
    assert match("PUT", "/hare/v1/pvs/SOME:PV") is None
    assert match("POST", "/elog/v1/apps/squirrel/entries") is None


def test_module_is_the_mount():
    assert {route.module for route in ROUTES} == {"squirrel", "magpie", "core"}
