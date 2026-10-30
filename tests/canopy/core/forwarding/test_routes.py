"""The forwarding allowlist against the real route table.

Every write or admin route must be named in exactly one of ``FORWARDABLE`` and
``NOT_FORWARDED``, so a route added later is never forwarded, or left out, without
someone deciding it; each forwardable entry must still exist behind the write gate,
and resolve here to the same route the app itself would pick.
"""

import pytest
from starlette.routing import compile_path

from canopy.core.security import require_write_access
from canopy.core.forwarding.routes import (
    SHARED,
    FACILITY,
    FORWARDABLE,
    NOT_FORWARDED,
    match,
)

from ..test_read_only_instance import ROUTES, REFUSED, _calls, _concrete

FORWARDED = {(route.method, route.path) for route in FORWARDABLE}


def test_every_write_or_admin_route_is_classified_once():
    assert not FORWARDED & set(NOT_FORWARDED)
    assert len(FORWARDED) == len(FORWARDABLE)
    unclassified = set(REFUSED) - FORWARDED - set(NOT_FORWARDED)
    assert not unclassified, (
        "Write or admin route(s) a forward instance hasn't been told about. Add each to FORWARDABLE "
        f"or NOT_FORWARDED in canopy.core.forwarding.routes: {sorted(unclassified)}"
    )
    stale = (FORWARDED | set(NOT_FORWARDED)) - set(REFUSED)
    assert not stale, f"entries naming no write or admin route: {sorted(stale)}"


@pytest.mark.parametrize("route", FORWARDABLE, ids=lambda r: f"{r.method} {r.path}")
def test_forwardable_routes_are_write_gated_and_resolve_to_themselves(route):
    served = {(method, path): api_route for method, path, api_route in ROUTES}
    assert require_write_access in _calls(served[(route.method, route.path)].dependant)

    concrete = _concrete(route.path)
    same_method = [path for method, path, _ in ROUTES if method == route.method]
    assert [path for path in same_method if compile_path(path)[0].match(concrete)] == [route.path]
    assert match(route.method, concrete, allow_shared=True) == (route, route.regex.match(concrete).groupdict())


def test_shared_tables_need_the_instance_policy():
    assert match("POST", "/squirrel/v1/tags", allow_shared=False) is None
    assert match("POST", "/squirrel/v1/tags", allow_shared=True)[0].route_class is SHARED
    assert match("POST", "/squirrel/v1/pvs/meta", allow_shared=False) is None
    assert match("POST", "/squirrel/v1/pvs", allow_shared=False)[0].route_class is FACILITY


def test_snapshot_changes_carry_the_age_check():
    route, params = match("DELETE", "/squirrel/v1/snapshots/abc", allow_shared=False)
    assert route.snapshot_param == "snapshot_id"
    assert params == {"snapshot_id": "abc"}
    assert match("POST", "/squirrel/v1/snapshots", allow_shared=False)[0].snapshot_param is None


def test_reads_and_unknown_paths_are_never_forwarded():
    assert match("GET", "/squirrel/v1/snapshots", allow_shared=True) is None
    assert match("POST", "/squirrel/v1/snapshots/abc/restore", allow_shared=True) is None
    assert match("PUT", "/hare/v1/pvs/SOME:PV", allow_shared=True) is None


def test_module_is_the_mount():
    assert {route.module for route in FORWARDABLE} == {"squirrel", "magpie"}
