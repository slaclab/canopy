"""``core.asgi.route_path`` matches Starlette's router on every root path, the gate's and the head's alike."""

import pytest
from starlette._utils import get_route_path

from canopy.core.asgi import route_path

CASES = [
    ("/squirrel/v1/pvs/p1", ""),
    ("/canopy/squirrel/v1/pvs/p1", "/canopy"),
    ("/canopy", "/canopy"),
    ("/canopy/", "/canopy"),
    ("/canopyx/squirrel/v1/pvs", "/canopy"),
    ("/squirrel/v1/pvs", "/canopy"),
    ("/canopy/squirrel/v1/pvs/p1", "/canopy/"),
    ("/api/canopy/health", "/api/canopy"),
]


@pytest.mark.parametrize(("path", "root_path"), CASES)
def test_the_path_is_the_one_the_router_matches(path, root_path):
    scope = {"type": "http", "path": path, "root_path": root_path}

    assert route_path(scope) == get_route_path(scope)


def test_the_prefix_comes_off_at_a_path_boundary():
    assert route_path({"path": "/canopy/squirrel/v1/pvs", "root_path": "/canopy"}) == "/squirrel/v1/pvs"
    assert route_path({"path": "/canopyx/squirrel", "root_path": "/canopy"}) == "/canopyx/squirrel"
