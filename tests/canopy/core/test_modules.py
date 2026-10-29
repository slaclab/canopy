"""Drift check: ``MODULES`` must track ``canopy.main``'s actual mounted sub-apps.

Hardcoded rather than derived at import time (see ``canopy.core.modules`` for why),
so this test is what keeps an eleventh mount from silently becoming un-scopable.
"""

import sys
import subprocess

import pytest
from starlette.routing import Mount

from canopy.main import app
from canopy.core.modules import MODULES, WILDCARD, split_modules, module_for_app


def test_modules_matches_actual_mounts():
    mounted = {route.path.strip("/") for route in app.routes if isinstance(route, Mount)}

    assert mounted | {"core"} == MODULES


def test_every_mount_and_the_parent_app_are_tagged_with_their_module():
    # module_for_app reads app.state.canopy_module, not the URL — an untagged
    # mount would raise AttributeError from the security dependency on its
    # first request rather than failing here at import time.
    assert module_for_app(app) == "core"

    for route in app.routes:
        if isinstance(route, Mount):
            assert module_for_app(route.app) == route.path.strip("/")


@pytest.mark.parametrize("module", sorted(MODULES - {"core"}))
def test_a_module_app_is_tagged_without_canopy_main(module):
    # A module's own tests import only its app, so the tag must not depend on
    # canopy.main having been imported. This process has already imported it, so
    # check in a fresh interpreter.
    code = (
        "import sys\n"
        f"from canopy.{module}.app import app\n"
        "from canopy.core.modules import module_for_app\n"
        "assert 'canopy.main' not in sys.modules\n"
        "print(module_for_app(app))\n"
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=60)

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == module


class TestSplitModules:
    def test_recognized_modules_pass_through(self):
        known, unknown = split_modules({"fox", "vole"})

        assert known == frozenset({"fox", "vole"})
        assert unknown == frozenset()

    def test_unrecognized_modules_are_separated_out(self):
        known, unknown = split_modules({"fox", "not-a-module"})

        assert known == frozenset({"fox"})
        assert unknown == frozenset({"not-a-module"})

    def test_wildcard_collapses_the_known_set(self):
        known, unknown = split_modules({"fox", WILDCARD})

        assert known == frozenset({WILDCARD})
        assert unknown == frozenset()
