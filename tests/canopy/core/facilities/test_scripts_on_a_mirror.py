"""The facility scripts write ``core.facility`` directly, so a mirror's must refuse before connecting.

A forward instance too: facilities are defined on its primary and replicate to it.
"""

import os
import sys
import subprocess
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[4] / "scripts"


@pytest.mark.parametrize("role", ["mirror", "forward"])
@pytest.mark.parametrize(
    "argv",
    [["create_facility.py", "site-a", "--name", "Site A"], ["seed_dev_facilities.py"]],
    ids=["create_facility", "seed_dev_facilities"],
)
def test_refuses_on_a_mirror(argv, role):
    env = {
        **os.environ,
        "CANOPY_ROLE": role,
        # Nothing listens here: reaching the database would fail differently.
        "CANOPY_DATABASE_URL": "postgresql+asyncpg://canopy:canopy@127.0.0.1:9/canopy",
    }

    out = subprocess.run(
        [sys.executable, str(SCRIPTS / argv[0]), *argv[1:]],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )

    assert out.returncode == 2, out.stderr
    assert "read-only mirror" in out.stderr
