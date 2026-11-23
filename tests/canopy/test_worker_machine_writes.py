"""``canopy-worker``'s task list per instance role: only a primary takes restores."""

import os
import sys
import subprocess

from canopy.worker import FUNCTIONS, worker_functions
from canopy.core.config import Settings
from canopy.squirrel.tasks import create_snapshot_task, restore_snapshot_task


def test_only_a_primary_registers_the_restore_task():
    on_a_mirror = worker_functions(Settings(role="mirror"))

    assert worker_functions(Settings(role="primary")) == FUNCTIONS
    assert restore_snapshot_task not in on_a_mirror
    # A capture only reads EPICS, so every worker takes one.
    assert create_snapshot_task in on_a_mirror


def test_a_mirrors_worker_takes_no_restores():
    # A fresh interpreter: WorkerSettings reads the role at import, and reloading
    # canopy.worker here would leak a mirror's task list into every later test.
    code = "from canopy.worker import WorkerSettings; print(' '.join(f.__name__ for f in WorkerSettings.functions))"

    def registered(role: str) -> list[str]:
        env = {**os.environ, "CANOPY_ROLE": role}
        out = subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, text=True)
        assert out.returncode == 0, out.stderr
        return out.stdout.split()

    assert "restore_snapshot_task" not in registered("mirror")
    assert "restore_snapshot_task" in registered("primary")
