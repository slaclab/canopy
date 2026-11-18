"""``canopy-worker``'s schedule per instance role: only a primary (an arm) schedules pruning."""

import os
import sys
import subprocess

import pytest

from canopy.worker import FUNCTIONS, prune_snapshots, worker_cron_jobs, worker_functions
from canopy.core.config import Settings
from canopy.squirrel.tasks import create_snapshot_task, restore_snapshot_task


def test_only_a_primary_schedules_pruning():
    [job] = worker_cron_jobs("primary")
    assert job.coroutine is prune_snapshots
    assert job.unique and not job.run_at_startup
    assert worker_cron_jobs("mirror") == []


def test_a_heads_worker_schedules_nothing():
    # A fresh interpreter: WorkerSettings reads the role at import, and reloading
    # canopy.worker here would leak a mirror's schedule into every later test.
    code = "from canopy.worker import WorkerSettings; print(len(WorkerSettings.cron_jobs))"
    env = {**os.environ, "CANOPY_ROLE": "mirror"}

    out = subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, text=True)

    assert out.returncode == 0, out.stderr
    assert out.stdout.split() == ["0"]


@pytest.mark.parametrize("role", ["primary", "mirror"])
def test_only_machine_access_registers_the_restore_task(role):
    with_write = worker_functions(Settings(role=role, machine_access="write"))
    with_read = worker_functions(Settings(role=role, machine_access="read"))

    assert with_write == FUNCTIONS
    assert restore_snapshot_task not in with_read
    # A capture only reads EPICS, so every worker takes one.
    assert create_snapshot_task in with_read


def test_a_heads_worker_restores_only_when_told_to():
    code = "from canopy.worker import WorkerSettings; print(' '.join(f.__name__ for f in WorkerSettings.functions))"

    def registered(**env: str) -> list[str]:
        out = subprocess.run([sys.executable, "-c", code], env={**os.environ, **env}, capture_output=True, text=True)
        assert out.returncode == 0, out.stderr
        return out.stdout.split()

    assert "restore_snapshot_task" not in registered(CANOPY_ROLE="mirror")
    assert "restore_snapshot_task" in registered(CANOPY_ROLE="mirror", CANOPY_MACHINE_ACCESS="write")
    assert "restore_snapshot_task" in registered(CANOPY_ROLE="primary")
