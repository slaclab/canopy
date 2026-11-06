"""``canopy-worker``'s schedule per instance role: a primary (an arm) prunes, a head that forwards delivers its outbox."""

import os
import sys
import subprocess

import pytest

from canopy.worker import FUNCTIONS, prune_snapshots, worker_cron_jobs, worker_functions
from canopy.core.config import Settings, get_settings
from canopy.squirrel.tasks import (
    deliver_outbox_task,
    create_snapshot_task,
    restore_snapshot_task,
)


def test_only_a_primary_schedules_pruning():
    [job] = worker_cron_jobs("primary")
    assert job.coroutine is prune_snapshots
    assert job.unique and not job.run_at_startup
    assert worker_cron_jobs("mirror") == []


def test_a_head_that_forwards_delivers_its_outbox_on_a_schedule(monkeypatch):
    monkeypatch.setenv("CANOPY_ROLE", "mirror")
    monkeypatch.setenv("CANOPY_ARMS__ARM1__FORWARD_URL", "https://arm1.example.org")
    monkeypatch.setenv("CANOPY_ARMS__ARM1__SERVICE_KEY", "arm1-service-token")
    monkeypatch.setenv("CANOPY_SQUIRREL__OUTBOX__INTERVAL_S", "20")
    get_settings.cache_clear()
    try:
        [job] = worker_cron_jobs("mirror")
    finally:
        get_settings.cache_clear()

    assert job.coroutine is deliver_outbox_task
    assert job.second == {0, 20, 40} and job.minute is None
    assert job.unique and job.run_at_startup
    assert deliver_outbox_task in FUNCTIONS


def test_a_heads_worker_without_arms_to_forward_to_schedules_nothing():
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
