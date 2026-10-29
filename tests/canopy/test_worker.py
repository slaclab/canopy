"""``canopy-worker``'s task list per instance role: a mirror registers only stateless tasks, and only a
primary schedules pruning."""

import os
import sys
import subprocess

from canopy.worker import (
    FUNCTIONS,
    MIRROR_FUNCTIONS,
    ping,
    convert_batch,
    prune_snapshots,
    worker_cron_jobs,
    worker_functions,
)


def test_mirror_registers_only_ping_and_convert_batch():
    assert worker_functions("mirror") == [ping, convert_batch]
    assert MIRROR_FUNCTIONS == [ping, convert_batch]


def test_primary_registers_every_task():
    assert worker_functions("primary") == FUNCTIONS
    assert {f.__name__ for f in FUNCTIONS} >= {"create_snapshot_task", "restore_snapshot_task"}


def test_worker_settings_follow_canopy_role():
    # A fresh interpreter: WorkerSettings reads the role at import, and reloading
    # canopy.worker here would leak a mirror task list into every later test.
    code = "from canopy.worker import WorkerSettings; print(' '.join(f.__name__ for f in WorkerSettings.functions))"
    env = {**os.environ, "CANOPY_ROLE": "mirror"}

    out = subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, text=True)

    assert out.returncode == 0, out.stderr
    assert out.stdout.split() == ["ping", "convert_batch"]


def test_only_a_primary_schedules_pruning():
    [job] = worker_cron_jobs("primary")
    assert job.coroutine is prune_snapshots
    assert job.unique and not job.run_at_startup
    assert worker_cron_jobs("mirror") == []
    assert worker_cron_jobs("forward") == []
