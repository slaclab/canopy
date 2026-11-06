"""``canopy-worker``'s schedule per instance role: only a primary (an arm) schedules pruning."""

import os
import sys
import subprocess

from canopy.worker import prune_snapshots, worker_cron_jobs


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
