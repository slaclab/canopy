"""Arq task plumbing: the progress sink, and the restore task's handling of ``pv_ids``."""

from sqlalchemy.ext.asyncio import AsyncSession

from canopy.squirrel import tasks
from canopy.squirrel.data.models import Pv
from canopy.squirrel.schemas.snapshot import NewSnapshotDTO
from canopy.squirrel.services.snapshot_service import SnapshotService
from tests.canopy.squirrel.test_services_snapshot import FakeEpicsIO, _ev, _NoHook


async def test_progress_sink_forwards_phases(monkeypatch):
    calls = []

    async def fake_report_progress(redis, job_id, percent, message=None, phases=None):
        calls.append((redis, job_id, percent, message, phases))

    monkeypatch.setattr(tasks, "report_progress", fake_report_progress)
    sink = tasks._progress_sink({"redis": "redis", "job_id": "j"})
    phases = [{"name": "Setpoints", "current": 1, "total": 4, "status": "running", "failures": 0}]

    await sink(1, 4, "writing", phases=phases)
    await sink(2, 4, "saving")
    await sink(0, 0, "nothing to do")

    assert calls == [
        ("redis", "j", 25, "writing", phases),
        ("redis", "j", 50, "saving", None),
        ("redis", "j", 0, "nothing to do", None),
    ]


async def test_restore_task_with_an_empty_pv_list_writes_nothing(db_session: AsyncSession, monkeypatch):
    """The worker half of the restore-bypass fix: `pv_ids=[]` reaching the task means
    "restore nothing" and must not be widened to "restore all"."""
    db_session.add_all([Pv(setpoint_address="A:SP"), Pv(setpoint_address="B:SP")])
    await db_session.commit()
    io = FakeEpicsIO({"A:SP": _ev(1.0), "B:SP": _ev(2.0)})
    summary = await SnapshotService(db_session, io, restore_hooks=[_NoHook()]).create_snapshot(
        NewSnapshotDTO(title="s")
    )
    await db_session.commit()
    io.writes.clear()

    async def fake_report_progress(*args, **kwargs):
        pass

    monkeypatch.setattr(tasks, "report_progress", fake_report_progress)
    monkeypatch.setattr(tasks, "_build_epics_io", lambda ctx: io)

    result = await tasks.restore_snapshot_task({"redis": None, "job_id": "j"}, summary.id, pv_ids=[])

    assert result["totalPvs"] == 0
    assert io.writes == {}
