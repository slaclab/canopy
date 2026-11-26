"""canopy-worker builds the converter's batch service at startup, so a rejected color scheme is logged then."""

import io
import logging
import zipfile
from pathlib import Path

import pytest
import fakeredis.aioredis

from canopy import worker
from canopy.core import color_scheme
from canopy.converter import tasks
from canopy.core.color_scheme import DEGRADED_KEY, clear_color_scheme_cache
from canopy.squirrel.restore_hooks import registry

SCHEMES = Path(__file__).resolve().parents[2] / "_fixtures" / "schemes"
EDL = (Path(__file__).parent / "fixtures" / "min.edl").read_bytes()
SCHEME_ENV = ("CANOPY_CONVERTER__SCHEME_PATH", "CANOPY_THEME_FILE", "CANOPY_CONVERTER__COLOR_DEF_PATH")


@pytest.fixture(autouse=True)
def _isolated(monkeypatch):
    for name in SCHEME_ENV:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(tasks, "_service", None)
    monkeypatch.setattr(registry, "_hooks", [])
    monkeypatch.setattr(registry, "_hook_providers", None)
    clear_color_scheme_cache()
    yield
    clear_color_scheme_cache()


async def _start_worker() -> dict:
    ctx = {"redis": fakeredis.aioredis.FakeRedis()}
    await worker.on_startup(ctx)
    return ctx


def _records(caplog, logger_name: str) -> list[logging.LogRecord]:
    return [r for r in caplog.records if r.name == logger_name and r.levelno >= logging.ERROR]


async def test_startup_builds_the_batch_service():
    ctx = await _start_worker()
    try:
        assert tasks._service is not None
        assert tasks._service.scheme_meta["served"] == "canopy-default"
    finally:
        await ctx["epics"].close()


async def test_a_rejected_scheme_is_logged_once_at_startup_and_reused_by_the_first_batch(monkeypatch, caplog):
    monkeypatch.setenv("CANOPY_THEME_FILE", str(SCHEMES / "dangling-alias.json"))
    with caplog.at_level(logging.ERROR, logger=color_scheme.__name__):
        ctx = await _start_worker()
        try:
            assert len(_records(caplog, color_scheme.__name__)) == 1
            service = tasks._service
            assert service is not None and service.degraded == [DEGRADED_KEY]

            buf = io.BytesIO()
            with zipfile.ZipFile(buf, "w") as zf:
                zf.writestr("a.edl", EDL)
            await ctx["redis"].set(tasks.input_key("tok"), buf.getvalue())
            manifest = await tasks.convert_batch({"redis": ctx["redis"], "job_id": "job1"}, "tok")
        finally:
            await ctx["epics"].close()
    assert manifest["succeeded"] == 1
    assert tasks._service is service
    assert len(_records(caplog, color_scheme.__name__)) == 1


async def test_a_service_that_cannot_be_built_does_not_stop_startup(monkeypatch, caplog):
    def unbuildable():
        raise RuntimeError("widget registry unavailable")

    monkeypatch.setattr(tasks, "_get_service", unbuildable)
    with caplog.at_level(logging.ERROR, logger=tasks.logger.name):
        ctx = await _start_worker()
    try:
        assert "epics" in ctx
        (record,) = _records(caplog, tasks.logger.name)
        assert record.exc_info is not None and "widget registry unavailable" in str(record.exc_info[1])
    finally:
        await ctx["epics"].close()
