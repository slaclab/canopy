"""Converter health endpoints."""

import logging
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from canopy.core import color_scheme
from canopy.main import app
from tests.conftest import READ_HEADERS
from canopy.converter.app import app as converter_app
from canopy.contracts.health import ConverterHealthSummaryDTO
from canopy.core.color_scheme import DEGRADED_KEY, clear_color_scheme_cache
from canopy.converter.dependencies import get_conversion_service
from canopy.converter.services.conversion_service import ConversionService

SUMMARY = "/converter/v1/health/summary"
SCHEMES = Path(__file__).resolve().parents[2] / "_fixtures" / "schemes"
BAD_SCHEMES = [SCHEMES / "unparseable.json", SCHEMES / "dangling-alias.json"]
SCHEME_ENV = ("CANOPY_CONVERTER__SCHEME_PATH", "CANOPY_THEME_FILE", "CANOPY_CONVERTER__COLOR_DEF_PATH")


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    for name in SCHEME_ENV:
        monkeypatch.delenv(name, raising=False)
    clear_color_scheme_cache()
    yield
    clear_color_scheme_cache()


def test_watchdog_returns_heartbeat():
    with TestClient(app) as client:
        resp = client.get("/converter/v1/health/watchdog")
    assert resp.status_code == 200
    body = resp.json()
    assert body["module"] == "converter"
    assert "version" in body
    assert body["uptimeS"] >= 0
    assert body["nowTsNs"] > 0


def test_summary_is_ok_while_the_scheme_loads():
    with TestClient(app) as client:
        resp = client.get(SUMMARY, headers=READ_HEADERS)
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok", "degraded": []}


@pytest.mark.parametrize("env", ["CANOPY_CONVERTER__SCHEME_PATH", "CANOPY_THEME_FILE"])
@pytest.mark.parametrize("path", BAD_SCHEMES, ids=lambda p: p.stem)
def test_summary_names_a_rejected_scheme(monkeypatch, path, env):
    monkeypatch.setenv(env, str(path))
    with TestClient(app) as client:
        resp = client.get(SUMMARY, headers=READ_HEADERS)
    assert resp.status_code == 200
    assert resp.json() == {"status": "degraded", "degraded": [DEGRADED_KEY]}


def test_the_summary_body_is_the_shared_contract():
    with TestClient(app) as client:
        resp = client.get(SUMMARY, headers=READ_HEADERS)
    parsed = ConverterHealthSummaryDTO.model_validate_json(resp.content, strict=True)
    assert parsed.model_dump(by_alias=True, mode="json") == resp.json()


def test_summary_requires_a_key():
    with TestClient(app) as client:
        resp = client.get(SUMMARY)
    assert resp.status_code == 401


def _good_scheme(tmp_path) -> Path:
    path = tmp_path / "good.json"
    path.write_text('{"id": "facility", "roles": {"OK": "#00ff00"}}')
    return path


def _post_edl(client):
    edl = (Path(__file__).parent / "fixtures" / "min.edl").read_bytes()
    return client.post(
        "/converter/v1/conversions",
        headers=READ_HEADERS,
        files={"file": ("min.edl", edl, "application/octet-stream")},
    )


def test_summary_reports_the_scheme_bound_at_startup_not_the_current_env(monkeypatch, tmp_path):
    """Started with a broken file, then the environment names a good one: conversions still use the
    rejected file's fallback, so the summary stays degraded until a restart."""
    monkeypatch.setenv("CANOPY_THEME_FILE", str(BAD_SCHEMES[0]))
    with TestClient(app) as client:
        monkeypatch.setenv("CANOPY_THEME_FILE", str(_good_scheme(tmp_path)))
        summary = client.get(SUMMARY, headers=READ_HEADERS)
        conversion = _post_edl(client)
    assert summary.json() == {"status": "degraded", "degraded": [DEGRADED_KEY]}
    assert conversion.status_code == 200
    assert any(w.startswith("Color scheme could not be loaded") for w in conversion.json()["warnings"])


def test_summary_stays_ok_and_quiet_when_the_env_later_names_a_broken_file(monkeypatch, tmp_path, caplog):
    monkeypatch.setenv("CANOPY_THEME_FILE", str(_good_scheme(tmp_path)))
    with TestClient(app) as client:
        monkeypatch.setenv("CANOPY_THEME_FILE", str(BAD_SCHEMES[1]))
        with caplog.at_level(logging.ERROR, logger=color_scheme.__name__):
            summary = client.get(SUMMARY, headers=READ_HEADERS)
        conversion = _post_edl(client)
    assert summary.json() == {"status": "ok", "degraded": []}
    assert [r for r in caplog.records if r.name == color_scheme.__name__] == []
    assert conversion.json()["scheme"]["id"] == "facility"


def test_summary_of_a_scheme_free_service_is_ok():
    with TestClient(app) as client:
        converter_app.dependency_overrides[get_conversion_service] = ConversionService
        try:
            resp = client.get(SUMMARY, headers=READ_HEADERS)
        finally:
            converter_app.dependency_overrides.pop(get_conversion_service, None)
    assert resp.json() == {"status": "ok", "degraded": []}
