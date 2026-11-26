"""The converter's color scheme: config precedence, injection into convert_bytes, and the DTO ``scheme``."""

import io
import json
import zipfile
from typing import Protocol, runtime_checkable
from pathlib import Path

import pytest
import fakeredis.aioredis
from fastapi.testclient import TestClient
from pydmconverter.colors import SchemeLike

import canopy.converter.tasks as tasks_module
from canopy.core import color_scheme
from canopy.main import app
from tests.conftest import READ_HEADERS
from canopy.converter.app import app as converter_app
from canopy.converter.config import ConverterConfig
from canopy.core.color_scheme import (
    DEFAULT_ID,
    get_color_scheme,
    resolve_scheme_path,
    clear_color_scheme_cache,
)
from canopy.converter.services import conversion_service as service_module
from canopy.converter.dependencies import get_conversion_service
from canopy.core.color_scheme_flatten import SchemeResult
from canopy.converter.services.conversion_service import ConversionService

FIXTURES = Path(__file__).parent / "fixtures"
EDL = (FIXTURES / "min.edl").read_bytes()

SCHEME_ENV = ("CANOPY_CONVERTER__SCHEME_PATH", "CANOPY_THEME_FILE", "CANOPY_CONVERTER__COLOR_DEF_PATH")


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    for name in SCHEME_ENV:
        monkeypatch.delenv(name, raising=False)
    clear_color_scheme_cache()
    tasks_module._service = None
    yield
    clear_color_scheme_cache()
    tasks_module._service = None


@pytest.fixture
def spy(monkeypatch):
    """Record the kwargs each convert_bytes call receives, then run the real converter."""
    calls: list[dict] = []
    real = service_module.convert_bytes

    def _spy(data, **kwargs):
        calls.append(kwargs)
        return real(data, **kwargs)

    monkeypatch.setattr(service_module, "convert_bytes", _spy)
    return calls


@pytest.fixture
def good_scheme(tmp_path) -> Path:
    path = tmp_path / "facility.json"
    path.write_text(json.dumps({"id": "facility", "extends": "canopy-default", "roles": {"OK": "#00ff00"}}))
    return path


@pytest.fixture
def broken_scheme(tmp_path) -> Path:
    path = tmp_path / "broken.json"
    path.write_text("{ this is not json")
    return path


def _scheme_warnings(warnings: list[str]) -> list[str]:
    return [w for w in warnings if w.startswith("Color scheme")]


# --- config ---------------------------------------------------------------------------------


def test_scheme_path_defaults_to_none():
    config = ConverterConfig()
    assert config.scheme_path is None
    assert config.color_def_path is None


def test_scheme_path_falls_back_to_theme_file(monkeypatch):
    """The converter's own setting stays unset; the one resolver supplies CANOPY_THEME_FILE."""
    monkeypatch.setenv("CANOPY_THEME_FILE", "/srv/theme.json")
    config = ConverterConfig()
    assert config.scheme_path is None
    assert resolve_scheme_path(config.scheme_path) == Path("/srv/theme.json").resolve()


def test_converter_scheme_path_wins_over_theme_file(monkeypatch):
    monkeypatch.setenv("CANOPY_THEME_FILE", "/srv/theme.json")
    monkeypatch.setenv("CANOPY_CONVERTER__SCHEME_PATH", "/srv/converter.json")
    config = ConverterConfig()
    assert config.scheme_path == Path("/srv/converter.json")
    assert resolve_scheme_path(config.scheme_path) == Path("/srv/converter.json").resolve()


def test_blank_theme_file_resolves_to_the_default(monkeypatch):
    monkeypatch.setenv("CANOPY_THEME_FILE", "  ")
    assert resolve_scheme_path(None) is None


def test_blank_scheme_env_is_unset(monkeypatch):
    monkeypatch.setenv("CANOPY_CONVERTER__SCHEME_PATH", "  ")
    monkeypatch.setenv("CANOPY_CONVERTER__COLOR_DEF_PATH", "")
    config = ConverterConfig()
    assert config.scheme_path is None
    assert config.color_def_path is None


def test_color_def_path_from_env(monkeypatch):
    monkeypatch.setenv("CANOPY_CONVERTER__COLOR_DEF_PATH", "/etc/phoebus/color.def")
    assert ConverterConfig().color_def_path == Path("/etc/phoebus/color.def")


def test_fields_settable_by_name():
    config = ConverterConfig(scheme_path="/a.json", color_def_path="/b.def")
    assert config.scheme_path == Path("/a.json")
    assert config.color_def_path == Path("/b.def")


# --- service --------------------------------------------------------------------------------


def test_scheme_free_service_passes_no_scheme(spy):
    service = ConversionService()
    result = service.convert(EDL, kind="edl")
    assert spy[0]["scheme"] is None
    assert "color_def_path" not in spy[0]
    assert service.scheme_meta is None
    assert result.scheme is None
    assert _scheme_warnings(result.warnings) == []


def test_injected_scheme_result_is_passed_through(spy, good_scheme):
    loaded = get_color_scheme(good_scheme)
    service = ConversionService(scheme=loaded)
    result = service.convert(EDL, kind="edl")
    assert spy[0]["scheme"] is loaded.result
    assert isinstance(spy[0]["scheme"], SchemeResult)
    assert "color_def_path" not in spy[0]
    assert service.scheme_meta == loaded.meta
    assert result.scheme == loaded.meta
    assert result.scheme["id"] == "facility"
    assert _scheme_warnings(result.warnings) == []


def test_color_def_path_passed_only_when_set(spy, tmp_path):
    color_def = tmp_path / "color.def"
    color_def.write_text("")
    ConversionService(color_def_path=color_def).convert(EDL, kind="edl")
    assert spy[0]["color_def_path"] == color_def
    ConversionService(color_def_path=None).convert(EDL, kind="edl")
    assert "color_def_path" not in spy[1]


def test_load_error_loads_once_and_warns_once_per_conversion(monkeypatch, broken_scheme):
    loads = []
    real = color_scheme.load_scheme

    def _counting(*args, **kwargs):
        loads.append(args)
        return real(*args, **kwargs)

    monkeypatch.setattr(color_scheme, "load_scheme", _counting)
    service = ConversionService(scheme=get_color_scheme(broken_scheme))
    # A second service on the same path (e.g. the batch worker) reuses the cached load.
    other = ConversionService(scheme=get_color_scheme(broken_scheme))
    results = [service.convert(EDL, kind="edl"), service.convert(EDL, kind="edl"), other.convert(EDL, kind="edl")]

    assert len(loads) == 1
    for result in results:
        assert len(_scheme_warnings(result.warnings)) == 1
        assert result.scheme["id"] is None
        assert result.scheme["served"] == DEFAULT_ID
        assert result.scheme["errors"]


# A synthetic .bob panel: a named display background and one named text color, as Phoebus saves them.
BOB_PANEL = (
    b'<?xml version="1.0" encoding="UTF-8"?>\n<display version="2.0.0"><name>t</name>'
    b'<background_color><color name="Display_Background" red="255" green="255" blue="255"/></background_color>'
    b'<widget type="textupdate" version="2.0.0"><name>T</name><pv_name>sim://x</pv_name>'
    b'<foreground_color><color name="Text" red="0" green="0" blue="0"/></foreground_color></widget>'
    b"</display>"
)


def _text_color(screen: dict):
    (widget,) = [c for c in screen["root"]["children"] if c["props"].get("pv") == "sim://x"]
    return widget["props"]["foregroundColor"]


def test_canopy_flattened_default_becomes_roles_in_a_bob_conversion():
    """canopy's own SchemeResult is what convert_bytes reads (flat, imports, sources): roles come out."""
    result = ConversionService(scheme=get_color_scheme()).convert(BOB_PANEL, kind="bob")
    assert result.screen["root"]["props"]["backgroundColor"] == {"$role": "Display_Background", "fallback": "#ffffff"}
    assert result.screen["schemaVersion"] == "1.1"
    assert _text_color(result.screen)["$role"] == "Text"
    assert result.scheme["id"] == DEFAULT_ID


def test_facility_import_flattened_by_canopy_reaches_the_converter(tmp_path):
    """A role and an import only the facility file has: the converter can only know them from canopy's flattening."""
    doc = {
        "id": "facility",
        "format": "canopy-color-scheme/1",
        "roles": {"Ink": {"light": "#000", "dark": "#f4f4f4"}},
        "import": {"phoebus": {"Text": "Ink"}},
    }
    path = tmp_path / "facility.json"
    path.write_text(json.dumps(doc))
    result = ConversionService(scheme=get_color_scheme(path)).convert(BOB_PANEL, kind="bob")
    assert result.scheme["id"] == "facility"
    assert result.scheme["errors"] == []
    color = _text_color(result.screen)
    assert color["$role"] == "Ink"
    assert color["fallback"] == "#000000"


@runtime_checkable
class _CheckableSchemeLike(SchemeLike, Protocol):
    """The converter's ``SchemeLike`` (static only), made checkable with ``isinstance`` here."""


def test_canopy_scheme_results_are_the_converters_scheme_like(good_scheme, broken_scheme):
    """``convert_bytes(scheme=...)`` takes any ``SchemeLike``: ``flat``, ``imports``, ``sources`` and ``meta``."""
    for path in (None, good_scheme, broken_scheme):
        result = get_color_scheme(path).result
        assert isinstance(result, SchemeResult)
        assert isinstance(result, _CheckableSchemeLike)
    assert not isinstance(object(), _CheckableSchemeLike)


def test_scheme_free_bob_conversion_has_no_roles():
    result = ConversionService().convert(BOB_PANEL, kind="bob")
    assert result.screen["root"]["props"]["backgroundColor"] == "#ffffff"
    assert result.screen["schemaVersion"] == "1.0"


# --- HTTP -----------------------------------------------------------------------------------


def _with_service(service: ConversionService):
    converter_app.dependency_overrides[get_conversion_service] = lambda: service


def _post(client, path: str, *, filename: str = "min.edl", content: bytes = EDL):
    return client.post(
        f"/converter/v1{path}",
        headers=READ_HEADERS,
        files={"file": (filename, content, "application/octet-stream")},
    )


@pytest.fixture
def broken_service(broken_scheme):
    _with_service(ConversionService(scheme=get_color_scheme(broken_scheme)))
    yield
    converter_app.dependency_overrides.pop(get_conversion_service, None)


def test_convert_response_carries_scheme_without_flat_hash(broken_service):
    with TestClient(app) as client:
        resp = _post(client, "/conversions")
    assert resp.status_code == 200
    body = resp.json()
    scheme = body["scheme"]
    assert set(scheme) == {"id", "sha256", "served", "errors"}
    assert scheme["id"] is None
    assert scheme["served"] == DEFAULT_ID
    assert scheme["errors"] and set(scheme["errors"][0]) == {"path", "message"}
    assert len(_scheme_warnings(body["warnings"])) == 1


def test_validate_ok_response_carries_scheme(broken_service):
    with TestClient(app) as client:
        resp = _post(client, "/conversions/validate")
    body = resp.json()
    assert body["ok"] is True
    assert body["scheme"]["id"] is None
    assert len(_scheme_warnings(body["warnings"])) == 1


def test_validate_error_response_carries_scheme(broken_service):
    with TestClient(app) as client:
        resp = _post(client, "/conversions/validate", filename="broken.ui", content=b"<not-a-ui/>")
    body = resp.json()
    assert body["ok"] is False
    assert body["scheme"]["id"] is None
    assert body["scheme"]["served"] == DEFAULT_ID


def test_lifespan_injects_configured_scheme(monkeypatch, good_scheme, spy):
    monkeypatch.setenv("CANOPY_CONVERTER__SCHEME_PATH", str(good_scheme))
    with TestClient(app) as client:
        resp = _post(client, "/conversions")
    assert resp.status_code == 200
    assert resp.json()["scheme"]["id"] == "facility"
    assert "scheme" in spy[0]


def test_lifespan_default_scheme_is_canopy_default():
    with TestClient(app) as client:
        resp = _post(client, "/conversions")
    scheme = resp.json()["scheme"]
    assert scheme["id"] == DEFAULT_ID
    assert scheme["served"] == DEFAULT_ID
    assert scheme["errors"] == []
    assert scheme["sha256"]


# --- batch ----------------------------------------------------------------------------------


async def test_batch_service_injects_scheme_and_warns_after_load_error(monkeypatch, broken_scheme, spy):
    monkeypatch.setenv("CANOPY_CONVERTER__SCHEME_PATH", str(broken_scheme))
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("a.edl", EDL)
        zf.writestr("b.edl", EDL)
    redis = fakeredis.aioredis.FakeRedis()
    await redis.set(tasks_module.input_key("tok"), buf.getvalue())

    manifest = await tasks_module.convert_batch({"redis": redis, "job_id": "job1"}, "tok")

    assert manifest["succeeded"] == 2
    assert tasks_module._get_service().scheme_meta["id"] is None
    assert all("scheme" in call for call in spy)
    for row in manifest["rows"]:
        assert len(_scheme_warnings(row["warnings"])) == 1
