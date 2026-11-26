"""get_color_scheme: one flatten per path, canopy-default on any error, and no converter needed."""

import sys
import json
import hashlib
import logging
import subprocess
import importlib.util
from pathlib import Path

import pytest

from canopy.core import color_scheme
from canopy.core.color_scheme import (
    THEME_FILE_ENV,
    DEFAULT_SCHEME_PATH,
    ColorSchemeSettings,
    get_color_scheme,
)
from canopy.core.color_scheme_flatten import SchemeResult, load_scheme


@pytest.fixture(autouse=True)
def _fresh_cache(monkeypatch):
    monkeypatch.delenv(THEME_FILE_ENV, raising=False)
    color_scheme.clear_color_scheme_cache()
    yield
    color_scheme.clear_color_scheme_cache()


def _errors(caplog):
    return [r for r in caplog.records if r.name == color_scheme.__name__ and r.levelno == logging.ERROR]


def _packaged_sha256() -> str:
    return hashlib.sha256(DEFAULT_SCHEME_PATH.read_bytes()).hexdigest()


def _facility(tmp_path: Path, scheme_id: str, name: str = "facility.json") -> Path:
    path = tmp_path / name
    path.write_text(json.dumps({"id": scheme_id, "extends": "canopy-default", "roles": {"Beam": "#123456"}}))
    return path


def test_unset_env_serves_canopy_default(caplog):
    scheme = get_color_scheme()
    assert scheme.meta["id"] == "canopy-default"
    assert scheme.meta["served"] == "canopy-default"
    assert scheme.meta["sha256"] == _packaged_sha256()
    assert scheme.meta["errors"] == []
    assert scheme.degraded == []
    assert isinstance(scheme.result, SchemeResult)
    assert scheme.flat_json == scheme.result.flat_json()
    assert scheme.flat_json["meta"] == scheme.meta
    assert "Text" in scheme.flat_json["roles"]
    assert scheme.imports["phoebus"]
    assert _errors(caplog) == []


def test_empty_env_counts_as_unset(monkeypatch):
    monkeypatch.setenv(THEME_FILE_ENV, "")
    assert get_color_scheme().meta["id"] == "canopy-default"


def test_valid_file_gives_facility_id(tmp_path, caplog):
    scheme = get_color_scheme(_facility(tmp_path, "facility-x"))
    assert scheme.meta["id"] == "facility-x"
    assert scheme.meta["served"] == "facility-x"
    assert scheme.meta["errors"] == []
    assert scheme.degraded == []
    assert "Beam" in scheme.flat_json["roles"]
    assert _errors(caplog) == []


def test_env_var_names_the_file(tmp_path, monkeypatch):
    monkeypatch.setenv(THEME_FILE_ENV, str(_facility(tmp_path, "from-env")))
    assert get_color_scheme().meta["id"] == "from-env"


@pytest.mark.parametrize(
    "content",
    [
        "{ not json",
        json.dumps({"id": "broken", "extends": "canopy-default", "roles": {"Beam": "@NoSuchRole"}}),
    ],
    ids=["unparseable", "broken-but-parseable"],
)
def test_bad_file_logs_once_and_serves_default(tmp_path, caplog, content):
    path = tmp_path / "bad.json"
    path.write_text(content)
    with caplog.at_level(logging.ERROR, logger=color_scheme.__name__):
        scheme = get_color_scheme(path)
    assert len(_errors(caplog)) == 1
    assert _errors(caplog)[0].getMessage().startswith("color scheme ")
    assert scheme.degraded == ["color-scheme"]
    assert scheme.meta["served"] == "canopy-default"
    assert scheme.meta["errors"]
    assert scheme.flat_json is not None
    assert "Beam" not in scheme.flat_json["roles"]
    assert scheme.flat_json["roles"] == load_scheme(None).flat["roles"]


def test_missing_file_is_degraded(tmp_path, caplog):
    with caplog.at_level(logging.ERROR, logger=color_scheme.__name__):
        scheme = get_color_scheme(tmp_path / "absent.json")
    assert len(_errors(caplog)) == 1
    assert scheme.degraded == ["color-scheme"]
    assert scheme.meta["served"] == "canopy-default"


def test_second_call_same_path_does_not_reflatten(tmp_path, caplog, monkeypatch):
    calls = []
    real = color_scheme.load_scheme

    def spy(*args, **kwargs):
        calls.append(args)
        return real(*args, **kwargs)

    monkeypatch.setattr(color_scheme, "load_scheme", spy)
    path = tmp_path / "bad.json"
    path.write_text("{ not json")
    with caplog.at_level(logging.ERROR, logger=color_scheme.__name__):
        first = get_color_scheme(path)
        # A relative spelling of the same file hits the same cache entry.
        monkeypatch.chdir(tmp_path)
        second = get_color_scheme("bad.json")
    assert second is first
    assert len(calls) == 1
    assert len(_errors(caplog)) == 1


def test_default_is_cached_too(monkeypatch):
    first = get_color_scheme()
    assert get_color_scheme(None) is first


# -- a facility file that keeps the id canopy-default -------------------------------------------------------------------


def _warnings(caplog):
    return [r for r in caplog.records if r.name == color_scheme.__name__ and r.levelno == logging.WARNING]


def test_a_facility_file_with_the_default_id_loads_with_one_warning(tmp_path, caplog):
    path = tmp_path / "copy.json"
    path.write_text(json.dumps({"id": "canopy-default", "roles": {"OK": "#00ff00"}}))
    with caplog.at_level(logging.WARNING, logger=color_scheme.__name__):
        scheme = get_color_scheme(path)
        get_color_scheme(path)
    # Served like any facility file: only the empty errors tell it from the fallback.
    assert scheme.meta["served"] == "canopy-default"
    assert scheme.meta["errors"] == []
    assert scheme.degraded == []
    assert "#00ff00" in scheme.flat_json["light"]
    assert len(_warnings(caplog)) == 1
    assert "give a facility scheme its own id" in _warnings(caplog)[0].getMessage()
    assert _errors(caplog) == []


def test_no_warning_for_the_built_in_default_or_a_facility_id(tmp_path, caplog):
    with caplog.at_level(logging.WARNING, logger=color_scheme.__name__):
        get_color_scheme()
        get_color_scheme(_facility(tmp_path, "facility-x"))
        broken = tmp_path / "broken.json"
        broken.write_text(json.dumps({"id": "canopy-default", "roles": {"A": "@Nope"}}))
        get_color_scheme(broken)
    assert _warnings(caplog) == []
    assert len(_errors(caplog)) == 1


# -- CANOPY_THEME_FILE through settings -------------------------------------------------------------------------------


def test_theme_file_is_a_setting(tmp_path, monkeypatch):
    path = _facility(tmp_path, "from-setting")
    monkeypatch.setenv(THEME_FILE_ENV, str(path))
    assert ColorSchemeSettings().theme_file == path
    assert ColorSchemeSettings(theme_file="  ").theme_file is None
    assert ColorSchemeSettings(theme_file=str(path)).theme_file == path


def test_theme_file_is_read_from_a_dotenv_file(tmp_path):
    # In a subprocess: tests/conftest.py turns .env reading off for the whole suite.
    path = _facility(tmp_path, "from-dotenv")
    (tmp_path / ".env").write_text(f"{THEME_FILE_ENV}={path}\n")
    code = "from canopy.core.color_scheme import get_color_scheme; print(get_color_scheme().meta['id'])"
    proc = subprocess.run(
        [sys.executable, "-c", code], cwd=tmp_path, capture_output=True, text=True, timeout=60, check=True
    )
    assert proc.stdout.strip() == "from-dotenv"


def test_changed_env_applies_after_clearing_the_cache(tmp_path, monkeypatch):
    monkeypatch.setenv(THEME_FILE_ENV, str(_facility(tmp_path, "first", "a.json")))
    assert get_color_scheme().meta["id"] == "first"
    monkeypatch.setenv(THEME_FILE_ENV, str(_facility(tmp_path, "second", "b.json")))
    color_scheme.clear_color_scheme_cache()
    assert get_color_scheme().meta["id"] == "second"
    monkeypatch.delenv(THEME_FILE_ENV)
    assert get_color_scheme().meta["id"] == "canopy-default"


def test_an_unrelated_bad_setting_does_not_stop_the_scheme(monkeypatch):
    monkeypatch.setenv("CANOPY_LOG_FORMAT", "not-a-format")
    monkeypatch.setenv("CANOPY_DEBUG", "not-a-bool")
    assert get_color_scheme().meta["served"] == "canopy-default"


# -- no converter -----------------------------------------------------------------------------------------------------

_WITHOUT_CONVERTER = """
import sys
sys.modules["pydmconverter"] = None  # any import of the converter now raises ImportError
from canopy.core.color_scheme import get_color_scheme
from canopy.core.color_scheme_cli import main
scheme = get_color_scheme()
assert scheme.degraded == [], scheme.degraded
assert scheme.flat_json is not None and scheme.flat_json["meta"]["served"] == "canopy-default"
assert len(scheme.flat_json["roles"]) == len(scheme.flat_json["light"]) == len(scheme.flat_json["dark"]) > 0
assert main(["check", sys.argv[1]]) == 0
assert not [name for name in sys.modules if name.startswith("pydmconverter") and sys.modules[name] is not None]
"""


def test_flattens_and_serves_the_default_without_the_converter():
    """The lean install: no ``convert`` extra, yet the default scheme flattens and serves."""
    proc = subprocess.run(
        [sys.executable, "-c", _WITHOUT_CONVERTER, str(DEFAULT_SCHEME_PATH)],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert proc.returncode == 0, proc.stderr


def test_canonical_default_in_sync_with_pydm():
    """PyDMConverter vendors a byte copy of canopy-default; found by path, without importing the converter."""
    spec = importlib.util.find_spec("pydmconverter")
    if spec is None or spec.origin is None:
        pytest.skip("PyDMConverter is not installed (no convert extra)")
    vendored = Path(spec.origin).parent / "ir" / "data" / "schemes" / "canopy-default.json"
    assert vendored.is_file(), f"PyDMConverter carries no canopy-default copy at {vendored}"
    assert DEFAULT_SCHEME_PATH.read_bytes() == vendored.read_bytes()
