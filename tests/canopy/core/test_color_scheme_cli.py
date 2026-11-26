"""``canopy-scheme check``: canopy's own scheme check (carried over from the converter's)."""

import sys
import json
import tomllib
import subprocess
from pathlib import Path
from importlib.metadata import entry_points

import pytest

from canopy.core import color_scheme_cli
from canopy.core.color_scheme_flatten import DEFAULT_SCHEME_PATH


def _run(capsys, *argv: str) -> tuple[int, str]:
    code = color_scheme_cli.main(list(argv))
    return code, capsys.readouterr().out


def test_check_default_is_clean(capsys):
    code, out = _run(capsys, "check", str(DEFAULT_SCHEME_PATH))
    assert code == 0
    assert out.strip().endswith("ok (21 roles)")


def test_check_accepts_the_spec_format_keys(tmp_path, capsys):
    path = tmp_path / "s.json"
    path.write_text(json.dumps({"$schema": "x", "format": "canopy-color-scheme/1", "id": "x", "roles": {"B": "#123"}}))
    code, out = _run(capsys, "check", str(path))
    assert code == 0
    assert out.strip() == f"{path}: ok (22 roles)"


def test_check_reports_dangling_alias(tmp_path, capsys):
    path = tmp_path / "s.json"
    path.write_text(json.dumps({"id": "x", "roles": {"Text": {"light": "@Nope", "dark": "#ffffff"}}}))
    code, out = _run(capsys, "check", str(path))
    assert code == 1
    assert "error: roles/Text/light: alias @Nope (light) does not name a role" in out
    assert out.strip().endswith("1 error(s)")


def test_check_reports_role_without_dark(tmp_path, capsys):
    path = tmp_path / "s.json"
    path.write_text(json.dumps({"id": "x", "roles": {"Brand": {"light": "#123456"}}}))
    code, out = _run(capsys, "check", str(path))
    assert code == 1
    assert "no dark value: Brand" in out


def test_check_names_a_role_with_a_slash_whole(tmp_path, capsys):
    path = tmp_path / "s.json"
    path.write_text(json.dumps({"id": "x", "roles": {"Beam/On": {"light": "#123456"}}}))
    code, out = _run(capsys, "check", str(path))
    assert code == 1
    # Parsing the error path printed "On" here (the second-to-last segment of roles/Beam/On/dark).
    assert [line for line in out.splitlines() if line.startswith("no dark value:")] == ["no dark value: Beam/On"]


def test_check_unreadable_file_fails(tmp_path, capsys):
    code, out = _run(capsys, "check", str(tmp_path / "missing.json"))
    assert code == 1
    assert out.startswith("error: <file>: cannot read ")


def test_a_subcommand_is_required(capsys):
    with pytest.raises(SystemExit) as exc:
        color_scheme_cli.main([])
    assert exc.value.code == 2


def test_runs_as_a_module(tmp_path):
    path = tmp_path / "s.json"
    path.write_text(json.dumps({"id": "x", "roles": {"OK": "@OK"}}))
    proc = subprocess.run(
        [sys.executable, "-m", "canopy.core.color_scheme_cli", "check", str(path)],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert proc.returncode == 1
    assert "alias cycle in light: OK -> OK" in proc.stdout


def test_canopy_scheme_is_a_console_script_of_canopy_only():
    """pyproject declares it, and no other installed distribution (the converter's old name) claims it."""
    pyproject = tomllib.loads((Path(__file__).resolve().parents[3] / "pyproject.toml").read_text(encoding="utf-8"))
    assert pyproject["project"]["scripts"]["canopy-scheme"] == "canopy.core.color_scheme_cli:main"
    (installed,) = entry_points(group="console_scripts", name="canopy-scheme")
    assert installed.dist is not None and installed.dist.name == "canopy"
    assert installed.load() is color_scheme_cli.main
