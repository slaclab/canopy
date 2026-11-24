"""``python -m canopy.core.color_scheme_cli check``: canopy's own scheme check (carried over from the converter's)."""

import sys
import json
import subprocess

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
