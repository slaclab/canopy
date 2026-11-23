"""Unit tests for the pure ConversionService (no HTTP layer)."""

import logging
import tempfile
from pathlib import Path
from xml.etree.ElementTree import ParseError

import pytest

from canopy.converter.errors import ConversionFailed, UnsupportedInput
from canopy.converter.services import conversion_service as cs
from canopy.converter.services.conversion_service import ConversionService

FIXTURES = Path(__file__).parent / "fixtures"


def _edl_bytes() -> bytes:
    return (FIXTURES / "min.edl").read_bytes()


def _edl_unknown_bytes() -> bytes:
    return (FIXTURES / "unknown_class.edl").read_bytes()


def test_filename_names_the_screen():
    result = ConversionService().convert(_edl_bytes(), kind="edl", filename="sdr_fb.edl")
    assert result.screen["id"] == "sdr_fb"
    assert result.screen["metadata"]["title"] == "sdr_fb"


def test_filename_directories_are_ignored():
    """Archive members carry directories; only the basename names the screen."""
    result = ConversionService().convert(_edl_bytes(), kind="edl", filename="misc/sub/sdr_fb.edl")
    assert result.screen["id"] == "sdr_fb"


def test_convert_edl_bytes_produces_validated_ir():
    result = ConversionService().convert(_edl_bytes(), kind="edl")
    assert result.screen["schemaVersion"] == "1.0"
    assert result.screen["id"]
    assert result.stats.widget_count >= 1


def test_edl_classes_are_fully_supported():
    """Every class in min.edl maps to a registry widget: no unknowns, no placeholders.

    min.edl's activeXTextDspClass has no ``editable``, so (as in EDM) it is a read-only
    pv-label rather than a pv-text-input.
    """
    result = ConversionService().convert(_edl_bytes(), kind="edl")
    types = {c["type"] for c in result.screen["root"]["children"]}
    assert types == {"text-label", "pv-label", "pv-button", "rectangle"}
    assert result.unknown_widgets == []
    assert not any("placeholder" in w for w in result.warnings)


def test_unknown_edl_class_is_grouped_by_original_class():
    """An unsupported EDM class is detected, rendered as a placeholder, and grouped.

    Keeps positive coverage on the unknown-widget branch of _summarize: every other
    test asserts unknown_widgets == [], so without this a regression that stopped
    detecting unknowns (or stopped grouping them) would pass unnoticed.
    """
    result = ConversionService().convert(_edl_unknown_bytes(), kind="edl")
    types = {c["type"] for c in result.screen["root"]["children"]}
    assert "unknown-widget" in types
    assert [(u.original_class, u.count) for u in result.unknown_widgets] == [("activeFooBarClass", 1)]
    assert any("placeholder" in w for w in result.warnings)


def test_bad_kind_raises_unsupported_input(monkeypatch):
    """An unknown kind is a 415, rejected before the converter is called."""

    def _not_called(*_args, **_kwargs):
        raise AssertionError("convert_bytes called for an unknown kind")

    monkeypatch.setattr(cs, "convert_bytes", _not_called)
    with pytest.raises(UnsupportedInput):
        ConversionService().convert(b"whatever", kind="bogus")  # type: ignore[arg-type]


@pytest.mark.parametrize("exc", [KeyError("widget-type"), TypeError("'NoneType' object is not subscriptable")])
def test_unexpected_failure_maps_to_conversion_failed_and_logs(monkeypatch, caplog, exc):
    """A failure on a known kind that is not a parse error is a 422 (ConversionFailed) and logs its traceback.

    The catch is intentionally broad (pydmconverter uses KeyError for both bad input
    and bugs), so an unexpected error must still leave a traceback in the log.
    """

    def _boom(*_args, **_kwargs):
        raise exc

    monkeypatch.setattr(cs, "convert_bytes", _boom)
    with caplog.at_level(logging.ERROR, logger=cs.logger.name):
        with pytest.raises(ConversionFailed):
            ConversionService().convert(b"data", kind="edl")
    assert any(r.levelno >= logging.ERROR and r.exc_info for r in caplog.records)


def test_search_paths_reach_convert_bytes_for_edl_only(monkeypatch):
    """Configured EDMDATAFILES-style dirs go to convert_bytes for .edl, not .ui."""
    seen = {}

    def _fake_convert_bytes(data, **kwargs):
        seen[kwargs["kind"]] = kwargs.get("search_paths")
        raise ValueError("stop here")

    monkeypatch.setattr(cs, "convert_bytes", _fake_convert_bytes)
    service = ConversionService(search_paths=[Path("/displays/lcls"), "/displays/pps"])
    for kind in ("edl", "ui"):
        # A ValueError on a known kind is a conversion failure (422).
        with pytest.raises(ConversionFailed):
            service.convert(b"x", kind=kind)
    assert seen == {"edl": [str(Path("/displays/lcls")), "/displays/pps"], "ui": None}


def test_symbol_file_on_search_path_is_found(tmp_path):
    """A symbol file that only exists on a search path renders instead of being dropped."""
    (tmp_path / "sym.edl").write_bytes((FIXTURES / "symbol_state.edl").read_bytes())
    screen_edl = (
        b"4 0 1\nbeginScreenProperties\nmajor 4\nminor 0\nrelease 1\nx 0\ny 0\nw 200\nh 100\n"
        b"endScreenProperties\n\n"
        b"# (Symbol)\nobject activeSymbolClass\nbeginObjectProperties\nmajor 4\nminor 0\nrelease 0\n"
        b'x 10\ny 10\nw 100\nh 50\nfile "sym.edl"\nnumPvs 1\ncontrolPvs {\n  0 "TEST:STATE"\n}\nnumStates 1\nminValues {\n  0 0\n}\n'
        b"maxValues {\n  0 10\n}\nendObjectProperties\n"
    )
    without = ConversionService().convert(screen_edl, kind="edl", filename="host.edl")
    with_paths = ConversionService(search_paths=[tmp_path]).convert(screen_edl, kind="edl", filename="host.edl")
    assert any("symbol file" in w.lower() for w in without.warnings)
    assert not any("symbol file" in w.lower() for w in with_paths.warnings)
    assert with_paths.stats.widget_count > without.stats.widget_count


@pytest.mark.parametrize(
    "exc", [ValueError("invalid literal for int() with base 10: 'abc'"), ParseError("syntax error: line 1, column 0")]
)
def test_malformed_input_maps_to_conversion_failed_and_warns(monkeypatch, caplog, exc):
    """A parse error on a known kind is a 422 logged as one warning line, not as a server fault."""

    def _boom(*_args, **_kwargs):
        raise exc

    monkeypatch.setattr(cs, "convert_bytes", _boom)
    with caplog.at_level(logging.WARNING, logger=cs.logger.name):
        with pytest.raises(ConversionFailed):
            ConversionService().convert(b"data", kind="ui")
    records = [r for r in caplog.records if r.name == cs.logger.name]
    assert [r.levelno for r in records] == [logging.WARNING]
    assert not records[0].exc_info


def test_failure_detail_names_the_upload_not_the_staging_dir():
    """Parser errors quote the staged temp path; the client is shown only the upload's name."""
    with pytest.raises(ConversionFailed) as info:
        ConversionService().convert(b'<ui version="4.0"></ui>', kind="ui", filename="bad.ui")
    detail = info.value.detail
    assert "bad.ui: no root <widget> element" in detail
    assert "pydmconv-" not in detail
    assert tempfile.gettempdir() not in detail
