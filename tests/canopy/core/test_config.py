"""Core ``Settings``: the log level default and its environment override, and the history fields."""

import pytest
from pydantic import ValidationError

from canopy.core.config import Settings, get_settings


def test_log_level_defaults_to_info(monkeypatch):
    monkeypatch.delenv("CANOPY_LOG_LEVEL", raising=False)
    assert Settings().log_level == "INFO"


def test_log_level_can_be_set_explicitly():
    assert Settings(log_level="WARNING").log_level == "WARNING"


def test_log_level_is_read_from_the_environment(monkeypatch):
    monkeypatch.setenv("CANOPY_LOG_LEVEL", "DEBUG")
    get_settings.cache_clear()
    try:
        assert get_settings().log_level == "DEBUG"
    finally:
        get_settings.cache_clear()


@pytest.mark.parametrize("value", ["0", "-3"])
def test_window_days_must_be_positive(monkeypatch, value):
    # A non-positive window would put prune's cutoff at or after now.
    monkeypatch.setenv("CANOPY_WINDOW_DAYS", value)

    with pytest.raises(ValidationError):
        Settings()


def test_window_days_is_read_from_the_environment(monkeypatch):
    monkeypatch.setenv("CANOPY_WINDOW_DAYS", "30")

    assert Settings().window_days == 30


@pytest.mark.parametrize("name", ["CANOPY_HISTORY_URL", "CANOPY_WINDOW_DAYS"])
def test_a_blank_history_field_is_unset(monkeypatch, name):
    # How Helm and compose render a value nobody set.
    monkeypatch.setenv(name, "")

    settings = Settings()

    assert (settings.history_url, settings.window_days) == (None, None)


def test_instance_is_required(monkeypatch):
    monkeypatch.delenv("CANOPY_INSTANCE", raising=False)

    with pytest.raises(ValidationError, match="instance"):
        Settings(_env_file=None)


@pytest.mark.parametrize("name", ["arm1", "head", "site_a", "a" * 48])
def test_instance_names_that_fit_a_subscription_name(monkeypatch, name):
    monkeypatch.setenv("CANOPY_INSTANCE", name)

    assert Settings(_env_file=None).instance == name


@pytest.mark.parametrize("name", ["", "Site-A", "site-a", "a" * 49, "arm 1"])
def test_instance_names_that_are_refused(monkeypatch, name):
    # A blank value is refused too: the name has no default.
    monkeypatch.setenv("CANOPY_INSTANCE", name)

    with pytest.raises(ValidationError, match="instance"):
        Settings(_env_file=None)
