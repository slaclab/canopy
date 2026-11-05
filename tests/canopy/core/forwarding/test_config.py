"""``CANOPY_FORWARD__*`` parsing."""

import pytest
from pydantic import ValidationError

from canopy.core.forwarding.config import ForwardConfig


@pytest.mark.parametrize("name", ["PRIMARY_URL", "SERVICE_KEY", "PRIMARY_WINDOW_DAYS", "CA_BUNDLE"], ids=str.lower)
def test_a_blank_value_is_unset(monkeypatch, name):
    # Helm and compose render an unset value as an empty string.
    monkeypatch.setenv(f"CANOPY_FORWARD__{name}", "  ")

    assert getattr(ForwardConfig(), name.lower()) is None


def test_values_still_parse(monkeypatch):
    monkeypatch.setenv("CANOPY_FORWARD__PRIMARY_URL", "https://primary.example.org/")
    monkeypatch.setenv("CANOPY_FORWARD__SERVICE_KEY", "service-token")
    monkeypatch.setenv("CANOPY_FORWARD__PRIMARY_WINDOW_DAYS", "30")

    config = ForwardConfig()

    assert config.primary_url == "https://primary.example.org"
    assert config.service_key.get_secret_value() == "service-token"
    assert config.primary_window_days == 30


def test_the_window_is_at_least_a_day(monkeypatch):
    monkeypatch.setenv("CANOPY_FORWARD__PRIMARY_WINDOW_DAYS", "0")

    with pytest.raises(ValidationError):
        ForwardConfig()
