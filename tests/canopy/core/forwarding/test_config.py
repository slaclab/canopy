"""``CANOPY_FORWARD__*`` and each arm's forward settings (``CANOPY_ARMS__<arm>__FORWARD_URL``, ``__SERVICE_KEY``)."""

import pytest
from pydantic import ValidationError

from canopy.core.arms import ArmsConfig
from canopy.core.forwarding.config import ForwardConfig


def test_a_blank_ca_bundle_is_unset(monkeypatch):
    # Helm and compose render an unset value as an empty string.
    monkeypatch.setenv("CANOPY_FORWARD__CA_BUNDLE", "  ")

    assert ForwardConfig().ca_bundle is None


def test_the_shared_settings(monkeypatch):
    monkeypatch.setenv("CANOPY_FORWARD__WAIT_S", "0.5")
    monkeypatch.setenv("CANOPY_FORWARD__TIMEOUT_S", "10")

    config = ForwardConfig()

    assert (config.wait_s, config.timeout_s, config.allow_insecure_http) == (0.5, 10.0, False)


def test_an_arm_forwards_with_both_settings(monkeypatch):
    monkeypatch.setenv("CANOPY_ARMS__ARM1__FORWARD_URL", "https://arm1.example.org/")
    monkeypatch.setenv("CANOPY_ARMS__ARM1__SERVICE_KEY", "service-token")
    monkeypatch.setenv("CANOPY_ARMS__ARM2__PUBLISHER_DSN", "host=arm2.example.org")

    arms = ArmsConfig().arms

    assert arms["arm1"].forwards
    assert arms["arm1"].forward_url == "https://arm1.example.org"
    assert arms["arm1"].service_key.get_secret_value() == "service-token"
    assert not arms["arm2"].forwards


@pytest.mark.parametrize("name", ["FORWARD_URL", "SERVICE_KEY"])
def test_one_without_the_other_is_refused(monkeypatch, name):
    monkeypatch.setenv(f"CANOPY_ARMS__ARM1__{name}", "https://arm1.example.org")

    with pytest.raises(ValidationError, match="set together or not at all"):
        ArmsConfig()


def test_blank_forward_settings_are_unset(monkeypatch):
    monkeypatch.setenv("CANOPY_ARMS__ARM1__FORWARD_URL", " ")
    monkeypatch.setenv("CANOPY_ARMS__ARM1__SERVICE_KEY", "")
    monkeypatch.setenv("CANOPY_ARMS__ARM1__PUBLISHER_DSN", "host=arm1.example.org")

    assert not ArmsConfig().arms["arm1"].forwards
