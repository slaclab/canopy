"""``canopy.core.arms``: the head's arms setting and the subscription names it implies."""

import pytest
from pydantic import ValidationError

from canopy.core.arms import Arm, ArmsConfig, get_arms_config, subscription_name


def test_no_arms_by_default():
    assert get_arms_config().arms == {}


def test_each_arm_is_its_own_variables(monkeypatch):
    monkeypatch.setenv("CANOPY_ARMS__ARM1__PUBLISHER_DSN", "host=arm1.example.org password=s3cret-1")
    monkeypatch.setenv("CANOPY_ARMS__ARM2__PUBLISHER_DSN", "host=arm2.example.org password=s3cret-2")

    arms = get_arms_config().arms

    # Variable names are case-insensitive, so the name comes back as the arm's lowercase CANOPY_INSTANCE.
    assert sorted(arms) == ["arm1", "arm2"]
    assert arms["arm1"].publisher_dsn.get_secret_value() == "host=arm1.example.org password=s3cret-1"
    assert "s3cret" not in repr(get_arms_config())


def test_arms_as_json(monkeypatch):
    monkeypatch.setenv("CANOPY_ARMS", '{"arm1": {"publisher_dsn": "host=arm1.example.org"}, "arm2": {}}')

    arms = get_arms_config().arms

    assert sorted(arms) == ["arm1", "arm2"]
    assert arms["arm2"] == Arm()


@pytest.mark.parametrize("name", ["", "Arm1", "arm-1", "arm 1", "x" * 49])
def test_names_must_suit_a_subscription_name(name):
    with pytest.raises(ValidationError, match="arm names must match"):
        ArmsConfig(arms={name: Arm()})


def test_the_longest_name_fits_postgres():
    name = "x" * 48
    ArmsConfig(arms={name: Arm()})
    assert len(subscription_name(name).encode()) == 63


def test_subscription_name():
    assert subscription_name("arm1") == "canopy_history_arm1"
