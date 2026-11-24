"""``canopy.core.arms``: the head's arms setting and the subscription names it implies."""

import traceback

import pytest
from pydantic import ValidationError

from canopy.core.arms import (
    Arm,
    ArmsConfig,
    ArmsConfigError,
    get_arms_config,
    subscription_name,
)


def test_no_arms_by_default():
    assert get_arms_config().arms == {}


def test_each_arm_is_its_own_variables(monkeypatch):
    monkeypatch.setenv("CANOPY_ARMS__ARM1__PUBLISHER_DSN", "postgresql://replicator:s3cret-1@arm1.example.org/canopy")
    monkeypatch.setenv("CANOPY_ARMS__ARM2__PUBLISHER_DSN", "postgresql://replicator:s3cret-2@arm2.example.org/canopy")

    arms = get_arms_config().arms

    # Variable names are case-insensitive, so the name comes back as the arm's lowercase CANOPY_INSTANCE.
    assert sorted(arms) == ["arm1", "arm2"]
    assert arms["arm1"].publisher_dsn.get_secret_value() == "postgresql://replicator:s3cret-1@arm1.example.org/canopy"
    assert "s3cret" not in repr(get_arms_config())


def test_arms_as_json(monkeypatch):
    monkeypatch.setenv("CANOPY_ARMS", '{"arm1": {"publisher_dsn": "postgresql://arm1.example.org/canopy"}, "arm2": {}}')

    arms = get_arms_config().arms

    assert sorted(arms) == ["arm1", "arm2"]
    assert arms["arm2"] == Arm()


@pytest.mark.parametrize("name", ["", "Arm1", "arm-1", "arm 1", "x" * 49])
def test_names_must_suit_a_subscription_name(name):
    with pytest.raises(ValidationError, match="arm names must match"):
        ArmsConfig(arms={name: Arm()})


@pytest.mark.parametrize(
    ("value", "says"),
    [
        # A bad name: pydantic's input is the whole setting, every arm's DSN with it.
        (
            '{"Bad-Name": {"publisher_dsn": "postgresql://r:s3cret@x/canopy"}}',
            "CANOPY_ARMS: Value error, arm names must match",
        ),
        # key=value, which the subscription would take but the CLI's own connection can't.
        (
            '{"arm1": {"publisher_dsn": "host=x password=s3cret"}}',
            "CANOPY_ARMS__ARM1__PUBLISHER_DSN: Value error, must be a postgresql:// URI",
        ),
        # An arm that isn't an object: the input is the DSN itself.
        ('{"arm1": "postgresql://r:s3cret@x/canopy"}', "CANOPY_ARMS__ARM1: Input should be a valid dictionary"),
        # Not JSON at all.
        ('{"arm1": {"publisher_dsn": "postgresql://r:s3cret@x/canopy"', 'error parsing value for field "arms"'),
    ],
)
def test_an_invalid_setting_is_reported_without_its_values(monkeypatch, value, says):
    monkeypatch.setenv("CANOPY_ARMS", value)

    with pytest.raises(ArmsConfigError) as caught:
        get_arms_config()

    assert says in str(caught.value)
    shown = {"str": str(caught.value), "repr": repr(caught.value)}
    shown["traceback"] = "".join(traceback.format_exception(caught.value))
    assert [how for how, text in shown.items() if "s3cret" in text] == []


@pytest.mark.parametrize(
    "dsn", ["postgresql://replicator@arm1.example.org/canopy", "postgres://arm1.example.org/canopy"]
)
def test_a_publisher_dsn_is_either_uri_scheme(dsn):
    assert Arm(publisher_dsn=dsn).publisher_dsn.get_secret_value() == dsn


def test_a_key_value_heartbeat_dsn_is_refused_without_its_password(monkeypatch):
    # The heartbeat and switch commands connect with asyncpg, which takes only the URI form.
    monkeypatch.setenv("CANOPY_ARMS__ARM1__HEARTBEAT_DSN", "host=x user=canopy_heartbeat password=s3cret-beat")

    with pytest.raises(ArmsConfigError) as caught:
        get_arms_config()

    assert "CANOPY_ARMS__ARM1__HEARTBEAT_DSN: Value error, must be a postgresql:// URI" in str(caught.value)
    shown = [str(caught.value), repr(caught.value), "".join(traceback.format_exception(caught.value))]
    assert [text for text in shown if "s3cret-beat" in text] == []


def test_the_longest_name_fits_postgres():
    name = "x" * 48
    ArmsConfig(arms={name: Arm()})
    assert len(subscription_name(name).encode()) == 63


def test_subscription_name():
    assert subscription_name("arm1") == "canopy_history_arm1"
