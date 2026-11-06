"""The replication gauges: what a scrape shows for each side's status, and a refresh that fails."""

import logging
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.engine import make_url

from canopy.core.config import get_settings
from canopy.core.replication.metrics import ReplicationCollector, lsn_bytes

from .conftest import DB_DSN, metric_samples


def _slot(**overrides) -> dict:
    slot = {
        "slot_name": "canopy_history_arm1",
        "kind": "subscription",
        "active": True,
        "wal_status": "reserved",
        "safe_wal_size": 1_000,
        "retained_bytes": 300,
        "lag_bytes": 40,
    }
    return slot | overrides


def _publisher(*slots: dict, cap: int | None = 2_000) -> dict:
    return {"side": "publisher", "slots": list(slots), "max_slot_wal_keep_bytes": cap}


def _subscription(arm: str = "arm1", **overrides) -> dict:
    subscription = {
        "arm": arm,
        "name": f"canopy_history_{arm}",
        "exists": True,
        "enabled": True,
        "worker": {"pid": 4242, "last_msg_receipt_time": (datetime.now(UTC) - timedelta(seconds=12)).isoformat()},
        "applied_lsn": "1/2",
        "apply_error_count": 3,
        "sync_error_count": 0,
        "tables_not_ready": {"squirrel.pv": "copying"},
    }
    return subscription | overrides


def _head(*subscriptions: dict, unexpected: tuple[str, ...] = ()) -> dict:
    return {"side": "subscriber", "subscriptions": list(subscriptions), "unexpected": list(unexpected)}


def _labels(**labels) -> frozenset:
    return frozenset(labels.items())


SLOT = {"slot": "canopy_history_arm1", "kind": "subscription"}
ARM1, ARM2 = {"arm": "arm1"}, {"arm": "arm2"}


def test_a_primary_reports_each_slot():
    samples = metric_samples(
        _publisher(_slot(), _slot(slot_name="pg_16390_sync_16384", kind="tablesync", active=False))
    )

    assert samples[("canopy_replication_status_up", _labels())] == 1
    assert samples[("canopy_replication_slot_active", _labels(**SLOT))] == 1
    assert samples[("canopy_replication_slot_active", _labels(slot="pg_16390_sync_16384", kind="tablesync"))] == 0
    assert samples[("canopy_replication_slot_retained_bytes", _labels(**SLOT))] == 300
    assert samples[("canopy_replication_slot_lag_bytes", _labels(**SLOT))] == 40
    assert samples[("canopy_replication_slot_safe_wal_bytes", _labels(**SLOT))] == 1_000
    assert samples[("canopy_replication_max_slot_wal_keep_bytes", _labels())] == 2_000
    states = {
        state: samples[("canopy_replication_slot_wal_status", _labels(**SLOT, wal_status=state))]
        for state in ("reserved", "extended", "unreserved", "lost")
    }
    assert states == {"reserved": 1, "extended": 0, "unreserved": 0, "lost": 0}


def test_unset_slot_fields_and_no_wal_cap_leave_their_series_out():
    samples = metric_samples(_publisher(_slot(wal_status=None, safe_wal_size=None), cap=None))

    names = {name for name, _ in samples}
    assert "canopy_replication_slot_wal_status" not in names
    assert "canopy_replication_slot_safe_wal_bytes" not in names
    assert "canopy_replication_max_slot_wal_keep_bytes" not in names
    assert samples[("canopy_replication_slot_active", _labels(**SLOT))] == 1


def test_a_primary_without_slots_reports_only_that_it_read_them():
    assert metric_samples(_publisher(cap=None)) == {("canopy_replication_status_up", _labels()): 1}


def test_a_head_reports_each_arm():
    samples = metric_samples(
        _head(_subscription(), _subscription("arm2", applied_lsn="0/10", apply_error_count=0, tables_not_ready={}))
    )

    for name in ("exists", "enabled", "worker_up"):
        assert samples[(f"canopy_replication_subscription_{name}", _labels(**ARM1))] == 1
        assert samples[(f"canopy_replication_subscription_{name}", _labels(**ARM2))] == 1
    assert 12 <= samples[("canopy_replication_subscription_receive_age_seconds", _labels(**ARM1))] < 60
    assert samples[("canopy_replication_subscription_apply_errors_total", _labels(**ARM1))] == 3
    assert samples[("canopy_replication_subscription_apply_errors_total", _labels(**ARM2))] == 0
    assert samples[("canopy_replication_subscription_sync_errors_total", _labels(**ARM1))] == 0
    assert samples[("canopy_replication_subscription_tables_not_ready", _labels(**ARM1))] == 1
    assert samples[("canopy_replication_subscription_tables_not_ready", _labels(**ARM2))] == 0
    assert samples[("canopy_replication_subscription_applied_lsn_bytes", _labels(**ARM1))] == (1 << 32) + 2
    assert samples[("canopy_replication_subscription_applied_lsn_bytes", _labels(**ARM2))] == 0x10
    assert samples[("canopy_replication_subscriptions_unexpected", _labels())] == 0
    assert {
        dict(labels).get("arm") for name, labels in samples if name.startswith("canopy_replication_subscription_")
    } == {
        "arm1",
        "arm2",
    }


@pytest.mark.parametrize("worker", [None, {"pid": None, "last_msg_receipt_time": None}])
def test_a_stopped_worker_has_no_receive_age(worker):
    """A disabled subscription keeps its pg_stat_subscription row, with no pid; the other arm's is unaffected."""
    samples = metric_samples(_head(_subscription(), _subscription("arm2", worker=worker, applied_lsn=None)))

    assert samples[("canopy_replication_subscription_worker_up", _labels(**ARM1))] == 1
    assert samples[("canopy_replication_subscription_worker_up", _labels(**ARM2))] == 0
    assert ("canopy_replication_subscription_receive_age_seconds", _labels(**ARM1)) in samples
    assert ("canopy_replication_subscription_receive_age_seconds", _labels(**ARM2)) not in samples
    assert ("canopy_replication_subscription_applied_lsn_bytes", _labels(**ARM2)) not in samples


def test_an_arm_without_its_subscription_says_so():
    samples = metric_samples(_head({"arm": "arm1", "name": "canopy_history_arm1", "exists": False}))
    assert samples == {
        ("canopy_replication_status_up", _labels()): 1,
        ("canopy_replication_subscription_exists", _labels(**ARM1)): 0,
        ("canopy_replication_subscriptions_unexpected", _labels()): 0,
    }


def test_a_head_counts_the_subscriptions_no_arm_claims():
    samples = metric_samples(_head(unexpected=("canopy_history_gone", "canopy_history_old")))
    assert samples == {
        ("canopy_replication_status_up", _labels()): 1,
        ("canopy_replication_subscriptions_unexpected", _labels()): 2,
    }


def test_no_status_means_status_up_zero_and_nothing_else():
    assert metric_samples(None) == {("canopy_replication_status_up", _labels()): 0}


def test_lsn_bytes():
    assert lsn_bytes("0/0") == 0
    assert lsn_bytes("16/B374D848") == (0x16 << 32) + 0xB374D848


@pytest.fixture
def settings_env(monkeypatch):
    """Set ``CANOPY_*`` variables for the code under test, with the settings cache cleared around it."""

    def set_env(**values: str) -> None:
        for name, value in values.items():
            monkeypatch.setenv(name, value)
        get_settings.cache_clear()

    yield set_env
    get_settings.cache_clear()


async def test_a_failed_refresh_reports_status_down_and_warns_once(settings_env, caplog):
    settings_env(CANOPY_DATABASE_URL="postgresql+asyncpg://canopy:canopy@127.0.0.1:1/canopy")
    collector = ReplicationCollector()
    collector.status = {"side": "publisher", "slots": []}

    with caplog.at_level(logging.WARNING, logger="canopy.core.replication.metrics"):
        await collector.refresh()
        await collector.refresh()

    assert collector.status is None
    assert len([r for r in caplog.records if "unavailable" in r.getMessage()]) == 1


async def test_a_head_whose_arms_setting_is_unreadable_reports_status_down(db, settings_env):
    settings_env(CANOPY_DATABASE_URL=_app_url(DB_DSN), CANOPY_ROLE="mirror", CANOPY_ARMS="{not json")
    collector = ReplicationCollector()
    await collector.refresh()
    assert collector.status is None


async def test_refresh_reads_the_primary_side(db, settings_env, caplog):
    settings_env(CANOPY_DATABASE_URL=_app_url(DB_DSN), CANOPY_ROLE="primary")
    collector = ReplicationCollector()
    collector._failing = True  # as after an outage

    with caplog.at_level(logging.INFO, logger="canopy.core.replication.metrics"):
        await collector.refresh()

    assert collector.status["side"] == "publisher"
    assert collector.status["max_slot_wal_keep_bytes"] is None  # the default: no cap
    assert "available again" in caplog.text


async def test_refresh_reads_the_head_side(db, settings_env):
    settings_env(
        CANOPY_DATABASE_URL=_app_url(DB_DSN), CANOPY_ROLE="mirror", CANOPY_ARMS__ARM1__PUBLISHER_DSN="host=arm1"
    )
    collector = ReplicationCollector()
    await collector.refresh()
    assert collector.status == {
        "side": "subscriber",
        "subscriptions": [{"arm": "arm1", "name": "canopy_history_arm1", "exists": False}],
        "unexpected": [],
    }


def _app_url(dsn: str) -> str:
    return make_url(dsn).set(drivername="postgresql+asyncpg").render_as_string(hide_password=False)
