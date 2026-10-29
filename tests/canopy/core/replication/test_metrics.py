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
        "slot_name": "canopy_history_site_a",
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


def _subscriber(**overrides) -> dict:
    subscription = {
        "name": "canopy_history_site_a",
        "exists": True,
        "enabled": True,
        "worker": {"pid": 4242, "last_msg_receipt_time": (datetime.now(UTC) - timedelta(seconds=12)).isoformat()},
        "applied_lsn": "1/2",
        "apply_error_count": 3,
        "sync_error_count": 0,
        "tables_not_ready": {"squirrel.pv": "copying"},
    }
    return {"side": "subscriber", "subscription": subscription | overrides}


def _labels(**labels) -> frozenset:
    return frozenset(labels.items())


SLOT = {"slot": "canopy_history_site_a", "kind": "subscription"}
SUB = {"subscription": "canopy_history_site_a"}


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


def test_a_mirror_reports_its_subscription():
    samples = metric_samples(_subscriber())

    assert samples[("canopy_replication_subscription_exists", _labels(**SUB))] == 1
    assert samples[("canopy_replication_subscription_enabled", _labels(**SUB))] == 1
    assert samples[("canopy_replication_subscription_worker_up", _labels(**SUB))] == 1
    assert 12 <= samples[("canopy_replication_subscription_receive_age_seconds", _labels(**SUB))] < 60
    assert samples[("canopy_replication_subscription_apply_errors_total", _labels(**SUB))] == 3
    assert samples[("canopy_replication_subscription_sync_errors_total", _labels(**SUB))] == 0
    assert samples[("canopy_replication_subscription_tables_not_ready", _labels(**SUB))] == 1
    assert samples[("canopy_replication_subscription_applied_lsn_bytes", _labels(**SUB))] == (1 << 32) + 2


@pytest.mark.parametrize("worker", [None, {"pid": None, "last_msg_receipt_time": None}])
def test_a_stopped_worker_has_no_receive_age(worker):
    """A disabled subscription keeps its pg_stat_subscription row, with no pid."""
    samples = metric_samples(_subscriber(worker=worker, applied_lsn=None))

    assert samples[("canopy_replication_subscription_worker_up", _labels(**SUB))] == 0
    names = {name for name, _ in samples}
    assert "canopy_replication_subscription_receive_age_seconds" not in names
    assert "canopy_replication_subscription_applied_lsn_bytes" not in names


def test_a_mirror_without_its_subscription_says_so():
    samples = metric_samples({"side": "subscriber", "subscription": {"name": "canopy_history_site_a", "exists": False}})
    assert samples == {
        ("canopy_replication_status_up", _labels()): 1,
        ("canopy_replication_subscription_exists", _labels(**SUB)): 0,
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


async def test_a_mirror_without_an_instance_name_reports_status_down(db, settings_env):
    settings_env(CANOPY_DATABASE_URL=_app_url(DB_DSN), CANOPY_ROLE="mirror")
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


async def test_refresh_reads_the_mirror_side(db, settings_env):
    settings_env(CANOPY_DATABASE_URL=_app_url(DB_DSN), CANOPY_ROLE="mirror", CANOPY_INSTANCE="site_a")
    collector = ReplicationCollector()
    await collector.refresh()
    assert collector.status == {
        "side": "subscriber",
        "subscription": {"name": "canopy_history_site_a", "exists": False},
    }


def _app_url(dsn: str) -> str:
    return make_url(dsn).set(drivername="postgresql+asyncpg").render_as_string(hide_password=False)
