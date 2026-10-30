"""The prune gate's judgement, from status shaped like ``publisher_status`` output. No database.

``test_split_e2e.py`` reads the same facts from a real primary and mirror.
"""

import pytest

from canopy.squirrel.prune import PRUNED_TABLES
from canopy.core.replication.gate import GateResult, evaluate_gate
from canopy.core.replication.config import PruneConfig
from canopy.core.replication.registry import REGISTRY, TableClass
from canopy.core.replication.publisher import walsender_publications

SLOT = "canopy_history_site_a"
TABLES = list(PRUNED_TABLES)
CONFIG = PruneConfig(max_lag_s=60, heartbeat_max_age_h=3)


def _status(**slot) -> dict:
    """A split primary whose mirror streams canopy_ref and canopy_ts, caught up; ``slot`` overrides."""
    return {
        "layout": "split",
        "publications": [
            {"name": "canopy_ref", "exists": True, "tables": ["squirrel.pv", "squirrel.pv_tag"]},
            {"name": "canopy_ts", "exists": True, "tables": TABLES},
        ],
        "slots": [
            {
                "slot_name": SLOT,
                "active": True,
                "wal_status": "reserved",
                "walsender_publications": ["canopy_ref", "canopy_ts"],
                "walsender_flush_lag_s": None,
                "walsender_reply_age_s": 4.0,
                **slot,
            }
        ],
    }


HEARTBEAT = {"backup_ok": True, "wal_archive_ok": True, "drift_ok": True, "applied_lsn": "0/16B3748", "age_h": 0.5}


def _judge(*, status=None, delete_publications=None, heartbeat=HEARTBEAT) -> GateResult:
    return evaluate_gate(
        slot_name=SLOT,
        tables=TABLES,
        status=status or _status(),
        delete_publications=delete_publications or {},
        heartbeat=heartbeat,
        config=CONFIG,
    )


def test_a_switched_caught_up_mirror_with_good_checks_passes():
    result = _judge()
    assert result.ok and result.failures == {}


@pytest.mark.parametrize(
    ("change", "failed"),
    [
        ({"delete_publications": {"squirrel.snapshot": ["canopy_history"]}}, "no_delete_publication"),
        ({"status": {**_status(), "slots": []}}, "slot_active"),
        ({"status": _status(active=False)}, "slot_active"),
        ({"status": _status(walsender_publications=["canopy_history"])}, "streams_split"),
        ({"status": _status(walsender_publications=None)}, "streams_split"),
        ({"status": _status(wal_status="extended")}, "wal_status_reserved"),
        ({"status": _status(walsender_flush_lag_s=75.0)}, "lag"),
        ({"status": _status(walsender_reply_age_s=90.0)}, "lag"),
        ({"status": _status(walsender_reply_age_s=None)}, "lag"),
        ({"heartbeat": None}, "heartbeat_fresh"),
        ({"heartbeat": {**HEARTBEAT, "age_h": 3.5}}, "heartbeat_fresh"),
        ({"heartbeat": {**HEARTBEAT, "drift_ok": False}}, "heartbeat_ok"),
    ],
    ids=[
        "deletes-published",
        "no-slot",
        "slot-inactive",
        "still-on-canopy_history",
        "streams-unknown",
        "wal-at-risk",
        "flush-lag",
        "stale-reply",
        "no-reply",
        "no-heartbeat",
        "old-heartbeat",
        "drift-failed",
    ],
)
def test_each_failed_check_is_named(change, failed):
    result = _judge(**change)

    assert not result.ok
    assert list(result.failures) == [failed], result.failures


def test_the_reasons_say_what_to_fix():
    failures = _judge(
        status=_status(walsender_publications=None),
        delete_publications={"squirrel.snapshot": ["canopy_history"]},
        heartbeat={**HEARTBEAT, "backup_ok": False, "drift_ok": False},
    ).failures

    assert failures["no_delete_publication"] == "squirrel.snapshot is published with deletes by canopy_history"
    assert "pg_read_all_stats" in failures["streams_split"]
    assert failures["heartbeat_ok"] == "the mirror reported failed checks: backup_ok, drift_ok"


def test_a_publication_that_does_not_exist_carries_nothing():
    status = _status()
    status["publications"][1]["exists"] = False

    assert list(_judge(status=status).failures) == ["streams_split"]


def test_an_inactive_slot_is_still_checked_for_wal_at_risk():
    failures = _judge(status=_status(active=False, wal_status="lost")).failures

    assert set(failures) == {"slot_active", "wal_status_reserved"}


@pytest.mark.parametrize(
    ("command", "publications"),
    [
        (
            "START_REPLICATION SLOT \"s\" LOGICAL 0/0 (proto_version '4', origin 'any', "
            'publication_names \'"canopy_ref","canopy_ts"\')',
            ["canopy_ref", "canopy_ts"],
        ),
        (
            "START_REPLICATION SLOT \"s\" LOGICAL 0/0 (proto_version '4', publication_names '\"canopy_history\"')",
            ["canopy_history"],
        ),
        ("<insufficient privilege>", None),
        (None, None),
        ('START_REPLICATION SLOT "s" PHYSICAL 0/0', None),
    ],
    ids=["split", "single", "hidden", "idle", "physical"],
)
def test_walsender_publications_are_read_from_its_command(command, publications):
    assert walsender_publications(command) == publications


def test_the_pruned_tables_are_time_series():
    # Their split-layout publication carries no deletes; a reference table's deletes would reach history.
    assert all(REGISTRY.tables[table] is TableClass.TIME_SERIES for table in PRUNED_TABLES)
