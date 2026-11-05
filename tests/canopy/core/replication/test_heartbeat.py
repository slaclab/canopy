"""``heartbeat`` on a mirror: what it records on the primary, with both sides stubbed."""

from datetime import UTC, datetime

import pytest

from canopy.core.replication import subscriber
from canopy.core.replication.config import ReplicationConfig


class _FakePrimary:
    """Records the heartbeat row ``heartbeat`` writes."""

    def __init__(self):
        self.row = None

    async def fetchval(self, _sql, *args):
        self.row = args
        return datetime(2026, 11, 1, tzinfo=UTC)

    async def close(self):
        pass


@pytest.fixture
def primary(monkeypatch):
    fake = _FakePrimary()

    async def connect(_config, _command):
        return fake, "postgresql://beat:secret@primary.example.org/canopy"

    monkeypatch.setattr(subscriber, "_connect_primary", connect)
    return fake


def _status(monkeypatch, *, enabled=True, not_ready=None):
    async def status(_conn, *, instance, registry):
        return {
            "subscription": {
                "exists": True,
                "name": f"canopy_history_{instance}",
                "enabled": enabled,
                "applied_lsn": "0/16B3748",
                "tables_not_ready": not_ready or {},
            }
        }

    monkeypatch.setattr(subscriber, "subscriber_status", status)


async def _beat(drift_ok=True) -> dict:
    return await subscriber.heartbeat(
        None,
        role="mirror",
        instance="site_a",
        config=ReplicationConfig(),
        backup_ok=True,
        wal_archive_ok=True,
        drift_ok=drift_ok,
    )


async def test_a_ready_mirror_records_its_checks(monkeypatch, primary):
    _status(monkeypatch)

    row = await _beat()

    assert row["drift_ok"] is True
    assert primary.row == ("canopy_history_site_a", True, True, True, "0/16B3748")


@pytest.mark.parametrize(
    ("state", "reason"),
    [
        ({"not_ready": {"squirrel.snapshot": "copying"}}, "Tables still copying (squirrel.snapshot: copying)"),
        ({"enabled": False}, "Subscription canopy_history_site_a is disabled"),
    ],
    ids=["copying", "disabled"],
)
async def test_unchecked_tables_record_the_drift_check_failed(monkeypatch, primary, caplog, state, reason):
    _status(monkeypatch, **state)

    row = await _beat()

    # --drift-ok can't vouch for tables nobody compared, and pruning rows a copy hasn't
    # read yet would keep them from history.
    assert row["drift_ok"] is False and primary.row[3] is False
    assert reason in caplog.text
