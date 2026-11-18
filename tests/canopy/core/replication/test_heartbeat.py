"""``heartbeat`` on a head: what it records on one arm, with both sides stubbed."""

from datetime import UTC, datetime

import pytest
from pydantic import SecretStr

from canopy.core.arms import Arm
from canopy.core.replication import subscriber
from canopy.core.replication.sql import Refused

ARMS = {
    "arm1": Arm(heartbeat_dsn=SecretStr("postgresql://beat:secret@arm1.example.org/canopy")),
    "arm2": Arm(heartbeat_dsn=SecretStr("postgresql://beat:secret@arm2.example.org/canopy")),
}


class _FakeArm:
    """Records the heartbeat row ``heartbeat`` writes."""

    def __init__(self):
        self.row = None

    async def fetchval(self, _sql, *args):
        self.row = args
        return datetime(2026, 11, 1, tzinfo=UTC)

    async def close(self):
        pass


@pytest.fixture
def reached(monkeypatch):
    """The fake arm connection, and which arm and DSN ``heartbeat`` connected to."""
    fake = _FakeArm()
    fake.connected = []

    async def connect(name, arm, _command):
        fake.connected.append((name, arm.heartbeat_dsn.get_secret_value()))
        return fake, arm.heartbeat_dsn.get_secret_value()

    monkeypatch.setattr(subscriber, "_connect_arm", connect)
    return fake


def _status(monkeypatch, **per_arm):
    """Stub ``subscriber_status``: each arm ready and enabled unless ``per_arm`` says otherwise."""

    async def status(_conn, *, arms, only, registry):
        state = {"enabled": True, "not_ready": {}, **per_arm.get(only, {})}
        return {
            "subscriptions": [
                {
                    "arm": only,
                    "exists": True,
                    "name": f"canopy_history_{only}",
                    "enabled": state["enabled"],
                    "applied_lsn": "0/16B3748",
                    "tables_not_ready": state["not_ready"],
                }
            ]
        }

    monkeypatch.setattr(subscriber, "subscriber_status", status)


async def _beat(arm="arm1", drift_ok=True) -> dict:
    return await subscriber.heartbeat(
        None, role="mirror", arm=arm, arms=ARMS, backup_ok=True, wal_archive_ok=True, drift_ok=drift_ok
    )


async def test_a_ready_arm_gets_the_heads_checks_for_it(monkeypatch, reached):
    _status(monkeypatch)

    row = await _beat("arm2")

    assert row["arm"] == "arm2" and row["drift_ok"] is True
    assert reached.connected == [("arm2", "postgresql://beat:secret@arm2.example.org/canopy")]
    # Keyed by the head's slot on that arm, which the arm's prune gate looks up.
    assert reached.row == ("canopy_history_arm2", True, True, True, "0/16B3748")


@pytest.mark.parametrize(
    ("state", "reason"),
    [
        (
            {"not_ready": {"squirrel.snapshot": "copying"}},
            "Tables still copying from arm arm1 (squirrel.snapshot: copying)",
        ),
        ({"enabled": False}, "Subscription canopy_history_arm1 is disabled"),
    ],
    ids=["copying", "disabled"],
)
async def test_unchecked_tables_record_the_drift_check_failed(monkeypatch, reached, caplog, state, reason):
    _status(monkeypatch, arm1=state)

    row = await _beat("arm1")

    # --drift-ok can't vouch for tables nobody compared, and pruning rows a copy hasn't
    # read yet would keep them from history.
    assert row["drift_ok"] is False and reached.row[3] is False
    assert reason in caplog.text


async def test_one_arms_copy_doesnt_fail_another_arms_drift_check(monkeypatch, reached):
    _status(monkeypatch, arm1={"not_ready": {"squirrel.snapshot": "copying"}})

    row = await _beat("arm2")

    assert row["drift_ok"] is True and reached.row[0] == "canopy_history_arm2"


async def test_an_unknown_arm_is_refused_before_connecting(reached):
    with pytest.raises(Refused, match="no arm named 'arm9'"):
        await _beat("arm9")
    assert reached.connected == []
