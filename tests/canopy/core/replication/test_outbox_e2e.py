"""A head's snapshot outbox with two arms, one of them cut off (Postgres 16, the ``octopus`` fixture).

Each arm's HTTP side is played by ``FakeArms``: it runs the arm's real captured import
against that arm's database, as a forwarded request, the way the arm's API would, and
then waits on the head's subscription for the snapshot, as ``deliver_capture`` does.
The head's queue, its delivery and the snapshot API run as the head's app role.
"""

from uuid import uuid4
from datetime import UTC, datetime, timedelta

import pytest

from canopy.core import db
from canopy.core.arms import subscription_name
from canopy.core.errors import CanopyError, PrimaryTimeout, PrimaryUnavailable
from canopy.squirrel.config import OutboxConfig
from canopy.core.forwarding.client import Caller, ArmRefused, wait_until_applied
from canopy.squirrel.outbox.delivery import submit, deliver_arm, send_queued
from canopy.squirrel.schemas.snapshot import BundleValueDTO, SnapshotBundleDTO
from canopy.squirrel.services.snapshot_service import SnapshotService

from .conftest import Octopus, eventually, _subscribed
from .test_forward_e2e import (  # noqa: F401 — head_app is a fixture
    head_app,
    _captured_import,
)

CALLER = Caller(on_behalf_of="operator@head", facilities=["*"])
CONFIG = OutboxConfig()
pytestmark = pytest.mark.usefixtures("head_app")


class FakeArms:
    """The arms' captured imports, with the head's connection to some of them cut."""

    def __init__(self, octopus: Octopus, monkeypatch):
        self.octopus, self.monkeypatch = octopus, monkeypatch
        self.down: set[str] = set()
        self.applied_then_lost: set[str] = set()  # the arm saves it, the head never hears back
        self.sent: list[tuple[str, str, bool]] = []

    async def __call__(self, arm: str, bundle: SnapshotBundleDTO, caller: Caller, *, strict: bool = False) -> dict:
        self.sent.append((arm, str(bundle.id), strict))
        if arm in self.down:
            raise PrimaryUnavailable(f"Arm '{arm}' could not be reached; nothing was sent to it")
        try:
            _, position = await _captured_import(self.octopus, arm, bundle, self.monkeypatch, strict=strict)
        except CanopyError as exc:  # what the arm's API answers with
            raise ArmRefused(arm, exc.status_code, exc.error_code.value, exc.detail) from None
        if arm in self.applied_then_lost:
            self.applied_then_lost.discard(arm)
            raise PrimaryTimeout(f"Arm '{arm}' did not answer")
        snapshot_id = str(bundle.id)

        async def is_here(conn) -> bool:
            return bool(await conn.fetchval("SELECT 1 FROM squirrel.snapshot WHERE id = $1::uuid", snapshot_id))

        await wait_until_applied(position, 30.0, subscription=subscription_name(arm), until=is_here)
        return {"type": "snapshot", "id": snapshot_id, "arm": arm}


@pytest.fixture
async def arms(octopus: Octopus, monkeypatch) -> FakeArms:
    head = octopus.head
    await head.admin.execute(f'GRANT SELECT ON pg_replication_origin_status TO "{head.app}"')
    await head.admin.execute(f'GRANT EXECUTE ON FUNCTION pg_show_replication_origin_status() TO "{head.app}"')
    fake = FakeArms(octopus, monkeypatch)
    monkeypatch.setattr("canopy.squirrel.outbox.delivery.deliver_capture", fake)
    return fake


def _bundle(arm: str, ids: dict[str, str], minutes: int) -> SnapshotBundleDTO:
    return SnapshotBundleDTO(
        format="canopy.squirrel.snapshot-bundle",
        version=1,
        source_instance="head",
        id=str(uuid4()),
        title=f"{arm} at +{minutes}",
        created_date=datetime(2026, 11, 6, 14, 2, tzinfo=UTC) + timedelta(minutes=minutes),
        created_by="operator@head",
        facility=f"fac_{arm}",
        values=[BundleValueDTO(pv_id=ids["pv"], pv_name="SIM:PV:1:SETPOINT", setpoint_value={"value": minutes})],
    )


async def _submit(arm: str, bundle: SnapshotBundleDTO, ids: dict[str, str]) -> dict:
    return await submit(arm, bundle, CALLER, facility_id=ids["facility"], config=CONFIG)


async def _make_due(octopus: Octopus) -> None:
    """Skip the backoff: every pending row's next attempt is now."""
    await octopus.head.admin.execute("UPDATE squirrel.snapshot_outbox SET next_attempt_at = now()")


async def _listed(octopus: Octopus) -> dict[str, str | None]:
    async with db.session_scope() as session:
        page = await SnapshotService(session, None).list_snapshots(page_size=50)
    return {row.id: (row.delivery.state if row.delivery else None) for row in page.results}


async def test_an_unreachable_arms_snapshots_wait_then_arrive_in_order(octopus: Octopus, arms: FakeArms):
    arm1, arm2 = octopus.arms.values()
    ids1 = await _subscribed(octopus, arm1.name)
    ids2 = await _subscribed(octopus, arm2.name)
    head = octopus.head
    arms.down.add(arm2.name)

    first, second = _bundle(arm2.name, ids2, 0), _bundle(arm2.name, ids2, 5)
    queued = await _submit(arm2.name, first, ids2)
    behind = await _submit(arm2.name, second, ids2)
    saved = await _submit(arm1.name, _bundle(arm1.name, ids1, 1), ids1)

    # arm1 took its snapshot at once; arm2's wait on the head, the second behind the first.
    assert saved["arm"] == arm1.name and "delivery" not in saved
    assert (queued["delivery"]["state"], queued["delivery"]["attempts"]) == ("pending", 1)
    assert "could not be reached" in queued["delivery"]["reason"]
    assert (behind["delivery"]["attempts"], [a for a, *_ in arms.sent].count(arm2.name)) == (0, 1)
    listed = await _listed(octopus)
    assert (listed[str(first.id)], listed[str(second.id)], listed[saved["id"]]) == ("pending", "pending", None)
    assert await arm2.admin.fetchval("SELECT count(*) FROM squirrel.snapshot WHERE id = $1", str(first.id)) == 0

    # Still cut off: a run backs off and holds the queue.
    await _make_due(octopus)
    assert (await deliver_arm(arm2.name, send_queued, config=CONFIG))["retried"] == 1

    # arm2 is back: its snapshots go oldest first, keep their ids and capture times, and
    # come back to the head as ordinary snapshots.
    arms.down.clear()
    await _make_due(octopus)
    arms.sent.clear()
    assert await deliver_arm(arm2.name, send_queued, config=CONFIG) == {"delivered": 2, "retried": 0, "failed": 0}

    assert arms.sent == [(arm2.name, str(first.id), True), (arm2.name, str(second.id), True)]
    for bundle in (first, second):
        row = await arm2.admin.fetchrow(
            "SELECT owner_instance, created_at, created_by FROM squirrel.snapshot WHERE id = $1", str(bundle.id)
        )
        assert (row["owner_instance"], row["created_at"], row["created_by"]) == (
            arm2.name,
            bundle.created_date,
            "operator@head",
        )
    assert await head.admin.fetchval("SELECT count(*) FROM squirrel.snapshot_outbox") == 0
    listed = await _listed(octopus)
    assert (listed[str(first.id)], listed[str(second.id)]) == (None, None)


async def test_a_lost_answer_is_resent_and_saved_once(octopus: Octopus, arms: FakeArms):
    _, arm2 = octopus.arms.values()
    ids = await _subscribed(octopus, arm2.name)
    arms.applied_then_lost.add(arm2.name)
    bundle = _bundle(arm2.name, ids, 0)

    queued = await _submit(arm2.name, bundle, ids)
    assert queued["delivery"]["state"] == "pending"

    await _make_due(octopus)
    await deliver_arm(arm2.name, send_queued, config=CONFIG)

    # The arm saved it once; the resend changed nothing, and the head's row is gone.
    assert await arm2.admin.fetchval("SELECT count(*) FROM squirrel.snapshot WHERE id = $1", str(bundle.id)) == 1
    await eventually(lambda: _outbox_empty(octopus))


async def _outbox_empty(octopus: Octopus) -> bool:
    return await octopus.head.admin.fetchval("SELECT count(*) = 0 FROM squirrel.snapshot_outbox")


async def test_a_pv_moved_while_cut_off_fails_the_snapshot_until_retried(octopus: Octopus, arms: FakeArms):
    _, arm2 = octopus.arms.values()
    ids = await _subscribed(octopus, arm2.name)
    head = octopus.head
    # A second PV in arm2's facility.
    moving = str(uuid4())
    await arm2.admin.execute(
        "INSERT INTO squirrel.pv (id, setpoint_address, owner_instance) VALUES ($1, 'SIM:PV:2:SETPOINT', $2)",
        moving,
        arm2.name,
    )
    await arm2.admin.execute(
        "INSERT INTO squirrel.pv_facility (pv_id, facility_id) VALUES ($1, $2)", moving, ids["facility"]
    )
    arms.down.add(arm2.name)
    moved, other = _bundle(arm2.name, ids, 0), _bundle(arm2.name, ids, 1)
    moved.values.append(BundleValueDTO(pv_id=moving, pv_name="SIM:PV:2:SETPOINT", setpoint_value={"value": 7}))
    await _submit(arm2.name, moved, ids)
    await _submit(arm2.name, other, ids)

    # Meanwhile on arm2, the second PV leaves the facility.
    await arm2.admin.execute("DELETE FROM squirrel.pv_facility WHERE pv_id = $1", moving)
    arms.down.clear()
    await _make_due(octopus)
    counts = await deliver_arm(arm2.name, send_queued, config=CONFIG)

    # Strict: refused with the PV named, and the queue went on without it.
    assert counts == {"delivered": 1, "retried": 0, "failed": 1}
    row = await head.admin.fetchrow(
        "SELECT state, last_error FROM squirrel.snapshot_outbox WHERE id = $1", str(moved.id)
    )
    assert row["state"] == "failed" and row["last_error"].startswith("import_would_skip:")
    assert "SIM:PV:2:SETPOINT" in row["last_error"]
    assert (await _listed(octopus))[str(moved.id)] == "failed"

    # retry --allow-skips sends it without strict: saved without the moved PV's value.
    await head.admin.execute(
        "UPDATE squirrel.snapshot_outbox SET state = 'pending', strict = false, next_attempt_at = now() WHERE id = $1",
        str(moved.id),
    )
    assert (await deliver_arm(arm2.name, send_queued, config=CONFIG))["delivered"] == 1
    values = await arm2.admin.fetch(
        "SELECT pv_id::text FROM squirrel.snapshot_value WHERE snapshot_id = $1", str(moved.id)
    )
    assert [v["pv_id"] for v in values] == [ids["pv"]]
    await eventually(lambda: _outbox_empty(octopus))
