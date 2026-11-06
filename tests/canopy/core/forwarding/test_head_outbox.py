"""A head's outbox behind the router: a queued snapshot is answered here, never sent to its arm.

Builds on ``test_head.py``'s copy (rows of ``head``, ``arm1`` and ``arm2``); arm1 is
mocked with ``httpx_mock``, which fails any request nothing was registered for.
"""

import uuid
from datetime import UTC, datetime

import pytest
from sqlalchemy import select, update

from canopy.core import db
from canopy.squirrel.config import OutboxConfig
from canopy.squirrel.data.models import SnapshotOutbox
from canopy.core.facilities.models import Facility
from canopy.squirrel.outbox.delivery import enqueue
from canopy.squirrel.schemas.snapshot import BUNDLE_FORMAT, SnapshotBundleDTO
from canopy.squirrel.data.repositories import OutboxRepository

from .test_head import (  # noqa: F401 — the head's copy and a client over it
    HEADERS,
    copy,
    http,
)


async def _queue(copy: dict, **state) -> str:  # noqa: F811 — the fixture's name
    bundle = SnapshotBundleDTO(
        format=BUNDLE_FORMAT,
        version=1,
        id=str(uuid.uuid4()),
        title="taken while arm1 was away",
        created_date=datetime.now(UTC),
        created_by="console@head",
        facility=copy["arm1"]["facility"],
    )
    async with db.get_session_maker()() as session:
        facility_id = await session.scalar(select(Facility.id).where(Facility.slug == copy["arm1"]["facility"]))
        await enqueue(
            OutboxRepository(session),
            arm="arm1",
            bundle=bundle,
            facility_id=facility_id,
            caller={"on_behalf_of": "console@head", "facilities": ["*"]},
            config=OutboxConfig(),
        )
        if state:
            await session.execute(update(SnapshotOutbox).where(SnapshotOutbox.id == str(bundle.id)).values(**state))
        await session.commit()
    return str(bundle.id)


@pytest.mark.parametrize(
    ("method", "path", "body"),
    [
        ("PATCH", "/squirrel/v1/snapshots/{id}", {"title": "new"}),
        ("PUT", "/squirrel/v1/snapshots/{id}", {"retain": True}),
        ("DELETE", "/squirrel/v1/snapshots/{id}", None),
        ("POST", "/squirrel/v1/snapshots/{id}/restore", {}),
    ],
)
async def test_a_change_to_a_queued_snapshot_is_answered_here(http, copy, method, path, body):  # noqa: F811
    queued = await _queue(copy)

    resp = await http.request(method, path.format(id=queued), json=body, headers=HEADERS)

    assert resp.status_code == 409, resp.text
    assert resp.json()["code"] == "snapshot_pending"
    assert "waiting for delivery to arm1" in resp.json()["detail"]


async def test_a_capture_for_an_arm_with_a_full_backlog_is_refused_at_once(http, copy, monkeypatch):  # noqa: F811
    enqueued = []

    async def fake_enqueue(function, *args, **kwargs):
        enqueued.append(function)
        return "job-1"

    monkeypatch.setattr("canopy.jobs.gateway.enqueue", fake_enqueue)
    monkeypatch.setenv("CANOPY_SQUIRREL__OUTBOX__MAX_SNAPSHOTS", "1")
    queued = await _queue(copy, state="failed", last_error="x: y")
    capture = {"title": "t", "facility": copy["arm1"]["facility"]}

    # Full, but nothing waits: the capture may reach arm1, so it's taken.
    resp = await http.post("/squirrel/v1/snapshots", json=capture, headers=HEADERS)
    assert resp.status_code == 200, resp.text

    async with db.get_session_maker()() as session:
        await session.execute(update(SnapshotOutbox).where(SnapshotOutbox.id == queued).values(state="pending"))
        await session.commit()
    resp = await http.post("/squirrel/v1/snapshots", json=capture, headers=HEADERS)

    assert resp.status_code == 503, resp.text
    assert resp.json()["code"] == "outbox_full"
    assert enqueued == ["create_snapshot_task"]
    # The head's own facility is never held up by an arm's outbox.
    resp = await http.post(
        "/squirrel/v1/snapshots", json={"title": "t", "facility": copy["head"]["facility"]}, headers=HEADERS
    )
    assert resp.status_code == 200, resp.text
