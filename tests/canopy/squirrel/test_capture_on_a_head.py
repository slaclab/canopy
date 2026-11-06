"""Capture on a history head: taken here, saved here or on the facility's arm.

The head is ``head`` with a forwardable ``arm1`` (mocked with ``httpx_mock``) and a
read-only ``arm2``. Rows are made as replication brings them, each under its owner's
name (``as_instance``). Postgres via squirrel's ``test_engine``.
"""

import json
from uuid import UUID, uuid4
from types import SimpleNamespace

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import text, select
from pytest_httpx import HTTPXMock
from sqlalchemy.ext.asyncio import AsyncSession

from tests.conftest import WRITE_HEADERS
from canopy.squirrel import tasks, capture
from canopy.core.config import get_settings
from canopy.core.errors import NotOwner, PrimaryUnavailable
from canopy.squirrel.capture import CaptureTarget, capture_target
from canopy.squirrel.data.models import Pv, Snapshot, pv_facility
from canopy.core.facilities.models import Facility
from canopy.core.forwarding.client import Caller, ArmRefused
from canopy.squirrel.services.snapshot_service import SnapshotService
from tests.canopy.squirrel.test_services_snapshot import FakeEpicsIO, _ev, _NoHook

ARM1 = "https://arm1.example.org"
IMPORT = f"{ARM1}/squirrel/v1/snapshots/import?mode=captured"
CALLER = {"on_behalf_of": "console@head", "facilities": ["*"]}


@pytest.fixture
def head(monkeypatch):
    monkeypatch.setenv("CANOPY_ROLE", "mirror")
    monkeypatch.setenv("CANOPY_INSTANCE", "head")
    monkeypatch.setenv("CANOPY_ARMS__ARM1__FORWARD_URL", ARM1)
    monkeypatch.setenv("CANOPY_ARMS__ARM1__SERVICE_KEY", "arm1-service-token")
    monkeypatch.setenv("CANOPY_ARMS__ARM2__PUBLISHER_DSN", "host=arm2.example.org")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest_asyncio.fixture
async def sites(db_session: AsyncSession, head, as_instance) -> dict[str, dict[str, str]]:
    """For each owner: a facility with a child, a PV in the child, and one PV outside it."""
    made = {}
    for owner in ("head", "arm1", "arm2"):
        with as_instance(owner):
            parent = Facility(slug=f"{owner}-site", name=owner)
            db_session.add(parent)
            await db_session.flush()
            child = Facility(slug=f"{owner}-child", name="child", parent_id=parent.id)
            inside = Pv(setpoint_address=f"{owner.upper()}:IN:SP", readback_address=f"{owner.upper()}:IN:RB")
            outside = Pv(setpoint_address=f"{owner.upper()}:OUT:SP")
            db_session.add_all([child, inside, outside])
            await db_session.flush()
            await db_session.execute(pv_facility.insert().values(pv_id=inside.id, facility_id=child.id))
            await db_session.commit()
            made[owner] = {"facility": parent.id, "slug": parent.slug, "inside": inside.id, "outside": outside.id}
    return made


class TestTheRoute:
    async def test_the_heads_facility_is_saved_here(self, client, sites, enqueued):
        resp = await client.post("/v1/snapshots", json={"title": "t", "facility": "head-site"}, headers=WRITE_HEADERS)

        assert resp.status_code == 200, resp.text
        [(function, _, kwargs)] = enqueued
        assert function == "create_snapshot_task"
        assert "arm" not in kwargs and "caller" not in kwargs

    async def test_an_arm1_facility_is_taken_here_for_arm1(self, client, sites, enqueued):
        resp = await client.post("/v1/snapshots", json={"title": "t", "facility": "arm1-site"}, headers=WRITE_HEADERS)

        assert resp.status_code == 200, resp.text
        [(_, _, kwargs)] = enqueued
        assert kwargs["arm"] == "arm1"
        assert kwargs["facility_id"] == sites["arm1"]["facility"]
        assert kwargs["caller"]["on_behalf_of"].endswith("@head")
        assert kwargs["caller"]["facilities"] == ["*"]

    async def test_a_read_only_arms_facility_is_refused_before_queueing(self, client, sites, enqueued):
        resp = await client.post("/v1/snapshots", json={"title": "t", "facility": "arm2-site"}, headers=WRITE_HEADERS)

        assert resp.status_code == 403
        assert resp.json()["code"] == "not_owner"
        assert enqueued == []

    @pytest.mark.parametrize(
        ("query", "arm", "status"),
        [
            ("owner=arm1", "arm1", 200),
            ("owner=head", None, 200),
            ("owner=arm2", None, 403),
            ("owner=nosuch", None, 403),
        ],
    )
    async def test_with_no_facility_the_owner_param_decides(self, client, sites, enqueued, query, arm, status):
        resp = await client.post(f"/v1/snapshots?{query}", json={"title": "t"}, headers=WRITE_HEADERS)

        assert resp.status_code == status, resp.text
        assert [kwargs.get("arm") for _, _, kwargs in enqueued] == ([arm] if status == 200 else [])

    async def test_an_owner_param_the_facility_contradicts_is_refused(self, client, sites, enqueued):
        resp = await client.post(
            "/v1/snapshots?owner=head", json={"title": "t", "facility": "arm1-site"}, headers=WRITE_HEADERS
        )

        assert resp.status_code == 403
        assert "owned by 'arm1', not 'head'" in resp.json()["detail"]
        assert enqueued == []


def test_capture_target_on_an_arm(monkeypatch):
    # The suite runs as arm1, a primary: it saves only its own.
    assert capture_target(None, None, "S") == CaptureTarget()
    assert capture_target("arm1", "arm1", "S") == CaptureTarget()
    with pytest.raises(NotOwner):
        capture_target("arm2", None, "S")
    with pytest.raises(NotOwner):
        capture_target(None, "head", "S")


class TestTheTask:
    """The head's worker: read the arm's PVs here, send one bundle, wait for it to come back."""

    @pytest.fixture
    def worker(self, monkeypatch):
        io = FakeEpicsIO(
            {
                address: _ev(1.5)
                for owner in ("HEAD", "ARM1", "ARM2")
                for address in (f"{owner}:IN:SP", f"{owner}:OUT:SP")
            }
            | {"ARM1:IN:RB": _ev(1.4)}
        )
        waits = []

        async def no_progress(*args, **kwargs):
            pass

        async def wait_until_applied(commit_lsn, timeout_s, *, subscription, until=None):
            waits.append((commit_lsn, subscription, until is not None))
            return True

        monkeypatch.setattr(tasks, "report_progress", no_progress)
        monkeypatch.setattr(tasks, "_build_epics_io", lambda ctx: io)
        monkeypatch.setattr(capture, "wait_until_applied", wait_until_applied)
        return SimpleNamespace(io=io, waits=waits, ctx={"redis": None, "job_id": "j"})

    @staticmethod
    def _saved(request: httpx.Request, *, skipped: list | None = None, created: bool = True) -> httpx.Response:
        bundle = json.loads(request.content)
        summary = {
            "id": bundle["id"],
            "ownerInstance": "arm1",
            "title": bundle["title"],
            "createdDate": bundle["createdDate"],
            "createdBy": "console@head",
            "pvCount": len(bundle["values"]),
            "facility": bundle["facility"],
        }
        return httpx.Response(
            200,
            json={"snapshot": summary, "skipped": skipped or [], "created": created},
            headers={"X-Canopy-Commit-LSN": "0/16B3748"},
        )

    async def test_one_bundle_goes_to_arm1_and_the_head_waits(self, sites, worker, httpx_mock: HTTPXMock, db_session):
        httpx_mock.add_callback(self._saved, method="POST", url=IMPORT)

        result = await tasks.create_snapshot_task(
            worker.ctx, "Taken here", facility_id=sites["arm1"]["facility"], arm="arm1", caller=CALLER
        )

        sent = httpx_mock.get_request()
        assert sent.headers["X-API-Key"] == "arm1-service-token"
        assert sent.headers["X-Canopy-On-Behalf-Of"] == "console@head"
        assert json.loads(sent.headers["X-Canopy-On-Behalf-Of-Facilities"]) == ["*"]
        bundle = json.loads(sent.content)
        assert UUID(bundle["id"]) and bundle["createdDate"]
        assert (bundle["sourceInstance"], bundle["facility"], bundle["title"]) == ("head", "arm1-site", "Taken here")
        # Only arm1's PVs in the facility's subtree.
        [value] = bundle["values"]
        assert value["pvId"] == sites["arm1"]["inside"]
        assert value["setpointValue"]["value"] == 1.5 and value["readbackValue"]["value"] == 1.4
        assert result == {"type": "snapshot", "id": bundle["id"], "arm": "arm1"}
        assert worker.waits == [("0/16B3748", "canopy_history_arm1", True)]
        # Nothing saved here: the snapshot is arm1's, and comes back through replication.
        assert (await db_session.scalar(select(Snapshot.id))) is None

    async def test_skipped_values_reach_the_job_result(self, sites, worker, httpx_mock: HTTPXMock):
        skipped = [{"pvId": str(uuid4()), "pvName": "ARM1:IN:SP"}]
        httpx_mock.add_callback(lambda request: self._saved(request, skipped=skipped), method="POST", url=IMPORT)

        result = await tasks.create_snapshot_task(
            worker.ctx, "t", facility_id=sites["arm1"]["facility"], arm="arm1", caller=CALLER
        )

        assert result["skipped"] == skipped

    async def test_a_refusal_fails_the_job_with_the_arms_reason(self, sites, worker, httpx_mock: HTTPXMock):
        httpx_mock.add_response(
            method="POST", url=IMPORT, status_code=404, json={"code": "facility_not_found", "detail": "gone"}
        )

        with pytest.raises(ArmRefused) as refused:
            await tasks.create_snapshot_task(
                worker.ctx, "t", facility_id=sites["arm1"]["facility"], arm="arm1", caller=CALLER
            )

        assert (refused.value.status, refused.value.code, refused.value.detail) == (404, "facility_not_found", "gone")
        assert worker.waits == []

    async def test_an_unreachable_arm_fails_the_job(self, sites, worker, httpx_mock: HTTPXMock):
        httpx_mock.add_exception(httpx.ConnectError("refused"))

        with pytest.raises(PrimaryUnavailable, match="nothing was sent"):
            await tasks.create_snapshot_task(
                worker.ctx, "t", facility_id=sites["arm1"]["facility"], arm="arm1", caller=CALLER
            )

    async def test_strict_goes_with_the_bundle(self, sites, worker, httpx_mock: HTTPXMock, db_session):
        httpx_mock.add_callback(self._saved, method="POST", url=f"{IMPORT}&strict=true")
        service = SnapshotService(db_session, worker.io)
        captured = await service.capture_values(sites["arm1"]["facility"], owner="arm1")
        bundle = await service.bundle_captured(
            captured, snapshot_id=str(uuid4()), title="t", description=None, created_by=None, facility_id=None
        )

        result = await capture.deliver_capture("arm1", bundle, Caller(**CALLER), strict=True)

        assert result["arm"] == "arm1"

    async def test_the_heads_own_facility_is_saved_here_with_its_capture_time(self, sites, worker, db_session):
        result = await tasks.create_snapshot_task(worker.ctx, "Mine", facility_id=sites["head"]["facility"])

        saved = await db_session.get(Snapshot, result["id"])
        assert result == {"type": "snapshot", "id": saved.id}
        assert saved.owner_instance == "head"
        values = (await db_session.execute(text("SELECT pv_id::text, timestamp FROM squirrel.snapshot_value"))).all()
        assert [pv_id for pv_id, _ in values] == [sites["head"]["inside"]]
        # The snapshot's time is its capture's, the same as its values'.
        assert saved.created_at == values[0][1]


async def test_restore_values_from_a_list(db_session: AsyncSession):
    pv = Pv(setpoint_address="LIST:SP")
    db_session.add(pv)
    await db_session.commit()
    io = FakeEpicsIO()
    service = SnapshotService(db_session, io, restore_hooks=[_NoHook()])
    values = [
        SimpleNamespace(pv_id=pv.id, setpoint_value={"value": 4.0}),
        SimpleNamespace(pv_id=str(uuid4()), setpoint_value={"value": 9.0}),
    ]

    result = await service.restore_values(values)

    assert io.writes == {"LIST:SP": 4.0}
    assert (result.total_pvs, result.success_count) == (1, 1)
    io.writes.clear()
    assert (await service.restore_values(values, SimpleNamespace(pv_ids=[str(uuid4())]))).total_pvs == 0
    assert io.writes == {}
