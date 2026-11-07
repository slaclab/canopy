"""A history head sending writes to their owners (arm1 mocked with ``httpx_mock``).

The owner lookup (``owner``) and the caller's facility check (``reach``, true unless
set) are stubbed here, so these need no database; ``test_owners.py`` covers the
lookup itself and ``test_head.py`` all of it together. Keys are fakes bound to the
head's registry.
"""

import json
from urllib.parse import unquote

import httpx
import pytest
from pytest_httpx import HTTPXMock
from prometheus_client import REGISTRY
from fastapi.testclient import TestClient

from canopy.main import app
from canopy.core.config import get_settings
from canopy.core.modules import WILDCARD
from canopy.core.security import APIKey
from canopy.core.forwarding import owners, middleware

from .conftest import ARM1, ARM1_SERVICE_KEY

WRITER = APIKey(
    token="writer", read=True, write=True, modules=frozenset({WILDCARD}), facilities=frozenset({"a"}), name="console"
)
READER = APIKey(token="reader", read=True, write=False, modules=frozenset({WILDCARD}), name="viewer")
MAGPIE_ONLY = APIKey(token="magpie", read=True, write=True, modules=frozenset({"magpie"}), name="configs")
OPERATOR = APIKey(token="operator", read=True, write=True, modules=frozenset({WILDCARD}), name="opérateur")
ADMIN = APIKey(token="admin", read=True, write=True, admin=True, modules=frozenset({WILDCARD}), name="ops")
HIERARCHY = APIKey(
    token="hierarchy",
    read=True,
    write=True,
    admin=True,
    modules=frozenset({WILDCARD}),
    facilities=frozenset({WILDCARD}),
    name="ops-all",
)
SNAPSHOT_ID = "6f1c2c1e-0d4b-4c43-9a55-2f0d7a3c9b10"


@pytest.fixture
def owner(monkeypatch):
    """Stub the owner lookup: ``owner("arm1")``; records each call's route and arguments."""
    calls = []

    def _set(name: str | None):
        async def owner_of(route, params, body, *, requested, local):
            calls.append({"route": route.path, "params": params, "body": body, "requested": requested})
            return name

        monkeypatch.setattr(owners, "owner_of", owner_of)
        return calls

    return _set


@pytest.fixture(autouse=True)
def reach(monkeypatch):
    """Stub the caller's facility check: ``reach(False)`` puts the caller outside every row; returns the calls."""
    calls = []
    reaches = {"value": True}

    async def caller_reaches(route, params, key):
        calls.append((route.path, params, key.name))
        return reaches["value"]

    monkeypatch.setattr(owners, "caller_reaches", caller_reaches)

    def _set(value: bool):
        reaches["value"] = value
        return calls

    return _set


@pytest.fixture
def client(head, bind_keys):
    bind_keys(WRITER, READER, MAGPIE_ONLY, OPERATOR, ADMIN, HIERARCHY)
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture
def jobs(monkeypatch):
    remembered = {}

    async def remember(job_id: str, arm: str) -> None:
        remembered[job_id] = arm

    for layer in _middleware_stack():
        monkeypatch.setattr(layer, "remember_job", remember)
    return remembered


def _middleware_stack():
    stack = app.middleware_stack or app.build_middleware_stack()
    app.middleware_stack = stack
    found, layer = [], stack
    while layer is not None:
        if isinstance(layer, middleware.ForwardingMiddleware):
            found.append(layer)
        layer = getattr(layer, "app", None)
    return found


def _as(key: APIKey, **extra: str) -> dict[str, str]:
    return {"X-API-Key": key.token, **extra}


def test_an_arms_write_goes_there_as_the_heads_service_key(client, owner, httpx_mock: HTTPXMock):
    owner("arm1")
    httpx_mock.add_response(method="DELETE", url=f"{ARM1}/squirrel/v1/snapshots/{SNAPSHOT_ID}", json=True)

    resp = client.delete(f"/squirrel/v1/snapshots/{SNAPSHOT_ID}", headers=_as(WRITER))

    assert resp.status_code == 200, resp.text
    sent = httpx_mock.get_request()
    assert sent.headers["X-API-Key"] == ARM1_SERVICE_KEY
    assert sent.headers["X-Canopy-On-Behalf-Of"] == "console@head"
    assert json.loads(sent.headers["X-Canopy-On-Behalf-Of-Facilities"]) == ["a"]


def test_the_lookup_gets_the_path_parameters_body_and_owner_param(client, owner, httpx_mock: HTTPXMock):
    calls = owner("arm1")
    httpx_mock.add_response(method="POST", url=f"{ARM1}/squirrel/v1/pvs?owner=arm1", json={"id": "p1"})

    resp = client.post("/squirrel/v1/pvs?owner=arm1", json={"setpointAddress": "SIM:1"}, headers=_as(WRITER))

    assert resp.status_code == 200, resp.text
    assert calls == [
        {"route": "/squirrel/v1/pvs", "params": {}, "body": {"setpointAddress": "SIM:1"}, "requested": "arm1"}
    ]
    assert json.loads(httpx_mock.get_request().content) == {"setpointAddress": "SIM:1"}


def test_the_owner_param_is_read_only_where_the_route_takes_it(client, owner, httpx_mock: HTTPXMock):
    calls = owner("arm1")
    httpx_mock.add_response(method="PUT", json={})

    client.put("/squirrel/v1/pvs/p1?owner=arm2", json={}, headers=_as(WRITER))

    assert calls[0]["requested"] is None


def test_only_the_body_query_and_content_headers_go_on(client, owner, httpx_mock: HTTPXMock):
    owner("arm1")
    httpx_mock.add_response(
        method="PUT",
        url=f"{ARM1}/squirrel/v1/pvs/pv-1?dryRun=true",
        status_code=201,
        json={"id": "pv-1"},
        headers={"Set-Cookie": "arm=1", "X-Canopy-Something": "kept"},
    )

    resp = client.put(
        "/squirrel/v1/pvs/pv-1?dryRun=true",
        json={"description": "new"},
        headers=_as(WRITER, Cookie="sso=secret"),
    )

    assert resp.status_code == 201
    assert resp.json() == {"id": "pv-1"}
    assert resp.headers["x-canopy-something"] == "kept"
    assert "set-cookie" not in resp.headers
    sent = httpx_mock.get_request()
    assert json.loads(sent.content) == {"description": "new"}
    assert sent.headers["content-type"] == "application/json"
    assert "cookie" not in sent.headers


def test_a_non_ascii_name_travels_percent_encoded(client, owner, httpx_mock: HTTPXMock):
    owner("arm1")
    httpx_mock.add_response(method="POST", url=f"{ARM1}/squirrel/v1/tags", json={})

    resp = client.post("/squirrel/v1/tags", json={"name": "Area"}, headers=_as(OPERATOR))

    assert resp.status_code == 200, resp.text
    sent = httpx_mock.get_request().headers["X-Canopy-On-Behalf-Of"]
    assert sent == "op%C3%A9rateur@head"
    assert unquote(sent) == "opérateur@head"


def test_the_arms_errors_come_back_as_they_are(client, owner, httpx_mock: HTTPXMock):
    owner("arm1")
    httpx_mock.add_response(method="DELETE", status_code=403, json={"code": "insufficient_facility_access"})

    resp = client.delete("/squirrel/v1/pvs/p1", headers=_as(WRITER))

    assert resp.status_code == 403
    assert resp.json() == {"code": "insufficient_facility_access"}


def test_a_magpie_key_forwards_magpie_writes(client, owner, httpx_mock: HTTPXMock):
    owner("arm1")
    httpx_mock.add_response(method="DELETE", url=f"{ARM1}/magpie/v1/configs/trace/abc", json=True)

    assert client.delete("/magpie/v1/configs/trace/abc", headers=_as(MAGPIE_ONLY)).status_code == 200


def test_a_facility_route_needs_the_hierarchy_gate(client, owner, httpx_mock: HTTPXMock):
    owner("arm1")
    httpx_mock.add_response(method="PUT", url=f"{ARM1}/v1/facilities/site-a", json={})

    # Admin, but not over every facility: the app here refuses it, nothing goes to the arm.
    refused = client.put("/v1/facilities/site-a", json={"name": "A"}, headers=_as(ADMIN))
    sent = client.put("/v1/facilities/site-a", json={"name": "A"}, headers=_as(HIERARCHY))

    assert refused.status_code == 403
    assert refused.json()["code"] == "insufficient_facility_access"
    assert sent.status_code == 200
    assert len(httpx_mock.get_requests()) == 1


def test_the_root_path_is_not_sent_to_the_arm(head, bind_keys, owner, httpx_mock: HTTPXMock):
    bind_keys(WRITER)
    owner("arm1")
    httpx_mock.add_response(method="DELETE", url=f"{ARM1}/squirrel/v1/pvs/p1", json=True)
    client = TestClient(app, root_path="/canopy", raise_server_exceptions=False)

    # As uvicorn --root-path sends it: the prefix is part of the path.
    assert client.delete("/canopy/squirrel/v1/pvs/p1", headers=_as(WRITER)).status_code == 200


class TestLeftToTheAppHere:
    """Never sent to an arm (``httpx_mock`` fails the test on any request)."""

    def test_no_key_or_a_bad_one(self, client, owner):
        owner("arm1")

        assert client.delete("/squirrel/v1/pvs/p1").json()["code"] == "invalid_api_key"
        assert client.delete("/squirrel/v1/pvs/p1", headers={"X-API-Key": "nope"}).status_code == 401

    def test_a_read_key(self, client, owner):
        owner("arm1")
        resp = client.delete("/squirrel/v1/pvs/p1", headers=_as(READER))

        assert resp.status_code == 403
        assert resp.json()["code"] == "insufficient_write_access"

    def test_a_key_for_another_module(self, client, owner):
        owner("arm1")
        resp = client.delete("/squirrel/v1/pvs/p1", headers=_as(MAGPIE_ONLY))

        assert resp.status_code == 403
        assert resp.json()["code"] == "insufficient_module_access"

    def test_a_caller_naming_someone_else(self, client, owner):
        owner("arm1")
        resp = client.delete(
            "/squirrel/v1/pvs/p1",
            headers=_as(WRITER, **{"X-Canopy-On-Behalf-Of": "x", "X-Canopy-On-Behalf-Of-Facilities": "[]"}),
        )

        assert resp.status_code == 403
        assert resp.json()["code"] == "forwarded_write_refused"

    @pytest.fixture
    def app_here(self, monkeypatch):
        """Replace the app behind the middleware; returns what it received."""
        seen = []

        async def app_here(scope, receive, send):
            seen.append(await receive())
            await send({"type": "http.response.start", "status": 299, "headers": []})
            await send({"type": "http.response.body", "body": b""})

        monkeypatch.setattr(_middleware_stack()[0], "app", app_here)
        return seen

    @pytest.mark.parametrize("name", ["head", "arm2", "nosuch", None])
    def test_an_owner_that_isnt_a_forwardable_arm(self, client, owner, app_here, name):
        owner(name)

        resp = client.post("/squirrel/v1/pvs", json={"setpointAddress": "SIM:1"}, headers=_as(WRITER))

        assert resp.status_code == 299
        # The body read to find the owner is handed on to the app whole.
        assert json.loads(app_here[0]["body"]) == {"setpointAddress": "SIM:1"}

    def test_a_caller_outside_the_rows_facilities(self, client, owner, reach, app_here):
        owner("arm1")
        calls = reach(False)

        resp = client.patch(f"/squirrel/v1/snapshots/{SNAPSHOT_ID}", json={"title": "x"}, headers=_as(WRITER))

        # The app here answers as any instance would; nothing goes to arm1, up or down.
        assert resp.status_code == 299
        assert json.loads(app_here[0]["body"]) == {"title": "x"}
        assert calls == [("/squirrel/v1/snapshots/{snapshot_id}", {"snapshot_id": SNAPSHOT_ID}, "console")]

    def test_the_facility_check_is_only_for_an_arms_row(self, client, owner, reach, app_here):
        owner("head")
        calls = reach(False)

        client.delete("/squirrel/v1/pvs/p1", headers=_as(WRITER))

        assert calls == []

    def test_a_captured_import(self, client, owner, app_here):
        calls = owner("arm1")

        resp = client.post("/squirrel/v1/snapshots/import?mode=captured", json={"x": 1}, headers=_as(WRITER))

        assert calls == []
        assert resp.status_code == 299
        assert json.loads(app_here[0]["body"]) == {"x": 1}

    def test_nothing_on_a_head_without_a_forwardable_arm(self, client, owner, monkeypatch):
        calls = owner("arm1")
        monkeypatch.delenv("CANOPY_ARMS__ARM1__FORWARD_URL")
        monkeypatch.delenv("CANOPY_ARMS__ARM1__SERVICE_KEY")
        monkeypatch.setenv("CANOPY_ARMS__ARM1__PUBLISHER_DSN", "host=arm1.example.org")

        client.delete("/squirrel/v1/pvs/p1", headers=_as(WRITER))

        assert calls == []

    def test_a_restore_with_machine_access_runs_here(self, client, owner, app_here, monkeypatch):
        calls = owner("arm1")
        monkeypatch.setenv("CANOPY_MACHINE_ACCESS", "write")
        get_settings.cache_clear()

        resp = client.post(f"/squirrel/v1/snapshots/{SNAPSHOT_ID}/restore", json={}, headers=_as(WRITER))

        assert resp.status_code == 299
        assert calls == []

    def test_a_capture_is_taken_here(self, client, owner, app_here):
        calls = owner("arm1")

        resp = client.post("/squirrel/v1/snapshots", json={"title": "t", "facility": "a"}, headers=_as(WRITER))

        assert resp.status_code == 299
        assert calls == []

    def test_nothing_on_a_primary(self, bind_keys, owner):
        bind_keys(WRITER)
        calls = owner("arm1")

        TestClient(app, raise_server_exceptions=False).delete("/squirrel/v1/pvs/p1", headers=_as(WRITER))

        assert calls == []


class TestArmTrouble:
    def test_unreachable_means_nothing_was_sent(self, client, owner, httpx_mock: HTTPXMock):
        owner("arm1")
        httpx_mock.add_exception(httpx.ConnectError("connection refused"))

        resp = client.delete("/squirrel/v1/pvs/p1", headers=_as(WRITER))

        assert resp.status_code == 502
        assert resp.json()["code"] == "primary_unavailable"
        assert "Arm 'arm1' could not be reached; nothing was sent" in resp.json()["detail"]

    def test_no_answer_may_have_been_applied(self, client, owner, httpx_mock: HTTPXMock):
        owner("arm1")
        httpx_mock.add_exception(httpx.ReadTimeout("timed out"))

        resp = client.delete("/squirrel/v1/pvs/p1", headers=_as(WRITER))

        assert resp.status_code == 504
        assert resp.json()["code"] == "primary_unavailable"
        assert "may or may not have been applied" in resp.json()["detail"]

    def test_plain_http_needs_the_override(self, client, owner, httpx_mock: HTTPXMock, monkeypatch):
        owner("arm1")
        monkeypatch.setenv("CANOPY_ARMS__ARM1__FORWARD_URL", "http://arm1.example.org/")

        assert "https" in client.delete("/squirrel/v1/pvs/p1", headers=_as(WRITER)).json()["detail"]

        monkeypatch.setenv("CANOPY_FORWARD__ALLOW_INSECURE_HTTP", "true")
        httpx_mock.add_response(method="DELETE", url="http://arm1.example.org/squirrel/v1/pvs/p1", json=True)
        assert client.delete("/squirrel/v1/pvs/p1", headers=_as(WRITER)).status_code == 200


class TestServiceKeyRefused:
    """An arm refusing the head's service key is the head's error, not the caller's."""

    PATCH = f"/squirrel/v1/snapshots/{SNAPSHOT_ID}"

    def _count(self, method: str, route: str) -> float:
        labels = {"method": method, "route": route, "arm": "arm1", "status": "502", "outcome": "key_refused"}
        return REGISTRY.get_sample_value("canopy_forwarded_requests_total", labels) or 0.0

    def test_a_401_names_the_arm_and_its_answer(self, client, owner, httpx_mock: HTTPXMock):
        owner("arm1")
        httpx_mock.add_response(status_code=401, json={"code": "invalid_api_key", "detail": "Invalid API key"})
        before = self._count("PATCH", "/squirrel/v1/snapshots/{snapshot_id}")

        resp = client.patch(self.PATCH, json={"title": "x"}, headers=_as(WRITER))

        assert resp.status_code == 502
        assert resp.json() == {
            "code": "service_key_refused",
            "detail": "Arm 'arm1' refused this head's service key for it (401 invalid_api_key: Invalid API key)",
        }
        assert self._count("PATCH", "/squirrel/v1/snapshots/{snapshot_id}") == before + 1

    def test_a_401_without_a_canopy_body(self, client, owner, httpx_mock: HTTPXMock):
        owner("arm1")
        httpx_mock.add_response(status_code=401, text="Unauthorized")

        resp = client.patch(self.PATCH, json={"title": "x"}, headers=_as(WRITER))

        assert resp.json()["detail"] == "Arm 'arm1' refused this head's service key for it (401: Unauthorized)"

    @pytest.mark.parametrize(
        "code",
        [
            "insufficient_read_access",
            "insufficient_write_access",
            "insufficient_admin_access",
            "insufficient_module_access",
        ],
    )
    def test_a_403_for_the_keys_access(self, client, owner, httpx_mock: HTTPXMock, code):
        owner("arm1")
        httpx_mock.add_response(status_code=403, json={"code": code, "detail": "no"})

        resp = client.patch(self.PATCH, json={"title": "x"}, headers=_as(WRITER))

        assert resp.status_code == 502
        assert resp.json()["code"] == "service_key_refused"
        assert f"(403 {code}: no)" in resp.json()["detail"]

    def test_a_facility_write_the_key_isnt_scoped_for(self, client, owner, httpx_mock: HTTPXMock):
        owner("arm1")
        detail = "API key is not scoped to 'core'"
        httpx_mock.add_response(status_code=403, json={"code": "insufficient_module_access", "detail": detail})

        resp = client.put("/v1/facilities/site-a", json={"name": "A"}, headers=_as(HIERARCHY))

        assert resp.status_code == 502
        assert resp.json()["detail"].endswith(f"(403 insufficient_module_access: {detail})")

    def test_a_facility_refusal_on_a_facility_route_is_the_keys(self, client, owner, httpx_mock: HTTPXMock):
        # The caller passed the wildcard gate here, so only the service key can lack it.
        owner("arm1")
        httpx_mock.add_response(status_code=403, json={"code": "insufficient_facility_access", "detail": "no"})

        resp = client.delete("/v1/facilities/site-a", headers=_as(HIERARCHY))

        assert resp.status_code == 502
        assert resp.json()["code"] == "service_key_refused"

    @pytest.mark.parametrize("code", ["insufficient_facility_access", "forwarded_write_refused", "not_owner"])
    def test_any_other_403_comes_back_as_it_is(self, client, owner, httpx_mock: HTTPXMock, code):
        # A facility refusal here may be the caller's own grant; forwarded_write_refused names forwarding.
        owner("arm1")
        httpx_mock.add_response(status_code=403, json={"code": code, "detail": "no"})

        resp = client.patch(self.PATCH, json={"title": "x"}, headers=_as(WRITER))

        assert (resp.status_code, resp.json()) == (403, {"code": code, "detail": "no"})


class TestReadYourWrites:
    @pytest.fixture
    def waited(self, monkeypatch):
        calls = []

        def fake_wait(applied: bool):
            async def wait_until_applied(commit_lsn: str, timeout_s: float, *, subscription: str) -> bool:
                calls.append((commit_lsn, timeout_s, subscription))
                return applied

            monkeypatch.setattr(middleware, "wait_until_applied", wait_until_applied)
            return calls

        return fake_wait

    def test_applied_in_time_on_the_arms_subscription(self, client, owner, httpx_mock: HTTPXMock, waited):
        owner("arm1")
        calls = waited(True)
        httpx_mock.add_response(json={}, headers={"X-Canopy-Commit-LSN": "0/16B3748"})

        resp = client.delete("/squirrel/v1/pvs/p1", headers=_as(WRITER))

        assert calls == [("0/16B3748", 2.0, "canopy_history_arm1")]
        assert resp.headers["x-canopy-commit-lsn"] == "0/16B3748"
        assert "x-canopy-replica-stale" not in resp.headers

    def test_not_applied_in_time(self, client, owner, httpx_mock: HTTPXMock, waited):
        owner("arm1")
        waited(False)
        httpx_mock.add_response(json={}, headers={"X-Canopy-Commit-LSN": "0/16B3748"})

        resp = client.delete("/squirrel/v1/pvs/p1", headers=_as(WRITER))

        assert resp.status_code == 200
        assert resp.headers["x-canopy-replica-stale"] == "true"

    def test_a_browser_on_another_origin_can_read_both_headers(self, client, owner, httpx_mock: HTTPXMock, waited):
        owner("arm1")
        waited(False)
        httpx_mock.add_response(json={}, headers={"X-Canopy-Commit-LSN": "0/16B3748"})

        resp = client.delete("/squirrel/v1/pvs/p1", headers=_as(WRITER, Origin="https://ui.example.org"))

        exposed = {name.strip().lower() for name in resp.headers["access-control-expose-headers"].split(",")}
        assert {"x-canopy-commit-lsn", "x-canopy-replica-stale"} <= exposed
        assert resp.headers["x-canopy-replica-stale"] == "true"

    def test_no_commit_no_wait(self, client, owner, httpx_mock: HTTPXMock, waited, jobs):
        owner("arm1")
        calls = waited(False)
        httpx_mock.add_response(json={"jobId": "j1", "statusUrl": "/jobs/v1/j1"})

        resp = client.post(f"/squirrel/v1/snapshots/{SNAPSHOT_ID}/restore", json={}, headers=_as(WRITER))

        assert calls == []
        assert "x-canopy-replica-stale" not in resp.headers


class TestArchivedImmutable:
    @pytest.fixture
    def live_here(self, monkeypatch):
        def _set(live: bool):
            async def snapshot_is_live(snapshot_id: str) -> bool:
                return live

            monkeypatch.setattr(owners, "snapshot_is_live", snapshot_is_live)

        return _set

    @pytest.mark.parametrize("method", ["PUT", "PATCH", "DELETE"])
    def test_an_arm_404_for_a_snapshot_still_here_is_history(self, client, owner, live_here, httpx_mock, method):
        owner("arm1")
        live_here(True)
        httpx_mock.add_response(status_code=404, json={"code": "snapshot_not_found", "detail": "gone"})

        resp = client.request(method, f"/squirrel/v1/snapshots/{SNAPSHOT_ID}", json={}, headers=_as(WRITER))

        assert resp.status_code == 409
        assert resp.json()["code"] == "archived_immutable"
        assert "no longer on arm 'arm1'" in resp.json()["detail"]

    def test_a_snapshot_deleted_here_too_stays_a_404(self, client, owner, live_here, httpx_mock):
        owner("arm1")
        live_here(False)
        httpx_mock.add_response(status_code=404, json={"code": "snapshot_not_found"})

        resp = client.delete(f"/squirrel/v1/snapshots/{SNAPSHOT_ID}", headers=_as(WRITER))

        assert resp.json() == {"code": "snapshot_not_found"}

    def test_only_a_snapshots_404(self, client, owner, live_here, httpx_mock):
        owner("arm1")
        live_here(True)
        httpx_mock.add_response(status_code=404, json={"code": "pv_not_found"})

        assert client.delete("/squirrel/v1/pvs/p1", headers=_as(WRITER)).json() == {"code": "pv_not_found"}


class TestJobs:
    """A job a forwarded write started runs on its arm; polling it here asks that arm."""

    @pytest.fixture
    def no_local_job(self, monkeypatch):
        async def get_status(job_id):
            return None

        monkeypatch.setattr("canopy.jobs.gateway.get_status", get_status)

    @pytest.fixture
    def job_on(self, monkeypatch):
        def _set(arm: str | None):
            async def get_forwarded_job_arm(job_id):
                return arm

            monkeypatch.setattr("canopy.jobs.gateway.get_forwarded_job_arm", get_forwarded_job_arm)

        return _set

    def test_a_forwarded_job_is_remembered(self, client, owner, httpx_mock: HTTPXMock, jobs):
        owner("arm1")
        httpx_mock.add_response(json={"jobId": "r1", "statusUrl": "/jobs/v1/r1"})

        resp = client.post(f"/squirrel/v1/snapshots/{SNAPSHOT_ID}/restore", json={}, headers=_as(WRITER))

        assert resp.json()["jobId"] == "r1"
        assert jobs == {"r1": "arm1"}

    def test_an_answer_without_a_job_isnt(self, client, owner, httpx_mock: HTTPXMock, jobs):
        owner("arm1")
        httpx_mock.add_response(status_code=409, json={"jobId": "x", "code": "conflict"})

        client.post(f"/squirrel/v1/snapshots/{SNAPSHOT_ID}/restore", json={}, headers=_as(WRITER))

        assert jobs == {}

    def test_polling_asks_the_arm(self, client, httpx_mock: HTTPXMock, no_local_job, job_on):
        job_on("arm1")
        httpx_mock.add_response(
            method="GET", url=f"{ARM1}/jobs/v1/j1", json={"id": "j1", "status": "running", "progress": 40}
        )

        resp = client.get("/jobs/v1/j1", headers=_as(OPERATOR))

        assert resp.status_code == 200, resp.text
        assert resp.json()["status"] == "running"
        sent = httpx_mock.get_request()
        assert sent.headers["X-API-Key"] == ARM1_SERVICE_KEY
        assert sent.headers["X-Canopy-On-Behalf-Of"] == "op%C3%A9rateur@head"

    def test_unknown_on_the_arm_too(self, client, httpx_mock: HTTPXMock, no_local_job, job_on):
        job_on("arm1")
        httpx_mock.add_response(method="GET", url=f"{ARM1}/jobs/v1/j2", status_code=404, json={})

        assert client.get("/jobs/v1/j2", headers=_as(READER)).json()["code"] == "job_not_found"

    @pytest.mark.parametrize("arm", [None, "arm2"])
    def test_a_job_not_forwarded_here_is_a_404(self, client, no_local_job, job_on, arm):
        job_on(arm)

        assert client.get("/jobs/v1/j3", headers=_as(READER)).status_code == 404

    def test_a_primary_never_asks(self, bind_keys, no_local_job, job_on):
        bind_keys(READER)
        job_on("arm1")

        assert TestClient(app).get("/jobs/v1/j3", headers=_as(READER)).status_code == 404
