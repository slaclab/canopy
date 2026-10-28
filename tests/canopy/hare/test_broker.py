"""PVBroker: on-demand monitors, diff fan-out, refcounted teardown.

Driven deterministically via FakeConnector (emit on demand) + FakeWebSocket,
without starting the background loops — tests call `_drain_nowait` / `_flush`,
and `drain_seeds` to wait for the background seed a subscribe starts.
"""

import asyncio
import contextlib

import pytest

from tests.conftest import wait_for
from canopy.epics.base import PVWriteFailed
from canopy.hare.config import HareConfig
from canopy.contracts.pv import PVValue
from canopy.contracts.sample import AlarmSeverity
from canopy.epics.pv_protocol import InvalidPVName
from tests.canopy.hare.conftest import FakeWebSocket
from canopy.hare.services.broker import PVBroker


class TestResourceCaps:
    def test_event_queue_is_bounded(self, fake_connector):
        broker = PVBroker(fake_connector, HareConfig(event_queue_maxsize=7))
        assert broker._queue.maxsize == 7

    async def test_subscribe_caps_pvs_per_message(self, fake_connector):
        broker = PVBroker(fake_connector, HareConfig(max_pvs_per_message=3))
        broker.add_client("c1", FakeWebSocket())
        await broker.subscribe("c1", [f"PV:{i}" for i in range(10)])
        # Only the first 3 reach the connector as monitors.
        assert len(fake_connector.callbacks) == 3

    async def test_subscribe_caps_total_per_client(self, fake_connector):
        broker = PVBroker(fake_connector, HareConfig(max_subscriptions_per_client=4))
        broker.add_client("c1", FakeWebSocket())
        await broker.subscribe("c1", ["PV:0", "PV:1", "PV:2"])
        await broker.subscribe("c1", ["PV:3", "PV:4", "PV:5"])  # only PV:3 fits
        assert len(broker.registry.client_pvs("c1")) == 4

    async def test_resubscribe_is_idempotent_under_cap(self, fake_connector):
        broker = PVBroker(fake_connector, HareConfig(max_subscriptions_per_client=2))
        broker.add_client("c1", FakeWebSocket())
        await broker.subscribe("c1", ["PV:A", "PV:B"])
        # Re-subscribing existing PVs must not be rejected by the cap.
        await broker.subscribe("c1", ["PV:A", "PV:B"])
        assert broker.registry.client_pvs("c1") == {"PV:A", "PV:B"}


async def test_subscribe_starts_monitor_and_sends_initial(broker, fake_connector):
    ws = FakeWebSocket()
    broker.add_client("c1", ws)
    await broker.subscribe("c1", ["PV:A"])
    await broker.drain_seeds()

    assert "PV:A" in fake_connector.callbacks  # monitor started
    assert ws.sent and ws.sent[0]["type"] == "initial"
    assert "PV:A" in ws.sent[0]["data"]


async def test_subscribe_skips_invalid_names(broker, fake_connector):
    # A malformed name in the batch (bare prefix, empty) must be dropped, not
    # reach the connector, and must not stop the valid PVs from subscribing.
    ws = FakeWebSocket()
    broker.add_client("c1", ws)
    await broker.subscribe("c1", ["PV:A", "pva://", "", "PVA://"])

    assert "PV:A" in fake_connector.callbacks
    assert "pva://" not in fake_connector.callbacks
    assert "" not in fake_connector.callbacks
    assert "PVA://" not in fake_connector.callbacks


async def test_subscribe_notifies_client_of_rejected_names(broker, fake_connector):
    # Rejected names must be surfaced to the client so it can tell them apart
    # from valid PVs that simply haven't produced a value yet.
    ws = FakeWebSocket()
    broker.add_client("c1", ws)
    await broker.subscribe("c1", ["PV:A", "pva://", ""])

    errors = [m for m in ws.sent if m["type"] == "error"]
    assert errors
    err = errors[0]
    assert err["code"] == "invalid_pv_name"
    assert set(err["pvNames"]) == {"pva://", ""}


async def test_subscribe_all_valid_sends_no_error(broker, fake_connector):
    ws = FakeWebSocket()
    broker.add_client("c1", ws)
    await broker.subscribe("c1", ["PV:A", "PV:B"])

    assert not any(m["type"] == "error" for m in ws.sent)


async def test_get_value_rejects_invalid_name(broker):
    with pytest.raises(InvalidPVName):
        await broker.get_value("pva://")


async def test_put_rejects_invalid_name(broker):
    with pytest.raises(InvalidPVName):
        await broker.put("", 1.0)


async def test_event_flushed_as_diff(broker, fake_connector):
    ws = FakeWebSocket()
    broker.add_client("c1", ws)
    await broker.subscribe("c1", ["PV:A"])
    ws.sent.clear()

    fake_connector.emit("PV:A", 1.23, severity=AlarmSeverity.MINOR, ts=99)
    await broker._drain_nowait()
    await broker._flush()

    diffs = [m for m in ws.sent if m["type"] == "diff"]
    assert diffs
    payload = diffs[-1]["data"]["PV:A"]
    assert payload["value"] == 1.23
    assert payload["severity"] == "MINOR"
    assert payload["tsNs"] == 99  # camelCase on the wire


async def test_diff_only_goes_to_subscribers(broker, fake_connector):
    ws1, ws2 = FakeWebSocket(), FakeWebSocket()
    broker.add_client("c1", ws1)
    broker.add_client("c2", ws2)
    await broker.subscribe("c1", ["PV:A"])
    await broker.subscribe("c2", ["PV:B"])
    ws1.sent.clear()
    ws2.sent.clear()

    fake_connector.emit("PV:A", 5.0)
    await broker._drain_nowait()
    await broker._flush()

    assert any(m["type"] == "diff" for m in ws1.sent)
    assert not any(m["type"] == "diff" for m in ws2.sent)


async def test_refcount_keeps_monitor_until_last_subscriber_leaves(broker, fake_connector):
    ws1, ws2 = FakeWebSocket(), FakeWebSocket()
    broker.add_client("c1", ws1)
    broker.add_client("c2", ws2)
    await broker.subscribe("c1", ["PV:A"])
    await broker.subscribe("c2", ["PV:A"])

    await broker.unsubscribe("c1", ["PV:A"])
    assert "PV:A" in fake_connector.callbacks  # c2 still subscribed

    await broker.unsubscribe("c2", ["PV:A"])
    assert "PV:A" not in fake_connector.callbacks  # monitor closed


async def test_remove_client_closes_its_monitors(broker, fake_connector):
    ws = FakeWebSocket()
    broker.add_client("c1", ws)
    await broker.subscribe("c1", ["PV:A"])
    await broker.remove_client("c1")
    assert "PV:A" not in fake_connector.callbacks


async def test_get_value_falls_back_to_one_shot(broker, fake_connector):
    # Not subscribed -> broker issues a one-shot connector.get
    value = await broker.get_value("PV:Z")
    assert value.pv == "PV:Z"
    assert value.connected is True


async def test_initial_frame_includes_metadata(broker, fake_connector):
    ws = FakeWebSocket()
    broker.add_client("c1", ws)
    await broker.subscribe("c1", ["PV:A"])
    await broker.drain_seeds()

    initial = ws.sent[0]
    assert initial["type"] == "initial"
    assert "meta" in initial
    meta = initial["meta"]["PV:A"]
    assert meta["valueKind"] == "scalar"  # camelCase on the wire
    assert meta["units"] == "mm"


async def test_diff_frame_carries_no_metadata(broker, fake_connector):
    ws = FakeWebSocket()
    broker.add_client("c1", ws)
    await broker.subscribe("c1", ["PV:A"])
    ws.sent.clear()

    fake_connector.emit("PV:A", 1.0)
    await broker._drain_nowait()
    await broker._flush()

    diff = [m for m in ws.sent if m["type"] == "diff"][-1]
    assert "meta" not in diff
    assert diff["data"]["PV:A"]["valueKind"] == "scalar"


async def test_get_describe_falls_back_to_one_shot(broker):
    meta = await broker.get_describe("PV:Q")
    assert meta.pv == "PV:Q"
    assert meta.value_kind.value == "scalar"


async def test_put_delegates_to_connector(broker, fake_connector):
    await broker.put("PV:A", 42.0)
    assert fake_connector.puts == [("PV:A", 42.0, True, None)]


async def test_put_passes_wait_and_timeout(broker, fake_connector):
    await broker.put("PV:A", 1, wait=False, timeout=0.5)
    assert fake_connector.puts == [("PV:A", 1, False, 0.5)]


async def test_put_propagates_connector_failure(broker, fake_connector):
    fake_connector.put_error = PVWriteFailed("boom")
    with pytest.raises(PVWriteFailed):
        await broker.put("PV:A", 1.0)


async def test_put_invalidates_cache_so_read_is_fresh(broker, fake_connector):
    # A monitored PV's value is cached; after a put, get_value must not serve the
    # stale cache -- it re-reads through the connector so put-then-read is consistent.
    broker.add_client("c1", FakeWebSocket())
    await broker.subscribe("c1", ["PV:A"])
    await broker.drain_seeds()
    fake_connector.emit("PV:A", 1.0)
    await broker._drain_nowait()
    assert (await broker.get_value("PV:A")).value == 1.0  # served from cache

    await broker.put("PV:A", 9.9)
    assert "PV:A" not in broker._latest  # cache invalidated
    assert (await broker.get_value("PV:A")).value == 0.0  # fresh one-shot connector.get


class TestLoopSupervision:
    async def test_flush_loop_survives_iteration_error(self, broker, monkeypatch):
        # A single failing flush must not kill the loop: fan-out has to keep
        # working for every client after a transient error.
        broker.add_client("c1", FakeWebSocket())
        await broker.subscribe("c1", ["PV:A"])

        calls = {"n": 0}
        real_flush = broker._flush

        async def flaky_flush():
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("transient flush error")
            await real_flush()

        monkeypatch.setattr(broker, "_flush", flaky_flush)
        broker._running = True
        task = asyncio.create_task(broker._flush_loop())
        try:
            assert await wait_for(lambda: calls["n"] >= 2)  # kept flushing after the error
            assert not task.done()  # survived the first-iteration error
        finally:
            broker._running = False
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    async def test_supervisor_restarts_crashed_loop_when_running(self, broker):
        broker._running = True

        async def crash():
            raise RuntimeError("scaffolding boom")

        dead = asyncio.create_task(crash(), name="heartbeat")
        with contextlib.suppress(BaseException):
            await dead
        broker._tasks = [dead]

        broker._supervise_loop(dead)  # simulate the done-callback firing

        assert len(broker._tasks) == 1
        replacement = broker._tasks[0]
        assert replacement is not dead
        assert not replacement.done()

        broker._running = False
        replacement.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await replacement

    async def test_supervisor_does_not_restart_when_stopped(self, broker):
        broker._running = False

        async def crash():
            raise RuntimeError("boom")

        dead = asyncio.create_task(crash(), name="flush")
        with contextlib.suppress(BaseException):
            await dead
        broker._tasks = [dead]

        broker._supervise_loop(dead)

        assert broker._tasks == [dead]  # no restart during shutdown


def _record_calls(connector, method: str, *, failures: int = 0) -> list[str]:
    """Wrap a connector coroutine method so every call is recorded and the first few raise.

    Parameters
    ----------
    connector : FakeConnector
        Connector whose method is replaced on the instance.
    method : str
        Name of the coroutine method to wrap, such as ``"get"`` or ``"describe"``.
    failures : int, optional
        Number of leading calls that raise `TimeoutError`; later calls delegate
        to the original method.

    Returns
    -------
    list[str]
        PV names passed to the wrapped method, one entry per call in call order.
    """
    original = getattr(connector, method)
    calls: list[str] = []

    async def wrapped(pv, **kwargs):
        """Record the call, then raise or delegate to the original method.

        Parameters
        ----------
        pv : str
            PV name the broker passed.
        **kwargs
            Keyword arguments forwarded to the original method.

        Returns
        -------
        object
            The original method's result.

        Raises
        ------
        TimeoutError
            For each of the first ``failures`` calls.
        """
        calls.append(pv)
        if len(calls) <= failures:
            raise TimeoutError(f"{method} not ready")
        return await original(pv, **kwargs)

    setattr(connector, method, wrapped)
    return calls


def _initial_frames(ws: FakeWebSocket) -> list[dict]:
    """Collect the ``initial`` frames a client received.

    Parameters
    ----------
    ws : FakeWebSocket
        Client channel whose sent frames are inspected.

    Returns
    -------
    list[dict]
        Every frame of type ``initial``, in send order.
    """
    return [m for m in ws.sent if m["type"] == "initial"]


def _disconnected(pv: str) -> PVValue:
    """Build the ``connected: false`` value a connector returns for a channel whose read timed out."""
    return PVValue(pv=pv, value=None, severity=AlarmSeverity.INVALID, ts_ns=0, connected=False)


def _gate_get(connector, gated: set[str], *, dead: bool = False) -> tuple[asyncio.Event, list[str]]:
    """Make ``connector.get`` hold the given PVs' reads until the returned gate opens, recording every call.

    Stands in for a CA channel: a PV not in ``gated`` answers at once; a gated
    PV answers only once the gate opens, with ``connected: false`` when ``dead``
    is set (a read that timed out) and its normal value otherwise.

    Parameters
    ----------
    connector : FakeConnector
        Connector whose ``get`` is replaced on the instance.
    gated : set[str]
        PV names whose reads block on the gate.
    dead : bool, optional
        Whether a gated read reports the channel disconnected when released.

    Returns
    -------
    tuple[asyncio.Event, list[str]]
        The gate to open, and the PV names passed to ``get`` in call order.
    """
    original = connector.get
    gate = asyncio.Event()
    calls: list[str] = []

    async def gated_get(pv, **kwargs):
        calls.append(pv)
        if pv in gated:
            await gate.wait()
            if dead:
                return _disconnected(pv)
        return await original(pv, **kwargs)

    connector.get = gated_get
    return gate, calls


class _GatedWebSocket(FakeWebSocket):
    """A client whose first send blocks until ``release`` is set, so a fan-out can be caught mid-send."""

    def __init__(self) -> None:
        super().__init__()
        self.release = asyncio.Event()
        self.blocked = False

    async def send_json(self, data) -> None:
        if not self.blocked:
            self.blocked = True
            await self.release.wait()
        await super().send_json(data)


def _initials_with_meta(ws: FakeWebSocket, pv: str) -> list[dict]:
    """Return the ``initial`` frames a client received that carry metadata for ``pv``."""
    return [m for m in _initial_frames(ws) if pv in m["meta"]]


async def test_subscribe_returns_before_the_seed_read_answers(fake_connector):
    """``subscribe`` returns while the PV's read is still pending; the value arrives later in its own ``initial``."""
    broker = PVBroker(fake_connector, HareConfig())
    gate, gets = _gate_get(fake_connector, {"PV:SLOW"})
    ws = FakeWebSocket()
    broker.add_client("c1", ws)

    await asyncio.wait_for(broker.subscribe("c1", ["PV:SLOW"]), 0.5)
    await asyncio.sleep(0)
    assert gets == ["PV:SLOW"]
    assert not _initial_frames(ws)

    gate.set()
    await broker.drain_seeds()
    (initial,) = _initial_frames(ws)
    assert "PV:SLOW" in initial["data"]
    assert "PV:SLOW" in initial["meta"]


async def test_dead_pv_in_a_batch_does_not_delay_the_live_pvs(fake_connector):
    """A live PV's value and metadata arrive while a dead PV in the same batch is still pending.

    The dead PV follows in its own frame as ``connected: false`` without
    metadata once its read gives up, and is never described.
    """
    broker = PVBroker(fake_connector, HareConfig())
    gate, _ = _gate_get(fake_connector, {"PV:DEAD"}, dead=True)
    describes = _record_calls(fake_connector, "describe")
    ws = FakeWebSocket()
    broker.add_client("c1", ws)

    await broker.subscribe("c1", ["PV:LIVE", "PV:DEAD"])
    assert await wait_for(lambda: _initials_with_meta(ws, "PV:LIVE"), timeout=1.0)
    assert not any("PV:DEAD" in m["data"] for m in _initial_frames(ws))

    gate.set()
    await broker.drain_seeds()
    (dead_frame,) = [m for m in _initial_frames(ws) if "PV:DEAD" in m["data"]]
    assert dead_frame["data"]["PV:DEAD"]["connected"] is False
    assert "PV:DEAD" not in dead_frame["meta"]
    assert describes == ["PV:LIVE"]


async def test_dead_pv_in_an_earlier_frame_does_not_delay_a_later_subscribe(fake_connector):
    """A client's later subscribe is answered while a dead PV from an earlier frame is still pending."""
    broker = PVBroker(fake_connector, HareConfig())
    gate, _ = _gate_get(fake_connector, {"PV:DEAD"}, dead=True)
    ws = FakeWebSocket()
    broker.add_client("c1", ws)

    await broker.subscribe("c1", ["PV:DEAD"])
    await asyncio.wait_for(broker.subscribe("c1", ["PV:LIVE"]), 0.5)
    assert await wait_for(lambda: _initials_with_meta(ws, "PV:LIVE"), timeout=1.0)

    gate.set()
    await broker.drain_seeds()


async def test_seed_read_uses_the_seed_timeout(fake_connector):
    """The seed's read passes ``seed_timeout_s`` instead of leaving the connector to its own timeout."""
    broker = PVBroker(fake_connector, HareConfig(seed_timeout_s=0.25))
    original = fake_connector.get
    timeouts: list[float | None] = []

    async def recording_get(pv, *, timeout=None):
        timeouts.append(timeout)
        return await original(pv, timeout=timeout)

    fake_connector.get = recording_get
    broker.add_client("c1", FakeWebSocket())
    await broker.subscribe("c1", ["PV:A"])
    await broker.drain_seeds()

    assert timeouts == [0.25]


async def test_disconnected_read_skips_describe_and_is_served_from_cache_within_backoff(fake_connector):
    """A read answered ``connected: false`` is cached, sets the retry deadline and skips the describe.

    A subscriber inside the backoff window gets the disconnected value from the
    cache without another read.
    """
    broker = PVBroker(fake_connector, HareConfig(seed_retry_backoff_s=3600))
    gate, gets = _gate_get(fake_connector, {"PV:DEAD"}, dead=True)
    gate.set()
    describes = _record_calls(fake_connector, "describe")

    broker.add_client("c1", FakeWebSocket())
    await broker.subscribe("c1", ["PV:DEAD"])
    await broker.drain_seeds()
    assert broker._latest["PV:DEAD"].connected is False
    assert "PV:DEAD" not in broker._meta
    assert "PV:DEAD" in broker._seed_retry_after
    assert describes == []

    late = FakeWebSocket()
    broker.add_client("c2", late)
    await broker.subscribe("c2", ["PV:DEAD"])
    await broker.drain_seeds()
    assert gets == ["PV:DEAD"]
    (initial,) = _initial_frames(late)
    assert initial["data"]["PV:DEAD"]["connected"] is False


async def test_disconnected_seed_is_re_read_after_backoff(fake_connector):
    """A PV whose seed found it disconnected is read again by a subscribe after the backoff, and described once it answers.

    Every subscriber then receives an ``initial`` with the connected value and
    its metadata.
    """
    broker = PVBroker(fake_connector, HareConfig(seed_retry_backoff_s=0))
    original = fake_connector.get
    gets: list[str] = []

    async def flaky_get(pv, **kwargs):
        gets.append(pv)
        if len(gets) == 1:
            return _disconnected(pv)
        return await original(pv, **kwargs)

    fake_connector.get = flaky_get
    describes = _record_calls(fake_connector, "describe")

    first = FakeWebSocket()
    broker.add_client("c1", first)
    await broker.subscribe("c1", ["PV:FLAKY"])
    await broker.drain_seeds()
    assert broker._latest["PV:FLAKY"].connected is False
    assert describes == []

    broker.add_client("c2", FakeWebSocket())
    await broker.subscribe("c2", ["PV:FLAKY"])
    await broker.drain_seeds()

    assert gets == ["PV:FLAKY", "PV:FLAKY"]
    assert describes == ["PV:FLAKY"]
    assert broker._latest["PV:FLAKY"].connected is True
    assert "PV:FLAKY" not in broker._seed_retry_after
    (with_meta,) = _initials_with_meta(first, "PV:FLAKY")
    assert with_meta["data"]["PV:FLAKY"]["connected"] is True


async def test_channel_connecting_after_a_dead_seed_gets_its_metadata(fake_connector):
    """The first connected monitor update of a PV whose seed found it dead starts its describe, despite the backoff.

    The subscriber then receives an ``initial`` carrying both the value and the
    metadata; the value itself is not read again.
    """
    broker = PVBroker(fake_connector, HareConfig(seed_retry_backoff_s=3600))
    gate, gets = _gate_get(fake_connector, {"PV:DEAD"}, dead=True)
    gate.set()
    describes = _record_calls(fake_connector, "describe")
    ws = FakeWebSocket()
    broker.add_client("c1", ws)
    await broker.subscribe("c1", ["PV:DEAD"])
    await broker.drain_seeds()
    assert describes == []

    fake_connector.emit("PV:DEAD", 1.5)
    await broker._drain_nowait()
    await broker.drain_seeds()

    assert gets == ["PV:DEAD"]
    assert describes == ["PV:DEAD"]
    assert "PV:DEAD" not in broker._seed_retry_after
    (with_meta,) = _initials_with_meta(ws, "PV:DEAD")
    assert with_meta["data"]["PV:DEAD"]["value"] == 1.5


async def test_connect_edge_during_a_pending_initial_send_is_not_lost(fake_connector):
    """A channel that connects while its dead seed's ``initial`` send is pending still gets its metadata.

    The running task cannot be started again, so it seeds once more after the
    send instead, and the describe is not dropped.
    """
    broker = PVBroker(fake_connector, HareConfig(seed_retry_backoff_s=3600))
    gate, gets = _gate_get(fake_connector, {"PV:DEAD"}, dead=True)
    gate.set()
    describes = _record_calls(fake_connector, "describe")
    ws = _GatedWebSocket()
    broker.add_client("c1", ws)

    await broker.subscribe("c1", ["PV:DEAD"])
    assert await wait_for(lambda: ws.blocked)
    fake_connector.emit("PV:DEAD", 2.5)
    await broker._drain_nowait()
    assert describes == []

    ws.release.set()
    await asyncio.wait_for(broker.drain_seeds(), 1.0)

    assert gets == ["PV:DEAD"]
    assert describes == ["PV:DEAD"]
    (with_meta,) = _initials_with_meta(ws, "PV:DEAD")
    assert with_meta["data"]["PV:DEAD"]["value"] == 2.5
    assert not broker._seed_again


async def test_client_unsubscribing_during_fan_out_receives_no_initial(fake_connector):
    """A client that unsubscribes while an earlier client's send is pending gets nothing from that fan-out."""
    broker = PVBroker(fake_connector, HareConfig())
    gate, _ = _gate_get(fake_connector, {"PV:SEED"})
    first, second = _GatedWebSocket(), _GatedWebSocket()
    broker.add_client("c1", first)
    broker.add_client("c2", second)
    await broker.subscribe("c1", ["PV:SEED"])
    await broker.subscribe("c2", ["PV:SEED"])

    gate.set()
    assert await wait_for(lambda: first.blocked or second.blocked)
    stayer, leaver, leaver_id = (first, second, "c2") if first.blocked else (second, first, "c1")
    await broker.unsubscribe(leaver_id, ["PV:SEED"])
    stayer.release.set()
    # A send to the leaver would block on its unreleased gate and time this out.
    await asyncio.wait_for(broker.drain_seeds(), 1.0)

    assert "PV:SEED" in _initial_frames(stayer)[0]["data"]
    assert not _initial_frames(leaver)
    leaver.release.set()


async def test_connected_updates_do_not_repeat_a_failed_describe(fake_connector):
    """Only the connect edge starts a describe; further connected updates inside the backoff window do not."""
    broker = PVBroker(fake_connector, HareConfig(seed_retry_backoff_s=3600))
    describes = _record_calls(fake_connector, "describe", failures=5)
    broker.add_client("c1", FakeWebSocket())
    await broker.subscribe("c1", ["PV:A"])
    await broker.drain_seeds()
    assert describes == ["PV:A"]

    for value in (1.0, 2.0, 3.0):
        fake_connector.emit("PV:A", value)
        await broker._drain_nowait()
        await broker.drain_seeds()

    assert describes == ["PV:A"]


async def test_monitor_update_during_the_seed_read_is_kept(fake_connector):
    """A monitor update that lands while the seed's read is pending is newer and is not overwritten by the read."""
    broker = PVBroker(fake_connector, HareConfig())
    gate, _ = _gate_get(fake_connector, {"PV:A"})
    broker.add_client("c1", FakeWebSocket())
    await broker.subscribe("c1", ["PV:A"])
    await asyncio.sleep(0)

    fake_connector.emit("PV:A", 7.0)
    await broker._drain_nowait()
    gate.set()
    await broker.drain_seeds()

    assert broker._latest["PV:A"].value == 7.0
    assert "PV:A" in broker._meta


async def test_failed_seed_is_retried_for_late_subscribers(fake_connector):
    """A later subscribe of an already-monitored PV retries a seed whose ``get`` failed.

    With a zero backoff window, the retry's result is fanned out to every
    subscriber of the PV, so the first client also gets the value its own seed
    could not fetch.
    """
    broker = PVBroker(fake_connector, HareConfig(seed_retry_backoff_s=0))
    _record_calls(fake_connector, "get", failures=1)

    first = FakeWebSocket()
    broker.add_client("c1", first)
    await broker.subscribe("c1", ["PV:SEED"])
    await broker.drain_seeds()
    assert not _initial_frames(first)

    late = FakeWebSocket()
    broker.add_client("c2", late)
    await broker.subscribe("c2", ["PV:SEED"])
    await broker.drain_seeds()
    for ws in (first, late):
        (initial,) = _initial_frames(ws)
        assert "PV:SEED" in initial["data"]
        assert "PV:SEED" in initial["meta"]


async def test_failed_seed_is_not_retried_within_backoff(fake_connector):
    """A subscribe inside the backoff window does not call ``get`` again for a PV whose seed failed.

    With no value to show the channel connected, the describe is skipped too,
    so nothing is cached and the late subscriber gets no ``initial`` for it.
    """
    broker = PVBroker(fake_connector, HareConfig(seed_retry_backoff_s=3600))
    gets = _record_calls(fake_connector, "get", failures=1)
    describes = _record_calls(fake_connector, "describe")

    broker.add_client("c1", FakeWebSocket())
    await broker.subscribe("c1", ["PV:SEED"])
    await broker.drain_seeds()

    late = FakeWebSocket()
    broker.add_client("c2", late)
    await broker.subscribe("c2", ["PV:SEED"])
    await broker.drain_seeds()

    assert gets == ["PV:SEED"]
    assert describes == []
    assert not _initial_frames(late)


async def test_failed_seed_is_retried_after_backoff_and_success_clears_deadline(fake_connector):
    """A failed seed sets a retry deadline; a retry after the window that succeeds clears it."""
    broker = PVBroker(fake_connector, HareConfig(seed_retry_backoff_s=0))
    gets = _record_calls(fake_connector, "get", failures=1)

    broker.add_client("c1", FakeWebSocket())
    await broker.subscribe("c1", ["PV:SEED"])
    await broker.drain_seeds()
    assert "PV:SEED" in broker._seed_retry_after

    broker.add_client("c2", FakeWebSocket())
    await broker.subscribe("c2", ["PV:SEED"])
    await broker.drain_seeds()

    assert gets == ["PV:SEED", "PV:SEED"]
    assert "PV:SEED" in broker._latest
    assert "PV:SEED" not in broker._seed_retry_after


@pytest.mark.parametrize(
    ("backoff_s", "expected_describes"),
    [pytest.param(3600, 1, id="within-backoff"), pytest.param(0, 2, id="after-backoff")],
)
async def test_describe_failure_retries_only_describe_after_backoff(fake_connector, backoff_s, expected_describes):
    """A PV whose value seeded but whose ``describe`` failed re-runs only ``describe``, and only after the window.

    The cached value is never fetched again; ``describe`` is retried by a later
    subscribe once the backoff has elapsed and skipped while it has not.
    """
    broker = PVBroker(fake_connector, HareConfig(seed_retry_backoff_s=backoff_s))
    gets = _record_calls(fake_connector, "get")
    describes = _record_calls(fake_connector, "describe", failures=1)

    broker.add_client("c1", FakeWebSocket())
    await broker.subscribe("c1", ["PV:META"])
    await broker.drain_seeds()
    broker.add_client("c2", FakeWebSocket())
    await broker.subscribe("c2", ["PV:META"])
    await broker.drain_seeds()

    assert gets == ["PV:META"]
    assert describes == ["PV:META"] * expected_describes


async def test_stop_monitors_clears_seed_retry_deadline(fake_connector):
    """Releasing a PV drops its seed retry deadline and seed lock, so re-subscribing seeds it at once despite the window."""
    broker = PVBroker(fake_connector, HareConfig(seed_retry_backoff_s=3600))
    gets = _record_calls(fake_connector, "get", failures=1)

    broker.add_client("c1", FakeWebSocket())
    await broker.subscribe("c1", ["PV:SEED"])
    await broker.drain_seeds()
    assert "PV:SEED" in broker._seed_retry_after
    assert "PV:SEED" in broker._seed_locks

    await broker.unsubscribe("c1", ["PV:SEED"])
    assert "PV:SEED" not in broker._seed_retry_after
    assert "PV:SEED" not in broker._seed_locks

    again = FakeWebSocket()
    broker.add_client("c2", again)
    await broker.subscribe("c2", ["PV:SEED"])
    await broker.drain_seeds()

    assert gets == ["PV:SEED", "PV:SEED"]
    assert "PV:SEED" in _initial_frames(again)[0]["data"]


async def test_concurrent_subscribes_share_one_seed_attempt(fake_connector):
    """Two clients subscribing an unseeded PV while its read is pending share one ``get`` and ``describe``.

    Both receive the result when it lands.
    """
    broker = PVBroker(fake_connector, HareConfig(seed_retry_backoff_s=3600))
    gate, gets = _gate_get(fake_connector, {"PV:SEED"})
    describes = _record_calls(fake_connector, "describe")

    first, second = FakeWebSocket(), FakeWebSocket()
    broker.add_client("c1", first)
    broker.add_client("c2", second)
    await broker.subscribe("c1", ["PV:SEED"])
    await broker.subscribe("c2", ["PV:SEED"])
    assert len(broker._seed_tasks) == 1

    gate.set()
    await broker.drain_seeds()

    assert gets == ["PV:SEED"]
    assert describes == ["PV:SEED"]
    for ws in (first, second):
        (initial,) = _initial_frames(ws)
        assert "PV:SEED" in initial["data"]
        assert "PV:SEED" in initial["meta"]


async def test_unsubscribe_during_a_pending_seed_leaves_the_leaver_nothing(fake_connector):
    """A client that unsubscribes while the seed is pending gets no frame when it completes; the other subscriber does."""
    broker = PVBroker(fake_connector, HareConfig())
    gate, _ = _gate_get(fake_connector, {"PV:SEED"})
    stayer, leaver = FakeWebSocket(), FakeWebSocket()
    broker.add_client("c1", stayer)
    broker.add_client("c2", leaver)
    await broker.subscribe("c1", ["PV:SEED"])
    await broker.subscribe("c2", ["PV:SEED"])

    await broker.unsubscribe("c2", ["PV:SEED"])
    gate.set()
    await broker.drain_seeds()

    assert "PV:SEED" in _initial_frames(stayer)[0]["data"]
    assert not _initial_frames(leaver)
    assert "PV:SEED" not in broker.registry.client_pvs("c2")


async def test_release_cancels_the_pending_seed(fake_connector):
    """Releasing a PV cancels its pending seed, so nothing is cached and the next subscriber seeds afresh."""
    broker = PVBroker(fake_connector, HareConfig(seed_retry_backoff_s=3600))
    gate, gets = _gate_get(fake_connector, {"PV:SEED"})

    broker.add_client("c1", FakeWebSocket())
    await broker.subscribe("c1", ["PV:SEED"])
    await asyncio.sleep(0)
    task = broker._seed_tasks["PV:SEED"]
    await broker.unsubscribe("c1", ["PV:SEED"])
    await asyncio.wait([task], timeout=1.0)

    assert task.cancelled()
    assert "PV:SEED" not in broker._seed_tasks
    assert "PV:SEED" not in broker._latest
    assert "PV:SEED" not in broker._meta
    assert "PV:SEED" not in broker._seed_retry_after

    gate.set()
    later = FakeWebSocket()
    broker.add_client("c2", later)
    await broker.subscribe("c2", ["PV:SEED"])
    await broker.drain_seeds()
    assert gets == ["PV:SEED", "PV:SEED"]
    assert "PV:SEED" in _initial_frames(later)[0]["data"]


def _uncancellable(gate: asyncio.Event):
    """Wait on ``gate`` even when cancelled, like a connector read that cannot be aborted mid-flight."""

    async def wait() -> None:
        try:
            await gate.wait()
        except asyncio.CancelledError:
            await gate.wait()

    return wait


async def test_seed_finishing_after_release_leaves_no_cached_state(fake_connector):
    """A seed whose read outlives its PV's release caches nothing once it finishes.

    The read ignores the cancellation the release sends, standing in for a
    connector call that cannot be aborted; the stale seed lock keeps its late
    result out of the cache, and the next subscriber seeds the PV afresh.
    """
    broker = PVBroker(fake_connector, HareConfig(seed_retry_backoff_s=3600))
    original_get = fake_connector.get
    release = asyncio.Event()
    gets: list[str] = []

    async def stubborn_get(pv, **kwargs):
        gets.append(pv)
        await _uncancellable(release)()
        return await original_get(pv, **kwargs)

    fake_connector.get = stubborn_get

    broker.add_client("c1", FakeWebSocket())
    await broker.subscribe("c1", ["PV:SEED"])
    await asyncio.sleep(0)
    orphaned = broker._seed_tasks["PV:SEED"]
    await broker.unsubscribe("c1", ["PV:SEED"])
    release.set()
    await asyncio.wait_for(orphaned, 1.0)

    assert "PV:SEED" not in broker._latest
    assert "PV:SEED" not in broker._meta
    assert "PV:SEED" not in broker._seed_retry_after

    later = FakeWebSocket()
    broker.add_client("c2", later)
    await broker.subscribe("c2", ["PV:SEED"])
    await broker.drain_seeds()
    assert gets == ["PV:SEED", "PV:SEED"]
    assert "PV:SEED" in _initial_frames(later)[0]["data"]


async def test_orphaned_seed_finishing_last_keeps_the_newer_seed(fake_connector):
    """A released PV's seed that finishes after a re-subscribe's seed does not overwrite it.

    The first read ignores its cancellation and is released only after the
    second has completed, so the orphaned attempt finishes last; the cache
    keeps the second value.
    """
    broker = PVBroker(fake_connector, HareConfig(seed_retry_backoff_s=3600))
    gates = [asyncio.Event(), asyncio.Event()]
    calls = 0

    async def gated_get(pv, **kwargs):
        nonlocal calls
        index = calls
        calls += 1
        await _uncancellable(gates[index])()
        return PVValue(pv=pv, value=float(index + 1), severity=AlarmSeverity.NO_ALARM, ts_ns=index + 1, connected=True)

    fake_connector.get = gated_get

    broker.add_client("c1", FakeWebSocket())
    await broker.subscribe("c1", ["PV:SEED"])
    await asyncio.sleep(0)
    orphaned = broker._seed_tasks["PV:SEED"]
    await broker.unsubscribe("c1", ["PV:SEED"])
    broker.add_client("c2", FakeWebSocket())
    await broker.subscribe("c2", ["PV:SEED"])
    await asyncio.sleep(0)
    assert calls == 2

    gates[1].set()
    await broker.drain_seeds()
    assert broker._latest["PV:SEED"].value == 2.0

    gates[0].set()
    await asyncio.wait_for(orphaned, 1.0)
    assert broker._latest["PV:SEED"].value == 2.0


async def test_seed_queued_for_a_slot_skips_a_pv_released_meanwhile(fake_connector):
    """A seed still waiting for a ``seed_concurrency`` slot when its PV is released is cancelled and caches nothing.

    With one slot, PV:B's seed queues behind PV:A's pending read and PV:B is
    released before the slot frees, so the next subscriber of PV:B seeds it afresh.
    """
    broker = PVBroker(fake_connector, HareConfig(seed_concurrency=1, seed_retry_backoff_s=3600))
    gate, gets = _gate_get(fake_connector, {"PV:A"})

    broker.add_client("c1", FakeWebSocket())
    await broker.subscribe("c1", ["PV:A", "PV:B"])
    await asyncio.sleep(0)
    assert gets == ["PV:A"]
    await broker.unsubscribe("c1", ["PV:B"])
    gate.set()
    await broker.drain_seeds()

    assert gets == ["PV:A"]
    assert "PV:B" not in broker._latest
    assert "PV:B" not in broker._meta
    assert "PV:B" not in broker._seed_locks
    assert "PV:B" not in broker._seed_tasks

    later = FakeWebSocket()
    broker.add_client("c2", later)
    await broker.subscribe("c2", ["PV:B"])
    await broker.drain_seeds()
    assert gets == ["PV:A", "PV:B"]
    assert "PV:B" in _initial_frames(later)[0]["data"]


async def test_stop_cancels_pending_seeds(fake_connector):
    """``stop`` cancels seeds still in flight rather than waiting for their reads."""
    broker = PVBroker(fake_connector, HareConfig())
    _gate_get(fake_connector, {"PV:SLOW"})
    broker.add_client("c1", FakeWebSocket())
    await broker.subscribe("c1", ["PV:SLOW"])
    await asyncio.sleep(0)
    task = broker._seed_tasks["PV:SLOW"]

    await asyncio.wait_for(broker.stop(), 1.0)

    assert task.cancelled()
    assert not broker._seed_tasks


async def test_monitor_update_queued_before_release_is_not_cached(broker, fake_connector):
    """A monitor update still queued when its PV is released is discarded when drained.

    Nothing is cached or buffered for the released PV, so a later read goes to
    the connector instead of serving the update from before the release.
    """
    broker.add_client("c1", FakeWebSocket())
    await broker.subscribe("c1", ["PV:A"])
    fake_connector.emit("PV:A", 7.0)
    await broker.unsubscribe("c1", ["PV:A"])
    await broker._drain_nowait()

    assert "PV:A" not in broker._latest
    assert "PV:A" not in broker._buffer
    assert (await broker.get_value("PV:A")).value == 0.0


async def test_released_monitor_update_delivered_after_resubscribe_is_not_cached(broker, fake_connector):
    """An update from a released monitor that arrives after a re-subscribe does not replace the fresh seed.

    The EPICS backend hands updates to the loop with ``call_soon_threadsafe``, so
    the old monitor's callback can still run once a new subscribe has seeded the
    PV and started a new monitor. Only the new monitor's updates are cached.
    """
    broker.add_client("c1", FakeWebSocket())
    await broker.subscribe("c1", ["PV:A"])
    await broker.drain_seeds()
    released_callback = fake_connector.callbacks["PV:A"]
    await broker.unsubscribe("c1", ["PV:A"])
    broker.add_client("c2", FakeWebSocket())
    await broker.subscribe("c2", ["PV:A"])
    await broker.drain_seeds()

    released_callback(PVValue(pv="PV:A", value=7.0, severity=AlarmSeverity.NO_ALARM, ts_ns=2, connected=True))
    await broker._drain_nowait()
    assert broker._latest["PV:A"].value == 0.0
    assert "PV:A" not in broker._buffer

    fake_connector.emit("PV:A", 3.0)
    await broker._drain_nowait()
    assert broker._latest["PV:A"].value == 3.0
    assert broker._buffer["PV:A"].value == 3.0
