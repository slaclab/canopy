"""PVBroker — the in-process live-PV broker.

Owns the connector, the subscription registry, a latest-value cache, and three
background loops (consume / flush / heartbeat). Monitors are started and stopped
on demand: the first subscriber to a PV starts an EPICS monitor, the last to
leave stops it. Updates are buffered and flushed as diffs every
``ws_batch_interval_ms`` to the clients subscribed to each PV. A newly
subscribed PV is seeded (one-shot read + describe) by a background task of its
own, so a subscribe never waits on the connector.
"""

import time
import asyncio
import logging
from typing import Protocol, runtime_checkable
from collections import defaultdict
from collections.abc import Callable, Iterable, Awaitable

from canopy.epics.base import PVConnector, Subscription
from canopy.core.errors import ErrorCode
from canopy.hare.config import HareConfig
from canopy.contracts.pv import PVMeta, PVValue, PVWriteValue
from canopy.epics.pv_protocol import InvalidPVName, validate_pv_name
from canopy.hare.services.subscriptions import SubscriptionRegistry

LifecycleHook = Callable[[list[str]], Awaitable[None]]

logger = logging.getLogger(__name__)


@runtime_checkable
class ClientChannel(Protocol):
    """What the broker needs from a connected client (FastAPI WebSocket fits)."""

    async def send_json(self, data) -> None: ...
    async def close(self) -> None: ...


class PVBroker:
    """Fan live PV updates from one connector out to subscribed WebSocket clients.

    Parameters
    ----------
    connector : PVConnector
        Backend used for monitors, one-shot reads, describes and writes.
    config : HareConfig
        Batching, heartbeat, resource-cap and seeding settings.
    """

    def __init__(self, connector: PVConnector, config: HareConfig) -> None:
        self._connector = connector
        self._config = config
        self._registry = SubscriptionRegistry()
        self._channels: dict[str, ClientChannel] = {}
        self._monitors: dict[str, Subscription] = {}
        self._latest: dict[str, PVValue] = {}
        self._meta: dict[str, PVMeta] = {}
        self._seed_retry_after: dict[str, float] = {}
        # One lock per subscribed PV, replaced on each release and re-subscribe.
        # Besides serializing seeds it marks the subscription: seeds and monitor
        # updates carry the lock they started under and are cached only while
        # it is still the PV's, so replacing a live PV's lock drops its updates.
        self._seed_locks: dict[str, asyncio.Lock] = {}
        # Seeds run in one background task per PV, shared by every subscriber
        # and bounded broker-wide, so a subscribe returns before any connector
        # read and a channel that never connects delays only itself. A PV's
        # task is cancelled when the PV is released.
        self._seed_sem = asyncio.Semaphore(config.seed_concurrency)
        self._seed_tasks: dict[str, asyncio.Task] = {}
        # PVs whose running seed task must seed again after its fan-out, because
        # the channel connected while that fan-out was in progress.
        self._seed_again: set[str] = set()
        self._buffer: dict[str, PVValue] = {}
        self._buffer_lock = asyncio.Lock()
        # Bounded so a fast producer can't grow the queue without limit; the
        # QueueFull path in _on_event then applies backpressure by dropping.
        self._queue: asyncio.Queue[tuple[PVValue, asyncio.Lock]] = asyncio.Queue(maxsize=config.event_queue_maxsize)
        self._running = False
        self._tasks: list[asyncio.Task] = []
        self._activation_hooks: list[tuple[LifecycleHook, set[str]]] = []
        self._deactivation_hooks: list[tuple[LifecycleHook, set[str]]] = []

    @property
    def registry(self) -> SubscriptionRegistry:
        return self._registry

    # --- lifecycle ------------------------------------------------------- #

    async def start(self) -> None:
        if self._running:
            return
        await self._connector.start()
        self._running = True
        self._tasks = [self._spawn_loop(name) for name in ("consume", "flush", "heartbeat")]
        logger.info(
            "PVBroker started (backend=%s, batch=%dms)",
            getattr(self._connector, "backend_name", "?"),
            self._config.ws_batch_interval_ms,
        )

    _LOOP_FACTORIES: dict[str, str] = {
        "consume": "_consume_loop",
        "flush": "_flush_loop",
        "heartbeat": "_heartbeat_loop",
    }

    def _spawn_loop(self, name: str) -> asyncio.Task:
        """Create a supervised background-loop task."""
        coro = getattr(self, self._LOOP_FACTORIES[name])()
        task = asyncio.create_task(coro, name=name)
        task.add_done_callback(self._supervise_loop)
        return task

    def _supervise_loop(self, task: asyncio.Task) -> None:
        """Done-callback: a background loop should only finish on shutdown.

        The loop bodies already swallow per-iteration errors, so a task
        reaching here while the broker is still running means an unexpected
        failure in the loop scaffolding itself. Log it loudly (never silent)
        and restart the loop so diffs/heartbeats keep flowing to every client.
        """
        if task.cancelled():
            return
        exc = task.exception()
        if exc is None:
            return
        name = task.get_name()
        logger.error("broker loop %r exited unexpectedly; restarting", name, exc_info=exc)
        if self._running:
            self._tasks = [self._spawn_loop(name) if t is task else t for t in self._tasks]

    async def stop(self) -> None:
        self._running = False
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._tasks = []
        for task in self._seed_tasks.values():
            task.cancel()
        await self.drain_seeds()
        self._seed_again.clear()
        for pv, sub in list(self._monitors.items()):
            try:
                await sub.close()
            except Exception:
                logger.exception("error closing monitor for %s", pv)
        self._monitors.clear()
        for client_id in list(self._channels):
            channel = self._channels.pop(client_id, None)
            if channel is not None:
                try:
                    await channel.close()
                except Exception:
                    pass
        await self._connector.close()
        logger.info("PVBroker stopped")

    # --- client lifecycle ------------------------------------------------ #

    def add_client(self, client_id: str, channel: ClientChannel) -> None:
        self._channels[client_id] = channel
        self._registry.add_client(client_id)

    async def remove_client(self, client_id: str) -> None:
        affected_pvs = list(self._registry.client_pvs(client_id))
        deactivated = self._registry.remove_client(client_id)
        await self._stop_monitors(deactivated)
        if affected_pvs:
            await self._fire_deactivation_hooks(affected_pvs, client_id)
        self._channels.pop(client_id, None)

    @staticmethod
    def _accept_names(pvs: list[str]) -> tuple[list[str], list[str]]:
        """Split a subscribe batch into ``(accepted, rejected)`` names, logging each drop.

        A single bad name (empty, over-long, whitespace- or protocol-prefixed
        garbage) must not abort the whole batch or reach the EPICS connector, so
        it is skipped here rather than raised - the rest of the client's PVs
        still subscribe. Rejected names are returned so the caller can tell the
        client which ones were dropped instead of leaving them to look like PVs
        that simply never produced a value.
        """
        accepted: list[str] = []
        rejected: list[str] = []
        for pv in pvs:
            try:
                accepted.append(validate_pv_name(pv))
            except InvalidPVName as exc:
                logger.warning("ignoring invalid PV name in subscribe: %s", exc)
                rejected.append(pv)
        return accepted, rejected

    def _apply_caps(self, client_id: str, pvs: list[str]) -> list[str]:
        """Enforce the per-message and per-client subscription caps.

        Returns the accepted names: already-subscribed PVs pass through
        (idempotent, no budget cost), new PVs are admitted up to the client's
        remaining budget. Excess is dropped with a warning so one client can't
        exhaust the shared broker's memory / EPICS connections.
        """
        if len(pvs) > self._config.max_pvs_per_message:
            logger.warning(
                "subscribe batch of %d exceeds per-message cap %d; truncating",
                len(pvs),
                self._config.max_pvs_per_message,
            )
            pvs = pvs[: self._config.max_pvs_per_message]

        existing = self._registry.client_pvs(client_id)
        budget = max(0, self._config.max_subscriptions_per_client - len(existing))
        accepted: list[str] = []
        seen: set[str] = set()
        new_admitted = 0
        dropped = 0
        for pv in pvs:
            if pv in seen:
                continue
            seen.add(pv)
            if pv in existing:
                accepted.append(pv)
            elif new_admitted < budget:
                accepted.append(pv)
                new_admitted += 1
            else:
                dropped += 1
        if dropped:
            logger.warning(
                "client %s hit subscription cap %d; dropped %d PV(s)",
                client_id,
                self._config.max_subscriptions_per_client,
                dropped,
            )
        return accepted

    async def subscribe(self, client_id: str, pvs: list[str]) -> list[str]:
        """Subscribe a client to PVs and send it their cached values and metadata.

        Invalid names and names past the caps are dropped rather than raised, so
        callers must use the return value rather than assume the whole request
        was honoured. Invalid names are reported to the client in an
        ``invalid_pv_name`` error frame.

        The client is sent one ``initial`` frame at once with whatever value and
        static metadata the broker already holds for the batch. Every accepted
        PV still missing either half is then seeded in the background (see
        `_ensure_seed_task`) and arrives in a further one-PV ``initial`` frame
        when its seed completes, so a channel that never connects delays neither
        the other PVs in the batch nor the client's later frames. A PV whose
        last seed left either half missing is not re-seeded until its
        ``seed_retry_backoff_s`` window has elapsed, so repeated subscribes of
        an unreachable PV do not each cost a connector timeout.

        Parameters
        ----------
        client_id : str
            Identifier of a client registered with `add_client`.
        pvs : list[str]
            Requested PV names; may contain duplicates and invalid names.

        Returns
        -------
        list[str]
            The PV names subscribed after validation, de-duplication and cap
            enforcement.
        """
        pvs, rejected = self._accept_names(pvs)
        if rejected:
            await self._send(
                client_id,
                {
                    "type": "error",
                    "code": ErrorCode.INVALID_PV_NAME.value,
                    "message": f"rejected {len(rejected)} invalid PV name(s)",
                    "pvNames": rejected,
                },
            )
        if not pvs:
            return []
        pvs = self._apply_caps(client_id, pvs)
        if not pvs:
            return []
        previously_subscribed = self._registry.client_pvs(client_id)
        newly = self._registry.subscribe(client_id, pvs)
        for pv in pvs:
            self._seed_locks.setdefault(pv, asyncio.Lock())
        await self._start_monitors(newly)
        new_for_client = [pv for pv in pvs if pv not in previously_subscribed]
        if new_for_client:
            await self._fire_activation_hooks(new_for_client, client_id)
        await self._send_initial(client_id, pvs)
        for pv in pvs:
            if self._needs_seed(pv):
                self._ensure_seed_task(pv)
        return pvs

    async def _send_initial(self, client_id: str, pvs: Iterable[str]) -> None:
        """Send a client an ``initial`` frame with the cached value and metadata of the given PVs.

        Nothing is sent when none of the PVs has either half cached.

        Parameters
        ----------
        client_id : str
            Identifier of a client registered with `add_client`.
        pvs : Iterable[str]
            Validated PV names to report.
        """
        pvs = list(pvs)
        data = {pv: self._latest[pv] for pv in pvs if pv in self._latest}
        meta = {pv: self._meta[pv] for pv in pvs if pv in self._meta}
        if not data and not meta:
            return
        await self._send(
            client_id,
            {
                "type": "initial",
                "data": {pv: val.model_dump(by_alias=True) for pv, val in data.items()},
                "meta": {pv: m.model_dump(by_alias=True) for pv, m in meta.items()},
                "count": len(data),
            },
        )

    def _needs_seed(self, pv: str) -> bool:
        """Report whether a PV is missing its cached value or metadata and is past its retry deadline.

        Parameters
        ----------
        pv : str
            Validated name of a subscribed PV.

        Returns
        -------
        bool
            ``True`` when either half is uncached and no retry deadline is
            pending, otherwise ``False``.
        """
        if pv in self._latest and pv in self._meta:
            return False
        now = time.monotonic()
        return now >= self._seed_retry_after.get(pv, now)

    def _ensure_seed_task(self, pv: str, *, again_if_running: bool = False) -> None:
        """Start a PV's background seed unless one is already running.

        The task takes a broker-wide ``seed_concurrency`` slot and the PV's seed
        lock, runs `_seed_pv`, and then sends every current subscriber of the PV
        a one-PV ``initial`` frame with whatever the seed left cached, including
        a ``connected: false`` value for a channel that did not answer. One task
        serves every subscriber; `_stop_monitors` cancels it when the PV is
        released. A PV with no seed lock has already been released and is
        skipped.

        Parameters
        ----------
        pv : str
            Validated name of a subscribed PV.
        again_if_running : bool, optional
            When a task is already running, ask it to seed once more after its
            fan-out instead of doing nothing. Used for a channel that connects
            while its task is still sending the result of the previous attempt.
        """
        running = self._seed_tasks.get(pv)
        if running is not None and not running.done():
            if again_if_running:
                self._seed_again.add(pv)
            return
        lock = self._seed_locks.get(pv)
        if lock is None:
            return
        task = asyncio.create_task(self._run_seed(pv, lock), name=f"seed:{pv}")
        self._seed_tasks[pv] = task
        task.add_done_callback(lambda done, pv=pv: self._forget_seed_task(pv, done))

    def _forget_seed_task(self, pv: str, task: asyncio.Task) -> None:
        """Done-callback: drop a finished seed task unless a newer one has replaced it."""
        if self._seed_tasks.get(pv) is task:
            del self._seed_tasks[pv]

    async def _run_seed(self, pv: str, lock: asyncio.Lock) -> None:
        """Seed one PV under the concurrency bound, then fan the result out to its subscribers.

        Each send re-checks that the PV is still current and the client still
        subscribed, since an earlier client's send can block while another
        unsubscribes or the PV is released. A connect edge that `_ingest` sees
        during the fan-out is recorded in ``_seed_again``, and the task seeds once
        more afterwards rather than dropping the describe.

        Parameters
        ----------
        pv : str
            Validated name of a subscribed PV.
        lock : asyncio.Lock
            The PV's seed lock when the task was created.
        """
        try:
            while True:
                async with self._seed_sem, lock:
                    if self._seed_locks.get(pv) is not lock:
                        return
                    if pv in self._latest and pv in self._meta:
                        return
                    await self._seed_pv(pv, lock)
                # An edge seen during the attempt was handled by the attempt
                # itself; only one seen from here on needs another pass.
                self._seed_again.discard(pv)
                for client_id in list(self._registry.subscribers(pv)):
                    if self._seed_locks.get(pv) is not lock:
                        return
                    if client_id in self._registry.subscribers(pv):
                        await self._send_initial(client_id, [pv])
                if self._seed_locks.get(pv) is not lock or pv not in self._seed_again:
                    return
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("background seed of %s failed", pv)

    def _cancel_seed(self, pv: str) -> None:
        """Forget a PV's seed task and cancel it, unless it is the task running now.

        A seed task can release its own PV when the send to the PV's last
        subscriber fails. Cancelling itself there would abort the release
        half-way, so it is left to finish; its stale lock stops it caching or
        sending anything.

        Parameters
        ----------
        pv : str
            Validated name of a PV being released.
        """
        task = self._seed_tasks.pop(pv, None)
        if task is not None and task is not asyncio.current_task():
            task.cancel()

    async def drain_seeds(self) -> None:
        """Wait for every seed task in flight to finish. Drives `stop`; also called directly by tests."""
        while True:
            pending = [task for task in self._seed_tasks.values() if not task.done()]
            if not pending:
                return
            await asyncio.gather(*pending, return_exceptions=True)

    async def _seed_pv(self, pv: str, lock: asyncio.Lock) -> None:
        """Fetch whichever of the seed value and static metadata a PV is missing.

        The value is read whenever no connected value is cached, so a PV whose
        earlier read found it disconnected is read again once its retry window
        allows. The read uses ``seed_timeout_s``, shorter than the connector's
        own timeout: the PV's monitor delivers the value anyway once a slow
        channel connects, so a channel that never connects costs one short read
        rather than two connector timeouts. A connected monitor update that
        lands during the read is newer than the read's result and is kept.

        Metadata is fetched only once the cached value shows the channel
        connected, since describing a channel that did not answer the read
        would only time out as well. For a channel that connects later,
        `_ingest` starts the describe when its first connected update arrives.
        Per-PV failures are logged and swallowed.

        When either half is still missing after the attempt, the PV's retry
        deadline is set ``seed_retry_backoff_s`` ahead; when both halves are
        present the deadline is cleared. A deadline whose PV later receives its
        value through the monitor is kept until `_stop_monitors` releases the
        PV, so the deadlines are bounded by the PVs that have subscribers.

        `_stop_monitors` discards the PV's seed lock on release and a later
        subscribe creates a new one, so the attempt writes only while ``lock``
        is still the PV's. An attempt that outlives its PV's release leaves no
        cached value, metadata or deadline behind and never overwrites what a
        newer attempt fetched.

        Parameters
        ----------
        pv : str
            Validated name of a subscribed PV.
        lock : asyncio.Lock
            The PV's seed lock, held by the caller.
        """

        def current() -> bool:
            return self._seed_locks.get(pv) is lock

        def connected_value_cached() -> bool:
            cached = self._latest.get(pv)
            return cached is not None and cached.connected

        if not connected_value_cached():
            try:
                value = await self._connector.get(pv, timeout=self._config.seed_timeout_s)
            except Exception as exc:
                logger.debug("initial get failed for %s: %s", pv, exc)
            else:
                if current() and not connected_value_cached():
                    self._latest[pv] = value
        if not current():
            return
        if connected_value_cached() and pv not in self._meta:
            try:
                meta = await self._connector.describe(pv)
            except Exception as exc:
                logger.debug("describe failed for %s: %s", pv, exc)
            else:
                if current():
                    self._meta[pv] = meta
        if not current():
            return
        if pv in self._latest and pv in self._meta:
            self._seed_retry_after.pop(pv, None)
        else:
            self._seed_retry_after[pv] = time.monotonic() + self._config.seed_retry_backoff_s

    async def unsubscribe(self, client_id: str, pvs: list[str]) -> None:
        if not pvs:
            return
        # Only PVs the client actually held were released here. Firing on the raw
        # request list would let any client deactivate a PV it never subscribed
        # to -- e.g. a REST-warmed one held only by `vole-internal`.
        held = self._registry.client_pvs(client_id)
        released = [pv for pv in pvs if pv in held]
        deactivated = self._registry.unsubscribe(client_id, pvs)
        await self._stop_monitors(deactivated)
        if released:
            await self._fire_deactivation_hooks(released, client_id)

    # --- monitor lifecycle ----------------------------------------------- #

    async def _start_monitors(self, pvs: Iterable[str]) -> None:
        """Start a monitor for each given PV that does not already have one.

        Each monitor's callback is bound to the PV's seed lock at the time it
        starts, so `_ingest` can tell its updates from those of a monitor
        released earlier. A PV released while an earlier PV's monitor was
        starting has no lock left and gets no monitor. A monitor that fails to
        start is logged and does not stop the rest of the batch.

        Parameters
        ----------
        pvs : Iterable[str]
            PVs that just gained their first subscriber.
        """
        for pv in pvs:
            lock = self._seed_locks.get(pv)
            if pv in self._monitors or lock is None:
                continue
            try:
                self._monitors[pv] = await self._connector.subscribe(
                    pv, lambda value, lock=lock: self._on_event(value, lock)
                )
            except Exception as exc:
                logger.error("failed to start monitor for %s: %s", pv, exc)

    async def _stop_monitors(self, pvs: Iterable[str]) -> None:
        """Close the monitors of released PVs and discard their cached state.

        A monitor that fails to close is logged and does not stop the rest of
        the batch. The cached value, static metadata, seed retry deadline, seed
        lock and pending seed task of every given PV are dropped whether or not
        it had a monitor, so a later subscribe seeds the PV afresh.

        Parameters
        ----------
        pvs : Iterable[str]
            PVs that have no subscribers left.
        """
        for pv in pvs:
            sub = self._monitors.pop(pv, None)
            if sub is not None:
                try:
                    await sub.close()
                except Exception:
                    logger.exception("error closing monitor for %s", pv)
            self._latest.pop(pv, None)
            self._meta.pop(pv, None)
            self._seed_retry_after.pop(pv, None)
            self._seed_locks.pop(pv, None)
            self._seed_again.discard(pv)
            self._cancel_seed(pv)

    # --- event ingestion ------------------------------------------------- #

    def _on_event(self, value: PVValue, lock: asyncio.Lock) -> None:
        """Connector callback, with the seed lock its monitor started under. Non-blocking; must run on the event loop thread."""
        try:
            self._queue.put_nowait((value, lock))
        except asyncio.QueueFull:
            logger.warning("broker queue full; dropping update for %s", value.pv)

    async def _consume_loop(self) -> None:
        while self._running:
            try:
                value, lock = await self._queue.get()
                await self._ingest(value, lock)
                await self._drain_nowait()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("broker consume loop iteration failed")

    async def _drain_nowait(self) -> None:
        """Process all currently-queued events without blocking. Drives `_consume_loop`; also called directly by tests."""
        while True:
            try:
                value, lock = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            await self._ingest(value, lock)

    async def _ingest(self, value: PVValue, lock: asyncio.Lock) -> None:
        """Cache a monitor update and buffer it for the next diff flush.

        `_stop_monitors` discards a PV's seed lock on release and a later
        subscribe creates a new one, so the update is kept only while ``lock``
        is still the PV's. An update from a released monitor, whether still
        queued at release or delivered after a re-subscribe, is neither cached
        nor buffered.

        A connected update for a PV with no cached metadata, whose previous
        value was missing or disconnected, starts a background describe through
        `_ensure_seed_task` regardless of any retry deadline: the channel has
        just connected, so the metadata its seed could not fetch is available
        now. If the PV's seed task is still sending its previous result, the
        task seeds again afterwards instead. Later connected updates do not
        repeat a describe that failed.

        Parameters
        ----------
        value : PVValue
            Update delivered by the PV's monitor.
        lock : asyncio.Lock
            The PV's seed lock when that monitor started.
        """
        if self._seed_locks.get(value.pv) is not lock:
            return
        previous = self._latest.get(value.pv)
        self._latest[value.pv] = value
        if value.connected and value.pv not in self._meta and (previous is None or not previous.connected):
            self._ensure_seed_task(value.pv, again_if_running=True)
        async with self._buffer_lock:
            self._buffer[value.pv] = value

    async def _flush_loop(self) -> None:
        interval = self._config.ws_batch_interval_ms / 1000.0
        while self._running:
            await asyncio.sleep(interval)
            try:
                await self._flush()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("broker flush loop iteration failed")

    async def _flush(self) -> None:
        async with self._buffer_lock:
            if not self._buffer:
                return
            updates = self._buffer
            self._buffer = {}
        per_client: dict[str, dict[str, dict]] = defaultdict(dict)
        for pv, value in updates.items():
            payload = value.model_dump(by_alias=True)
            for client_id in self._registry.subscribers(pv):
                per_client[client_id][pv] = payload
        for client_id, data in per_client.items():
            frame = {"type": "diff", "data": data, "count": len(data), "timestamp": time.time()}
            await self._send(client_id, frame)

    async def _heartbeat_loop(self) -> None:
        while self._running:
            await asyncio.sleep(self._config.heartbeat_interval_s)
            try:
                message = {
                    "type": "heartbeat",
                    "timestamp": time.time(),
                    "backend": getattr(self._connector, "backend_name", None),
                }
                for client_id in list(self._channels):
                    await self._send(client_id, message)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("broker heartbeat loop iteration failed")

    async def _send(self, client_id: str, message: dict) -> None:
        channel = self._channels.get(client_id)
        if channel is None:
            return
        try:
            await channel.send_json(message)
        except Exception as exc:
            logger.debug("send to %s failed: %s; dropping client", client_id, exc)
            self._channels.pop(client_id, None)
            affected_pvs = list(self._registry.client_pvs(client_id))
            deactivated = self._registry.remove_client(client_id)
            await self._stop_monitors(deactivated)
            if affected_pvs:
                await self._fire_deactivation_hooks(affected_pvs, client_id)

    # --- reads ----------------------------------------------------------- #

    async def get_value(self, pv: str) -> PVValue:
        """Current value: cached if actively monitored, else a one-shot read."""
        validate_pv_name(pv)
        cached = self._latest.get(pv)
        if cached is not None:
            return cached
        return await self._connector.get(pv)

    async def get_describe(self, pv: str) -> PVMeta:
        """Static metadata: cached if actively monitored, else a one-shot describe."""
        validate_pv_name(pv)
        cached = self._meta.get(pv)
        if cached is not None:
            return cached
        return await self._connector.describe(pv)

    # --- writes ---------------------------------------------------------- #

    async def put(self, pv: str, value: PVWriteValue, *, wait: bool = True, timeout: float | None = None) -> None:
        """Write a value to a PV (one-shot, independent of any subscription).

        Delegates to the connector, which routes by protocol prefix and raises
        `PVWriteFailed` on failure or timeout. On success the cached latest value
        is dropped so an immediate `get_value` reflects the write instead of the
        pre-write cache. The monitor path repopulates the cache on its next tick.
        """
        validate_pv_name(pv)
        await self._connector.put(pv, value, wait=wait, timeout=timeout)
        self._latest.pop(pv, None)

    # --- introspection --------------------------------------------------- #

    def status(self) -> dict:
        return {
            "running": self._running,
            "backend": getattr(self._connector, "backend_name", None),
            "active_monitors": len(self._monitors),
            **self._registry.stats(),
            # After the spread so it wins over the registry's own ``clients``
            # bookkeeping key; the two are kept in step, but this is the count
            # of connected channels the health summary describes.
            "clients": len(self._channels),
        }

    # --- lifecycle hooks ------------------------------------------------- #

    def add_activation_hook(self, hook: LifecycleHook, *, exclude_clients: set[str] | None = None) -> None:
        self._activation_hooks.append((hook, exclude_clients or set()))

    def remove_activation_hook(self, hook: LifecycleHook) -> None:
        self._activation_hooks = [(h, ex) for h, ex in self._activation_hooks if h is not hook]

    def add_deactivation_hook(self, hook: LifecycleHook, *, exclude_clients: set[str] | None = None) -> None:
        self._deactivation_hooks.append((hook, exclude_clients or set()))

    def remove_deactivation_hook(self, hook: LifecycleHook) -> None:
        self._deactivation_hooks = [(h, ex) for h, ex in self._deactivation_hooks if h is not hook]

    async def _fire_activation_hooks(self, pvs: list[str], client_id: str) -> None:
        for hook, exclude in self._activation_hooks:
            if client_id in exclude:
                continue
            relevant = self._filter_newly_active_for_hook(pvs, exclude)
            if relevant:
                try:
                    await hook(relevant)
                except Exception:
                    logger.exception("activation hook %r failed", hook)

    async def _fire_deactivation_hooks(self, pvs: list[str], client_id: str) -> None:
        for hook, exclude in self._deactivation_hooks:
            if client_id in exclude:
                continue
            relevant = self._filter_deactivated_for_hook(pvs, exclude)
            if relevant:
                try:
                    await hook(relevant)
                except Exception:
                    logger.exception("deactivation hook %r failed", hook)

    def _filter_newly_active_for_hook(self, pvs: list[str], exclude: set[str]) -> list[str]:
        """Return PVs that just became active from the hook's perspective.

        A PV is "newly active" for a hook if it now has exactly one
        subscriber that is NOT in the hook's exclude set (meaning the
        subscribe that just happened was the first external one).
        """
        result = []
        for pv in pvs:
            subs = self._registry.subscribers(pv)
            external = subs - exclude
            if len(external) == 1:
                result.append(pv)
        return result

    def _filter_deactivated_for_hook(self, pvs: list[str], exclude: set[str]) -> list[str]:
        """Return PVs that are deactivated from the hook's perspective.

        A PV is "deactivated" for a hook when it has zero subscribers
        outside the hook's exclude set.
        """
        result = []
        for pv in pvs:
            subs = self._registry.subscribers(pv)
            external = subs - exclude
            if not external:
                result.append(pv)
        return result
