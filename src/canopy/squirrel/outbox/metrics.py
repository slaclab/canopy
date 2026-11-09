"""Prometheus gauges for a history head's snapshot outbox, from ``outbox_status``.

Like the replication gauges (``canopy.core.replication.metrics``), the numbers come
from a database query, which is async, while a collector's ``collect()`` isn't: the
``/metrics`` handler awaits ``refresh()`` just before rendering. It reads over a
short-lived connection with a two-second budget, and a failed read leaves only
``canopy_squirrel_outbox_status_up 0``; it never fails the scrape.

Only an instance that delivers to arms reports anything, and it reports every one of
those arms, with zeros while its queue is empty. The ``arm`` label is the arm's name
in ``CANOPY_ARMS``, as on the per-arm replication gauges.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Iterable

import asyncpg
from prometheus_client import REGISTRY, CollectorRegistry
from prometheus_client.core import Metric, GaugeMetricFamily
from prometheus_client.registry import Collector

from canopy.core.config import get_settings
from canopy.squirrel.config import get_squirrel_config
from canopy.core.replication.sql import asyncpg_dsn
from canopy.squirrel.data.models import OUTBOX_STATES
from canopy.core.forwarding.client import forwardable_arms
from canopy.squirrel.outbox.status import outbox_status

logger = logging.getLogger(__name__)

TIMEOUT_S = 2.0
"""Budget for one refresh, connection included."""


class OutboxCollector(Collector):
    """Yields the gauges of the last status ``refresh()`` read; ``None`` means it failed."""

    def __init__(self, arms: Callable[[], list[str]] = lambda: sorted(forwardable_arms())) -> None:
        self.arms = arms
        self.status: dict | None = None
        self.enabled = False
        self._failing = False

    async def refresh(self) -> None:
        """Read the outbox status over a connection of its own, within ``TIMEOUT_S``. Never raises."""
        arms = self.arms()
        self.enabled = bool(arms)
        if not self.enabled:
            self.status = None
            return
        conn = None
        try:
            async with asyncio.timeout(TIMEOUT_S):
                conn = await asyncpg.connect(
                    asyncpg_dsn(get_settings().database_url), server_settings={"application_name": "canopy-metrics"}
                )
                status = await outbox_status(conn, arms=arms, config=get_squirrel_config().outbox)
        except Exception as exc:  # a scrape must never fail on this
            if not self._failing:
                logger.warning("Outbox metrics unavailable until the status can be read: %r", exc)
            self.status, self._failing = None, True
        else:
            if self._failing:
                logger.info("Outbox metrics available again")
            self.status, self._failing = status, False
        finally:
            if conn is not None:
                try:
                    await conn.close(timeout=TIMEOUT_S)
                except Exception:
                    conn.terminate()

    def collect(self) -> Iterable[Metric]:
        if not self.enabled:
            return
        status = self.status
        yield GaugeMetricFamily(
            "canopy_squirrel_outbox_status_up",
            "1 when the last scrape read the snapshot outbox; the other outbox series need it.",
            value=0 if status is None else 1,
        )
        if status is None:
            return
        yield from status_families(status)


def status_families(status: dict) -> Iterable[Metric]:
    """The outbox gauges for one ``outbox_status`` result."""
    snapshots = GaugeMetricFamily(
        "canopy_squirrel_outbox_snapshots",
        "Snapshots queued for an arm, by state: pending (waiting or retrying), delivered (saved on the arm, "
        "not replicated back yet), failed (refused by the arm).",
        labels=["arm", "state"],
    )
    size = GaugeMetricFamily(
        "canopy_squirrel_outbox_bytes", "Bundle bytes queued for an arm, every state counted.", labels=["arm"]
    )
    pending_age = GaugeMetricFamily(
        "canopy_squirrel_outbox_oldest_pending_age_seconds",
        "Age of the arm's oldest snapshot not yet saved there. Absent when none is waiting.",
        labels=["arm"],
    )
    delivered_age = GaugeMetricFamily(
        "canopy_squirrel_outbox_oldest_delivered_age_seconds",
        "Time since the arm saved its oldest snapshot that hasn't replicated back. Absent when none.",
        labels=["arm"],
    )
    for arm, queue in status["arms"].items():
        for state in OUTBOX_STATES:
            snapshots.add_metric([arm, state], queue[state])
        size.add_metric([arm], queue["bytes"])
        if queue["oldest_pending_age_s"] is not None:
            pending_age.add_metric([arm], queue["oldest_pending_age_s"])
        if queue["oldest_delivered_age_s"] is not None:
            delivered_age.add_metric([arm], queue["oldest_delivered_age_s"])
    yield from (snapshots, size, pending_age, delivered_age)
    yield GaugeMetricFamily(
        "canopy_squirrel_outbox_max_snapshots",
        "Most snapshots one arm's outbox holds (CANOPY_SQUIRREL__OUTBOX__MAX_SNAPSHOTS).",
        value=status["max_snapshots"],
    )
    yield GaugeMetricFamily(
        "canopy_squirrel_outbox_max_bytes",
        "Most bundle bytes one arm's outbox holds (CANOPY_SQUIRREL__OUTBOX__MAX_BYTES).",
        value=status["max_bytes"],
    )


COLLECTOR = OutboxCollector()


def register(registry: CollectorRegistry = REGISTRY) -> None:
    """Register the collector; ``canopy.squirrel.app`` calls this once at import."""
    registry.register(COLLECTOR)


async def refresh() -> None:
    """Re-read the outbox the registered collector reports; the ``/metrics`` handler awaits it."""
    await COLLECTOR.refresh()
