"""Prometheus gauges for replication to a history mirror, from the status ``replication status`` prints.

Read at scrape time, like the module gauges (docs/observability/metrics.md), with
one difference: the status comes from database queries, which are async, and a
collector's ``collect()`` is not. So the ``/metrics`` handler awaits ``refresh()``
just before rendering. It runs the replication CLI's own status function over a
short-lived connection, and the collector yields families from that snapshot, so
the gauges and ``python -m canopy.core.replication status`` can't disagree.

A primary reports its logical slots. Any other role reports its subscription. A
refresh that fails (database down, a mirror without ``CANOPY_INSTANCE``) leaves only
``canopy_replication_status_up 0``; it never fails the scrape.

A mirror reports where apply has got to (``applied_lsn``), not how far behind it
is. ``received_lsn`` follows every keepalive, which covers WAL the primary wrote to
tables it doesn't publish, so the gap between the two grows while nothing waits to
be applied. The lag to alert on is the primary's per-slot ``lag_bytes``: WAL the
mirror hasn't confirmed yet.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from collections.abc import Iterable

import asyncpg
from prometheus_client import REGISTRY, CollectorRegistry
from prometheus_client.core import Metric, GaugeMetricFamily, CounterMetricFamily
from prometheus_client.registry import Collector

from canopy.core.config import get_settings
from canopy.core.replication.sql import asyncpg_dsn
from canopy.core.replication.config import get_replication_config
from canopy.core.replication.migration import side_for_role
from canopy.core.replication.publisher import publisher_status
from canopy.core.replication.subscriber import subscriber_status

logger = logging.getLogger(__name__)

TIMEOUT_S = 2.0
"""Budget for one refresh, connection included."""

WAL_STATUSES = ("reserved", "extended", "unreserved", "lost")


async def read_status(conn: asyncpg.Connection, *, role: str, instance: str | None) -> dict:
    """This side's replication status, as ``python -m canopy.core.replication status`` reports it.

    A primary's also carries ``max_slot_wal_keep_bytes``: the WAL cap on every slot,
    ``None`` when there is none.
    """
    if side_for_role(role) == "subscriber":
        return await subscriber_status(conn, instance=instance)
    status = await publisher_status(conn, config=get_replication_config())
    cap = await conn.fetchval("SELECT pg_size_bytes(current_setting('max_slot_wal_keep_size'))")
    status["max_slot_wal_keep_bytes"] = cap if cap >= 0 else None
    return status


def lsn_bytes(lsn: str) -> int:
    """A WAL position (``"16/B374D848"``) as a byte offset."""
    high, _, low = lsn.partition("/")
    return (int(high, 16) << 32) + int(low, 16)


class ReplicationCollector(Collector):
    """Yields the gauges of the last status ``refresh()`` read; ``None`` means it failed."""

    def __init__(self) -> None:
        self.status: dict | None = None
        self._failing = False

    async def refresh(self) -> None:
        """Read this instance's status over its own connection, within ``TIMEOUT_S``. Never raises."""
        settings = get_settings()
        conn = None
        try:
            async with asyncio.timeout(TIMEOUT_S):
                conn = await asyncpg.connect(
                    asyncpg_dsn(settings.database_url), server_settings={"application_name": "canopy-metrics"}
                )
                status = await read_status(conn, role=settings.role, instance=settings.instance)
        except Exception as exc:  # a scrape must never fail on this
            if not self._failing:
                logger.warning("Replication metrics unavailable until the status can be read: %r", exc)
            self.status, self._failing = None, True
        else:
            if self._failing:
                logger.info("Replication metrics available again")
            self.status, self._failing = status, False
        finally:
            if conn is not None:
                try:
                    await conn.close(timeout=TIMEOUT_S)
                except Exception:
                    conn.terminate()

    def collect(self) -> Iterable[Metric]:
        status = self.status
        yield GaugeMetricFamily(
            "canopy_replication_status_up",
            "1 when the last scrape read this instance's replication status; the other replication series need it.",
            value=0 if status is None else 1,
        )
        if status is None:
            return
        if status["side"] == "publisher":
            yield from _slot_families(status)
        else:
            yield from _subscription_families(status["subscription"])


def _slot_families(status: dict) -> Iterable[Metric]:
    labels = ["slot", "kind"]
    active = GaugeMetricFamily(
        "canopy_replication_slot_active", "1 while a mirror is streaming from the logical slot.", labels=labels
    )
    wal_status = GaugeMetricFamily(
        "canopy_replication_slot_wal_status",
        "1 for the slot's wal_status. reserved is healthy; lost means the WAL the mirror needs is gone.",
        labels=[*labels, "wal_status"],
    )
    retained = GaugeMetricFamily(
        "canopy_replication_slot_retained_bytes", "WAL the slot keeps on disk (from its restart_lsn).", labels=labels
    )
    lag = GaugeMetricFamily(
        "canopy_replication_slot_lag_bytes",
        "WAL the mirror hasn't confirmed applying yet (from the slot's confirmed_flush_lsn).",
        labels=labels,
    )
    safe = GaugeMetricFamily(
        "canopy_replication_slot_safe_wal_bytes",
        "WAL that can still be written before the slot loses what it needs (only under a WAL cap).",
        labels=labels,
    )
    for slot in status["slots"]:
        values = [slot["slot_name"], slot["kind"]]
        active.add_metric(values, 1 if slot["active"] else 0)
        if slot["wal_status"] is not None:  # NULL until the slot has reserved WAL
            for state in WAL_STATUSES:
                wal_status.add_metric([*values, state], 1 if slot["wal_status"] == state else 0)
        for family, field in ((retained, "retained_bytes"), (lag, "lag_bytes"), (safe, "safe_wal_size")):
            if slot[field] is not None:
                family.add_metric(values, slot[field])
    yield from (active, wal_status, retained, lag, safe)
    if (cap := status.get("max_slot_wal_keep_bytes")) is not None:
        yield GaugeMetricFamily(
            "canopy_replication_max_slot_wal_keep_bytes",
            "max_slot_wal_keep_size: WAL any one slot may keep. Absent when unlimited.",
            value=cap,
        )


def _subscription_families(sub: dict) -> Iterable[Metric]:
    labels, values = ["subscription"], [sub["name"]]

    def gauge(name: str, help_text: str, value: float) -> GaugeMetricFamily:
        family = GaugeMetricFamily(name, help_text, labels=labels)
        family.add_metric(values, value)
        return family

    def counter(name: str, help_text: str, value: float) -> CounterMetricFamily:
        family = CounterMetricFamily(name, help_text, labels=labels)
        family.add_metric(values, value)
        return family

    yield gauge(
        "canopy_replication_subscription_exists", "1 when this mirror's subscription exists.", int(sub["exists"])
    )
    if not sub["exists"]:
        return
    # pg_stat_subscription keeps a row for a subscription with no worker running, with pid NULL.
    worker = sub["worker"] if sub["worker"] is not None and sub["worker"]["pid"] is not None else None
    yield gauge("canopy_replication_subscription_enabled", "1 while the subscription is enabled.", int(sub["enabled"]))
    yield gauge(
        "canopy_replication_subscription_worker_up",
        "1 while the subscription's apply worker is running.",
        int(worker is not None),
    )
    if worker is not None and worker["last_msg_receipt_time"] is not None:
        received = datetime.fromisoformat(worker["last_msg_receipt_time"])
        yield gauge(
            "canopy_replication_subscription_receive_age_seconds",
            "Seconds since the apply worker last heard from the primary (keepalives included), "
            "by this host's clock against the database's.",
            (datetime.now(UTC) - received).total_seconds(),
        )
    yield counter(
        "canopy_replication_subscription_apply_errors",
        "Errors applying changes from the primary; apply retries the same transaction until it succeeds.",
        sub["apply_error_count"],
    )
    yield counter(
        "canopy_replication_subscription_sync_errors",
        "Errors during a table's initial copy.",
        sub["sync_error_count"],
    )
    yield gauge(
        "canopy_replication_subscription_tables_not_ready",
        "Subscribed tables still waiting for or in their initial copy.",
        len(sub["tables_not_ready"]),
    )
    if sub["applied_lsn"] is not None:
        yield gauge(
            "canopy_replication_subscription_applied_lsn_bytes",
            "The primary's WAL position of the last transaction applied here: a position, not a lag.",
            lsn_bytes(sub["applied_lsn"]),
        )


COLLECTOR = ReplicationCollector()


def register(registry: CollectorRegistry = REGISTRY) -> None:
    """Register the collector; ``canopy.main`` calls this once at import."""
    registry.register(COLLECTOR)


async def refresh() -> None:
    """Re-read the status the registered collector reports; the ``/metrics`` handler awaits it."""
    await COLLECTOR.refresh()
