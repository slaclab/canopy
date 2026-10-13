"""Hare's Prometheus gauges — the exemplar for module-level metrics.

The pattern (docs/observability/metrics.md): module gauges expose the numbers
the module's ``/health/summary`` already reports, read from the owning service
at scrape time rather than pushed on every state change. Nothing in the
broker or the WebSocket hot path knows these gauges exist.

The gauges are declared once at import (``prometheus_client`` registers each
name exactly once per process) and pointed at the live broker by the
lifespan, so a scrape before startup or after shutdown reads zeros.
"""

from collections.abc import Callable

from prometheus_client import Gauge

from canopy.hare.services.broker import PVBroker

CLIENTS = Gauge(
    "canopy_hare_clients",
    "Broker clients: WebSocket sessions plus in-process consumers such as Vole.",
)
ACTIVE_MONITORS = Gauge("canopy_hare_active_monitors", "PVs with a live EPICS monitor.")
SUBSCRIPTIONS = Gauge("canopy_hare_subscriptions", "PV subscriptions summed over all broker clients.")

# Gauge -> key in PVBroker.status(), the same dict /health/summary is built from.
_STATUS_FIELDS: dict[Gauge, str] = {
    CLIENTS: "clients",
    ACTIVE_MONITORS: "active_monitors",
    SUBSCRIPTIONS: "subscriptions",
}


def bind(broker: PVBroker | None) -> None:
    """Point the gauges at ``broker``; ``None`` (shutdown) makes them read 0."""
    for gauge, field in _STATUS_FIELDS.items():
        gauge.set_function(_reader(broker, field))


def _reader(broker: PVBroker | None, field: str) -> Callable[[], float]:
    if broker is None:
        return lambda: 0
    return lambda: broker.status()[field]
