"""Hare's module gauges read the broker at scrape time, mirroring /health/summary."""

from prometheus_client import REGISTRY
from fastapi.testclient import TestClient

from canopy.main import app
from tests.conftest import READ_HEADERS


def _gauge(name: str) -> float:
    return REGISTRY.get_sample_value(name)


def test_gauges_agree_with_the_health_summary_while_a_client_is_subscribed():
    with TestClient(app) as client:
        with client.websocket_connect("/hare/v1/ws/pvs?api_key=test-read") as ws:
            ws.send_json({"type": "subscribe", "pvNames": ["SIM:METRICS:A", "SIM:METRICS:B"]})
            for _ in range(5):
                if ws.receive_json()["type"] == "initial":
                    break

            summary = client.get("/hare/v1/health/summary", headers=READ_HEADERS).json()
            assert summary["subscriptions"] >= 2

            assert _gauge("canopy_hare_clients") == summary["clients"]
            assert _gauge("canopy_hare_active_monitors") == summary["activeMonitors"]
            assert _gauge("canopy_hare_subscriptions") == summary["subscriptions"]


def test_gauges_read_zero_once_the_lifespan_has_unbound_the_broker():
    with TestClient(app):
        pass
    assert _gauge("canopy_hare_clients") == 0
    assert _gauge("canopy_hare_active_monitors") == 0
    assert _gauge("canopy_hare_subscriptions") == 0
