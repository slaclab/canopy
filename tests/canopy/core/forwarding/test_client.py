"""Small pieces of the forwarding client. The wait itself runs on two arms and a head in
``tests/canopy/core/replication/test_forward_e2e.py``."""

import time
import asyncio
from types import SimpleNamespace
from contextlib import asynccontextmanager

import pytest

from canopy.core.modules import WILDCARD
from canopy.core.security import APIKey
from canopy.core.forwarding import client
from canopy.core.forwarding.client import (
    Caller,
    lsn_value,
    on_behalf_of,
    forward_target,
    forwardable_arms,
    check_forward_urls,
    wait_until_applied,
)

from .conftest import ARM1, ARM1_SERVICE_KEY


def test_lsn_values_order_like_postgres():
    assert lsn_value("0/16B3748") == 0x16B3748
    assert lsn_value("1/0") > lsn_value("0/FFFFFFFF")


def test_on_behalf_of_names_the_key_and_instance(head):
    key = APIKey(read=True, write=True, modules=frozenset({WILDCARD}), name="console")

    assert on_behalf_of(key) == "console@head"
    assert len(on_behalf_of(APIKey(read=True, write=True, modules=frozenset(), name="x" * 300))) == 255
    assert on_behalf_of(APIKey(read=True, write=True, modules=frozenset(), name="tab\there")) == "tab?here@head"


def test_a_caller_is_plain_data(head):
    key = APIKey(read=True, write=True, modules=frozenset(), facilities=frozenset({"b", "a"}), name="console")

    assert Caller.of(key) == Caller(on_behalf_of="console@head", facilities=["a", "b"])


def test_only_an_arm_with_both_forward_settings_is_a_target(head):
    arm1 = forward_target("arm1")

    assert (arm1.name, arm1.url, arm1.service_key, arm1.subscription) == (
        "arm1",
        ARM1,
        ARM1_SERVICE_KEY,
        "canopy_history_arm1",
    )
    assert ARM1_SERVICE_KEY not in repr(arm1)
    assert forward_target("arm2") is None
    assert forward_target("head") is None
    assert forwardable_arms() == {"arm1"}


def test_a_plain_http_forward_url_needs_the_override(head, monkeypatch):
    check_forward_urls()
    monkeypatch.setenv("CANOPY_ARMS__ARM1__FORWARD_URL", "http://arm1.example.org")

    with pytest.raises(ValueError, match="https:// URL, for: arm1"):
        check_forward_urls()

    monkeypatch.setenv("CANOPY_FORWARD__ALLOW_INSECURE_HTTP", "true")
    check_forward_urls()


async def test_an_unreadable_position_means_stale_not_an_error():
    assert await wait_until_applied("not-an-lsn", 1.0, subscription="canopy_history_arm1") is False


class TestTheWaitIsBounded:
    """``timeout_s`` covers getting a connection and every query, not just the time between polls."""

    @staticmethod
    def _engine(monkeypatch, *, pool_stalls: bool = False):
        @asynccontextmanager
        async def connect():
            if pool_stalls:
                await asyncio.sleep(3600)

            async def get_raw_connection():
                return SimpleNamespace(driver_connection=object())

            yield SimpleNamespace(get_raw_connection=get_raw_connection)

        monkeypatch.setattr(client, "get_engine", lambda: SimpleNamespace(connect=connect))

    @staticmethod
    def _applied(monkeypatch, *positions):
        remaining = list(positions)
        asked = []

        async def applied_lsn(conn, subscription):
            asked.append(subscription)
            if not remaining:
                await asyncio.sleep(3600)  # the query stalls
            return remaining.pop(0)

        monkeypatch.setattr(client, "applied_lsn", applied_lsn)
        return asked

    async def test_a_stalled_pool(self, monkeypatch):
        self._engine(monkeypatch, pool_stalls=True)
        started = time.monotonic()

        assert await wait_until_applied("0/16B3748", 0.2, subscription="canopy_history_arm1") is False
        assert time.monotonic() - started < 2

    async def test_a_stalled_query(self, monkeypatch):
        self._engine(monkeypatch)
        self._applied(monkeypatch)
        started = time.monotonic()

        assert await wait_until_applied("0/16B3748", 0.2, subscription="canopy_history_arm1") is False
        assert time.monotonic() - started < 2

    async def test_applied_before_the_timeout_on_that_arms_subscription(self, monkeypatch):
        self._engine(monkeypatch)
        asked = self._applied(monkeypatch, None, "0/16B3740", "0/16B3748")

        assert await wait_until_applied("0/16B3748", 5.0, subscription="canopy_history_arm1") is True
        assert set(asked) == {"canopy_history_arm1"}
