"""Small pieces of the forwarding client. The wait itself runs on two servers in
``tests/canopy/core/replication/test_forward_e2e.py``."""

import time
import asyncio
from types import SimpleNamespace
from contextlib import asynccontextmanager

import pytest

from canopy.core.config import get_settings
from canopy.core.modules import WILDCARD
from canopy.core.security import APIKey
from canopy.core.forwarding import client
from canopy.core.forwarding.client import lsn_value, on_behalf_of, wait_until_applied


def test_lsn_values_order_like_postgres():
    assert lsn_value("0/16B3748") == 0x16B3748
    assert lsn_value("1/0") > lsn_value("0/FFFFFFFF")


def test_on_behalf_of_names_the_key_and_instance(forward_role):
    key = APIKey(read=True, write=True, modules=frozenset({WILDCARD}), name="console")

    assert on_behalf_of(key) == "console@site_a"
    assert len(on_behalf_of(APIKey(read=True, write=True, modules=frozenset(), name="x" * 300))) == 255
    assert on_behalf_of(APIKey(read=True, write=True, modules=frozenset(), name="tab\there")) == "tab?here@site_a"


@pytest.mark.parametrize("lsn", ["0/16B3748", "not-an-lsn"])
async def test_no_instance_name_means_stale_not_an_error(monkeypatch, lsn):
    monkeypatch.delenv("CANOPY_INSTANCE", raising=False)
    get_settings.cache_clear()
    try:
        assert await wait_until_applied(lsn, 1.0) is False
    finally:
        get_settings.cache_clear()


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

        async def applied_lsn(conn, subscription):
            if not remaining:
                await asyncio.sleep(3600)  # the query stalls
            return remaining.pop(0)

        monkeypatch.setattr(client, "applied_lsn", applied_lsn)

    async def test_a_stalled_pool(self, forward_role, monkeypatch):
        self._engine(monkeypatch, pool_stalls=True)
        started = time.monotonic()

        assert await wait_until_applied("0/16B3748", 0.2) is False
        assert time.monotonic() - started < 2

    async def test_a_stalled_query(self, forward_role, monkeypatch):
        self._engine(monkeypatch)
        self._applied(monkeypatch)
        started = time.monotonic()

        assert await wait_until_applied("0/16B3748", 0.2) is False
        assert time.monotonic() - started < 2

    async def test_applied_before_the_timeout(self, forward_role, monkeypatch):
        self._engine(monkeypatch)
        self._applied(monkeypatch, None, "0/16B3740", "0/16B3748")

        assert await wait_until_applied("0/16B3748", 5.0) is True
