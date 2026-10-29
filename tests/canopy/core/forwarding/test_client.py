"""Small pieces of the forwarding client. The wait itself runs on two servers in
``tests/canopy/core/replication/test_forward_e2e.py``."""

import pytest

from canopy.core.config import get_settings
from canopy.core.modules import WILDCARD
from canopy.core.security import APIKey
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
