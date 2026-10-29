"""``reseed`` without a database: table order, ``--since`` parsing, and the command's refusals."""

import argparse
from argparse import Namespace
from datetime import UTC, datetime, timezone, timedelta

import pytest

from canopy.core.config import get_settings
from canopy.core.replication import reseed
from canopy.core.replication.sql import ReplicationError
from canopy.core.replication.reseed import parse_since, parents_first
from canopy.core.replication.__main__ import build_parser

SQUIRREL_KEYS = {
    ("squirrel.tag", "squirrel.tag_group"),
    ("squirrel.pv_tag", "squirrel.pv"),
    ("squirrel.pv_tag", "squirrel.tag"),
    ("squirrel.meta_pv", "squirrel.pv"),
    ("squirrel.snapshot_value", "squirrel.pv"),
    ("squirrel.snapshot_value", "squirrel.snapshot"),
}


def test_parents_come_before_the_tables_that_reference_them():
    tables = sorted({table for pair in SQUIRREL_KEYS for table in pair} | {"magpie.config"})
    order = parents_first(tables, SQUIRREL_KEYS)
    assert sorted(order) == tables
    for child, parent in SQUIRREL_KEYS:
        assert order.index(parent) < order.index(child)


def test_a_foreign_key_cycle_is_an_error():
    with pytest.raises(ReplicationError, match="cycle"):
        parents_first(["a.x", "a.y"], {("a.x", "a.y"), ("a.y", "a.x")})


def test_since_without_a_zone_is_utc():
    assert parse_since("2026-10-01T12:00:00") == datetime(2026, 10, 1, 12, tzinfo=UTC)
    assert parse_since("2026-10-01T12:00:00-07:00") == datetime(2026, 10, 1, 12, tzinfo=timezone(timedelta(hours=-7)))
    with pytest.raises(argparse.ArgumentTypeError):
        parse_since("yesterday")


def test_since_is_required():
    assert build_parser().parse_args(["reseed", "--since", "2026-10-01"]).since == datetime(2026, 10, 1, tzinfo=UTC)
    with pytest.raises(SystemExit):
        build_parser().parse_args(["reseed"])


@pytest.fixture
def env(monkeypatch):
    def set_env(**values: str) -> None:
        for name, value in values.items():
            monkeypatch.setenv(name, value)
        get_settings.cache_clear()

    yield set_env
    get_settings.cache_clear()


ARGS = Namespace(command="reseed", since=datetime(2026, 10, 1, tzinfo=UTC))


async def test_refused_on_a_primary(env, caplog):
    env(CANOPY_ROLE="primary")
    assert await reseed.run(ARGS) == 2
    assert "runs on a mirror" in caplog.text


async def test_refused_without_the_readers_connection(env, monkeypatch, caplog):
    env(CANOPY_ROLE="mirror", CANOPY_INSTANCE="site_a")
    monkeypatch.delenv("CANOPY_REPLICATION__READER_DSN", raising=False)
    assert await reseed.run(ARGS) == 2
    assert "CANOPY_REPLICATION__READER_DSN" in caplog.text


async def test_a_failed_connection_is_exit_1_and_never_shows_the_password(env, caplog, capsys):
    secret = "s3cret-reader-pw"
    env(
        CANOPY_ROLE="mirror",
        CANOPY_INSTANCE="site_a",
        CANOPY_REPLICATION__READER_DSN=f"postgresql://reader:{secret}@127.0.0.1:1/canopy",
    )
    assert await reseed.run(ARGS) == 1
    assert secret not in caplog.text and secret not in capsys.readouterr().out
