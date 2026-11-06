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


RESEED = ["reseed", "--arm", "arm1"]


def test_cut_wait_defaults_to_a_minute():
    assert build_parser().parse_args([*RESEED, "--since", "2026-10-01"]).cut_wait == 60.0
    assert build_parser().parse_args([*RESEED, "--since", "2026-10-01", "--cut-wait", "5"]).cut_wait == 5.0


def test_since_and_arm_are_required():
    args = build_parser().parse_args([*RESEED, "--since", "2026-10-01"])
    assert (args.arm, args.since) == ("arm1", datetime(2026, 10, 1, tzinfo=UTC))
    for missing in (RESEED, ["reseed", "--since", "2026-10-01"]):
        with pytest.raises(SystemExit):
            build_parser().parse_args(missing)


@pytest.fixture
def env(monkeypatch):
    def set_env(**values: str) -> None:
        for name, value in values.items():
            monkeypatch.setenv(name, value)
        get_settings.cache_clear()

    monkeypatch.delenv("CANOPY_ARMS", raising=False)
    yield set_env
    get_settings.cache_clear()


ARGS = Namespace(command="reseed", arm="arm1", since=datetime(2026, 10, 1, tzinfo=UTC), cut_wait=1.0)


async def test_refused_on_an_arm(env, caplog):
    env(CANOPY_ROLE="primary", CANOPY_INSTANCE="arm1")
    assert await reseed.run(ARGS) == 2
    assert "runs on a mirror" in caplog.text


async def test_refused_for_an_arm_not_configured(env, caplog):
    env(CANOPY_ROLE="mirror", CANOPY_INSTANCE="head", CANOPY_ARMS__ARM2__READER_DSN="postgresql://r:pw@x/db")
    assert await reseed.run(ARGS) == 2
    assert "no arm named 'arm1'" in caplog.text


async def test_refused_without_the_arms_reader_connection(env, caplog):
    env(CANOPY_ROLE="mirror", CANOPY_INSTANCE="head", CANOPY_ARMS__ARM1__PUBLISHER_DSN="host=arm1-db")
    assert await reseed.run(ARGS) == 2
    assert "CANOPY_ARMS__ARM1__READER_DSN" in caplog.text


async def test_a_failed_connection_is_exit_1_and_never_shows_a_password(env, caplog, capsys):
    secret, other = "s3cret-reader-pw", "s3cret-publisher-pw"
    env(
        CANOPY_ROLE="mirror",
        CANOPY_INSTANCE="head",
        CANOPY_ARMS__ARM1__READER_DSN=f"postgresql://reader:{secret}@127.0.0.1:1/canopy",
        CANOPY_ARMS__ARM1__PUBLISHER_DSN=f"host=127.0.0.1 port=1 password={other}",
    )
    assert await reseed.run(ARGS) == 1
    out = caplog.text + capsys.readouterr().out
    assert secret not in out and other not in out
