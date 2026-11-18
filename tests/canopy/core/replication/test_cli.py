"""``python -m canopy.core.replication``: exit codes, the role check, and what reaches the output."""

import json
import logging

import pytest
from sqlalchemy.engine import make_url

from canopy.core.config import get_settings
from canopy.core.replication import __main__ as cli

from .conftest import DB_DSN


@pytest.fixture(autouse=True)
def drop_cli_handler():
    """Remove the stderr handler ``main`` installs, so it can't outlive this test's capture."""
    yield
    for handler in [h for h in cli.logger.handlers if getattr(h, "_canopy_replication", False)]:
        cli.logger.removeHandler(handler)


@pytest.fixture
def env(monkeypatch):
    """Set ``CANOPY_*`` variables for one test, rebuilding the cached ``Settings`` around it."""

    def set_env(**values: str) -> None:
        for key, value in values.items():
            monkeypatch.setenv(key, value)
        get_settings.cache_clear()

    yield set_env
    get_settings.cache_clear()


@pytest.fixture
def no_connect(monkeypatch):
    async def refuse(*args, **kwargs):
        raise AssertionError("the CLI connected before checking CANOPY_ROLE")

    monkeypatch.setattr(cli.asyncpg, "connect", refuse)


def test_a_command_is_required():
    with pytest.raises(SystemExit) as caught:
        cli.main([])
    assert caught.value.code == 2


@pytest.mark.parametrize(("command", "role"), [("publish", "mirror"), ("subscribe", "primary"), ("grants", "primary")])
def test_wrong_side_is_refused_without_connecting(env, no_connect, caplog, command, role):
    env(CANOPY_ROLE=role)
    assert cli.main([command]) == 2
    assert "refused:" in caplog.text


def test_connection_failure_exits_1_without_the_password(env, caplog):
    env(CANOPY_DATABASE_URL="postgresql+asyncpg://canopy:s3cret-local@127.0.0.1:1/canopy")
    assert cli.main(["status"]) == 1
    assert "cannot connect" in caplog.text
    assert "s3cret-local" not in caplog.text


def test_logging_is_configured_once():
    cli._configure_logging("text")
    cli._configure_logging("json")
    assert sum(getattr(h, "_canopy_replication", False) for h in cli.logger.handlers) == 1


def _sqlalchemy_url(dsn: str) -> str:
    return make_url(dsn).set(drivername="postgresql+asyncpg").render_as_string(hide_password=False)


async def _run(command: str, arm: str | None = None) -> int:
    # main() calls asyncio.run, which can't nest inside the test's loop.
    return await cli.run(command, arm)


async def test_publish_then_status_through_the_cli(db, config, env, capsys):
    env(
        CANOPY_DATABASE_URL=_sqlalchemy_url(DB_DSN),
        CANOPY_REPLICATION__REPLICATOR_ROLE=config.replicator_role,
        CANOPY_REPLICATION__READER_ROLE=config.reader_role,
    )
    assert await _run("publish") == 0
    assert await _run("status") == 0

    status = json.loads(capsys.readouterr().out)
    assert status["side"] == "publisher"
    assert status["publications"][0]["exists"] is True


async def test_a_refusal_inside_a_command_exits_2(db, env, caplog):
    caplog.set_level(logging.INFO, logger="canopy.core.replication")
    env(CANOPY_DATABASE_URL=_sqlalchemy_url(DB_DSN))
    await db.execute("CREATE TABLE magpie.mystery (id int PRIMARY KEY)")

    assert await _run("publish") == 2
    assert "unclassified tables: magpie.mystery" in caplog.text


async def test_head_status_through_the_cli(db, env, capsys):
    env(
        CANOPY_DATABASE_URL=_sqlalchemy_url(DB_DSN),
        CANOPY_ROLE="mirror",
        CANOPY_INSTANCE="head",
        CANOPY_ARMS__ARM1__PUBLISHER_DSN="host=arm1.example.org password=s3cret-arm",
        CANOPY_ARMS__ARM2__PUBLISHER_DSN="host=arm2.example.org password=s3cret-arm",
    )
    assert await _run("status") == 0
    every = json.loads(capsys.readouterr().out)
    assert [sub["arm"] for sub in every["subscriptions"]] == ["arm1", "arm2"]

    assert await _run("status", arm="arm2") == 0
    out = capsys.readouterr().out
    assert json.loads(out)["subscriptions"] == [{"arm": "arm2", "name": "canopy_history_arm2", "exists": False}]
    assert "s3cret-arm" not in out


async def test_an_unknown_arm_is_refused(db, env, caplog):
    env(CANOPY_DATABASE_URL=_sqlalchemy_url(DB_DSN), CANOPY_ROLE="mirror", CANOPY_ARMS__ARM1__PUBLISHER_DSN="host=x")
    assert await _run("refresh", arm="arm9") == 2
    assert "no arm named 'arm9'" in caplog.text


def test_arm_applies_only_on_a_head(env, no_connect, caplog):
    assert cli.main(["status", "--arm", "arm1"]) == 2
    assert "--arm applies on a head" in caplog.text


def test_an_invalid_arm_name_is_refused_without_echoing_its_settings(env, no_connect, caplog, monkeypatch):
    monkeypatch.setenv("CANOPY_ARMS", '{"Bad-Name": {"publisher_dsn": "host=x password=s3cret-arm"}}')
    assert cli.main(["status"]) == 2
    assert "CANOPY_ARMS: Value error, arm names must match" in caplog.text and "s3cret-arm" not in caplog.text


def test_a_missing_instance_is_refused(env, no_connect, caplog, monkeypatch):
    monkeypatch.delenv("CANOPY_INSTANCE", raising=False)
    env()
    assert cli.main(["status"]) == 2
    assert "refused: CANOPY_INSTANCE: Field required" in caplog.text


def test_only_head_commands_take_an_arm():
    parser = cli.build_parser()
    for command in ("subscribe", "refresh", "status"):
        assert parser.parse_args([command, "--arm", "arm1"]).arm == "arm1"
    for command in ("publish", "grants"):
        with pytest.raises(SystemExit):
            parser.parse_args([command, "--arm", "arm1"])
