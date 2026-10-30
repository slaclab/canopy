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


@pytest.mark.parametrize(
    ("command", "role"),
    [
        ("publish", "mirror"),
        ("publish", "forward"),
        ("subscribe", "primary"),
        ("grants", "primary"),
        ("switch", "primary"),
        ("heartbeat", "primary"),
    ],
)
def test_wrong_side_is_refused_without_connecting(env, no_connect, caplog, command, role):
    env(CANOPY_ROLE=role)
    assert cli.main([command]) == 2
    assert "refused:" in caplog.text


def test_connection_failure_exits_1_without_the_password(env, caplog):
    env(CANOPY_DATABASE_URL="postgresql+asyncpg://canopy:s3cret-local@127.0.0.1:1/canopy")
    assert cli.main(["status"]) == 1
    assert "cannot connect" in caplog.text
    assert "s3cret-local" not in caplog.text


def test_split_switch_and_heartbeat_options():
    parser = cli.build_parser()

    assert parser.parse_args(["publish", "--split"]).split is True
    assert parser.parse_args(["publish"]).split is False
    assert parser.parse_args(["subscribe", "--split"]).split is True
    assert parser.parse_args(["switch"]).timeout == 120.0
    checks = parser.parse_args(["heartbeat", "--backup-ok", "--drift-ok"])
    assert (checks.backup_ok, checks.wal_archive_ok, checks.drift_ok) == (True, False, True)


async def test_an_unreachable_primary_is_reported_without_the_heartbeat_password():
    from pydantic import SecretStr

    from canopy.core.replication.config import ReplicationConfig
    from canopy.core.replication.subscriber import ReplicationError, _connect_primary

    config = ReplicationConfig(heartbeat_dsn=SecretStr("postgresql://beat:s3cret-beat@127.0.0.1:1/canopy"))
    with pytest.raises(ReplicationError) as caught:
        await _connect_primary(config, "heartbeat")
    assert "cannot connect to the primary" in str(caught.value)
    assert "s3cret-beat" not in str(caught.value)


def test_logging_is_configured_once():
    cli._configure_logging("text")
    cli._configure_logging("json")
    assert sum(getattr(h, "_canopy_replication", False) for h in cli.logger.handlers) == 1


def _sqlalchemy_url(dsn: str) -> str:
    return make_url(dsn).set(drivername="postgresql+asyncpg").render_as_string(hide_password=False)


async def _run(command: str) -> int:
    # main() calls asyncio.run, which can't nest inside the test's loop.
    return await cli.run(command)


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


@pytest.mark.parametrize("role", ["mirror", "forward"])
async def test_mirror_status_through_the_cli(db, env, capsys, role):
    env(CANOPY_DATABASE_URL=_sqlalchemy_url(DB_DSN), CANOPY_ROLE=role, CANOPY_INSTANCE="lab")
    assert await _run("status") == 0
    assert json.loads(capsys.readouterr().out)["subscription"] == {"name": "canopy_history_lab", "exists": False}
