"""``python -m canopy.core.replication`` — set up and inspect replication from arms to a history head.

Run::

    python -m canopy.core.replication publish     # arm: after `alembic upgrade head`, every deploy
    python -m canopy.core.replication grants      # head: after `alembic upgrade head`, every deploy
    python -m canopy.core.replication refresh     # head: after `grants`, and hourly
    python -m canopy.core.replication subscribe   # head: once per arm, after its first migration
    python -m canopy.core.replication status      # either side: JSON on stdout
    python -m canopy.core.replication drift-check # head: compare with each arm, hourly after `refresh`
    python -m canopy.core.replication reseed --arm <arm> --since <time>  # head: refill one arm's gap
    python -m canopy.core.replication heartbeat --arm <arm> --backup-ok --wal-archive-ok --drift-ok
                                                  # head: hourly per arm, after its checks; writes to the arm

An arm that prunes old rows moves to the split publication layout once:
``publish --split`` on the arm, ``switch --arm <arm>`` on the head, then
``publish --split`` on the arm again (see ``canopy.core.replication.publisher``).

The database is ``CANOPY_DATABASE_URL`` and the side is ``CANOPY_ROLE`` (each
command refuses on the wrong one): an arm is a ``primary``, a head a ``mirror``.
On a head, ``subscribe``, ``refresh``, ``status`` and ``drift-check`` act on every
arm in ``CANOPY_ARMS__<arm>__…``, or on one with ``--arm <arm>``. ``subscribe`` reads each
arm's connection string from ``CANOPY_ARMS__<arm>__PUBLISHER_DSN``, never from the
command line, and nothing the CLI prints contains it. ``heartbeat`` and ``switch``
act on the one arm their ``--arm`` names and reach it through
``CANOPY_ARMS__<arm>__HEARTBEAT_DSN``, handled the same way.

Logs go to stderr (JSON with ``CANOPY_LOG_FORMAT=json``), so stdout carries only
``status`` output. Exit status: 0 done (including "nothing to do"), 1 a database
step failed, 2 refused (wrong role or a failed precondition; nothing changed).
``drift-check`` differs: 0 every table of every arm in step, 1 drift found, 2 the
check couldn't be completed (see ``canopy.core.replication.drift``). ``reseed``
acts on the one arm its ``--arm`` names (see ``canopy.core.replication.reseed``).
"""

from __future__ import annotations

import sys
import json
import asyncio
import logging
import argparse

import asyncpg
import pydantic

from canopy.core.arms import get_arms_config
from canopy.core.config import get_settings
from canopy.core.logging import JsonFormatter
from canopy.core.replication import drift, reseed
from canopy.core.replication.sql import (
    Refused,
    ReplicationError,
    redact,
    asyncpg_dsn,
    require_role,
)
from canopy.core.replication.config import get_replication_config
from canopy.core.replication.publisher import publish, publisher_status
from canopy.core.replication.subscriber import (
    grants,
    switch,
    refresh,
    heartbeat,
    subscribe,
    subscriber_status,
)

logger = logging.getLogger("canopy.core.replication")

# Which CANOPY_ROLE each command needs; checked before connecting so a command run
# on the wrong side fails without touching the database.
_COMMAND_ROLES = {
    "publish": "primary",
    "subscribe": "mirror",
    "refresh": "mirror",
    "grants": "mirror",
    "status": None,
    "switch": "mirror",
    "heartbeat": "mirror",
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m canopy.core.replication",
        description="Set up and inspect Postgres logical replication from arms to their history head.",
    )
    commands = parser.add_subparsers(dest="command", required=True, metavar="command")
    publish_parser = commands.add_parser(
        "publish", help="arm: create or update the publications and the replication grants"
    )
    publish_parser.add_argument(
        "--split",
        action="store_true",
        help="move to canopy_ref + canopy_ts: create them, or, once the head has switched, drop canopy_history",
    )
    for command, help_text in (
        ("subscribe", "head: create the subscription to each arm (once per arm)"),
        ("refresh", "head: copy tables each arm has started publishing"),
        ("status", "either side: replication state as JSON on stdout (per arm on a head)"),
        ("drift-check", "head: compare each arm's rows; exit 1 on drift, 2 when not everything was checked"),
    ):
        sub = commands.add_parser(command, help=help_text)
        sub.add_argument("--arm", metavar="ARM", help="act on this arm only (default: every configured arm)")
    commands.add_parser("grants", help="head: the app role reads every row and writes only the head's own")
    commands.choices["subscribe"].add_argument(
        "--split", action="store_true", help="subscribe to canopy_ref + canopy_ts (an arm already split; needs --arm)"
    )
    switch_parser = commands.add_parser(
        "switch", help="head: move one arm's subscription to canopy_ref + canopy_ts (once, after `publish --split`)"
    )
    switch_parser.add_argument("--arm", required=True, metavar="ARM", help="the arm whose subscription moves")
    switch_parser.add_argument(
        "--timeout", type=float, default=120.0, help="seconds to wait for the slot to catch up (default 120)"
    )
    heartbeat_parser = commands.add_parser(
        "heartbeat", help="head: record its check results for one arm on that arm (hourly, per arm)"
    )
    heartbeat_parser.add_argument("--arm", required=True, metavar="ARM", help="the arm the drift check was for")
    for flag in ("backup", "wal-archive", "drift"):
        heartbeat_parser.add_argument(
            f"--{flag}-ok", action="store_true", help=f"the {flag.replace('-', ' ')} check passed; omit if it failed"
        )
    check = commands.choices["drift-check"]
    check.add_argument("--days", type=_positive_int, help="compare time-series rows from the last DAYS days only")
    check.add_argument(
        "--settle",
        type=float,
        default=30.0,
        metavar="SECONDS",
        help="how long, per arm, to wait for that arm's slot to confirm its position and retry tables "
        "that came out inconclusive (default 30)",
    )
    fill = commands.add_parser(
        "reseed", help="head: copy what one arm changed since a time, then resume its replication from now"
    )
    fill.add_argument("--arm", required=True, metavar="ARM", help="the arm to re-seed; other arms are untouched")
    fill.add_argument(
        "--since",
        required=True,
        type=reseed.parse_since,
        metavar="TIME",
        help="ISO 8601 time (UTC if no zone), no later than the newest snapshot the head holds from that arm",
    )
    fill.add_argument(
        "--cut-wait",
        type=float,
        default=reseed.CUT_WAIT_S,
        metavar="SECONDS",
        help="how long to keep trying for an exact cut while the arm writes, before refusing (default 60)",
    )
    return parser


def _positive_int(value: str) -> int:
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError(f"must be at least 1, not {number}")
    return number


def _configure_logging(log_format: str) -> None:
    """Send this package's records to stderr, once."""
    if any(getattr(handler, "_canopy_replication", False) for handler in logger.handlers):
        return
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(JsonFormatter() if log_format == "json" else logging.Formatter("%(levelname)s %(message)s"))
    handler._canopy_replication = True  # type: ignore[attr-defined]
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


async def run(command: str, arm: str | None = None, options: argparse.Namespace | None = None) -> int:
    """Run one command against ``CANOPY_DATABASE_URL`` and return the exit status.

    ``options`` are the parsed command-line flags; ``None`` runs the command with its defaults.
    """
    options = options or build_parser().parse_args([command, *(["--arm", arm] if arm else [])])
    settings = get_settings()
    config = get_replication_config()
    try:
        arms = get_arms_config().arms
    except pydantic.ValidationError as exc:
        logger.error("refused: %s", _settings_errors(exc, "CANOPY_"))
        return 2
    local_dsn = asyncpg_dsn(settings.database_url)
    # The arms' publisher DSNs enter SQL text, and their heartbeat DSNs are second
    # connections. Redacting the local one too would mask its password wherever it
    # appears ("canopy" in dev); password-shaped text is masked regardless.
    secrets = [
        dsn.get_secret_value() for a in arms.values() for dsn in (a.publisher_dsn, a.heartbeat_dsn) if dsn is not None
    ]

    def relay_server_message(_conn: asyncpg.Connection, message: asyncpg.PostgresLogMessage) -> None:
        level = logging.WARNING if message.severity in ("WARNING", "ERROR") else logging.INFO
        logger.log(level, "server: %s", redact(message.message, secrets))

    try:
        if (required := _COMMAND_ROLES[command]) is not None:
            require_role(command, required, settings.role)
        if arm is not None and settings.role != "mirror":
            raise Refused("--arm applies on a head (CANOPY_ROLE=mirror); an arm reports only itself")
        conn = await asyncpg.connect(local_dsn, server_settings={"application_name": "canopy-replication"})
    except Refused as exc:
        logger.error("refused: %s", exc)
        return 2
    except (OSError, asyncpg.PostgresError) as exc:
        logger.error("cannot connect to the database: %s", redact(str(exc), secrets))
        return 1

    conn.add_log_listener(relay_server_message)
    try:
        if command == "publish":
            await publish(conn, role=settings.role, config=config, split=options.split)
        elif command == "subscribe":
            await subscribe(conn, role=settings.role, head=settings.instance, arms=arms, only=arm, split=options.split)
        elif command == "refresh":
            await refresh(conn, role=settings.role, arms=arms, only=arm)
        elif command == "grants":
            await grants(conn, role=settings.role, head=settings.instance, config=config)
        elif command == "switch":
            await switch(conn, role=settings.role, arm=arm, arms=arms, timeout=options.timeout)
        elif command == "heartbeat":
            await heartbeat(
                conn,
                role=settings.role,
                arm=arm,
                arms=arms,
                backup_ok=options.backup_ok,
                wal_archive_ok=options.wal_archive_ok,
                drift_ok=options.drift_ok,
            )
        elif settings.role == "mirror":
            print(json.dumps(await subscriber_status(conn, arms=arms, only=arm), indent=2))
        else:
            print(json.dumps(await publisher_status(conn, config=config), indent=2))
    except Refused as exc:
        logger.error("refused: %s", exc)
        return 2
    except ReplicationError as exc:
        logger.error("%s", exc)
        return 1
    except (OSError, asyncpg.PostgresError) as exc:
        logger.error("%s failed: %s", command, redact(str(exc), secrets))
        return 1
    finally:
        await conn.close()
    return 0


def _settings_errors(exc: pydantic.ValidationError, prefix: str) -> str:
    """Which settings are wrong and why, without the values: the error's own text would echo them."""
    return "; ".join(
        f"{prefix}{'__'.join(str(part) for part in error['loc']).upper()}: {error['msg']}" for error in exc.errors()
    )


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        settings = get_settings()
    except pydantic.ValidationError as exc:
        _configure_logging("text")
        logger.error("refused: %s", _settings_errors(exc, "CANOPY_"))
        return 2
    _configure_logging(settings.log_format)
    if args.command == "drift-check":
        return asyncio.run(drift.run(args))
    if args.command == "reseed":
        return asyncio.run(reseed.run(args))
    return asyncio.run(run(args.command, getattr(args, "arm", None), args))


if __name__ == "__main__":
    sys.exit(main())
