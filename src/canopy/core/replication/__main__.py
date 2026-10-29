"""``python -m canopy.core.replication`` — set up and inspect replication to a history mirror.

Run::

    python -m canopy.core.replication publish     # primary: after `alembic upgrade head`, every deploy
    python -m canopy.core.replication grants      # mirror: after `alembic upgrade head`, every deploy
    python -m canopy.core.replication refresh     # mirror: after `grants`, and hourly
    python -m canopy.core.replication subscribe   # mirror: once, after its first migration
    python -m canopy.core.replication status      # either side: JSON on stdout
    python -m canopy.core.replication drift-check # mirror: compare with the primary, hourly after `refresh`
    python -m canopy.core.replication reseed --since <time>  # mirror: refill a gap, then resume replication
    python -m canopy.core.replication heartbeat --backup-ok --wal-archive-ok --drift-ok
                                                  # mirror: hourly, after its checks; writes to the primary

A primary that prunes old rows moves to the split publication layout once:
``publish --split`` on the primary, ``switch`` on the mirror, then ``publish --split``
on the primary again (see ``canopy.core.replication.publisher``).

The database is ``CANOPY_DATABASE_URL`` and the side is ``CANOPY_ROLE`` (each
command refuses on the wrong one; ``forward`` counts as a mirror). ``subscribe``
and ``refresh`` name the subscription after ``CANOPY_INSTANCE``; ``subscribe``
reads the primary's connection string from ``CANOPY_REPLICATION__PUBLISHER_DSN``,
never from the command line, and nothing the CLI prints contains it. ``heartbeat``
and ``switch`` reach the primary through ``CANOPY_REPLICATION__HEARTBEAT_DSN``,
handled the same way.

Logs go to stderr (JSON with ``CANOPY_LOG_FORMAT=json``), so stdout carries only
``status`` output. Exit status: 0 done (including "nothing to do"), 1 a database
step failed, 2 refused (wrong role or a failed precondition; nothing changed).
``drift-check`` differs: 0 in step, 1 drift found, 2 the check couldn't run (see
``canopy.core.replication.drift``).
"""

from __future__ import annotations

import sys
import json
import asyncio
import logging
import argparse

import asyncpg

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
from canopy.core.replication.migration import side_for_role
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
        description="Set up and inspect Postgres logical replication from a primary to its history mirror.",
    )
    commands = parser.add_subparsers(dest="command", required=True, metavar="command")
    publish_parser = commands.add_parser(
        "publish", help="primary: create or update the publications and the replication grants"
    )
    subscribe_parser = commands.add_parser("subscribe", help="mirror: create the subscription to the primary (once)")
    commands.add_parser("refresh", help="mirror: copy tables the primary has started publishing")
    commands.add_parser("grants", help="mirror: read access for the app role, read-write on local tables")
    commands.add_parser("status", help="either side: replication state as JSON on stdout")
    publish_parser.add_argument(
        "--split",
        action="store_true",
        help="move to canopy_ref + canopy_ts: create them, or, once the mirror has switched, drop canopy_history",
    )
    subscribe_parser.add_argument(
        "--split", action="store_true", help="subscribe to canopy_ref + canopy_ts (a primary already split)"
    )
    switch_parser = commands.add_parser(
        "switch", help="mirror: move the subscription to canopy_ref + canopy_ts (once, after `publish --split`)"
    )
    switch_parser.add_argument(
        "--timeout", type=float, default=120.0, help="seconds to wait for the slot to catch up (default 120)"
    )
    heartbeat_parser = commands.add_parser(
        "heartbeat", help="mirror: record this mirror's check results on the primary (hourly)"
    )
    for check in ("backup", "wal-archive", "drift"):
        heartbeat_parser.add_argument(
            f"--{check}-ok", action="store_true", help=f"the {check.replace('-', ' ')} check passed; omit if it failed"
        )
    check = commands.add_parser("drift-check", help="mirror: compare each table with the primary; exit 1 on drift")
    check.add_argument("--days", type=_positive_int, help="compare time-series rows from the last DAYS days only")
    check.add_argument(
        "--settle",
        type=float,
        default=30.0,
        metavar="SECONDS",
        help="pause before re-reading tables that differ, to let apply catch up (default 30; 0 skips the re-check)",
    )
    fill = commands.add_parser(
        "reseed", help="mirror: copy what the primary changed since a time, then resume replication from now"
    )
    fill.add_argument(
        "--since",
        required=True,
        type=reseed.parse_since,
        metavar="TIME",
        help="ISO 8601 time (UTC if no zone), no later than the newest snapshot the mirror holds",
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


async def run(command: str, options: argparse.Namespace | None = None) -> int:
    """Run one command against ``CANOPY_DATABASE_URL`` and return the exit status.

    ``options`` are the parsed command-line flags; ``None`` runs the command with its defaults.
    """
    options = options or build_parser().parse_args([command])
    settings = get_settings()
    config = get_replication_config()
    publisher_dsn = config.publisher_dsn.get_secret_value() if config.publisher_dsn else None
    heartbeat_dsn = config.heartbeat_dsn.get_secret_value() if config.heartbeat_dsn else None
    local_dsn = asyncpg_dsn(settings.database_url)
    # Only the publisher DSN enters SQL text, and the heartbeat DSN is a second
    # connection. Redacting the local one too would mask its password wherever it
    # appears ("canopy" in dev); password-shaped text is masked regardless.
    secrets = [publisher_dsn, heartbeat_dsn]

    def relay_server_message(_conn: asyncpg.Connection, message: asyncpg.PostgresLogMessage) -> None:
        level = logging.WARNING if message.severity in ("WARNING", "ERROR") else logging.INFO
        logger.log(level, "server: %s", redact(message.message, secrets))

    try:
        if (required := _COMMAND_ROLES[command]) is not None:
            require_role(command, required, settings.role)
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
            await subscribe(conn, role=settings.role, instance=settings.instance, config=config, split=options.split)
        elif command == "refresh":
            await refresh(conn, role=settings.role, instance=settings.instance)
        elif command == "grants":
            await grants(conn, role=settings.role, config=config)
        elif command == "switch":
            await switch(conn, role=settings.role, instance=settings.instance, config=config, timeout=options.timeout)
        elif command == "heartbeat":
            await heartbeat(
                conn,
                role=settings.role,
                instance=settings.instance,
                config=config,
                backup_ok=options.backup_ok,
                wal_archive_ok=options.wal_archive_ok,
                drift_ok=options.drift_ok,
            )
        elif side_for_role(settings.role) == "subscriber":
            print(json.dumps(await subscriber_status(conn, instance=settings.instance), indent=2))
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


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    _configure_logging(get_settings().log_format)
    if args.command == "drift-check":
        return asyncio.run(drift.run(args))
    if args.command == "reseed":
        return asyncio.run(reseed.run(args))
    return asyncio.run(run(args.command, args))


if __name__ == "__main__":
    sys.exit(main())
