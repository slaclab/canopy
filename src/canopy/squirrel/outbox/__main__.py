"""``python -m canopy.squirrel.outbox`` — inspect a history head's snapshot outbox and act on failed rows.

Run on the head, against ``CANOPY_DATABASE_URL``::

    python -m canopy.squirrel.outbox status                  # JSON on stdout: each arm's queue, failed rows
    python -m canopy.squirrel.outbox retry <id>... | --arm <arm> [--allow-skips]
                                                             # send failed rows again, at the next delivery run
    python -m canopy.squirrel.outbox discard <id>            # delete one failed row and its bundle

``retry`` puts failed rows back in their arm's queue, in their original order, due at
once; the worker's next delivery run (every ``CANOPY_SQUIRREL__OUTBOX__INTERVAL_S``)
sends them, so the command needs no Redis. ``--allow-skips`` sends them without the
strict flag, so an arm that refused one with ``import_would_skip`` saves it without
the values of PVs that moved since the capture.

``discard`` deletes a failed row for good, bundle and all: it was never saved
anywhere else. Export it first if it should be kept
(``GET /squirrel/v1/snapshots/<id>/export`` on the head). Pending rows can't be
discarded.

Logs go to stderr, so stdout carries only JSON. Exit status: 0 done, 1 the database
step failed or the command was refused (an id that isn't a failed row).
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
from canopy.squirrel.config import get_squirrel_config
from canopy.core.replication.sql import asyncpg_dsn
from canopy.core.forwarding.client import forwardable_arms
from canopy.squirrel.outbox.status import outbox_status

logger = logging.getLogger("canopy.squirrel.outbox")

_RETRY = """
UPDATE squirrel.snapshot_outbox
SET state = 'pending', attempts = 0, next_attempt_at = now(), failed_at = NULL, last_error = NULL,
    strict = strict AND NOT $3
WHERE state = 'failed' AND (id::text = ANY($1::text[]) OR arm = $2)
RETURNING id::text
"""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m canopy.squirrel.outbox",
        description="Inspect a history head's snapshot outbox, and retry or discard snapshots an arm refused.",
    )
    commands = parser.add_subparsers(dest="command", required=True, metavar="command")
    commands.add_parser("status", help="each arm's queue and every failed row, as JSON on stdout")
    retry = commands.add_parser("retry", help="send failed rows again at the next delivery run")
    which = retry.add_mutually_exclusive_group(required=True)
    which.add_argument("ids", nargs="*", default=[], metavar="id", help="failed rows to retry")
    which.add_argument("--arm", help="retry every failed row of this arm")
    retry.add_argument(
        "--allow-skips",
        action="store_true",
        help="let the arm save them without the values of PVs that moved since the capture",
    )
    discard = commands.add_parser("discard", help="delete one failed row and its bundle (export it first)")
    discard.add_argument("id", help="the failed row to delete")
    return parser


def _configure_logging(log_format: str) -> None:
    if any(getattr(handler, "_canopy_outbox", False) for handler in logger.handlers):
        return
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(JsonFormatter() if log_format == "json" else logging.Formatter("%(levelname)s %(message)s"))
    handler._canopy_outbox = True  # type: ignore[attr-defined]
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


async def run(options: argparse.Namespace) -> int:
    """Run one command against ``CANOPY_DATABASE_URL`` and return the exit status."""
    try:
        conn = await asyncpg.connect(
            asyncpg_dsn(get_settings().database_url), server_settings={"application_name": "canopy-outbox"}
        )
    except (OSError, asyncpg.PostgresError) as exc:
        logger.error("cannot connect to the database: %s", exc)
        return 1
    try:
        if options.command == "status":
            status = await outbox_status(conn, arms=sorted(forwardable_arms()), config=get_squirrel_config().outbox)
            print(json.dumps(status, indent=2))
            return 0
        if options.command == "retry":
            return await _retry(conn, options)
        return await _discard(conn, options.id)
    except (OSError, asyncpg.PostgresError) as exc:
        logger.error("%s failed: %s", options.command, exc)
        return 1
    finally:
        await conn.close()


async def _retry(conn: asyncpg.Connection, options: argparse.Namespace) -> int:
    retried = [row["id"] for row in await conn.fetch(_RETRY, options.ids, options.arm, options.allow_skips)]
    print(json.dumps({"retried": retried}, indent=2))
    if missing := sorted(set(options.ids) - set(retried)):
        logger.error("not failed rows of the outbox, so not retried: %s", ", ".join(missing))
        return 1
    logger.info("%d row(s) queued again; the next delivery run sends them", len(retried))
    return 0


async def _discard(conn: asyncpg.Connection, entry_id: str) -> int:
    row = await conn.fetchrow(
        "DELETE FROM squirrel.snapshot_outbox WHERE id::text = $1 AND state = 'failed' RETURNING arm, size_bytes",
        entry_id,
    )
    if row is None:
        state = await conn.fetchval("SELECT state FROM squirrel.snapshot_outbox WHERE id::text = $1", entry_id)
        if state is None:
            logger.error("refused: the outbox has no row %s", entry_id)
        else:
            logger.error("refused: %s is %s, not failed; only a failed row can be discarded", entry_id, state)
        return 1
    logger.warning("Discarded snapshot %s for %s (%d bytes); it is gone", entry_id, row["arm"], row["size_bytes"])
    print(json.dumps({"discarded": entry_id, "arm": row["arm"], "bytes": row["size_bytes"]}, indent=2))
    return 0


def main(argv: list[str] | None = None) -> int:
    options = build_parser().parse_args(argv)
    _configure_logging(get_settings().log_format)
    return asyncio.run(run(options))


if __name__ == "__main__":
    sys.exit(main())
