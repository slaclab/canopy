"""What the outbox holds, per arm: one function for the CLI's ``status`` and the gauges.

Ages are measured on the database's clock, the one ``queued_at`` and
``delivered_at`` were written with.
"""

from __future__ import annotations

from collections.abc import Iterable

import asyncpg

from canopy.squirrel.config import OutboxConfig

_BY_ARM = """
SELECT arm,
       count(*) FILTER (WHERE state = 'pending') AS pending,
       count(*) FILTER (WHERE state = 'delivered') AS delivered,
       count(*) FILTER (WHERE state = 'failed') AS failed,
       coalesce(sum(size_bytes), 0) AS bytes,
       extract(epoch FROM now() - min(queued_at) FILTER (WHERE state = 'pending')) AS oldest_pending_age_s,
       extract(epoch FROM now() - min(delivered_at) FILTER (WHERE state = 'delivered')) AS oldest_delivered_age_s
FROM squirrel.snapshot_outbox
GROUP BY arm
"""

_FAILED = """
SELECT id::text, arm, title, created_at, failed_at, attempts, last_error
FROM squirrel.snapshot_outbox
WHERE state = 'failed'
ORDER BY arm, seq
"""


async def outbox_status(conn: asyncpg.Connection, *, arms: Iterable[str], config: OutboxConfig) -> dict:
    """The outbox per arm, as ``python -m canopy.squirrel.outbox status`` prints it.

    Every arm in ``arms`` (the arms this head delivers to) is listed, with zeros when it
    has nothing queued; so is an arm that has rows but is no longer configured.
    ``failed`` lists each refused snapshot with the arm's reason.
    """
    by_arm = {
        arm: {
            "pending": 0,
            "delivered": 0,
            "failed": 0,
            "bytes": 0,
            "oldest_pending_age_s": None,
            "oldest_delivered_age_s": None,
        }
        for arm in arms
    }
    for row in await conn.fetch(_BY_ARM):
        by_arm[row["arm"]] = {
            "pending": row["pending"],
            "delivered": row["delivered"],
            "failed": row["failed"],
            "bytes": int(row["bytes"]),
            "oldest_pending_age_s": _seconds(row["oldest_pending_age_s"]),
            "oldest_delivered_age_s": _seconds(row["oldest_delivered_age_s"]),
        }
    failed = [
        {
            "id": row["id"],
            "arm": row["arm"],
            "title": row["title"],
            "created_at": row["created_at"].isoformat(),
            "failed_at": row["failed_at"].isoformat() if row["failed_at"] else None,
            "attempts": row["attempts"],
            "reason": row["last_error"],
        }
        for row in await conn.fetch(_FAILED)
    ]
    return {
        "arms": dict(sorted(by_arm.items())),
        "failed": failed,
        "max_snapshots": config.max_snapshots,
        "max_bytes": config.max_bytes,
    }


def _seconds(value) -> float | None:
    return None if value is None else float(value)
