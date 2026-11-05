"""Import snapshots from NDJSON files into the squirrel schema.

Canopy's ``POST /squirrel/v1/snapshots`` captures *live* EPICS values, so it cannot
load historical snapshots. This script fills that gap for migrations from
react-squirrel-backend: it reads NDJSON (one JSON snapshot per line) and writes the
``snapshot`` + ``snapshot_value`` rows directly, reusing
``canopy.squirrel.services.bulk_insert`` so the rows land exactly as a live capture
would (COPY, pre-serialized JSONB, one transaction per file).

PVs are matched to the existing registry by ``setpoint_address``; this script never
creates PVs or tags. Values whose address is absent (or unregistered) are skipped and
counted, which keeps a partially-registered archive importable instead of failing.

Usage (inside the api container, which has canopy + the DB URL on hand)::

    docker exec -i canopy-api uv run python scripts/import_snapshots.py /data/snap.ndjson
    docker exec -i canopy-api uv run python scripts/import_snapshots.py --dry-run /data/*.ndjson
"""

from __future__ import annotations

import sys
import json
import uuid
import asyncio
import logging
import argparse
from typing import Any
from datetime import UTC, date, datetime
from pathlib import Path

from sqlalchemy import select

from canopy.core.db import session_scope
from canopy.squirrel.data.models import Pv, Snapshot
from canopy.squirrel.services.bulk_insert import bulk_insert_snapshot_values

logger = logging.getLogger("import_snapshots")

# Imported snapshots are attributed to this author so they are distinguishable from
# live captures (which carry the API key's user, or NULL).
CREATED_BY = "Import"


def _normalize_datetime(value: Any) -> datetime:
    """Coerce a snapshot's ``created_at`` to an aware UTC datetime.

    Naive timestamps are the common case in the archive (``2026-07-20T06:17:24``);
    they are treated as UTC rather than local so an import is reproducible on any host.
    """
    if isinstance(value, datetime):
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=UTC)
    if isinstance(value, str):
        try:
            return _normalize_datetime(datetime.fromisoformat(value))
        except ValueError:
            pass
    return datetime.now(UTC)


def _build_value_row(
    snapshot_id: str,
    entry: dict,
    pv_ids: dict[str, str],
    captured_at: datetime,
) -> dict | None:
    """Map one NDJSON ``values`` entry to a ``bulk_insert`` row dict, or None to skip.

    ``status``/``severity``/``timestamp`` are read out of the setpoint blob when the
    source recorded them, falling back to the snapshot's own capture time.
    """
    address = entry.get("setpoint_address")
    if not address:
        return None
    pv_id = pv_ids.get(address)
    if not pv_id:
        return None

    setpoint = entry.get("setpoint_value")
    timestamp = captured_at
    status = severity = None
    if isinstance(setpoint, dict):
        status = setpoint.get("status")
        severity = setpoint.get("severity")
        raw_ts = setpoint.get("timestamp")
        if isinstance(raw_ts, str):
            try:
                timestamp = _normalize_datetime(datetime.fromisoformat(raw_ts))
            except ValueError:
                pass

    return {
        "snapshot_id": snapshot_id,
        "pv_id": pv_id,
        "pv_name": address,
        "setpoint_value": setpoint if isinstance(setpoint, dict) else None,
        "readback_value": entry.get("readback_value") if isinstance(entry.get("readback_value"), dict) else None,
        "status": status,
        "severity": severity,
        "timestamp": timestamp,
    }


def _iter_snapshots(path: Path):
    """Yield ``(line_number, parsed_object)`` for each non-blank NDJSON line."""
    with path.open() as handle:
        for line_number, line in enumerate(handle, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                yield line_number, json.loads(line)
            except json.JSONDecodeError as exc:
                logger.warning("%s:%d skipped — invalid JSON (%s)", path.name, line_number, exc)


async def _load_pv_ids(session) -> dict[str, str]:
    """Map ``setpoint_address`` -> pv id for every registered PV that has one."""
    result = await session.execute(select(Pv.setpoint_address, Pv.id).where(Pv.setpoint_address.is_not(None)))
    return {address: pv_id for address, pv_id in result}


async def import_file(path: Path, *, dry_run: bool) -> dict[str, int]:
    """Import one NDJSON file. Returns per-file counts; commits once at the end."""
    stats = {"snapshots": 0, "values": 0, "skipped": 0}

    async with session_scope() as session:
        pv_ids = await _load_pv_ids(session)
        logger.info("%s: %d registered PVs available for matching", path.name, len(pv_ids))

        for line_number, payload in _iter_snapshots(path):
            title = payload.get("title")
            if not title:
                logger.warning("%s:%d skipped — no title", path.name, line_number)
                continue

            captured_at = _normalize_datetime(payload.get("created_at"))
            snapshot_id = str(uuid.uuid4())

            rows: list[dict] = []
            for entry in payload.get("values") or []:
                row = _build_value_row(snapshot_id, entry, pv_ids, captured_at)
                if row is None:
                    stats["skipped"] += 1
                else:
                    rows.append(row)

            stats["snapshots"] += 1
            stats["values"] += len(rows)
            logger.info(
                "%s:%d %r — %d values (%d unmatched so far)",
                path.name,
                line_number,
                title[:60],
                len(rows),
                stats["skipped"],
            )

            if dry_run:
                continue

            # created_at is set explicitly so the imported snapshot keeps its original
            # capture time rather than today's date (TimestampMixin would default it).
            session.add(
                Snapshot(
                    id=snapshot_id,
                    title=title,
                    description=payload.get("description"),
                    created_by=CREATED_BY,
                    created_at=captured_at,
                )
            )
            await session.flush()  # satisfy snapshot_value's FK before the COPY
            await bulk_insert_snapshot_values(session, rows)

        if dry_run:
            await session.rollback()
        else:
            await session.commit()

    return stats


async def main_async(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("files", nargs="+", type=Path, help="NDJSON file(s) to import")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and match against the PV registry, then roll back without writing.",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)

    missing = [p for p in args.files if not p.is_file()]
    if missing:
        parser.error("no such file(s): " + ", ".join(str(p) for p in missing))

    totals = {"snapshots": 0, "values": 0, "skipped": 0}
    for path in args.files:
        stats = await import_file(path, dry_run=args.dry_run)
        for key, value in stats.items():
            totals[key] += value

    label = "DRY RUN (rolled back)" if args.dry_run else "IMPORTED"
    logger.info(
        "%s: %d snapshot(s), %d value(s), %d value(s) skipped (no/unregistered setpoint address)",
        label,
        totals["snapshots"],
        totals["values"],
        totals["skipped"],
    )
    return 0


def main() -> int:
    return asyncio.run(main_async(sys.argv[1:]))


if __name__ == "__main__":
    raise SystemExit(main())
