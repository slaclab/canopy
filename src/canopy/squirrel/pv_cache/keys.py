"""Redis keys and the entry format shared by the PV cache's writer and readers.

The elected ``canopy-worker`` writes; snapshot create and the health endpoint read.
Each cached value is stored as ``{"gen": <lease generation>, "cachedAt": <unix seconds>,
"pv": <PVValue>}`` so a reader gets back exactly the ``PVValue`` a live
``connector.get()`` would return, and can tell which leader's lease wrote it.
"""

from __future__ import annotations

import json
from typing import Any

from canopy.contracts.pv import PVValue

# Hash: PV address (as registered, protocol prefix included) -> JSON entry.
VALUES_KEY = "canopy:squirrel:pv_cache:values"
# String: the leader's instance id, with a lease TTL.
LEADER_KEY = "canopy:squirrel:pv_cache:leader"
# String: the leader's JSON report, rewritten on every flush (its heartbeat).
STATUS_KEY = "canopy:squirrel:pv_cache:status"

# Entries per HSET / HMGET call, so a full 40k-PV warm-up isn't one multi-MB command.
BATCH_SIZE = 2000


def encode_entry(value: PVValue, cached_at: float, generation: str | None) -> str:
    return json.dumps({"gen": generation, "cachedAt": cached_at, "pv": value.model_dump(mode="json")})


def decode_entry(raw: bytes | str) -> tuple[str | None, PVValue]:
    """The generation that wrote the entry, and its value."""
    entry = json.loads(raw)
    return entry.get("gen"), PVValue.model_validate(entry["pv"])


async def read_status(redis: Any) -> dict[str, Any] | None:
    """The leader's last report, or ``None`` when no worker has ever led the cache."""
    raw = await redis.get(STATUS_KEY)
    return None if raw is None else json.loads(raw)
