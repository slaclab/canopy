"""Talking to the primary, and waiting for replication to bring a change back.

Shared by the middleware (forwarded writes) and the jobs route (status of a job a
forwarded write started on the primary).
"""

from __future__ import annotations

import json
import time
import asyncio
import logging
from urllib.parse import urlsplit

import httpx
import asyncpg
from sqlalchemy.exc import SQLAlchemyError

from canopy.core.db import get_engine
from canopy.core.config import get_settings
from canopy.core.errors import PrimaryTimeout, PrimaryUnavailable
from canopy.core.security import APIKey
from canopy.core.replication.sql import Refused
from canopy.core.forwarding.config import ForwardConfig
from canopy.core.forwarding.context import (
    ON_BEHALF_OF_HEADER,
    ON_BEHALF_OF_FACILITIES_HEADER,
)
from canopy.core.replication.subscriber import applied_lsn, subscription_name

logger = logging.getLogger(__name__)

# Request headers passed on to the primary. Everything else stays here: the caller's
# key, cookies and credentials from the history ingress, hop-by-hop headers.
PASSED_REQUEST_HEADERS = ("content-type", "accept")
# Response headers passed back, besides Canopy's own X-Canopy-* headers.
PASSED_RESPONSE_HEADERS = ("content-type", "location")

_POLL_S = 0.05


def on_behalf_of(key: APIKey) -> str:
    """The name the primary records for ``key``'s writes: ``<key name>@<instance>``.

    Unprintable characters become ``?`` and the result is cut to 255 characters, the
    primary's limit, so a key's odd name never turns into a 400 from the primary.
    """
    instance = get_settings().instance or "forward"
    name = f"{key.name}@{instance}"
    return "".join(char if char.isprintable() else "?" for char in name)[:255]


def check_config(config: ForwardConfig) -> tuple[str, str]:
    """The primary's base URL and the service key, or raise when forward mode is misconfigured.

    Raises
    ------
    PrimaryUnavailable
        When either is unset, or the URL isn't https without ``allow_insecure_http``.
    """
    if not config.primary_url or config.service_key is None:
        raise PrimaryUnavailable("Forward mode needs CANOPY_FORWARD__PRIMARY_URL and CANOPY_FORWARD__SERVICE_KEY")
    scheme = urlsplit(config.primary_url).scheme
    if scheme != "https" and not (scheme == "http" and config.allow_insecure_http):
        raise PrimaryUnavailable("CANOPY_FORWARD__PRIMARY_URL must be an https:// URL")
    return config.primary_url, config.service_key.get_secret_value()


async def send_to_primary(
    config: ForwardConfig,
    method: str,
    path: str,
    *,
    caller: APIKey,
    query: str = "",
    body: bytes = b"",
    headers: dict[str, str] | None = None,
) -> httpx.Response:
    """Send one request to the primary as this instance's service key, on ``caller``'s behalf.

    ``headers`` are the caller's; only ``PASSED_REQUEST_HEADERS`` go on. Never retried:
    a write that timed out may have been applied.

    Raises
    ------
    PrimaryUnavailable
        502, the primary couldn't be reached; nothing was sent.
    PrimaryTimeout
        504, the request went out but no answer came back; it may have been applied.
    """
    base_url, service_key = check_config(config)
    outgoing = {name: value for name, value in (headers or {}).items() if name.lower() in PASSED_REQUEST_HEADERS}
    outgoing |= {
        "X-API-Key": service_key,
        ON_BEHALF_OF_HEADER: on_behalf_of(caller),
        # The primary limits the request to the facilities both keys reach.
        ON_BEHALF_OF_FACILITIES_HEADER: json.dumps(sorted(caller.facilities)),
    }
    url = base_url + path + (f"?{query}" if query else "")
    try:
        async with httpx.AsyncClient(verify=config.ca_bundle or True, timeout=config.timeout_s) as client:
            return await client.request(method, url, content=body, headers=outgoing)
    except (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout) as exc:
        logger.warning("Primary unreachable for %s %s: %s", method, path, exc)
        raise PrimaryUnavailable("The primary could not be reached; nothing was sent to it") from None
    except httpx.TransportError as exc:
        logger.warning("No answer from the primary for %s %s: %s", method, path, exc)
        raise PrimaryTimeout(
            "The primary did not answer; the request may or may not have been applied there. "
            "Check before sending it again"
        ) from None


def lsn_value(lsn: str) -> int:
    """A ``pg_lsn`` text value (``"16/B374D848"``) as a comparable integer."""
    high, _, low = lsn.partition("/")
    return (int(high, 16) << 32) | int(low, 16)


async def wait_until_applied(commit_lsn: str, timeout_s: float) -> bool:
    """Wait up to ``timeout_s`` for this instance's subscription to apply ``commit_lsn``.

    Polls ``applied_lsn`` (``pg_replication_origin_status.remote_lsn``). ``False`` on a
    timeout, and also when the position can't be read (no subscription, no
    ``CANOPY_INSTANCE``, or no grant on the view): the caller then marks the answer
    stale, which is true as far as this instance can tell. Never raises; the write
    already succeeded on the primary.
    """
    try:
        target = lsn_value(commit_lsn)
        subscription = subscription_name(get_settings().instance)
    except (ValueError, Refused) as exc:
        logger.warning("Cannot wait for a forwarded write to replicate: %s", exc)
        return False
    deadline = time.monotonic() + timeout_s
    try:
        async with get_engine().connect() as conn:
            raw = (await conn.get_raw_connection()).driver_connection
            while True:
                applied = await applied_lsn(raw, subscription)
                if applied is not None and lsn_value(applied) >= target:
                    return True
                if time.monotonic() >= deadline:
                    return False
                await asyncio.sleep(_POLL_S)
    except asyncpg.InsufficientPrivilegeError:
        logger.warning(
            "Cannot read pg_replication_origin_status, so forwarded writes can't wait for replication. As a "
            "superuser, grant the app role SELECT on it and EXECUTE on pg_show_replication_origin_status()"
        )
    except (OSError, asyncpg.PostgresError, SQLAlchemyError) as exc:
        logger.warning("Cannot read the applied replication position: %s", exc)
    return False
