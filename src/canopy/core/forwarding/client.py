"""Talking to an arm, and waiting for replication to bring a change back from it.

Shared by the middleware (forwarded writes) and the jobs route (status of a job a
forwarded write started on an arm).
"""

from __future__ import annotations

import json
import asyncio
import logging
from dataclasses import field, dataclass
from urllib.parse import quote, urlsplit
from collections.abc import Callable, Awaitable

import httpx
import asyncpg
from sqlalchemy.exc import SQLAlchemyError

from canopy.core.db import get_engine
from canopy.core.arms import get_arms_config, subscription_name
from canopy.core.config import get_settings
from canopy.core.errors import PrimaryTimeout, ServiceKeyRefused, PrimaryUnavailable
from canopy.core.security import APIKey
from canopy.core.forwarding.config import ForwardConfig, get_forward_config
from canopy.core.forwarding.context import (
    ON_BEHALF_OF_HEADER,
    ON_BEHALF_OF_FACILITIES_HEADER,
)
from canopy.core.replication.subscriber import applied_lsn

logger = logging.getLogger(__name__)

# Request headers passed on to the arm. Everything else stays here: the caller's
# key, cookies and credentials from the head's ingress, hop-by-hop headers.
PASSED_REQUEST_HEADERS = ("content-type", "accept")
# Response headers passed back, besides Canopy's own X-Canopy-* headers.
PASSED_RESPONSE_HEADERS = ("content-type", "location")

_POLL_S = 0.05


def on_behalf_of(key: APIKey) -> str:
    """The name an arm records for ``key``'s writes: ``<key name>@<instance>``.

    Unprintable characters become ``?`` and the result is cut to 255 characters, the
    arm's limit, so a key's odd name never turns into a 400 from the arm.
    """
    name = f"{key.name}@{get_settings().instance}"
    return "".join(char if char.isprintable() else "?" for char in name)[:255]


@dataclass(frozen=True)
class Caller:
    """Who a forwarded request is for: the name the arm records, and the facility grant it applies.

    Plain data, so a queued job can carry it and rebuild it later.
    """

    on_behalf_of: str
    facilities: list[str]

    @classmethod
    def of(cls, key: APIKey) -> Caller:
        return cls(on_behalf_of=on_behalf_of(key), facilities=sorted(key.facilities))


@dataclass(frozen=True)
class ArmTarget:
    """A forwardable arm: where its API is, and the service key the head holds there."""

    name: str
    url: str
    service_key: str = field(repr=False)

    @property
    def subscription(self) -> str:
        """The head's subscription to this arm, which a forwarded write waits on."""
        return subscription_name(self.name)


def forward_target(name: str) -> ArmTarget | None:
    """The arm ``name`` if the head forwards writes to it, else ``None``.

    ``None`` too for a name that isn't an arm, such as this instance's own.
    """
    arm = get_arms_config().arms.get(name)
    if arm is None or not arm.forwards:
        return None
    return ArmTarget(name=name, url=arm.forward_url, service_key=arm.service_key.get_secret_value())


def forwardable_arms() -> set[str]:
    """The names of the arms the head forwards writes to."""
    return {name for name, arm in get_arms_config().arms.items() if arm.forwards}


def check_forward_urls(config: ForwardConfig | None = None) -> None:
    """Refuse an arm forward URL that isn't https, unless ``allow_insecure_http``.

    Raises
    ------
    ValueError
        Naming each arm whose URL is refused. Run at startup, so a head with a bad
        setting doesn't start.
    """
    config = config or get_forward_config()
    bad = sorted(
        name
        for name, arm in get_arms_config().arms.items()
        if arm.forwards and not _allowed_scheme(arm.forward_url, config)
    )
    if bad:
        raise ValueError(f"CANOPY_ARMS__<arm>__FORWARD_URL must be an https:// URL, for: {', '.join(bad)}")


def _allowed_scheme(url: str, config: ForwardConfig) -> bool:
    scheme = urlsplit(url).scheme
    return scheme == "https" or (scheme == "http" and config.allow_insecure_http)


async def send_to_arm(
    arm: ArmTarget,
    method: str,
    path: str,
    *,
    caller: Caller,
    query: str = "",
    body: bytes = b"",
    headers: dict[str, str] | None = None,
    config: ForwardConfig | None = None,
) -> httpx.Response:
    """Send one request to ``arm`` as the head's service key there, on ``caller``'s behalf.

    ``headers`` are the caller's; only ``PASSED_REQUEST_HEADERS`` go on. Never retried:
    a write that timed out may have been applied.

    Raises
    ------
    PrimaryUnavailable
        502, the arm couldn't be reached (or its URL isn't allowed); nothing was sent.
    PrimaryTimeout
        504, the request went out but no answer came back; it may have been applied.
    """
    config = config or get_forward_config()
    if not _allowed_scheme(arm.url, config):
        raise PrimaryUnavailable(f"The forward URL of arm '{arm.name}' must be an https:// URL")
    outgoing = {name: value for name, value in (headers or {}).items() if name.lower() in PASSED_REQUEST_HEADERS}
    outgoing |= {
        "X-API-Key": arm.service_key,
        # Percent-encoded UTF-8: header values are ASCII, a key's name needn't be.
        # Keeping @ and : readable costs nothing, since % itself is always encoded.
        ON_BEHALF_OF_HEADER: quote(caller.on_behalf_of, safe="@:"),
        # The arm limits the request to the facilities both keys reach.
        ON_BEHALF_OF_FACILITIES_HEADER: json.dumps(caller.facilities),
    }
    url = arm.url + path + (f"?{query}" if query else "")
    try:
        async with httpx.AsyncClient(verify=config.ca_bundle or True, timeout=config.timeout_s) as client:
            return await client.request(method, url, content=body, headers=outgoing)
    except (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout) as exc:
        logger.warning("Arm %s unreachable for %s %s: %s", arm.name, method, path, exc)
        raise PrimaryUnavailable(f"Arm '{arm.name}' could not be reached; nothing was sent to it") from None
    except httpx.TransportError as exc:
        logger.warning("No answer from arm %s for %s %s: %s", arm.name, method, path, exc)
        raise PrimaryTimeout(
            f"Arm '{arm.name}' did not answer; the request may or may not have been applied there. "
            "Check before sending it again"
        ) from None


class ArmRefused(Exception):
    """An arm answered a request the head sent it with an error status.

    Raised by callers that act on the arm's answer rather than relay it (a capture
    the head delivers). ``status`` is the HTTP status, 4xx or 5xx; ``code`` and
    ``detail`` are the arm's ``{code, detail}`` body, or ``None`` and the body's
    text when it sent something else.
    """

    def __init__(self, arm: str, status: int, code: str | None, detail: str) -> None:
        super().__init__(f"Arm '{arm}' answered {status} {code or ''}: {detail}".replace("  ", " "))
        self.arm = arm
        self.status = status
        self.code = code
        self.detail = detail

    @classmethod
    def from_answer(cls, arm: str, answer: httpx.Response) -> ArmRefused:
        try:
            body = answer.json()
        except ValueError:
            body = None
        if isinstance(body, dict) and isinstance(body.get("code"), str):
            return cls(arm, answer.status_code, body["code"], str(body.get("detail", "")))
        return cls(arm, answer.status_code, None, answer.text[:500])


def lsn_value(lsn: str) -> int:
    """A ``pg_lsn`` text value (``"16/B374D848"``) as a comparable integer."""
    high, _, low = lsn.partition("/")
    return (int(high, 16) << 32) | int(low, 16)


async def wait_until_applied(
    commit_lsn: str,
    timeout_s: float,
    *,
    subscription: str,
    until: Callable[[asyncpg.Connection], Awaitable[bool]] | None = None,
) -> bool:
    """Wait up to ``timeout_s`` for ``subscription`` to apply ``commit_lsn`` here.

    Polls ``applied_lsn`` (``pg_replication_origin_status.remote_lsn``). ``until``, if
    given, is asked on the same connection at each poll and ends the wait early when
    true: for a write that may already be here although the position isn't reached.
    ``False`` on a timeout, and also when the position can't be read (no such
    subscription, or no grant on the view): the caller then marks the answer stale,
    which is true as far as this instance can tell. Never raises; the write already
    succeeded on the arm.

    ``timeout_s`` bounds the whole wait, getting a connection from the pool and each
    query included, so a busy pool or a stalled database can't hold the answer.
    """
    try:
        target = lsn_value(commit_lsn)
    except ValueError as exc:
        logger.warning("Cannot wait for a forwarded write to replicate: %s", exc)
        return False
    try:
        async with asyncio.timeout(timeout_s):
            async with get_engine().connect() as conn:
                raw = (await conn.get_raw_connection()).driver_connection
                while True:
                    if until is not None and await until(raw):
                        return True
                    applied = await applied_lsn(raw, subscription)
                    if applied is not None and lsn_value(applied) >= target:
                        return True
                    await asyncio.sleep(_POLL_S)
    except TimeoutError:
        return False
    except asyncpg.InsufficientPrivilegeError:
        logger.warning(
            "Cannot read pg_replication_origin_status, so forwarded writes can't wait for replication. As a "
            "superuser, grant the app role SELECT on it and EXECUTE on pg_show_replication_origin_status()"
        )
    except (OSError, asyncpg.PostgresError, SQLAlchemyError) as exc:
        logger.warning("Cannot read the applied replication position: %s", exc)
    return False


# 403 codes an arm answers only about the service key: the head checked the caller's
# key against the route's gates first, and the arm checks these against its own key.
_SERVICE_KEY_REFUSALS = frozenset(
    {
        "insufficient_read_access",
        "insufficient_write_access",
        "insufficient_admin_access",
        "insufficient_module_access",
    }
)


def arm_error(answer: httpx.Response) -> tuple[str | None, str]:
    """An arm's error answer as ``(code, detail)``; ``None`` and the body's text when it isn't ``{code, detail}``."""
    try:
        body = answer.json()
    except ValueError:
        body = None
    if isinstance(body, dict) and isinstance(body.get("code"), str):
        return body["code"], str(body.get("detail", ""))
    return None, answer.text[:500]


def service_key_refusal(arm: str, answer: httpx.Response, *, manages_facilities: bool) -> ServiceKeyRefused | None:
    """The head's own error for ``arm`` refusing its service key, or ``None`` for any other answer.

    A 401 is always about the service key, since the caller's token never goes to the
    arm; so is a 403 for scope, module or admin access. A facility refusal is the
    service key's only on a route that manages facilities, whose gate the caller
    passed here with every facility. Elsewhere it may be the caller's own grant, and
    ``forwarded_write_refused`` already names forwarding: both go back as they are.
    Only the router relays this: a caller that acts on the arm's answer (a capture
    the head delivers) sees it raw.
    """
    if answer.status_code not in (401, 403):
        return None
    code, detail = arm_error(answer)
    if answer.status_code == 403 and code not in _SERVICE_KEY_REFUSALS:
        if not (manages_facilities and code == "insufficient_facility_access"):
            return None
    answered = f"{answer.status_code} {code}" if code else str(answer.status_code)
    return ServiceKeyRefused(f"Arm '{arm}' refused this head's service key for it ({answered}: {detail})")
