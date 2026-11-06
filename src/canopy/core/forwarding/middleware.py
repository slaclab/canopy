"""``ForwardingMiddleware``: both halves of forwarding at the HTTP edge.

On a history head (``CANOPY_ROLE=mirror``) with a forwardable arm, it finds the
owner of each routed write (``routes.ROUTES``) in the head's copy and, when that
owner is a forwardable arm, sends the write there; every other request goes on to
the app, whose services write the head's own rows and refuse an arm's with
``not_owner``. On a primary that accepts forwarded writes it adds
``X-Canopy-Commit-LSN`` to the answer of a forwarded request that committed a
write. Everywhere else it passes requests straight through.

It runs outside every sub-app's exception handlers, so it builds the uniform
``{code, detail}`` error body itself.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Awaitable

from starlette.types import Send, Scope, ASGIApp, Message, Receive
from prometheus_client import Counter
from starlette.responses import Response, JSONResponse
from starlette.datastructures import Headers, QueryParams

from canopy.core.config import get_settings
from canopy.core.errors import CanopyError, ArchivedImmutable
from canopy.core.security import APIKey, get_registry
from canopy.core.forwarding import owners, routes
from canopy.core.forwarding.client import (
    PASSED_RESPONSE_HEADERS,
    Caller,
    ArmTarget,
    send_to_arm,
    forward_target,
    forwardable_arms,
    wait_until_applied,
)
from canopy.core.forwarding.config import get_forward_config
from canopy.core.forwarding.context import (
    COMMIT_LSN_HEADER,
    ON_BEHALF_OF_HEADER,
    REPLICA_STALE_HEADER,
    ForwardedWrite,
    forwarded_write,
)

logger = logging.getLogger(__name__)

FORWARDED_REQUESTS = Counter(
    "canopy_forwarded_requests_total",
    "Writes a head sent to an arm, or refused on the way, by route template, arm, status code and outcome "
    "(applied, stale, not_waited, refused, failed).",
    ["method", "route", "arm", "status", "outcome"],
)

_WRITE_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})
_ON_BEHALF_OF = ON_BEHALF_OF_HEADER.lower().encode()

RememberJob = Callable[[str, str], Awaitable[None]]
"""Records that job ``job_id`` (first) runs on arm ``arm`` (second), for ``GET /jobs/v1/{id}``."""


def _route_path(scope: Scope) -> str:
    """The path the app routes on: ``scope["path"]`` without the ``root_path`` prefix.

    Behind a path-prefixed ingress (``uvicorn --root-path``) the server includes the
    prefix in ``path``; the arm has its own, so it is never sent on.
    """
    path, root_path = scope["path"], scope.get("root_path", "")
    if root_path and path.startswith(root_path + "/"):
        return path[len(root_path) :]
    return path


def _error(exc: CanopyError) -> JSONResponse:
    return JSONResponse(status_code=exc.status_code, content={"code": exc.error_code.value, "detail": exc.detail})


class ForwardingMiddleware:
    """Pure-ASGI middleware for forwarding. A no-op unless the role calls for it.

    ``remember_job`` records where a forwarded write's job runs; the jobs module
    passes it when the app is composed, so ``core`` imports no module.
    """

    def __init__(self, app: ASGIApp, remember_job: RememberJob | None = None) -> None:
        self.app = app
        self.remember_job = remember_job

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        settings = get_settings()
        if settings.role == "mirror" and scope["method"] in _WRITE_METHODS:
            if found := routes.match(scope["method"], _route_path(scope)):
                await self._route(scope, receive, send, *found)
                return
        elif settings.role == "primary" and settings.accept_forwarded_writes:
            if _ON_BEHALF_OF in dict(scope["headers"]):
                await self._serve_forwarded(scope, receive, send)
                return
        await self.app(scope, receive, send)

    # ------------------------------------------------------------------ #
    # Head
    # ------------------------------------------------------------------ #

    async def _route(
        self, scope: Scope, receive: Receive, send: Send, route: routes.WriteRoute, params: dict[str, str]
    ) -> None:
        """Send ``route``'s write to its owner if that's a forwardable arm; otherwise to the app here.

        The caller's key is checked first, against the gates the app's route has: an
        arm checks only the head's service key. A request the key can't make, or that
        names a caller itself (``X-Canopy-On-Behalf-Of``), goes to the app, which
        refuses it as it would anywhere.
        """
        arms = forwardable_arms()
        headers = Headers(scope=scope)
        query = QueryParams(scope.get("query_string", b""))
        if not arms or _ON_BEHALF_OF in dict(scope["headers"]):
            await self.app(scope, receive, send)
            return
        if route.local_when is not None and query.get(route.local_when[0]) == route.local_when[1]:
            await self.app(scope, receive, send)
            return
        if (caller := await _authorize(headers.get("x-api-key"), route)) is None:
            await self.app(scope, receive, send)
            return
        body = b""
        if route.reads_body:
            body = await _read_body(receive)
            receive = _replay(body, receive)
        parsed = owners.parse_body(body) if body else None
        local = get_settings().instance
        requested = query.get("owner") if route.owner_param else None
        owner = await owners.owner_of(route, params, parsed, requested=requested, local=local)
        if owner is None or owner == local or owner not in arms or (arm := forward_target(owner)) is None:
            await self.app(scope, receive, send)
            return
        await self._forward(scope, receive, send, route, params, arm, caller, body, parsed)

    async def _forward(
        self,
        scope: Scope,
        receive: Receive,
        send: Send,
        route: routes.WriteRoute,
        params: dict[str, str],
        arm: ArmTarget,
        caller: APIKey,
        body: bytes,
        parsed: object,
    ) -> None:
        headers = Headers(scope=scope)
        config = get_forward_config()
        outcome = "refused"
        try:
            await owners.require_unused(route, parsed)
            if not body:
                body = await _read_body(receive)
            outcome = "failed"
            answer = await send_to_arm(
                arm,
                scope["method"],
                _route_path(scope),
                caller=Caller.of(caller),
                query=scope.get("query_string", b"").decode("latin-1"),
                body=body,
                headers=dict(headers),
                config=config,
            )
            if answer.status_code == 404 and await _pruned_on_arm(route, params, answer):
                outcome = "refused"
                raise ArchivedImmutable(
                    f"Snapshot {params[route.owner.param]} is no longer on arm '{arm.name}'; it is kept here as "
                    "history and can't be changed"
                )
        except CanopyError as exc:
            FORWARDED_REQUESTS.labels(scope["method"], route.path, arm.name, str(exc.status_code), outcome).inc()
            await _error(exc)(scope, receive, send)
            return

        relayed = {
            name: value
            for name, value in answer.headers.items()
            if name.lower() in PASSED_RESPONSE_HEADERS or name.lower().startswith("x-canopy-")
        }
        outcome = "not_waited"
        if commit_lsn := answer.headers.get(COMMIT_LSN_HEADER):
            if await wait_until_applied(commit_lsn, config.wait_s, subscription=arm.subscription):
                outcome = "applied"
            else:
                outcome = "stale"
                relayed[REPLICA_STALE_HEADER] = "true"
        if self.remember_job is not None and (job_id := _job_id(answer)) is not None:
            await self.remember_job(job_id, arm.name)
        logger.info(
            "Forwarded %s %s to %s for %s: %d (%s)",
            scope["method"],
            route.path,
            arm.name,
            caller.name,
            answer.status_code,
            outcome,
        )
        FORWARDED_REQUESTS.labels(scope["method"], route.path, arm.name, str(answer.status_code), outcome).inc()
        await Response(content=answer.content, status_code=answer.status_code, headers=relayed)(scope, receive, send)

    # ------------------------------------------------------------------ #
    # Primary
    # ------------------------------------------------------------------ #

    async def _serve_forwarded(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Run a forwarded request, adding ``X-Canopy-Commit-LSN`` when it committed a write.

        ``core.security`` fills in ``on_behalf_of`` once it trusts the header, and
        ``core.db.get_session`` records ``commit_lsn``; the session commits before
        the response starts, so the position is known by then.
        """
        write = ForwardedWrite()
        token = forwarded_write.set(write)

        async def send_with_lsn(message: Message) -> None:
            if message["type"] == "http.response.start" and write.commit_lsn:
                message["headers"] = [
                    *message.get("headers", []),
                    (COMMIT_LSN_HEADER.lower().encode(), write.commit_lsn.encode()),
                ]
            await send(message)

        try:
            await self.app(scope, receive, send_with_lsn)
        finally:
            forwarded_write.reset(token)


async def _authorize(token: str | None, route: routes.WriteRoute) -> APIKey | None:
    """The caller's key here, if it passes ``route``'s gates; else ``None``.

    ``None`` too when the key store can't be read: the app then answers each of these
    the way it always does (401, 403 or 503).
    """
    if not token:
        return None
    try:
        key = await get_registry().get(token)
        if key is None or not key.allows_module(route.module):
            return None
        for gate in route.gates:
            gate(key)
    except CanopyError:
        return None
    return key


async def _pruned_on_arm(route: routes.WriteRoute, params: dict[str, str], answer) -> bool:
    """Whether the arm's 404 is for a snapshot it pruned, which this copy still holds as live.

    A snapshot held here as deleted is a user's delete that replicated, so the arm's
    404 is the right answer and is relayed as it is.
    """
    rule = route.owner
    if not isinstance(rule, routes.Row) or rule.table != "squirrel.snapshot":
        return False
    try:
        code = answer.json().get("code")
    except (ValueError, AttributeError):
        return False
    return code == "snapshot_not_found" and await owners.snapshot_is_live(params[rule.param])


def _job_id(answer) -> str | None:
    """The job id of an arm's ``JobCreatedDTO`` answer, if it is one."""
    if not 200 <= answer.status_code < 300:
        return None
    try:
        payload = answer.json()
    except (ValueError, json.JSONDecodeError):
        return None
    job_id = payload.get("jobId") if isinstance(payload, dict) else None
    return job_id if isinstance(job_id, str) and job_id else None


async def _read_body(receive: Receive) -> bytes:
    chunks = []
    while True:
        message = await receive()
        if message["type"] == "http.disconnect":
            break
        chunks.append(message.get("body", b""))
        if not message.get("more_body", False):
            break
    return b"".join(chunks)


def _replay(body: bytes, receive: Receive) -> Receive:
    """A ``receive`` that yields ``body`` once, for the app after the body was read here."""
    sent = False

    async def replay() -> Message:
        nonlocal sent
        if not sent:
            sent = True
            return {"type": "http.request", "body": body, "more_body": False}
        return await receive()

    return replay
