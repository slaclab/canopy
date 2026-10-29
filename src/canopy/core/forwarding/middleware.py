"""``ForwardingMiddleware``: both halves of forward mode at the HTTP edge.

On a forward instance it answers allowlisted writes (``routes.FORWARDABLE``) by
sending them to the primary; every other request goes on to the app, where a write
is refused with ``read_only_instance`` as on a mirror. On a primary that accepts
forwarded writes it adds ``X-Canopy-Commit-LSN`` to the answer of a forwarded
request that committed a write. Everywhere else it passes requests straight through.

It runs outside every sub-app's exception handlers, so it builds the uniform
``{code, detail}`` error body itself.
"""

from __future__ import annotations

import logging
from uuid import UUID
from datetime import UTC, datetime, timedelta

from sqlalchemy import text
from starlette.types import Send, Scope, ASGIApp, Message, Receive
from prometheus_client import Counter
from starlette.responses import Response, JSONResponse
from starlette.datastructures import Headers

from canopy.core.db import session_scope
from canopy.core.config import get_settings
from canopy.core.errors import (
    CanopyError,
    InvalidAPIKey,
    MachineReadOnly,
    ArchivedImmutable,
    InsufficientWriteAccess,
    InsufficientModuleAccess,
)
from canopy.core.security import MACHINE_READ_ONLY_DETAIL, APIKey, get_registry
from canopy.core.forwarding import routes
from canopy.core.forwarding.client import (
    PASSED_RESPONSE_HEADERS,
    on_behalf_of,
    send_to_primary,
    wait_until_applied,
)
from canopy.core.forwarding.config import ForwardConfig, get_forward_config
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
    "Writes a forward instance answered, by route template, status code and outcome "
    "(applied, stale, not_waited, refused, failed).",
    ["method", "route", "status", "outcome"],
)

_WRITE_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})


def _route_path(scope: Scope) -> str:
    """The path the app routes on: ``scope["path"]`` without the ``root_path`` prefix.

    Behind a path-prefixed ingress (``uvicorn --root-path``) the server includes the
    prefix in ``path``; the primary has its own, so it is never sent on.
    """
    path, root_path = scope["path"], scope.get("root_path", "")
    if root_path and path.startswith(root_path + "/"):
        return path[len(root_path) :]
    return path


def _error(exc: CanopyError) -> JSONResponse:
    return JSONResponse(status_code=exc.status_code, content={"code": exc.error_code.value, "detail": exc.detail})


class ForwardingMiddleware:
    """Pure-ASGI middleware for forward mode. A no-op unless the role calls for it."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        settings = get_settings()
        if settings.role == "forward" and scope["method"] in _WRITE_METHODS:
            config = get_forward_config()
            if found := routes.match(scope["method"], _route_path(scope), allow_shared=config.allow_shared):
                route, params = found
                await self._forward(scope, receive, send, config, route, params)
                return
        elif settings.role == "primary" and settings.accept_forwarded_writes:
            if ON_BEHALF_OF_HEADER.lower().encode() in dict(scope["headers"]):
                await self._serve_forwarded(scope, receive, send)
                return
        await self.app(scope, receive, send)

    # ------------------------------------------------------------------ #
    # Forward instance
    # ------------------------------------------------------------------ #

    async def _forward(
        self,
        scope: Scope,
        receive: Receive,
        send: Send,
        config: ForwardConfig,
        route: routes.ForwardRoute,
        params: dict[str, str],
    ) -> None:
        headers = Headers(scope=scope)
        outcome = "refused"
        try:
            caller = await self._authorize(headers.get("x-api-key"), route)
            await self._check_age(route, params, config)
            body = await _read_body(receive)
            outcome = "failed"
            answer = await send_to_primary(
                config,
                scope["method"],
                _route_path(scope),
                caller=caller,
                query=scope.get("query_string", b"").decode("latin-1"),
                body=body,
                headers=dict(headers),
            )
        except CanopyError as exc:
            FORWARDED_REQUESTS.labels(scope["method"], route.path, str(exc.status_code), outcome).inc()
            await _error(exc)(scope, receive, send)
            return

        relayed = {
            name: value
            for name, value in answer.headers.items()
            if name.lower() in PASSED_RESPONSE_HEADERS or name.lower().startswith("x-canopy-")
        }
        outcome = "not_waited"
        if commit_lsn := answer.headers.get(COMMIT_LSN_HEADER):
            if await wait_until_applied(commit_lsn, config.wait_s):
                outcome = "applied"
            else:
                outcome = "stale"
                relayed[REPLICA_STALE_HEADER] = "true"
        logger.info(
            "Forwarded %s %s for %s: %d (%s)",
            scope["method"],
            route.path,
            on_behalf_of(caller),
            answer.status_code,
            outcome,
        )
        FORWARDED_REQUESTS.labels(scope["method"], route.path, str(answer.status_code), outcome).inc()
        await Response(content=answer.content, status_code=answer.status_code, headers=relayed)(scope, receive, send)

    @staticmethod
    async def _authorize(token: str | None, route: routes.ForwardRoute) -> APIKey:
        """The caller's key on this instance, holding write scope on the route's module.

        Same order as the gates: an unknown key is a 401 and a machine route without
        machine access a ``machine_read_only`` before the key's scope is looked at,
        so no key is suggested as the fix.
        """
        if not token:
            raise InvalidAPIKey("Missing X-API-Key header")
        key = await get_registry().get(token)
        if key is None:
            raise InvalidAPIKey("Invalid API key")
        if not key.allows_module(route.module):
            raise InsufficientModuleAccess(f"API key is not scoped to '{route.module}'")
        if route.route_class is routes.MACHINE and not get_settings().machine_writes:
            raise MachineReadOnly(MACHINE_READ_ONLY_DETAIL)
        if not key.write:
            raise InsufficientWriteAccess("API key does not have write access")
        return key

    @staticmethod
    async def _check_age(route: routes.ForwardRoute, params: dict[str, str], config: ForwardConfig) -> None:
        """Refuse a change to a snapshot older than the primary's window, from this instance's copy.

        Retained snapshots are exempt: pruning keeps them on the primary. A snapshot
        this copy doesn't have (bad id, not replicated yet) or holds as deleted is
        left for the primary to answer.
        """
        if route.snapshot_param is None or config.primary_window_days is None:
            return
        snapshot_id = params[route.snapshot_param]
        try:
            UUID(snapshot_id)
        except ValueError:
            return
        async with session_scope() as session:
            snapshot = (
                await session.execute(
                    text("SELECT created_at, retain, deleted_at FROM squirrel.snapshot WHERE id = CAST(:id AS uuid)"),
                    {"id": snapshot_id},
                )
            ).first()
        if snapshot is None or snapshot.retain or snapshot.deleted_at is not None:
            return
        if snapshot.created_at < datetime.now(UTC) - timedelta(days=config.primary_window_days):
            raise ArchivedImmutable(
                f"Snapshot {snapshot_id} is older than the primary's {config.primary_window_days}-day window; "
                "it is kept here as history and can't be changed"
            )

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
