"""Prometheus metrics — the project-wide pattern (design: docs/observability/metrics.md).

One exposition endpoint per process (``GET /metrics`` on the parent app), one
default ``prometheus_client`` registry, metric names ``canopy_<module>_*`` for
module metrics and ``canopy_http_*`` for the shared HTTP middleware below.

Label discipline: HTTP metrics are labelled with the module tag each app
carries (``canopy.core.modules.tag_module``) and the matched route *template*
(``/v1/pvs/{pv}``), never raw request paths — unbounded label values blow up
the time-series index. A request that matches no route keeps its module label
(the tag comes from the app instance, not the URL, so it is bounded by
construction) and collapses the route into ``unmatched``.
"""

import time

from starlette.types import Send, Scope, ASGIApp, Message, Receive
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest

from canopy.core.modules import module_for_app

# Shared default buckets for API round-trips (5 ms .. 10 s). Module histograms
# should reuse these unless measurements demand otherwise.
DEFAULT_API_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)

UNMATCHED = "unmatched"
"""``route`` label for requests no FastAPI route claimed (404s, Starlette-served pages)."""

HTTP_REQUESTS = Counter(
    "canopy_http_requests_total",
    "HTTP requests handled, by method, module, route template, and status code.",
    ["method", "module", "route", "status"],
)

HTTP_REQUEST_DURATION = Histogram(
    "canopy_http_request_duration_seconds",
    "HTTP request wall-clock duration, by method, module, and route template.",
    ["method", "module", "route"],
    buckets=DEFAULT_API_BUCKETS,
)


class HTTPMetricsMiddleware:
    """Pure-ASGI middleware recording count + duration for every HTTP request.

    Runs on the parent app, so mounted sub-apps are covered without any
    per-module code. Both labels are read from the scope *after* the
    downstream app ran: ``scope["app"]`` is then the innermost app that
    handled the request (its module tag becomes ``module``) and
    ``scope["route"]`` the matched FastAPI route (its template becomes
    ``route``).

    Register it *before* ``CORSMiddleware`` so it runs inside CORS: preflight
    ``OPTIONS`` requests that CORS answers itself then never reach it. Only
    FastAPI routes (``APIRoute``) populate ``scope["route"]``; Starlette's own
    routes — every app's ``/openapi.json``, a sub-app's ``/docs`` — are counted
    under ``route="unmatched"`` with their real status code.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        method = scope["method"]
        status = "500"
        start = time.perf_counter()

        async def send_wrapper(message: Message) -> None:
            nonlocal status
            if message["type"] == "http.response.start":
                status = str(message["status"])
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            duration = time.perf_counter() - start
            module = _module_label(scope)
            route = _route_label(scope)
            HTTP_REQUESTS.labels(method=method, module=module, route=route, status=status).inc()
            HTTP_REQUEST_DURATION.labels(method=method, module=module, route=route).observe(duration)


def _module_label(scope: Scope) -> str:
    """Module tag of the innermost app that handled the request.

    Starlette stores each app on the scope as its ``__call__`` runs, so once the
    downstream call returns the scope holds the mounted sub-app for a
    ``/hare/...`` request and the parent app for everything else — including
    paths that matched no mount at all. A metrics label must never fail a
    request, so an app without a tag (which the module drift test forbids) is
    reported rather than raised.
    """
    try:
        return module_for_app(scope["app"])
    except (KeyError, AttributeError):
        return "untagged"


def _route_label(scope: Scope) -> str:
    route = scope.get("route")
    if route is None:
        return UNMATCHED
    return getattr(route, "path_format", None) or getattr(route, "path", UNMATCHED)


def render_latest() -> tuple[bytes, str]:
    """Exposition payload + content type for the ``/metrics`` endpoint."""
    return generate_latest(), CONTENT_TYPE_LATEST
