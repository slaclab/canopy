"""Canopy API entrypoint: composes the selected modules' sub-apps and lifespans into one FastAPI app."""

import json
import logging
from html import escape
from importlib import import_module
from contextlib import AsyncExitStack, asynccontextmanager
from collections.abc import Iterable

from fastapi import FastAPI, Request, Response, WebSocket, status
from starlette.types import Send, Scope, ASGIApp, Receive
from fastapi.responses import HTMLResponse, PlainTextResponse
from starlette.routing import Match, Mount
from starlette.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from canopy import __version__
from canopy.core.config import (
    MODULES_SCOPE_KEY,
    get_settings,
    parse_modules,
    resolve_modules,
)
from canopy.core.errors import (
    CanopyError,
    InvalidAPIKey,
    KeyStoreUnavailable,
    InsufficientModuleAccess,
    register_canopy_error_handler,
)
from canopy.core.health import database_ready
from canopy.core.logging import configure_logging
from canopy.core.metrics import HTTPMetricsMiddleware, render_latest
from canopy.core.modules import tag_module, module_for_app
from canopy.core.instance import router as instance_router
from canopy.core.instance import shipped_alembic_head
from canopy.core.security import (
    get_registry,
    token_from_scope,
    bind_key_registry,
    has_database_tier,
    build_key_registry,
)
from canopy.contracts.health import HealthDTO

configure_logging()
logger = logging.getLogger(__name__)

# The replication gauges read this process's database through asyncpg (the ``data``
# extra), so only an install with the database tier has them. Registered once, at
# import, as each module registers its own collector when its app is imported.
if has_database_tier():
    from canopy.core.replication import metrics as replication_metrics

    replication_metrics.register()

# Every module's name on the ``/docs`` page, in composition order (``ALL_MODULES``).
API_MODULES = (
    ("mole", "Mole"),
    ("hare", "Hare"),
    ("raccoon", "Raccoon"),
    ("vole", "Vole"),
    ("formula-engine", "Formula Engine"),
    ("squirrel", "Squirrel"),
    ("magpie", "Magpie"),
    ("jobs", "Jobs"),
    ("converter", "Converter"),
    ("analytics", "Analytics"),
    ("elog", "E-log"),
)


# The optional extra that provides each package a module may need beyond the core install.
_EXTRA_FOR_PACKAGE = {
    "sqlalchemy": "data",
    "asyncpg": "data",
    "alembic": "data",
    "arq": "data",
    "redis": "data",
    "pydmconverter": "convert",
}


def _import_sub_app(name: str, package: str):
    """Import ``canopy.<package>.app``, naming the missing extra when the install lacks one."""
    try:
        return import_module(f"canopy.{package}.app")
    except ModuleNotFoundError as exc:
        missing = (exc.name or "").partition(".")[0]
        extra = _EXTRA_FOR_PACKAGE.get(missing)
        if extra is None:
            raise
        raise ModuleNotFoundError(
            f"The {name} module needs the '{extra}' extra, which is not installed (no module named "
            f"'{missing}'). Install canopy[{extra}], or leave {name} out of CANOPY_MODULES; "
            "unset, CANOPY_MODULES selects every module.",
            name=exc.name,
        ) from exc


class _SelectedModules:
    """Publish the app's module selection on every ASGI scope.

    Mounted sub-apps are shared singletons and cannot see the app they are
    mounted in; the scope is what reaches them, so a route can tell whether a
    peer module is part of this deployment (``canopy.core.config.module_selected``).
    """

    def __init__(self, app: ASGIApp, modules: frozenset[str]) -> None:
        self.app = app
        self.modules = modules

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        scope[MODULES_SCOPE_KEY] = self.modules
        await self.app(scope, receive, send)


def _route_path(scope: Scope) -> str:
    """The path the router matches: ``scope["path"]`` without the ``root_path`` prefix.

    Behind a path-prefixed ingress (``uvicorn --root-path /canopy``) the server
    puts the prefix in front of ``path``; the router strips it, and so must the gate.
    """
    path, root_path = scope["path"], scope.get("root_path", "")
    if root_path and path.startswith(root_path) and path[len(root_path) : len(root_path) + 1] in ("", "/"):
        return path[len(root_path) :] or "/"
    return path


def _is_open(scope: Scope, probes: frozenset[str]) -> bool:
    """A ``GET`` on one of ``probes`` (the health probe paths) passes the closed-meta gate."""
    return scope["type"] == "http" and scope["method"] == "GET" and _route_path(scope) in probes


class _ClosedMeta:
    """Refuse every request that lacks a registry key for the module it targets, except a ``GET`` on a probe.

    The probes are ``/health`` and, on the database tier, ``/health/ready``. A probe
    says only whether the process is up or ready, and a kubelet probe can't easily
    send a key from a secret. An unmounted probe path is refused like any other.

    Per-route dependencies cannot do this: the docs and OpenAPI pages of the
    parent and of each mounted sub-app, the root document and the per-module
    watchdogs have none, and a CORS preflight is answered before routing.

    The target module is the one the router would dispatch to: the first route
    that fully matches the scope, its sub-app's tag for a mount and ``core`` for
    a parent route (or none). A key is admitted iff it is scoped to that module,
    whatever its scopes: each route still checks the scope it needs, so a
    write-only key can write and an admin-only key can manage keys, as without
    the gate. Refusals are the route dependencies' own errors: 401
    ``invalid_api_key`` for a missing or unknown key, 503 ``key_store_unavailable``
    when the key store cannot answer and 403 ``insufficient_module_access`` for a
    key scoped elsewhere.

    The gate sits inside CORSMiddleware and the HTTP metrics middleware. A CORS
    preflight, which a browser sends without the key, is answered by CORS and
    never reaches the gate; the real request is gated. A refusal carries the CORS
    headers, so a browser client can read its 401, and is counted in ``/metrics``.
    """

    _DETAIL = "A valid API key is required"

    def __init__(self, app: ASGIApp, *, router_app: FastAPI, probes: frozenset[str]) -> None:
        self.app = app
        self.router_app = router_app
        self.probes = probes

    def _module_for(self, scope: Scope) -> str:
        for route in self.router_app.router.routes:
            match, _ = route.matches(scope)
            if match == Match.FULL:
                return module_for_app(route.app) if isinstance(route, Mount) else "core"
        return "core"

    async def _refusal(self, scope: Scope) -> CanopyError | None:
        """The error this scope is refused with, or ``None`` to admit it."""
        token = token_from_scope(scope)
        if token is None:
            return InvalidAPIKey(self._DETAIL)
        try:
            key = await get_registry().get(token)
        except KeyStoreUnavailable as exc:
            return exc
        if key is None:
            return InvalidAPIKey(self._DETAIL)
        module = self._module_for(scope)
        if not key.allows_module(module):
            return InsufficientModuleAccess(f"API key is not scoped to '{module}'")
        return None

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return
        refused = None if _is_open(scope, self.probes) else await self._refusal(scope)
        if refused is None:
            await self.app(scope, receive, send)
            return

        status_code, code, detail = refused.status_code, refused.error_code.value, refused.detail
        refusal = JSONResponse({"code": code, "detail": detail}, status_code=status_code)
        if scope["type"] == "http":
            await refusal(scope, receive, send)
            return
        websocket = WebSocket(scope, receive, send)
        if "websocket.http.response" in scope.get("extensions", {}):
            await websocket.send_denial_response(refusal)
        else:
            # A server that cannot answer a handshake with an HTTP response.
            close_code = (
                status.WS_1011_INTERNAL_ERROR
                if status_code == status.HTTP_503_SERVICE_UNAVAILABLE
                else status.WS_1008_POLICY_VIOLATION
            )
            await websocket.close(code=close_code, reason=f"{code}: {detail}")


async def _facility_granted_to_key(facility_id: str, slug: str) -> bool:
    """Is any active API key's facility grant still naming this facility?

    Registered from here, not from ``core.keys``, to keep ``core.facilities`` free of
    imports out of ``core.keys`` — the reverse edge already exists, since
    ``keys.service`` validates grants through the facilities gateway. Grants store
    slugs, so ``facility_id`` is unused. Imported here: it needs the database tier.
    """
    from canopy.core.db import session_scope
    from canopy.core.keys.repositories import ApiKeyRepository

    async with session_scope() as session:
        return await ApiKeyRepository(session).count_granting(slug) > 0


def build_app(modules: Iterable[str] | str, *, closed_meta: bool | None = None) -> FastAPI:
    """Compose the Canopy API from the named modules.

    ``modules`` is an iterable of module names or one comma-separated string;
    an empty selection means every module and an unknown name raises
    ``ValueError``. Only the selected modules are imported, so a module whose
    dependencies are not installed costs nothing as long as it is left out.

    ``closed_meta`` puts the whole app behind the key registry except
    ``GET /health`` and ``GET /health/ready``; ``None`` takes the
    ``CANOPY_CLOSED_META`` setting. The key registry is bound by the lifespan,
    before anything is served.
    ``/v1/api-keys``, ``/v1/facilities`` and ``/health/ready`` are mounted only on
    an install with the database key tier (``has_database_tier``), whose ``/metrics``
    also reports replication; ``/v1/instance`` on every install.
    """
    database_tier = has_database_tier()
    if closed_meta is None:
        closed_meta = get_settings().closed_meta

    selected = parse_modules(modules) if isinstance(modules, str) else resolve_modules(modules)

    # Import here, not at module top: an unselected sub-app must never load. A
    # module's label is its mount and key scope; its package spells "-" as "_".
    packages = {name: name.replace("-", "_") for name in selected}
    sub_apps = {name: _import_sub_app(name, package) for name, package in packages.items()}

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        """Compose per-module lifespans. Mounted sub-apps don't run their own."""
        # The static key tier first, then the database when the install has it:
        # the long-lived credentials keep working when Postgres is unreachable,
        # including on a cold start when the cache has nothing stale to serve.
        # Built before anything can serve, so a bad CANOPY_API_KEYS or keys file
        # surfaces in the startup log instead of as an unexplained 401.
        bind_key_registry(build_key_registry(get_settings()))
        # Read the shipped migrations head now, off the request path; /v1/instance serves the cached value.
        shipped_alembic_head()
        if database_tier:
            from canopy.core.facilities import gateway as facilities_gateway

            facilities_gateway.register_in_use_checker(_facility_granted_to_key, "API key grants")
        async with AsyncExitStack() as stack:
            for name, module in sub_apps.items():
                await stack.enter_async_context(getattr(module, f"{packages[name]}_lifespan")())
            logger.info(f"Starting Canopy API [version {__version__}] with modules: {', '.join(selected)}")
            yield
        logger.info("Canopy API shutdown complete")

    app = FastAPI(
        title="Canopy",
        description="Control-system display platform — backend API",
        version=__version__,
        lifespan=lifespan,
        # One aggregate /docs over the selected modules instead (below).
        docs_url=None,
        redoc_url=None,
    )
    app.state.modules = selected
    tag_module(app, "core")

    if closed_meta:
        # Registered first, so it is the innermost layer: CORS answers a preflight
        # before it, and its refusals get the CORS headers and are counted.
        probes = frozenset({"/health", "/health/ready"} if database_tier else {"/health"})
        app.add_middleware(_ClosedMeta, router_app=app, probes=probes)
    # Registered before CORSMiddleware so it sits INSIDE it (Starlette wraps in reverse
    # registration order): preflight OPTIONS requests that CORS answers itself never
    # reach the router, and counting them would only inflate the "unmatched" series.
    app.add_middleware(HTTPMetricsMiddleware)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.add_middleware(_SelectedModules, modules=frozenset(selected))

    register_canopy_error_handler(app)

    if database_tier:
        # Imported here: key and facility management need the database, which a lean install lacks.
        from canopy.core.keys.routes import router as api_keys_router
        from canopy.core.facilities.routes import router as facilities_router

        app.include_router(api_keys_router)
        app.include_router(facilities_router)
    app.include_router(instance_router)

    for name, module in sub_apps.items():
        tag_module(module.app, name)
        app.mount(f"/{name}", module.app)

    @app.get("/health", tags=["meta"])
    async def health() -> HealthDTO:
        return HealthDTO.of()

    if database_tier:

        @app.get("/health/ready", tags=["meta"])
        async def ready() -> JSONResponse:
            """Readiness: 200 once the database answers, 503 while it doesn't. ``/health`` stays the liveness probe."""
            if await database_ready():
                return JSONResponse({"status": "ready"})
            return JSONResponse({"status": "unavailable", "database": "unreachable"}, status_code=503)

    @app.get("/metrics", tags=["meta"], response_class=PlainTextResponse)
    async def metrics() -> Response:
        """Prometheus exposition for this process."""
        if database_tier:
            await replication_metrics.refresh()
        payload, content_type = render_latest()
        return Response(content=payload, media_type=content_type)

    docs_specs = [(name, title) for name, title in API_MODULES if name in selected]

    @app.get("/docs", include_in_schema=False, response_class=HTMLResponse)
    async def docs_ui(request: Request) -> HTMLResponse:
        root_path = request.scope.get("root_path", "").rstrip("/")
        specs = [
            {"url": f"{root_path}/openapi.json", "name": "Canopy"},
            *[{"url": f"{root_path}/{path}/openapi.json", "name": name} for path, name in docs_specs],
        ]
        serialized_specs = json.dumps(specs).replace("<", "\\u003c")
        return HTMLResponse(
            f"""<!DOCTYPE html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<link type="text/css" rel="stylesheet" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui.css">
<title>{escape(f"{app.title} - Swagger UI")}</title>
</head>
<body>
<div id="swagger-ui"></div>
<script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
<script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-standalone-preset.js"></script>
<script>
const ui = SwaggerUIBundle({{
    dom_id: "#swagger-ui",
    urls: {serialized_specs},
    "urls.primaryName": "Canopy",
    validatorUrl: null,
    presets: [
        SwaggerUIBundle.presets.apis,
        SwaggerUIStandalonePreset
    ],
    layout: "StandaloneLayout",
}});
</script>
</body>
</html>"""
        )

    @app.get("/", tags=["meta"])
    async def root() -> dict[str, str]:
        return {
            "name": "Canopy",
            "version": __version__,
            "docs": "/docs",
            "health": "/health",
        }

    return app


app = build_app(get_settings().modules)
