# Metrics

Canopy exports Prometheus metrics from one place and under one set of rules,
so every module's instrumentation looks the same. The implementation lives in
`canopy.core.metrics`; Hare carries the module-level exemplar in
`canopy.hare.metrics`.

## Decisions

### One endpoint, app-level

`GET /metrics` is served by the **parent app** (`canopy.main`), not by the
module sub-apps. Prometheus expects one exposition endpoint per *process*, and
all modules share one process — per-module endpoints would each re-expose the
whole registry anyway.

The API is one of two process types. The Arq worker (`canopy-worker`, which
runs snapshot restores and batch conversions) exposes nothing yet. Worker-side
metrics need a second scrape target, served from the worker's startup hook
with `prometheus_client.start_http_server` on its own port, under the same
naming and label rules. That is a follow-up; nothing here changes for it.

The endpoint is unauthenticated, like `/health`, `/docs` and every
module's `/openapi.json`. This follows Prometheus's own security model, which
assumes exposition endpoints are reachable only from trusted networks and
leaves access control to the network rather than the application. The
[facility deployments](../deployment/facilities.md) bind the API directly on
hosts inside the lab's networks, and nothing documented exposes it beyond
them, so that assumption holds today. What the payload adds to the already
public route map is operational telemetry — request counts, latencies, status
codes, connected-client counts, process memory and the Python version — never
PV names, values or key names.

### Single default registry

Modules create metrics with the plain `prometheus_client` constructors, which
register into the default `REGISTRY`. No per-module sub-registries: merging
them at exposition adds machinery with no benefit at our scale.

!!! warning "Multi-process caveat"
    The default registry assumes one process. The production image runs a
    single uvicorn process per container (scale by container replication), so
    this holds. If gunicorn-style pre-fork workers ever appear, revisit with
    `prometheus_client.multiprocess`.

### Naming

- Module metrics: `canopy_<module>_<metric>` (e.g. `canopy_hare_subscriptions`).
- Shared HTTP metrics: `canopy_http_*` (from the middleware, below).
- Follow Prometheus conventions: `_total` for counters, base units in names
  (`_seconds`, `_bytes`).

### Label discipline

Labels must be low-cardinality. The HTTP metrics carry four. `status`
is a three-digit code, and `method` collapses to `other` outside the standard
HTTP methods, because Starlette still routes an unknown method token (as a
405) and a scanner could otherwise mint a series per spelling. The two that
need care:

- `module` — the tag each app carries via `canopy.core.modules.tag_module`,
  the same vocabulary API-key scopes use (`hare`, `vole`, …, and `core` for
  the parent app's own routes). It is read from the app instance that handled
  the request, never from the URL, so it is bounded by construction and
  unaffected by a path-prefixed ingress.
- `route` — the matched route *template* (`/v1/pvs/{pv}`), never the raw
  path. A request that matches no route collapses into `route="unmatched"`
  while keeping its `module`: a bot scan of `/wp-admin/...` lands in
  `module="core", route="unmatched"` and mints no new series.

Never label with PV names, session ids, key names, or anything else a client
chooses.

Two details: the middleware sits *inside* `CORSMiddleware`, so preflight
`OPTIONS` requests that CORS answers itself are not counted at all; and only
FastAPI routes carry a template, so the pages Starlette serves directly (every
app's `/openapi.json`, a sub-app's `/docs`) are counted under
`route="unmatched"` with their real status code.

### Module gauges mirror the health summary, read at scrape time

Each module already computes its operational numbers for
`/<module>/v1/health/summary`. Module gauges expose **the same numbers from
the same code**, read when Prometheus scrapes (`Gauge.set_function` over the
service's existing status call) rather than updated on every state change.
The hot paths — broker fan-out, buffer appends, formula evaluation — never
touch a metric, and the dashboard and the health endpoint cannot disagree.

Counters and histograms (things that *happen*, not things that *are*) are
the exception: those have to be incremented where the event occurs. Keep them
off per-update paths unless the measurement is worth the cost.

### Buckets

One shared histogram bucket set for API round-trips,
`canopy.core.metrics.DEFAULT_API_BUCKETS` (5 ms – 10 s). Module histograms
reuse it unless real measurements demand otherwise; don't hand-tune buckets
speculatively.

## What is instrumented today

- **Every HTTP route in every module**, via `HTTPMetricsMiddleware` on the
  parent app: `canopy_http_requests_total{method,module,route,status}` and
  `canopy_http_request_duration_seconds{method,module,route}`. Modules get
  this for free — no per-module code. WebSocket sessions are not HTTP
  requests and are not covered here; Hare's gauges are the view into them.
- **Hare** (the exemplar for module gauges): `canopy_hare_clients`,
  `canopy_hare_active_monitors`, `canopy_hare_subscriptions` — the broker's
  status, which is also what `/hare/v1/health/summary` reports. The client
  count includes in-process consumers (Vole holds a permanent broker client),
  exactly as the health summary does.

Candidates deliberately **not** built yet, each its own follow-up: Hare event
queue depth and dropped updates, Mole archiver round-trip latency, Vole
ring-buffer bytes and evictions, Fox evaluation latency, Squirrel restore
durations (a worker-side metric, so it also waits on the worker exposition
above).

## Adding a module metric

Use `canopy.hare.metrics` as the template:

1. Declare the metrics at module scope in `canopy/<module>/metrics.py` with a
   `canopy_<module>_` name. Import `DEFAULT_API_BUCKETS` from
   `canopy.core.metrics` for histograms.
2. For gauges, expose a `bind(service)` that points each gauge at the
   service's existing status method with `set_function`. The module's
   lifespan calls `bind(service)` right after it binds the gateway and
   `bind(None)` on shutdown. Nothing else in the module should know the
   gauges exist.
3. Check the label-discipline rules above before adding any label.
4. Add the metric names to the exposition test in
   `tests/canopy/core/test_metrics.py`, plus a module test that each gauge
   agrees with the health summary.

## Scraping

The `docker/observability/` overlay runs Prometheus (`:9090`) scraping
`api:8000/metrics` every 15 s, with a provisioned Grafana datasource
(`canopy-prometheus`). See the [setup guide](setup_guide.md).

That overlay is the only scraper today: the [facility deployments](../deployment/facilities.md)
run no Prometheus yet. The endpoint costs nothing to serve, and once a
facility Prometheus exists it only needs a scrape target of
`<api-host>:<API_PORT>/metrics`.
