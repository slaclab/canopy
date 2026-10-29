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
leaves access control to the network rather than the application.
[Production deployments](../deployment/production.md) must restrict access
to the API's metrics endpoint to trusted networks. What the payload adds to the already
public route map is operational telemetry — request counts, latencies, status
codes, connected-client counts, process memory and the Python version — never
PV names, values or key names.

### Single default registry

Modules create metrics with the plain `prometheus_client` constructors, or
register a collector, into the default `REGISTRY`. No per-module sub-registries: merging
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
the same code**, read when Prometheus scrapes rather than updated on every
state change: one custom collector per module fetches the service through the
module's gateway, calls its existing status method once, and yields a gauge
per field. The hot paths — broker fan-out, buffer appends, formula
evaluation — never touch a metric, and the dashboard and the health endpoint
cannot disagree.

Three properties of that collector are deliberate. Going through the gateway
means it rides the lifespan's existing bind and unbind, so a scrape before
startup or after shutdown reads zeros and nothing is bound twice. Calling the
status method once per scrape keeps a module with an expensive status from
paying for it once per gauge. And it catches exceptions from that call, logs,
and yields nothing, so one module's fault drops its own series from that
scrape and never the whole exposition. A collector that raises fails the
entire `/metrics` response, HTTP and process metrics included.

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
  Two requests arrive on their own and form a steady baseline under
  `module="core"`: the container healthcheck on `/health` and Prometheus's
  own scrape of `/metrics`. Each has its own `route` label, so a panel that
  wants user traffic excludes them with one selector.
- **Hare** (the exemplar for module gauges): `canopy_hare_clients`,
  `canopy_hare_active_monitors`, `canopy_hare_subscriptions` — one collector
  over the broker's status, which is also what `/hare/v1/health/summary`
  reports. The client
  count includes in-process consumers (Vole holds a permanent broker client),
  exactly as the health summary does.

- **Replication** to a history mirror (`canopy.core.replication.metrics`); see
  [Replication](#replication) below.

Candidates deliberately **not** built yet, each its own follow-up: Hare event
queue depth and dropped updates, Mole archiver round-trip latency, Vole
ring-buffer bytes and evictions, Fox evaluation latency, Squirrel restore
durations (a worker-side metric, so it also waits on the worker exposition
above).

## Replication

An instance with a history mirror reports its side of the replication, read
from the same status function as `python -m canopy.core.replication status`.
That status comes from database queries, which are async, and a collector's
`collect()` is not, so this is the one collector that doesn't fetch at
collect time: the `/metrics` handler awaits its refresh just before rendering.
The refresh opens its own short-lived connection (`CANOPY_DATABASE_URL`,
`application_name=canopy-metrics`) with a two-second budget. If it fails, the
scrape still succeeds and carries only `canopy_replication_status_up 0`.

| Metric | Labels | Meaning |
|--------|--------|---------|
| `canopy_replication_status_up` | | 1 when this scrape read the status |
| `canopy_replication_slot_active` | `slot`, `kind` | Primary: 1 while a mirror streams from the logical slot. `kind` is `subscription`, `tablesync` (an initial table copy) or `other` |
| `canopy_replication_slot_wal_status` | `slot`, `kind`, `wal_status` | 1 for the slot's current `wal_status` (`reserved`, `extended`, `unreserved`, `lost`) |
| `canopy_replication_slot_retained_bytes` | `slot`, `kind` | WAL the slot keeps on the primary's disk |
| `canopy_replication_slot_lag_bytes` | `slot`, `kind` | WAL the mirror hasn't confirmed applying: **the** replication lag |
| `canopy_replication_slot_safe_wal_bytes` | `slot`, `kind` | WAL left before the slot is lost (only under a cap) |
| `canopy_replication_max_slot_wal_keep_bytes` | | The primary's `max_slot_wal_keep_size`; absent when unlimited |
| `canopy_replication_subscription_exists`, `_enabled`, `_worker_up` | `subscription` | Mirror: the subscription, and whether its apply worker runs |
| `canopy_replication_subscription_receive_age_seconds` | `subscription` | Seconds since the primary last sent anything, keepalives included; the API host's clock against the database's, so clock skew shows up here |
| `canopy_replication_subscription_apply_errors_total`, `_sync_errors_total` | `subscription` | Errors applying changes, and during initial copies |
| `canopy_replication_subscription_tables_not_ready` | `subscription` | Tables still in their initial copy |
| `canopy_replication_subscription_applied_lsn_bytes` | `subscription` | The primary's WAL position of the last transaction applied: a position, not a lag |

A mirror has no honest apply-lag figure of its own. Its received position
follows every keepalive, which covers WAL the primary wrote to tables it
doesn't publish, so received minus applied grows while nothing is waiting.
Alert on the primary's `canopy_replication_slot_lag_bytes` instead. The
applied position is readable only by a superuser unless a superuser grants
the API's role both `SELECT ON pg_replication_origin_status` and
`EXECUTE ON FUNCTION pg_show_replication_origin_status()` (`pg_read_all_stats`
isn't enough). Without them that one series is absent.

`docker/observability/grafana/provisioning/alerting/replication.yaml` holds
the alert rules: on the primary, a slot inactive for 15 minutes, retaining
over half the WAL cap, out of `reserved`, or a slot with no cap at all; on the
mirror, a stopped apply worker, nothing received for five minutes, apply or
copy errors, and a copy unfinished after an hour; on either, a status that
can't be read. No contact point is provisioned.

## Adding a module metric

Use `canopy.hare.metrics` as the template:

1. Declare the metrics at module scope in `canopy/<module>/metrics.py` with a
   `canopy_<module>_` name. Import `DEFAULT_API_BUCKETS` from
   `canopy.core.metrics` for histograms.
2. For gauges, write a collector like `canopy.hare.metrics.HareCollector`:
   fetch the service through the module's gateway (treat "not bound" as
   zeros), call its status method once, yield a `GaugeMetricFamily` per
   field, and catch exceptions from that call so one module's fault cannot
   fail the scrape. Expose a `register()` that adds it to the default
   registry and call it once from the module's `app.py` at import. Nothing
   else in the module should know the collector exists.
3. Check the label-discipline rules above before adding any label.
4. Add the metric names to the exposition test in
   `tests/canopy/core/test_metrics.py`, plus module tests that each gauge
   agrees with the health summary and that a failing status call drops the
   module's series but not the scrape.

## Scraping

The `docker/observability/` overlay runs Prometheus (`:9090`) scraping
`api:8000/metrics` every 15 s, with a provisioned Grafana datasource
(`canopy-prometheus`). See the [setup guide](setup_guide.md).

The [production deployment guide](../deployment/production.md) does not
include Prometheus. To use a facility's Prometheus, configure a scrape target of
`<api-host>:<API_PORT>/metrics`.
