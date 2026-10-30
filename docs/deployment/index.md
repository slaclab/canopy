# Deployment overview

A Canopy deployment is one container image running as two process types,
backed by Postgres and Redis:

| Process | Command | Role |
|---------|---------|------|
| `api` | `uvicorn canopy.main:app` | The HTTP/WebSocket API |
| `worker` | `arq canopy.worker.WorkerSettings` | Background jobs (snapshot create/restore, batch conversion) |

Both run from the same image (`ghcr.io/slaclab/canopy`) and share the same
configuration. Postgres holds the persistent modules' data (one schema per
module); Redis carries the job queue.

Two deployment paths are documented here:

- [Docker Compose](docker.md) — the development stack, and the app-only
  production compose file.
- [Deploy at your facility](production.md) — image tags, required
  configuration, migrations, and health checks for a production instance.
- [History mirror](history.md) — a read-only copy of an instance fed by
  Postgres logical replication: topology, roles, release order, and runbooks.

## Configuration

Canopy is configured entirely through environment variables (optionally via a
`.env` file in development). There are no config files to mount. The
essentials:

| Variable | Purpose |
|----------|---------|
| `CANOPY_API_KEYS` | API-key registry — see below. Unset means every gated endpoint returns 401 |
| `CANOPY_EPICS__BACKEND` | `simulation` (default) or `epics`. **Production must set `epics`** |
| `EPICS_CA_ADDR_LIST`, `EPICS_PVA_ADDR_LIST`, ... | Standard EPICS network discovery variables (only with the `epics` backend) |
| `CANOPY_DATABASE_URL` | Postgres, asyncpg driver: `postgresql+asyncpg://user:pass@host:port/db` |
| `CANOPY_REDIS_URL` | Redis: `redis://[:password@]host:port/0` |
| `CANOPY_DEBUG` | Verbose errors in responses; keep `false` in production |
| `CANOPY_LOG_LEVEL` | Root log level for the API and worker (`DEBUG`, `INFO`, `WARNING`, ...). Default `INFO`; an unknown name falls back to `INFO` |
| `CANOPY_SQUIRREL__RESTORE_HOOKS` | Comma-separated restore hooks, applied in order; empty means none run. Once a hook's write fails for a PV, later hooks skip it, so list `write_to_con` before `magnet_trim` to trim a magnet only after its BCON is updated. Must reach the **worker**, which performs restores. Verify with `GET /squirrel/v1/health/restore-hooks`: `worker` is what the worker reported loading at startup, `api` is the API's view, and `matchesApi: false` flags a mismatch |
| `CANOPY_ROLE` | `primary` (default), `mirror` or `forward`. A mirror is a read-only copy of a primary's database: write and admin routes return 403 `read_only_instance` (API-key management still works, since keys are local to each instance), and the worker runs only `ping` and batch conversion. A forward instance is a mirror that sends an allowlisted set of writes to its primary (`CANOPY_FORWARD__*`). Set it on both the API and the worker |
| `CANOPY_INSTANCE`, `CANOPY_HISTORY_URL`, `CANOPY_WINDOW_DAYS` | Optional. Reported by `GET /v1/instance` with the role and the Alembic head shipped in the image: the instance name, the URL of its read-only history copy, and how many days of history it keeps (unset means all) |
| `CANOPY_ACCEPT_FORWARDED_WRITES` | Primary only, default `false`. When `true`, a service key (`scripts/create_key.py --service`) may send `X-Canopy-On-Behalf-Of`: the write is recorded under that name (`createdBy`), and an answer that committed a write carries `X-Canopy-Commit-LSN`. Otherwise the header is refused with 403 `forwarded_write_refused` |
| `CANOPY_MACHINE_ACCESS` | Forward only: `read` (default) or `write`. With `write`, a Hare PV write runs on the forward instance through its own EPICS gateway (so that gateway must allow writes) and a snapshot restore is forwarded to the primary; with `read` both return 403 `machine_read_only`. Ignored on a primary, which always controls the machine, and on a mirror, which never does. Reported as `machineAccess` by `GET /v1/instance` |
| `CANOPY_FORWARD__PRIMARY_URL`, `__SERVICE_KEY` | Forward only: the primary's API base URL (https) and a service key on it with write scope on `squirrel` (and `magpie` with `__ALLOW_SHARED`). Supply the key from a secret |
| `CANOPY_FORWARD__ALLOW_SHARED`, `__PRIMARY_WINDOW_DAYS`, `__WAIT_S`, `__TIMEOUT_S`, `__CA_BUNDLE` | Forward only. `ALLOW_SHARED` (default `false`) also forwards writes to tables every facility shares (tags, meta-PVs, Magpie configs). `PRIMARY_WINDOW_DAYS`, the primary's window, refuses changes to older snapshots with 409 `archived_immutable`. `WAIT_S` (default 2) bounds the wait for replication to bring a write back before the answer carries `X-Canopy-Replica-Stale: true`; the wait reads `pg_replication_origin_status`, so a superuser must grant the app role SELECT on it and EXECUTE on `pg_show_replication_origin_status()`. `TIMEOUT_S` (default 30) is per request to the primary; `CA_BUNDLE` verifies its certificate |
| `CANOPY_REPLICATION__PUBLISHER_DSN` | Mirror and forward only, for `python -m canopy.core.replication subscribe`: the libpq connection string the subscription uses to reach the primary as the replication role. Supply it from a secret; the CLI never prints it |
| `CANOPY_REPLICATION__READER_DSN` | Mirror and forward only, for `python -m canopy.core.replication drift-check`: a `postgresql://` URI to the primary as the read-only reader role. A secret, like the publisher DSN |
| `CANOPY_REPLICATION__HEARTBEAT_DSN` | Mirror and forward only, for `heartbeat` and `switch`: a `postgresql://` URI to the primary as the heartbeat role. `heartbeat` records the mirror's backup, WAL-archive and drift checks in the primary's `core.history_heartbeat`; `switch` reads the subscription slot's position. Supply it from a secret; the CLI never prints it |
| `CANOPY_REPLICATION__HEARTBEAT_ROLE` | The role `HEARTBEAT_DSN` logs in as (default `canopy_heartbeat`); `publish` lets it write `core.history_heartbeat` and nothing else |
| `CANOPY_PRUNE__ENABLED`, `__DRY_RUN` | Primary worker only. With `ENABLED=true` a nightly job removes snapshots older than `CANOPY_WINDOW_DAYS` that aren't retained; it only counts them until `DRY_RUN=false`. Before each snapshot it checks that no publication sends their deletes to the mirror (the split layout), that the mirror's slot is streaming and caught up, and that its heartbeat is fresh and OK; it needs `CANOPY_INSTANCE` to find the slot, and a superuser or `pg_read_all_stats` to see what the slot streams. Each run logs what it removed or which check stopped it |
| `CANOPY_PRUNE__MAX_LAG_S`, `__HEARTBEAT_MAX_AGE_H`, `__HOUR`, `__MAX_SNAPSHOTS_PER_RUN` | Prune limits: the mirror's largest flush lag and reply age (default 60 s), the oldest acceptable heartbeat (3 h), the UTC hour the job runs (3), and the most snapshots one run removes (500) |
| `CANOPY_REPLICATION__REPLICATOR_ROLE`, `__READER_ROLE`, `__APP_ROLE` | Role names for the replication CLI: on a primary, `publish` grants the replication role (default `replicator`) and the reader role (default `canopy_reader`) SELECT on the published tables; on a mirror, `grants` gives the app role (default `canopy_app`) read access to replicated tables and read-write on API keys |
| `CANOPY_<MODULE>__<FIELD>` | Per-module tuning (e.g. `CANOPY_VOLE__GLOBAL_BYTES_MAX`) |

The annotated template
[`.env.example`](https://github.com/slaclab/Canopy/blob/main/.env.example)
at the repo root documents every setting with its default.

!!! warning "Simulation is the default backend"
    A deployment that forgets `CANOPY_EPICS__BACKEND=epics` starts cleanly,
    passes health checks, and serves synthetic sine waves for every PV. The
    WebSocket `heartbeat` frame reports the active backend — check it when
    values look too smooth to be real.

### API keys

```
CANOPY_API_KEYS="<token>:<scope>[+<scope>][@<module>[+<module>]][,<token>:<scope>...]"
```

Issue **separate tokens per client class** with the minimum scope, and generate
long random values for production. The variable is parsed once at process
startup, so adding, rotating, or revoking a key is a config change plus a
restart.

See [API keys](api-keys.md) for the provisioning, rotation, and revocation
runbook.

## Database migrations

The schema is managed exclusively by Alembic — the app never creates tables
at runtime:

```bash
CANOPY_DATABASE_URL=postgresql+asyncpg://... uv run alembic upgrade head
```

The production image carries the migration tree, so run
`alembic upgrade head` from the new image before restarting services on it
(see [Upgrade](production.md#upgrade)). A fresh database serves nothing until
its first migration.

On a primary with a read-only history mirror, follow the upgrade with
`python -m canopy.core.replication publish`, which keeps the replication
publication in step with the tables. The mirror runs `alembic upgrade head`,
then `grants` and `refresh`, before its primary deploys; `subscribe` runs once.
`status` reports either side as JSON (see [History mirror](history.md)), and `drift-check`, run on the mirror after
the hourly `refresh`, compares each table with the primary: it exits 1 when the
copy has drifted (a change replication skipped) and 2 when it can't check. Run each side's migrations with its own
`CANOPY_ROLE`: a few migration steps run on the primary or on the mirror only (see
"Database Migrations" in `CONTRIBUTING.md`).

## Health checks

| Endpoint | Use |
|----------|-----|
| `GET /health` | Process liveness — used by the container healthchecks and post-deploy checks |
| `GET /health/ready` | Readiness: 200 `{"status": "ready"}` once the database answers `SELECT 1` within two seconds, 503 while it doesn't. Use it for a Kubernetes readiness probe; keep `/health` as the liveness probe, so a database outage takes the API out of rotation instead of restarting it |
| `GET /<module>/v1/health/watchdog` | Per-module liveness heartbeat (version, uptime) |
| `GET /<module>/v1/health/summary` | Per-module status rollup (requires read access) — e.g. Hare's reports backend, client and monitor counts |
| `GET /v1/instance` | `{role, instance, historyUrl, windowDays, alembicHead}` (requires read access on `core`). `alembicHead` is the migrations head shipped in the image, not the database's |
| `GET /metrics` | Prometheus exposition (unauthenticated, like `/health`): request counts and latency per module route, module gauges, and replication slot or subscription state — see [Metrics](../observability/metrics.md) |
