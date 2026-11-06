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
| `CANOPY_REDIS_URL` | Redis: `redis://[:password@]host:port/0`. One per instance: a primary and its mirror must not share a queue (give them separate Redis servers or database numbers), since a mirror's worker has no snapshot tasks and fails any primary job it takes |
| `CANOPY_DEBUG` | Verbose errors in responses; keep `false` in production |
| `CANOPY_LOG_LEVEL` | Root log level for the API and worker (`DEBUG`, `INFO`, `WARNING`, ...). Default `INFO`; an unknown name falls back to `INFO` |
| `CANOPY_SQUIRREL__RESTORE_HOOKS` | Extra writes Squirrel makes during a snapshot restore, such as magnet trims. Empty means none. Must reach the **worker**, which performs restores. See [Restore hooks](../squirrel/restore_hooks.md) |
| `CANOPY_ROLE` | `primary` (default) or `mirror`. A mirror is a read-only copy of a primary's database: write and admin routes return 403 `read_only_instance` (API-key management still works, since keys are local to each instance), and the worker runs only `ping` and batch conversion. Set it on both the API and the worker |
| `CANOPY_INSTANCE` | **Required.** This instance's name: lowercase letters, digits and underscores, at most 48 (e.g. `arm1`). Choose it once; it is permanent once the instance holds data. Set it on the API, the worker and every `alembic` run. Reported by `GET /v1/instance` |
| `CANOPY_HISTORY_URL`, `CANOPY_WINDOW_DAYS` | Optional. Reported by `GET /v1/instance` with the role, the instance name and the Alembic head shipped in the image: the URL of the instance's read-only history copy, and how many days of history it keeps (a whole number of at least 1; unset means all). A blank value counts as unset |
| `CANOPY_ARMS__<arm>__PUBLISHER_DSN` | History head only (`CANOPY_ROLE=mirror`): one per arm, named after that arm's `CANOPY_INSTANCE` (lowercase letters, digits and underscores, at most 48). The libpq connection string the head's subscription `canopy_history_<arm>` uses to reach the arm as its replication role. `python -m canopy.core.replication subscribe` reads it; supply it from a secret, and the CLI never prints it. The arms can also be set as JSON in `CANOPY_ARMS` |
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

On an arm (a primary with a history head), follow the upgrade with
`python -m canopy.core.replication publish`, which keeps the replication
publication in step with the tables. The head runs `alembic upgrade head`, then
`grants` and `refresh`, before any of its arms deploys; `subscribe` runs once per
arm. On the head, `subscribe`, `refresh` and `status` act on every arm in
`CANOPY_ARMS__<arm>__…`, or on one with `--arm <arm>`. `status` reports either
side as JSON.

## Health checks

| Endpoint | Use |
|----------|-----|
| `GET /health` | Process liveness — used by the container healthchecks and post-deploy checks |
| `GET /<module>/v1/health/watchdog` | Per-module liveness heartbeat (version, uptime) |
| `GET /<module>/v1/health/summary` | Per-module status rollup (requires read access) — e.g. Hare's reports backend, client and monitor counts |
| `GET /v1/instance` | `{role, instance, historyUrl, windowDays, alembicHead, arms}` (requires read access on `core`). `alembicHead` is the migrations head shipped in the image, not the database's, read once at startup. `arms` is empty except on a history head, where it lists each arm as `{name, subscription}`, with `subscription` one of `missing`, `disabled`, `stopped`, `copying` or `streaming`. It is `null` where the process finds no migrations tree, as in the dev image, or the tree has more than one head |
| `GET /metrics` | Prometheus exposition (unauthenticated, like `/health`): request counts and latency per module route, plus module gauges — see [Metrics](../observability/metrics.md) |
