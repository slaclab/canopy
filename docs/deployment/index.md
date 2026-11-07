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
- [History mirror](history.md) — a head that copies its arms over Postgres
  logical replication: topology, roles, release order, and runbooks.

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
| `CANOPY_REDIS_URL` | Redis: `redis://[:password@]host:port/0`. One per instance: two instances must not share a queue (give them separate Redis servers or database numbers), or one instance's worker runs the other's jobs against its own database |
| `CANOPY_DEBUG` | Verbose errors in responses; keep `false` in production |
| `CANOPY_LOG_LEVEL` | Root log level for the API and worker (`DEBUG`, `INFO`, `WARNING`, ...). Default `INFO`; an unknown name falls back to `INFO` |
| `CANOPY_SQUIRREL__RESTORE_HOOKS` | Extra writes Squirrel makes during a snapshot restore, such as magnet trims. Empty means none. Must reach the **worker**, which performs restores. See [Restore hooks](../squirrel/restore_hooks.md) |
| `CANOPY_ROLE` | `primary` (default) or `mirror`: which side of history replication this instance's database is on. A primary holds only rows it owns, publishes them and may prune. A mirror (the head) subscribes to its arms and holds their rows beside its own, and never prunes. Either way an instance writes only rows it owns (see [Row owners](#row-owners)); a mirror meant to stay read-only gives its users read keys only. Set it on the API, the worker and every `alembic` run |
| `CANOPY_INSTANCE` | **Required.** This instance's name: lowercase letters, digits and underscores, at most 48 (e.g. `arm1`). It is stamped on every row the instance creates, so it is permanent once the instance holds data: on a primary, `alembic` refuses to run when the database holds rows stamped with another name. Set it on the API, the worker and every `alembic` run. Reported by `GET /v1/instance` |
| `CANOPY_HISTORY_URL`, `CANOPY_WINDOW_DAYS` | Optional. Reported by `GET /v1/instance` with the role, the instance name and the Alembic head shipped in the image: the URL of the instance's read-only history copy, and how many days of history it keeps (a whole number of at least 1; unset means all). A blank value counts as unset |
| `CANOPY_ARMS__<arm>__PUBLISHER_DSN` | History head only (`CANOPY_ROLE=mirror`): one per arm, named after that arm's `CANOPY_INSTANCE` (lowercase letters, digits and underscores, at most 48). The libpq connection string (key=value or URI) the head's subscription `canopy_history_<arm>` uses to reach the arm as its replication role. `subscribe` and `refresh` also open it themselves before an initial copy, to refuse facility or Magpie slugs the head already holds, so the CLI's network must reach the arm at that address too. Supply it from a secret; the CLI never prints it. The arms can also be set as JSON in `CANOPY_ARMS` |
| `CANOPY_ARMS__<arm>__READER_DSN` | History head only, for `python -m canopy.core.replication drift-check`: a `postgresql://` URI to that arm as its read-only reader role (asyncpg needs the URI form). A secret, like the publisher DSN |
| `CANOPY_ARMS__<arm>__HEARTBEAT_DSN` | History head only, for an arm that prunes: a `postgresql://` URI to that arm as its heartbeat role. `heartbeat --arm <arm>` records the head's backup, WAL-archive and drift checks for that arm in the arm's `core.history_heartbeat`; `switch --arm <arm>` reads the arm's slot position. Supply it from a secret; the CLI never prints it |
| `CANOPY_ARMS__<arm>__FORWARD_URL`, `__SERVICE_KEY` | History head only, set together or not at all: where the head sends writes to that arm's rows (the arm's API base URL, https) and a service key minted on that arm (`scripts/create_key.py --service`). Without them the arm's rows are read-only on the head (403 `not_owner`). Supply the key from a secret. See [Writes on a head](history.md#writes-on-a-head) |
| `CANOPY_FORWARD__WAIT_S`, `__TIMEOUT_S`, `__CA_BUNDLE`, `__ALLOW_INSECURE_HTTP` | History head only, for every arm it forwards to. `WAIT_S` (default 2) bounds the wait for replication to bring a forwarded write back before the answer carries `X-Canopy-Replica-Stale: true`; the wait reads `pg_replication_origin_status`, so a superuser must grant the app role SELECT on it and EXECUTE on `pg_show_replication_origin_status()`. `TIMEOUT_S` (default 30) is per request to an arm; `CA_BUNDLE` verifies the arms' certificates; `ALLOW_INSECURE_HTTP` (default `false`) allows an `http://` forward URL, for development only |
| `CANOPY_MACHINE_ACCESS` | `read` or `write`; `write` on a primary and `read` on a mirror when unset. With `write` the instance runs Hare PV writes and snapshot restores through its own EPICS gateway (so that gateway must allow writes), and its worker registers the restore task; with `read` both are refused with 403 `machine_read_only`, and a history head sends a restore of a forwardable arm's snapshot to that arm. Captures run either way. Give `write` to one instance per facility. Reported as `machineAccess` by `GET /v1/instance`. Set it on the API and the worker |
| `CANOPY_ACCEPT_FORWARDED_WRITES` | Arm (`CANOPY_ROLE=primary`) only, default `false`. When `true`, a service key may send `X-Canopy-On-Behalf-Of`: the write is recorded under that name (`createdBy`), and an answer that committed a write carries `X-Canopy-Commit-LSN`. Otherwise the header is refused with 403 `forwarded_write_refused` |
| `CANOPY_REPLICATION__REPLICATOR_ROLE`, `__READER_ROLE`, `__APP_ROLE` | Role names for the replication CLI: on a primary, `publish` grants the replication role (default `replicator`) and the reader role (default `canopy_reader`) SELECT on the published tables; on a history head, `grants` lets the app role (default `canopy_app`) read every row and write only the head's own: row-level security on the tables that name a row's owner (facilities, PVs, snapshots, tag groups, Magpie configs) limits its writes to rows whose `owner_instance` is the head's `CANOPY_INSTANCE`, and any other role but the tables' owner sees none of those tables' rows. It also gets read-write on API keys |
| `CANOPY_REPLICATION__HEARTBEAT_ROLE` | On an arm: the role the head's heartbeat connection logs in as (default `canopy_heartbeat`); `publish` lets it write `core.history_heartbeat` and nothing else |
| `CANOPY_PRUNE__ENABLED`, `__DRY_RUN` | Arm worker only (`CANOPY_ROLE=primary`; a head never prunes). With `ENABLED=true` a nightly job removes snapshots older than `CANOPY_WINDOW_DAYS` that aren't retained; it only counts them until `DRY_RUN=false`. Before each snapshot it checks that no publication sends their deletes to the head (the split layout), that the head's slot `canopy_history_<CANOPY_INSTANCE>` is streaming and caught up, and that the head's heartbeat for this arm is fresh and OK; it needs a superuser or `pg_read_all_stats` to see what the slot streams. Each run logs what it removed or which check stopped it |
| `CANOPY_PRUNE__MAX_LAG_S`, `__HEARTBEAT_MAX_AGE_H`, `__HOUR`, `__MAX_SNAPSHOTS_PER_RUN` | Prune limits: the head's largest flush lag and reply age (default 60 s), the oldest acceptable heartbeat (3 h), the UTC hour the job runs (3), and the most snapshots one run removes (500) |
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

## Row owners

Every facility, PV, snapshot, tag group and Magpie config records the instance that
created it, in `owner_instance` (`ownerInstance` on the wire). It is set on insert
from `CANOPY_INSTANCE` and never changes; a mirror receives it with each arm's rows.
A snapshot's values, a PV's tags, facilities and meta-PV marker, and a tag group's
tags follow their parent.

An instance writes only rows it owns:

- Editing or deleting another instance's row answers `403 not_owner`, naming the
  owner to send it to.
- A new row belongs to the owner of what it's created in: a facility's parent, a
  PV's facilities, a snapshot's facility, a tag's group. A create with none of those
  belongs to this instance. Creating a root facility, a PV, a PV import, a snapshot, a meta-PV
  marker, a tag group, a tag import or a Magpie config also takes `?owner=`; an instance accepts only its own name
  there (a mirror's router forwards the creates it routes to that arm). A create this
  instance wouldn't own answers `403 not_owner`.
- A request that links rows of different owners (a PV with another owner's tag or
  facility, a facility under another owner's parent, an import whose rows span
  owners) answers `409 owner_mismatch`.
- A snapshot with no facility captures only this instance's PVs.

Facility slugs and Magpie slugs are unique everywhere. A PV's setpoint address and
a tag group's name are unique per owner, so a mirror can hold the same PV or group
name once for each arm; `ownerInstance` tells them apart.

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
arm. On the head, `subscribe`, `refresh`, `status` and `drift-check` act on every
arm in `CANOPY_ARMS__<arm>__…`, or on one with `--arm <arm>`. `status` reports
either side as JSON (see [History mirror](history.md)). `drift-check`, run on the head after the hourly `refresh`,
compares each arm's rows with the head's copy, then checks that every reference on
the head finds its parent. Per arm it exits 0 only when every table was compared and
is in step, 1 on drift, 2 when it couldn't check everything; a run exits with the
worst. Run each instance's migrations with its own `CANOPY_ROLE` and
`CANOPY_INSTANCE`: a few migration steps run on an arm or on the head only, and data
steps change only the rows the instance owns (see "Database Migrations" in
`CONTRIBUTING.md`).

## Health checks

| Endpoint | Use |
|----------|-----|
| `GET /health` | Process liveness — used by the container healthchecks and post-deploy checks |
| `GET /health/ready` | Readiness: 200 `{"status": "ready"}` once the database answers `SELECT 1` within two seconds, 503 while it doesn't. Use it for a Kubernetes readiness probe; keep `/health` as the liveness probe, so a database outage takes the API out of rotation instead of restarting it |
| `GET /<module>/v1/health/watchdog` | Per-module liveness heartbeat (version, uptime) |
| `GET /<module>/v1/health/summary` | Per-module status rollup (requires read access) — e.g. Hare's reports backend, client and monitor counts |
| `GET /v1/instance` | `{role, instance, historyUrl, windowDays, alembicHead, arms}` (requires read access on `core`). `alembicHead` is the migrations head shipped in the image, not the database's, read once at startup. `arms` is empty except on a history head, where it lists each arm as `{name, subscription}`, with `subscription` one of `missing`, `disabled`, `stopped`, `copying` or `streaming`. It is `null` where the process finds no migrations tree, as in the dev image, or the tree has more than one head |
| `GET /metrics` | Prometheus exposition (unauthenticated, like `/health`): request counts and latency per module route, module gauges, and replication state: an arm's slot, or a head's subscription to each arm — see [Metrics](../observability/metrics.md) |
