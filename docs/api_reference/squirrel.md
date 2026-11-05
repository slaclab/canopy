# Squirrel architecture

This page documents Squirrel's internal design decisions for developers
working on or extending the module. For what Squirrel does and how to use
it, see the [Squirrel guide](../squirrel/index.md).

## Layout

| Package | Role |
|---------|------|
| `api/v1/` | HTTP routes. They validate input, call a service, and enqueue jobs |
| `services/` | Domain logic: `SnapshotService`, `PvService`, `TagService` |
| `data/` | SQLAlchemy models (the `squirrel` Postgres schema) and repositories |
| `adapters/epics_io.py` | `EpicsIO`, batch EPICS reads and writes over the shared connector |
| `restore_hooks/` | The `RestoreHook` interface, the built-in hooks, and the registry |
| `tasks.py` | The Arq tasks the worker runs for capture and restore |
| `gateway.py` | The read-only surface other Canopy modules may import |

Services own no transaction. A request commits through the request session,
and a worker task commits through its own `session_scope`, so each caller
decides what is atomic.

## Why capture and restore are jobs

Both touch every PV in a snapshot, which can mean thousands of EPICS
operations, so they take longer than a request should. They also need the
EPICS connector, which only the worker builds. So the route enqueues an Arq
job and returns its id, and the worker does the work and reports progress
to Redis. The Jobs module serves that progress to clients without knowing
anything about Squirrel.

Everything else (listing, editing, comparing) runs in the request.

## Capture

`SnapshotService.create_snapshot` is the hot path.

- **One read per address.** Only setpoint addresses are unique; several PVs
  can share a readback. Capture maps each address to the list of PVs using
  it and reads it once, instead of once per PV.
- **A failed read never fails the batch.** `EpicsIO` turns any per-PV
  exception into a disconnected value, and capture stores no value for it.
  The snapshot keeps a row for the PV either way.
- **Bulk insert with `COPY`.** Values are written with PostgreSQL `COPY` on
  the session's own connection, in the same transaction as the snapshot row,
  so a snapshot is saved complete or not at all, and a retried job never
  finds a half-written one.
- **Values are JSON blobs.** Each setpoint and readback reading is stored as
  JSONB (`value`, `status`, `severity`, `timestamp`, `units`, `precision`), so
  more metadata can be captured later without a migration. Units, precision
  and limits come from a separate EPICS describe call, so v1 leaves them
  empty.

## Restore

`SnapshotService.restore_snapshot` plans everything before writing anything:

1. Build the write set: saved setpoints only, never readbacks.
2. Ask every hook for its extra writes. A `RestoreHookError` removes that PV
   from the write set, so a PV a hook can't handle is never partly restored.
3. Write the setpoints, then each hook's writes, as separate batches.

Each batch is a `RestorePhase`, reported to the job's `phases`. A failure
row's `phase` names the batch that failed, and a PV that fails in one phase
is skipped by later ones. A restore reads the database but never writes to
it.

## Paging

The snapshot list and the paged PV search use **keyset** paging: each
page continues after the last row of the previous one instead of skipping
a count of rows, so rows created or deleted between pages can't make a
later page repeat or skip one.

- **Snapshots** sort by `(created_at, id)`, newest first. The continuation
  token is that pair, base64-encoded. A token that doesn't decode is
  rejected with `400` rather than reaching the database.
- **PVs** sort by `id`, and the token is the last id.

`GET /pvs/search` uses offset paging instead, for clients that need page
numbers.

## PV identity

A PV is identified by its first address in the order setpoint, readback,
config. Only `setpoint_address` has a unique constraint in the database. The
rest of the rule is enforced in `PvService`, so that a PV with a setpoint
never collides with a readback-only PV that shares its readback.

## Restore hooks across processes

Hooks are resolved from `CANOPY_SQUIRREL__RESTORE_HOOKS` separately in each
process. Restores run in the worker, but the API is what operators can
query. So the worker publishes its resolved hooks to Redis when it starts,
and `GET /squirrel/v1/health/restore-hooks` shows that report next to the
API's own view. The report has no expiry: each worker start replaces it, and
it carries the time and host it came from, so a stale one can be recognised.

## Module reference

### Gateway

::: canopy.squirrel.gateway
    options:
      show_root_heading: true
      show_source: false

### Services

#### SnapshotService

::: canopy.squirrel.services.snapshot_service
    options:
      show_root_heading: true
      show_source: false
      members:
        - SnapshotService
        - RestorePhase

#### PvService

::: canopy.squirrel.services.pv_service
    options:
      show_root_heading: true
      show_source: false
      members:
        - PvService

### Restore hooks

::: canopy.squirrel.restore_hooks.base
    options:
      show_root_heading: true
      show_source: false
      members:
        - RestoreHook
        - RestoreHookError

::: canopy.squirrel.restore_hooks.registry
    options:
      show_root_heading: true
      show_source: false
      members:
        - RESTORE_HOOK_PROVIDERS
        - get_restore_hooks
        - describe_restore_hooks

### EPICS adapter

::: canopy.squirrel.adapters.epics_io
    options:
      show_root_heading: true
      show_source: false
      members:
        - EpicsIO
        - EpicsValue

### Configuration

::: canopy.squirrel.config
    options:
      show_root_heading: true
      show_source: false
      members:
        - SquirrelConfig
