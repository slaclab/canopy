# Squirrel configuration

Squirrel is configured through environment variables with the prefix
`CANOPY_SQUIRREL__`. Every setting has a default, so Squirrel runs with none
of them set.

!!! warning "Set these on the worker too"
    Snapshot capture and restore run in the Canopy worker, not the API, so the
    worker is the process whose settings decide what they do. Give the API and
    the worker the same values. To check which restore hooks each one loaded,
    see [Checking the configuration](restore_hooks.md#checking-the-configuration).

## Environment variables

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `CANOPY_SQUIRREL__RESTORE_HOOKS` | string | `""` | Comma-separated [restore hooks](restore_hooks.md) to run during a restore, in order. Empty means none run. |
| `CANOPY_SQUIRREL__EPICS_CONCURRENCY` | int | `64` | Maximum EPICS reads (capture) or writes (restore) in flight at once. |
| `CANOPY_SQUIRREL__EPICS_CHUNK_SIZE` | int | `500` | How many PVs are read or written between progress updates. |
| `CANOPY_SQUIRREL__EPICS_TIMEOUT_S` | float | unset | Timeout for each read or write, in seconds. Unset uses the EPICS connector's own timeout (`CANOPY_EPICS__CA_TIMEOUT_S` or `CANOPY_EPICS__PVA_TIMEOUT_S`, 5 seconds by default). |
| `CANOPY_SQUIRREL__RESTORE_HOOK` | string | `""` | **Retired.** The old single-hook setting. It is never applied; if it is set, the worker logs a warning at startup and the restore-hooks health check lists it under `warnings`. Use `RESTORE_HOOKS` instead. |

### Related settings

Squirrel also depends on these shared settings, documented in
[Deploy at your facility](../deployment/index.md):

| Variable | Why Squirrel needs it |
|----------|-----------------------|
| `CANOPY_DATABASE_URL` | Snapshots, PVs, and tags are stored in Postgres. |
| `CANOPY_REDIS_URL` | Capture and restore jobs are queued, and report progress, through Redis. |
| `CANOPY_EPICS__BACKEND` | `simulation` (the default) or `epics`. With `simulation`, capture reads synthetic values, and a restore's writes reach no IOC: the worker holds each written value and returns it for later reads. |
| `CANOPY_EPICS__SIM_PUT_ERROR_RATE`, `CANOPY_EPICS__SIM_PUT_FAIL_PVS`, `CANOPY_EPICS__SIM_SEED` | Make simulated writes fail, to try out partial restore failures without an IOC. See [Testing](testing.md). |

## Example .env

```bash
CANOPY_SQUIRREL__RESTORE_HOOKS=write_to_con,magnet_trim
CANOPY_SQUIRREL__EPICS_CONCURRENCY=128
CANOPY_SQUIRREL__EPICS_TIMEOUT_S=2.0
```

With Docker Compose, put these in the `.env` file next to the compose file.
Both the development and production compose files load it into the API and
the worker.

## Tuning guidance

### Concurrency (`EPICS_CONCURRENCY`)

A capture reads every tracked PV, and a restore writes every PV in its list, so
this setting mostly decides how long they take. Raise it for large PV lists
when IOCs and the network can take the load; lower it if a capture or
restore makes IOCs or gateways struggle. Reads and writes share the
setting.

### Timeout (`EPICS_TIMEOUT_S`)

A PV that doesn't answer holds a slot for the whole timeout. With the
connector's 5-second default, many disconnected PVs can slow a capture down
considerably. A shorter timeout makes captures of a machine with many
disconnected PVs finish faster, at the risk of missing slow IOCs. In a
capture, a PV that times out is still part of the snapshot, but with no saved
value; in a restore, it is listed as a failure.

### Progress updates (`EPICS_CHUNK_SIZE`)

Reads and writes are done in chunks of this many PVs, and progress is
reported after each chunk. Smaller chunks give smoother progress bars at
the cost of more Redis writes.

### Time limit

A capture or restore job is stopped after **10 minutes**. This limit is set
on the worker and is not configurable. If large captures hit it, raise
`EPICS_CONCURRENCY` or shorten `EPICS_TIMEOUT_S`.
