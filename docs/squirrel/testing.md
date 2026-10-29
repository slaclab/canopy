# Testing Squirrel

This page covers Squirrel's automated tests and how to try Squirrel by hand
without a real control system.

## Running the tests

Squirrel's tests are part of Canopy's suite:

```bash
# Every test
uv run pytest tests/

# Only Squirrel's tests
uv run pytest tests/canopy/squirrel/

# One file, or one test
uv run pytest tests/canopy/squirrel/test_restore_hooks.py
uv run pytest tests/canopy/squirrel/test_services_snapshot.py::test_restore_runs_every_hook_in_order

# Tests whose names match a pattern
uv run pytest tests/canopy/squirrel/ -k restore
```

### The test database

Most of Squirrel's tests need Postgres. They use a database named
`canopy_test`, which they create the first time they run, and they build
and drop Squirrel's tables around every test, so no migrations are needed.

By default they connect as `canopy:canopy` to `localhost:5432`, which is
where the development Compose stack publishes its Postgres. Starting just
that service is enough:

```bash
docker compose -f docker/docker-compose.yml up -d postgres
```

To use a different server, set:

| Variable | Default | Used for |
|----------|---------|----------|
| `CANOPY_TEST_DATABASE_URL` | `postgresql+asyncpg://canopy:canopy@localhost:5432/canopy_test` | The test database itself |
| `CANOPY_TEST_ADMIN_DSN` | `postgresql://canopy:canopy@localhost:5432/postgres` | Creating `canopy_test` if it is missing |

!!! note "Without Postgres, most tests are skipped, not failed"
    If Postgres can't be reached, every test that needs it is reported as
    skipped and the run still passes. Only the tests that need no database,
    such as the restore-hook and EPICS-adapter tests, actually run. Check
    the skip count before trusting a green run.

The tests need neither Redis nor EPICS. Endpoints that queue a job are
tested by recording what would have been queued, and the code that talks to
EPICS is tested with a fake.

## Writing tests

Tests live in `tests/canopy/squirrel/`. The fixtures in its `conftest.py`
cover most needs:

| Fixture | Provides |
|---------|----------|
| `client` | An HTTP client for the Squirrel app, using the test database. Paths are relative to the module, e.g. `/v1/snapshots` |
| `enqueued` | The jobs `client` requests would have queued, as `(function, args, kwargs)` |
| `db_session` | A database session for setting up data directly. Commit it, so requests made through `client` can see the data |
| `sample_pv`, `sample_tag_group` | A PV and a tag group, created through the API |

Send `READ_HEADERS` or `WRITE_HEADERS` from the same `conftest.py` as the
request headers to authenticate as a read-only or a read/write key.

Restore hooks are switched off for every test:
`CANOPY_SQUIRREL__RESTORE_HOOKS` is ignored. A test that needs hooks passes them
explicitly, as in `SnapshotService(..., restore_hooks=[MagnetTrimHook()])`.

### Testing an endpoint

From `test_api_snapshots.py`:

```python
from tests.canopy.squirrel.conftest import WRITE_HEADERS


async def test_create_enqueues_job(client, enqueued):
    r = await client.post("/v1/snapshots", headers=WRITE_HEADERS, json={"title": "T", "description": "D"})
    assert r.status_code == 200
    assert r.json() == {"jobId": "test-job-id", "statusUrl": "/jobs/v1/test-job-id"}
    assert enqueued[0][0] == "create_snapshot_task"
```

No worker runs, so this checks what was queued, not what the job does.

### Testing capture and restore

The capture and restore logic is tested by calling `SnapshotService`
directly with a fake EPICS layer instead of the real one. `FakeEpicsIO`, in
`tests/canopy/squirrel/test_services_snapshot.py`, serves set values for
reads, records every write, and can be told to fail writes to particular
PVs. From the same file, slightly shortened:

```python
async def test_restore_skips_later_hooks_after_an_earlier_hook_fails(db_session):
    await _seed_pv(db_session, "QUAD:LI21:201:BDES")
    io = FakeEpicsIO({"QUAD:LI21:201:BDES": _ev(3.0)})
    io.fail_writes.add("QUAD:LI21:201:BCON")
    svc = SnapshotService(db_session, io, restore_hooks=[WriteToConHook(), MagnetTrimHook()])
    summary = await svc.create_snapshot(NewSnapshotDTO(title="m"))

    io.writes.clear()
    result = await svc.restore_snapshot(summary.id)
    assert io.writes == {"QUAD:LI21:201:BDES": 3.0}
    assert result.failures[0]["phase"] == "WriteToConHook"
```

A read the fake has no value for comes back disconnected, which is how
tests cover PVs that don't answer.

## Trying Squirrel by hand

With the default `simulation` EPICS backend, Canopy needs no IOC: every PV
name reads as a value that changes over time, and writes always succeed.
That's enough to capture and restore snapshots through the API or the
Squirrel app. With the development Compose stack, run the database
migrations once (see [Docker Compose](../deployment/docker.md#database-migrations)),
then add PVs and capture snapshots as usual.

A PV name's ending sets the kind of value it reads as, for example
`:WAVEFORM` or `:WF` for a waveform, `:ENUM` for an enum, and `:STR` for a
string. Any other name reads as a number. See
[Backends](../hare/index.md#backends) for the full list.

### Simulating failed writes

To see how a restore reports failures without a real control system, make
some simulated writes fail. Set these in `docker/.env` (or the environment)
and restart the worker, which is what performs restores:

| Variable | Default | Effect |
|----------|---------|--------|
| `CANOPY_EPICS__SIM_PUT_ERROR_RATE` | `0.0` | Fraction of writes that fail at random, from `0.0` (none) to `1.0` (all) |
| `CANOPY_EPICS__SIM_PUT_FAIL_PVS` | empty | Comma-separated PVs whose writes always fail |
| `CANOPY_EPICS__SIM_SEED` | unset | Makes the random failures repeat in the same pattern on every run |

For example, to see a magnet's trim skipped after its `BCON` write fails:

```bash
CANOPY_SQUIRREL__RESTORE_HOOKS=write_to_con,magnet_trim
CANOPY_EPICS__SIM_PUT_FAIL_PVS=QUAD:LI21:201:BCON
```

Restoring a snapshot that includes `QUAD:LI21:201:BDES` then reports that PV
as failed in the `WriteToConHook` phase, and writes nothing to its `CTRL`
PV.

Only writes can be made to fail. Reads in the simulator always succeed, so
to see how a capture handles PVs that don't answer, use a real IOC.

!!! note "The API and the worker simulate separately"
    Each Canopy process runs its own simulator. A value written by a restore
    is held by the worker's simulator, so later captures read it back, but
    live values shown through Hare come from the API's simulator and don't
    change.

### With a soft IOC

To test against real Channel Access or PV Access, run one of the bundled
soft IOCs and switch the backend to `epics`, as described in
[Getting started](../getting_started.md#talking-to-real-epics). The IOCs serve
`TEST:` PVs only, and none of them are magnets, so the built-in restore
hooks have nothing to act on.
