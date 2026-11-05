# Import snapshots from NDJSON

`POST /squirrel/v1/snapshots` enqueues a *live* capture — the worker reads every registered PV from EPICS and persists what it finds. That is the right behaviour for taking a snapshot, but it means the REST API cannot load a snapshot that was captured elsewhere: there is nowhere to put historical values. Migrating an archive out of react-squirrel-backend (which exposed `POST /v1/import/snapshots` for this) therefore needs a different route.

`scripts/import_snapshots.py` is that route. It reads NDJSON and writes the `snapshot` and `snapshot_value` rows directly, reusing `canopy.squirrel.services.bulk_insert` so imported rows land exactly as a live capture's would — PostgreSQL `COPY`, pre-serialized JSONB, one transaction per file.

!!! note "PVs and tags are not created"
    The script matches values against the **existing** PV registry and never inserts PVs, tags, or tag groups. Register those first (`POST /squirrel/v1/pvs/multi` and `POST /squirrel/v1/tags/bulk`), or the values will be skipped for want of a `pv_id`.

## Input format

One JSON object per line. Blank lines are ignored, and a line that fails to parse is logged and skipped rather than aborting the run.

```json
{
  "title": "NC last SCORE before downtime 1.4mJ MFX",
  "description": "energy HXR: 9.77 GeV, peak current HXR: 3200.0 A, ...",
  "created_at": "2026-07-20T06:17:24",
  "values": [
    {
      "setpoint_address": "QUAD:IN20:831:EDES",
      "readback_address": "QUAD:IN20:831:EACT",
      "setpoint_value": { "value": "6.0000000E-3" },
      "readback_value": { "value": "6.0000000E-3" },
      "device": null,
      "tags": { "Region": "Gun to TD11" }
    }
  ]
}
```

| Field                       | Required | Notes                                                                                             |
| --------------------------- | -------- | ------------------------------------------------------------------------------------------------- |
| `title`                     | yes      | A line with no title is skipped.                                                                   |
| `description`               | no       | Stored verbatim.                                                                                   |
| `created_at`                | no       | ISO 8601. Naive timestamps are read as UTC; absent or unparseable falls back to now.                |
| `values[].setpoint_address` | yes      | The match key against `squirrel.pv.setpoint_address`. Entries without one are skipped.              |
| `values[].setpoint_value`   | no       | JSONB blob. `status`, `severity`, and `timestamp` are lifted out of it when the source recorded them. |
| `values[].readback_value`   | no       | JSONB blob.                                                                                        |

`device` and `tags` are read by the PV-registration endpoints, not by this script; it ignores them.

The imported snapshot's `created_at` is set explicitly, so it keeps its original capture time instead of the date it was imported. Imported snapshots carry `created_by = "Import"`, which distinguishes them from live captures in `GET /squirrel/v1/snapshots`.

## Run it

The script imports `canopy`, so run it where the package and `CANOPY_DATABASE_URL` are already set up — the API container. Mount the NDJSON somewhere the container can read it:

```bash
docker exec -i canopy-api \
  uv run python scripts/import_snapshots.py /data/snapshot.ndjson
```

Pass several files to import them in one go; each is committed as its own transaction, so a failure part-way through a batch leaves earlier files intact.

```bash
docker exec -i canopy-api \
  uv run python scripts/import_snapshots.py /data/*.ndjson
```

!!! warning "`scripts/` is not in the dev image"
    `docker/Dockerfile.dev` copies `src`, `tests`, and `pyproject.toml`; the compose `api` service mounts `../src` and `../tests` only. Neither `scripts/` nor the NDJSON files are visible inside the container by default, so add the mounts for the run — the same constraint applies to `alembic.ini` and `migrations/` when running migrations:

    ```bash
    docker run --rm --network canopy_default \
      -v "$PWD/src:/app/src" \
      -v "$PWD/scripts:/app/scripts" \
      -v "/path/to/snapshot.ndjson:/data/snapshot.ndjson:ro" \
      -e CANOPY_DATABASE_URL='postgresql+asyncpg://canopy:canopy@postgres:5432/canopy' \
      -e CANOPY_REDIS_URL='redis://:canopy@redis:6379/0' \
      -w /app canopy-api \
      uv run python scripts/import_snapshots.py /data/snapshot.ndjson
    ```

### Check before you write

`--dry-run` parses every line, resolves each value against the PV registry, reports the same counts as a real run, then rolls back. Use it to confirm the archive's addresses actually match the registry before committing 16k rows:

```bash
docker exec -i canopy-api \
  uv run python scripts/import_snapshots.py --dry-run /data/snapshot.ndjson
```

```text
snapshot.ndjson: 36534 registered PVs available for matching
snapshot.ndjson:1 'NC last SCORE before downtime 1.4mJ MFX' — 16253 values (3143 unmatched so far)
DRY RUN (rolled back): 1 snapshot(s), 16253 value(s), 3143 value(s) skipped (no/unregistered setpoint address)
```

## Skipped values

A value is skipped when it has no `setpoint_address`, or when that address is not in the PV registry. Both are counted together in the summary line. A large skip count is normal for real archives — readback-only entries have no setpoint address at all — but if it approaches the total, the PV registry is probably not populated for that archive. Compare against the registry directly:

```sql
SELECT count(*) FROM squirrel.pv WHERE setpoint_address IS NOT NULL;
```

## Verify

```bash
curl -s -H "X-API-Key: $CANOPY_API_KEY" \
  http://localhost:8000/squirrel/v1/snapshots
```

Each imported snapshot reports a `pvCount` matching the value count from the import summary, `createdBy: "Import"`, and the original `createdDate`.
