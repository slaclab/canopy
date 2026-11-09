# Squirrel REST API

All endpoints live under `/squirrel/v1`. Reads require a key with the `read`
scope and anything that changes data requires `write`, sent in the `X-API-Key`
header. The key must also be scoped to the `squirrel` module (see
[API keys](../deployment/api-keys.md#module-scoping)). With the API running,
`/squirrel/docs` serves the interactive reference generated from the code.

Snapshots, PVs, tag groups, and tags are identified by UUIDs. An id that is
not a valid UUID is treated like an unknown one and returns `404`.

## Owners

Every PV, meta PV, snapshot and tag group carries `ownerInstance`: the
`CANOPY_INSTANCE` of the instance that created it, which never changes. An
instance writes only rows it owns: editing or deleting another instance's row
answers `403 not_owner`, naming the owner. On a history head, which holds its
arms' rows beside its own, `ownerInstance` also tells apart two PVs with the same
setpoint address, or two tag groups with the same name, from different owners (see
[Row owners](../deployment/index.md#row-owners)).

A new row takes the owner of what it's created in (a PV's facilities, a
snapshot's facility, a tag's group). The creates below also take an optional
`owner` query parameter, for a create with nothing to take the owner from:

| Endpoint | |
|----------|---|
| `POST /squirrel/v1/snapshots` | Capture a snapshot |
| `POST /squirrel/v1/pvs`, `POST /squirrel/v1/pvs/multi` | Create PVs |
| `POST /squirrel/v1/pvs/meta` | Add a meta PV |
| `POST /squirrel/v1/tags`, `POST /squirrel/v1/tags/bulk` | Create a tag group, import tags |

`owner` defaults to this instance. An instance refuses with `403 not_owner` a
create it wouldn't own itself; a history head sends a create naming one of its
arms to that arm (see [Writes on a head](../deployment/history/operations.md#writes-on-a-head)).
A request that links rows of different owners answers `409 owner_mismatch`.

## Background jobs

Capturing and restoring a snapshot run as background jobs in the Canopy
worker. Both endpoints return as soon as the job is queued:

```json
{
  "jobId": "5f0e8a…",
  "statusUrl": "/jobs/v1/5f0e8a…"
}
```

Poll `statusUrl` (a `read` key is enough) until `status` is `completed` or
`failed`:

```
GET /jobs/v1/{jobId}
```

```json
{
  "id": "5f0e8a…",
  "function": "restore_snapshot_task",
  "status": "running",
  "progress": 40,
  "message": "Wrote 500/1250 PVs...",
  "phases": [
    {"name": "Setpoints", "current": 500, "total": 1250, "status": "running", "failures": 0},
    {"name": "WriteToConHook", "current": 0, "total": 310, "status": "pending", "failures": 0}
  ],
  "result": null,
  "error": null,
  "enqueuedAt": "2026-10-29T17:04:12Z",
  "startedAt": null,
  "finishedAt": null
}
```

| Field | Description |
|-------|-------------|
| `status` | `pending` (queued), `running`, `completed`, or `failed` |
| `progress` | Percent complete, 0–100 |
| `message` | Latest progress message |
| `phases` | Restore only: one entry for `Setpoints`, then one per [restore hook](restore_hooks.md). A phase's `status` is `pending`, `running`, `completed`, or `skipped` (nothing to write). `current`/`total` count writes; `failures` counts PVs |
| `result` | Set once `completed`; see [Capturing a snapshot](index.md#capturing-a-snapshot) and [Restoring a snapshot](index.md#restoring-a-snapshot) for its shape |
| `error` | Set once `failed` |
| `enqueuedAt`, `startedAt`, `finishedAt` | When the job was queued, started, and finished. `startedAt` and `finishedAt` are `null` until the job has finished |

A job's status and result are kept for **one hour** after it finishes; after
that, polling it returns `404 job_not_found`.

### Retries and time limits

- If a job fails with an error that looks transient (its message mentions a
  connection, a timeout, or the database), the worker retries it after 10
  seconds, up to **3 attempts** in all. The job stays `running` or `pending`
  in between.
- A retried **capture** starts over from scratch. Nothing from the failed
  attempt is saved, so a retry never leaves a partial snapshot.
- A retried **restore** also starts over from scratch, so it writes **every**
  PV in the restore again, including those the failed attempt had already
  written, and runs the restore hooks again.
- A single PV that can't be read or written does not fail the job, so it
  never causes a retry; it is recorded in the snapshot or in the restore's
  `failures` instead.
- A job that runs for more than **10 minutes** is stopped and reported as
  `failed`.

---

## Snapshots

### List snapshots

```
GET /squirrel/v1/snapshots
```

Lists snapshot summaries, newest first, one page at a time.

| Parameter | In | Type | Required | Description |
|-----------|----|------|----------|-------------|
| `title` | query | string | no | Case-insensitive substring of the title |
| `tags` | query | string, repeatable | no | Tag ids; matches snapshots holding a PV with any of these tags |
| `start` | query | string | no | Created at or after this ISO-8601 date-time. A value without a UTC offset is read as UTC; a bare date is rejected |
| `end` | query | string | no | Created at or before this ISO-8601 date-time, same rules as `start` |
| `pageSize` | query | int | no | 1–1000, default 100 |
| `continuationToken` | query | string | no | The previous page's `continuationToken`; omit for the first page |

Send the same filters with every page. Paging continues from where the
previous page ended, so snapshots created or deleted while you page never
make you see one twice or skip one.

??? example "Response"
    ```json
    {
      "results": [
        {
          "id": "8d0c…",
          "title": "Before ROD",
          "description": "Baseline before the optics change",
          "ownerInstance": "arm1",
          "createdDate": "2026-10-29T17:04:12Z",
          "createdBy": null,
          "pvCount": 1250,
          "facility": "injector",
          "retain": false,
          "importedFrom": null,
          "delivery": null,
          "metaPvs": [
            {
              "pvId": "9b2e…",
              "pvName": "SIM:BEND:01:BDES",
              "value": 10.0,
              "status": 0,
              "severity": 0,
              "timestamp": "2026-10-29T17:04:12Z",
              "units": null,
              "precision": null,
              "upperCtrlLimit": null,
              "lowerCtrlLimit": null
            }
          ]
        }
      ],
      "continuationToken": "eyJ0Ijo…",
      "totalCount": 42
    }
    ```

`continuationToken` is `null` on the last page. `totalCount` counts every
snapshot matching the filters. `metaPvs` holds each active
[meta PV](#meta-pvs)'s saved value. `facility` is the snapshot's facility slug,
`retain` keeps it from being pruned (see [Update a snapshot](#update-a-snapshot)),
and `importedFrom` is the id of the snapshot an [import](#import-a-snapshot)
brought back. Deleted snapshots are never listed.

**Pending and failed snapshots.** On a history head, a snapshot taken for an arm
that couldn't be reached waits in the head's outbox until the arm saves it (see
[When an arm is unreachable](../deployment/history/operations.md#when-an-arm-is-unreachable)).
Until then it is listed and served like any other, under the same id, with
`delivery` set; `delivery` is `null` for every stored snapshot:

```json
"delivery": {
  "state": "pending",
  "arm": "arm2",
  "queuedAt": "2026-11-06T14:02:11Z",
  "attempts": 3,
  "reason": "arm2 could not be reached"
}
```

`state` is `pending` while it waits, or once the arm has saved it and it hasn't
replicated back yet (`reason` "Saved on arm2; waiting for replication"), and
`failed` when the arm refused it, with the arm's reason. Compare and export work on
such a snapshot. Editing or deleting it answers `409 snapshot_pending`, and so does
restoring it, except where the head has `CANOPY_MACHINE_ACCESS=write`, which
restores it from its bundle.

| Status | Code | Condition |
|--------|------|-----------|
| 400 | `bad_request` | `start` is after `end`, or `continuationToken` is malformed |
| 422 | — | `start` or `end` is not a date-time with a time of day |

### Capture a snapshot

```
POST /squirrel/v1/snapshots
```

Queues a capture and returns `{jobId, statusUrl}` (see
[Background jobs](#background-jobs)). Requires `write`. With a `facility` it
captures the PVs assigned to that facility or one below it; without one, every PV
this instance owns.

| Parameter | In | Type | Required | Description |
|-----------|----|------|----------|-------------|
| `owner` | query | string | no | The instance that will own the snapshot when there is no `facility` (see [Owners](#owners)) |

??? example "Request body"
    ```json
    {
      "title": "Before ROD",
      "description": "Baseline before the optics change",
      "facility": "injector"
    }
    ```

`title` is required (1–255 characters); `description` and `facility` (a slug)
are optional.

The finished job's `result` is `{"type": "snapshot", "id": "<snapshot id>"}`.
On a history head, a snapshot of an arm's facility is read here and saved on that
arm under the same id: the result adds `"arm"`, and `skipped` lists the values the
arm left out. When the arm can't take it, the job still completes, with the
snapshot waiting in the head's outbox: its message is "Captured; waiting for
*arm*" and the result adds `"delivery": {"state": "pending", ...}`.

| Status | Code | Condition |
|--------|------|-----------|
| 403 | `not_owner` | The facility, or `owner`, is an instance this one can't save for |
| 404 | `facility_not_found` | No facility with this slug |
| 503 | `outbox_full` | On a history head: the arm's outbox is full, and the snapshot could only queue |

### Get a snapshot

```
GET /squirrel/v1/snapshots/{snapshotId}
```

Returns the snapshot summary plus its saved values, sorted by PV address.

| Parameter | In | Type | Required | Description |
|-----------|----|------|----------|-------------|
| `limit` | query | int | no | Return at most this many values; omit for all |
| `offset` | query | int | no | Skip this many values first, default 0 |

??? example "Response"
    ```json
    {
      "id": "8d0c…",
      "title": "Before ROD",
      "description": "Baseline before the optics change",
      "createdDate": "2026-10-29T17:04:12Z",
      "createdBy": null,
      "pvCount": 1250,
      "metaPvs": [],
      "pvValues": [
        {
          "pvId": "41a7…",
          "pvName": "SIM:QUAD:01:BDES",
          "device": "SIM:QUAD:01",
          "setpointName": "SIM:QUAD:01:BDES",
          "readbackName": "SIM:QUAD:01:BACT",
          "setpointValue": {"value": 12.5, "status": 0, "severity": 0, "timestamp": "2026-10-29T17:04:12Z", "units": null, "precision": null, "upperCtrlLimit": null, "lowerCtrlLimit": null},
          "readbackValue": {"value": 12.49, "status": 0, "severity": 0, "timestamp": "2026-10-29T17:04:12Z", "units": null, "precision": null, "upperCtrlLimit": null, "lowerCtrlLimit": null},
          "status": 0,
          "severity": 0,
          "timestamp": "2026-10-29T17:04:12Z",
          "tags": [{"id": "c3f1…", "name": "S01", "groupName": "Area"}]
        }
      ]
    }
    ```

- `pvCount` is the total number of saved values, whatever `limit` is.
- `setpointValue` or `readbackValue` is `null` when the PV has no such address
  or its read failed during capture.
- The row-level `status` and `severity` are the setpoint's, or the readback's
  when there is no setpoint reading: for a readback-only PV, or when the
  setpoint's read failed during capture.
- `units`, `precision`, and the control limits are not captured yet and are
  always `null`.
- On a history head, a snapshot still waiting for its arm is served from the
  bundle in the head's outbox, with [`delivery`](#list-snapshots) set.

| Status | Code | Condition |
|--------|------|-----------|
| 404 | `snapshot_not_found` | No snapshot with this id |

### Update a snapshot

```
PATCH /squirrel/v1/snapshots/{snapshotId}
PUT /squirrel/v1/snapshots/{snapshotId}
```

Changes a snapshot's `title`, `description` and `retain`; saved values never
change. Both methods apply only the fields sent. Returns the updated summary.
Requires `write`. A snapshot with `retain` set is never pruned: set it on a
snapshot a service outside Canopy refers to (see
[Turn pruning on](../deployment/history/operations.md#turn-pruning-on)).

??? example "Request body"
    ```json
    {
      "title": "Before ROD (good)"
    }
    ```

| Status | Code | Condition |
|--------|------|-----------|
| 403 | `not_owner` | Another instance owns the snapshot |
| 404 | `snapshot_not_found` | No snapshot with this id |
| 409 | `snapshot_pending` | On a history head: the snapshot is still waiting for its arm |

### Delete a snapshot

```
DELETE /squirrel/v1/snapshots/{snapshotId}
```

Deletes the snapshot: it is no longer listed or served, and every read answers
`404`, but its row and saved values are kept, and the delete reaches a history
copy like any change. Returns `true`. Requires `write`.

| Status | Code | Condition |
|--------|------|-----------|
| 403 | `not_owner` | Another instance owns the snapshot |
| 404 | `snapshot_not_found` | No snapshot with this id |
| 409 | `snapshot_pending` | On a history head: the snapshot is still waiting for its arm |

### Export a snapshot

```
GET /squirrel/v1/snapshots/{snapshotId}/export
```

Returns the snapshot with every saved value as a bundle, for
[import](#import-a-snapshot) on another instance. Each value carries its PV's id
and its addresses as this instance holds them. Requires `read`.

??? example "Response"
    ```json
    {
      "format": "canopy.squirrel.snapshot-bundle",
      "version": 1,
      "sourceInstance": "head",
      "id": "8d0c…",
      "title": "Before ROD",
      "description": null,
      "createdDate": "2026-10-29T17:04:12Z",
      "createdBy": "operator@head",
      "facility": "injector",
      "values": [
        {
          "pvId": "41a7…",
          "pvName": "SIM:QUAD:01:BDES",
          "setpointAddress": "SIM:QUAD:01:BDES",
          "readbackAddress": "SIM:QUAD:01:BACT",
          "configAddress": null,
          "setpointValue": {"value": 12.5},
          "readbackValue": {"value": 12.49},
          "status": 0,
          "severity": 0,
          "timestamp": "2026-10-29T17:04:12Z"
        }
      ]
    }
    ```

| Status | Code | Condition |
|--------|------|-----------|
| 404 | `snapshot_not_found` | No snapshot with this id |

### Import a snapshot

```
POST /squirrel/v1/snapshots/import
```

Saves a snapshot from a bundle that [export](#export-a-snapshot) produced.
Requires `write`. Nothing is read from or written to EPICS.

| Parameter | In | Type | Required | Description |
|-----------|----|------|----------|-------------|
| `mode` | query | string | no | `history` (default) or `captured` |
| `strict` | query | bool | no | Refuse, saving nothing, when any value would be left out. Default `false` |

- **`history`** brings back a snapshot exported from a history copy, typically to
  restore one an arm has pruned. The new snapshot has a new id, today's time, a
  "Restored from history" title and `importedFrom` set to the bundle's id;
  `createdBy` stays the original's.
- **`captured`** is how a history head saves a snapshot it took for this
  instance. Only a service key may send it, naming in `X-Canopy-On-Behalf-Of` the
  user it was taken for, who becomes its author. It keeps the bundle's id, time,
  title and description, and is idempotent on the id: sending it again answers
  `created: false` and saves nothing. The answer always carries
  `X-Canopy-Commit-LSN`.

Either way, a value is kept only when its PV exists here, the key could restore
it, and, for a bundle with a facility, the PV is assigned to that facility or one
below it. The new snapshot belongs to the bundle's facility, which must be this
instance's.

??? example "Response"
    ```json
    {
      "snapshot": {"id": "f31a…", "title": "Restored from history: Before ROD", "importedFrom": "8d0c…"},
      "skipped": [{"pvId": "77d0…", "pvName": "SIM:QUAD:02:BDES"}],
      "created": true
    }
    ```

`snapshot` is the saved snapshot's summary (shortened above), and `skipped` lists
the values left out.

| Status | Code | Condition |
|--------|------|-----------|
| 400 | `nothing_to_import` | None of the bundle's values could be imported |
| 403 | `forwarded_write_refused` | `mode=captured` from a key that isn't a service key, or without `X-Canopy-On-Behalf-Of` |
| 403 | `insufficient_facility_access` | The key is limited to some facilities, and the bundle has no facility or one outside them |
| 403 | `not_owner` | Another instance owns the bundle's facility |
| 404 | `facility_not_found` | No facility with the bundle's slug |
| 409 | `snapshot_id_conflict` | `mode=captured`: a snapshot with this id exists with a different time or facility |
| 409 | `import_would_skip` | `strict=true` and some values would be left out; the detail names the PVs |
| 422 | — | A malformed bundle, or one with a PV twice |

### Restore a snapshot

```
POST /squirrel/v1/snapshots/{snapshotId}/restore
```

Queues a restore of the snapshot's saved setpoints and returns
`{jobId, statusUrl}` (see [Background jobs](#background-jobs)). Requires
`write`. See [Restoring a snapshot](index.md#restoring-a-snapshot) for what is
written, in what order, and the result's shape.

The body is optional. To restore only some PVs, list their ids in `pvIds`.

??? example "Request body"
    ```json
    {
      "pvIds": ["41a7…", "9b2e…"]
    }
    ```

Without a body, the whole snapshot is restored. A [deleted](#delete-a-pv)
PV's saved value is never written.

| Status | Code | Condition |
|--------|------|-----------|
| 403 | `machine_read_only` | This instance has `CANOPY_MACHINE_ACCESS=read`, and the restore can't go to the snapshot's arm |
| 403 | `not_owner` | On a history head with `read`: the snapshot belongs to an arm the head doesn't forward to |
| 404 | `snapshot_not_found` | No snapshot with this id |
| 409 | `snapshot_pending` | On a history head with `read`: the snapshot is still waiting for its arm |

### Compare two snapshots

```
GET /squirrel/v1/snapshots/{snapshotId1}/compare/{snapshotId2}
```

Compares two snapshots under each PV's tolerances and lists the PVs that
differ. See [Comparing snapshots](index.md#comparing-snapshots) for the rules.

??? example "Response"
    ```json
    {
      "snapshot1Id": "8d0c…",
      "snapshot2Id": "77aa…",
      "differences": [
        {
          "pvId": "41a7…",
          "pvName": "SIM:QUAD:01:BDES",
          "value1": 12.5,
          "value2": 12.9,
          "withinTolerance": false
        }
      ],
      "matchCount": 1249,
      "differenceCount": 1
    }
    ```

A difference shows the setpoints, or the readbacks when only the readbacks
differ.

| Status | Code | Condition |
|--------|------|-----------|
| 404 | `snapshot_not_found` | Either snapshot does not exist |

---

## PVs

Every endpoint that returns a PV uses the same shape.

??? example "Example PV"
    ```json
    {
      "id": "41a7…",
      "setpointAddress": "SIM:QUAD:01:BDES",
      "readbackAddress": "SIM:QUAD:01:BACT",
      "configAddress": null,
      "device": "SIM:QUAD:01",
      "description": "Sector 1 quad field",
      "absTolerance": 0.01,
      "relTolerance": 0.0,
      "tags": [{"id": "c3f1…", "name": "S01", "description": null}],
      "createdDate": "2026-10-29T17:04:12Z",
      "lastModifiedDate": "2026-10-29T17:04:12Z",
      "createdBy": null,
      "lastModifiedBy": null
    }
    ```

A text search matches a case-insensitive substring of any address, the
device, or the description. The text is used as a SQL `LIKE` pattern, so
`_` matches any one character and `%` any run of characters: `QUAD_` also
finds `SIM:QUAD:01:BDES`. `*` and `?` are matched literally.

### Search PVs (paged)

```
GET /squirrel/v1/pvs/paged
```

| Parameter | In | Type | Required | Description |
|-----------|----|------|----------|-------------|
| `pvName` | query | string | no | Text search |
| `tagFilters` | query | string | no | JSON object of tag-group id → tag ids, e.g. `{"a1d4…": ["c3f1…"]}`. A PV matches if it has at least one of the listed tags from **every** listed group |
| `pageSize` | query | int | no | 1–1000, default 100 |
| `continuationToken` | query | string | no | The previous page's `continuationToken`; omit for the first page |

Returns `{results, continuationToken, totalCount}`, like
[List snapshots](#list-snapshots).

| Status | Code | Condition |
|--------|------|-----------|
| 400 | `bad_request` | `tagFilters` is not valid JSON |

### Search PVs (offset)

```
GET /squirrel/v1/pvs/search
```

| Parameter | In | Type | Required | Description |
|-----------|----|------|----------|-------------|
| `q` | query | string | no | Text search |
| `devices` | query | string, repeatable | no | Exact device names |
| `tags` | query | string, repeatable | no | Tag ids; matches PVs with any of them |
| `limit` | query | int | no | 1–1000, default 100 |
| `offset` | query | int | no | Default 0 |

Returns `{results, totalCount, limit, offset}`.

### Search PVs (unpaged)

```
GET /squirrel/v1/pvs?pvName=…
```

Returns a plain list of up to 1000 matching PVs. Kept for older clients;
prefer the paged search.

### List devices

```
GET /squirrel/v1/pvs/devices
```

Returns every distinct device name, sorted, as a list of strings.

### Create a PV

```
POST /squirrel/v1/pvs
```

Requires `write`. Returns the created PV.

??? example "Request body"
    ```json
    {
      "setpointAddress": "SIM:QUAD:01:BDES",
      "readbackAddress": "SIM:QUAD:01:BACT",
      "device": "SIM:QUAD:01",
      "description": "Sector 1 quad field",
      "absTolerance": 0.01,
      "relTolerance": 0.0,
      "tags": ["c3f1…"]
    }
    ```

At least one of `setpointAddress`, `readbackAddress`, and `configAddress` is
required; the rest are optional. Tolerances default to `0`, which means
values must match exactly. `tags` takes tag ids.

A PV is identified by its first address in the order setpoint, readback,
config. Two PVs with the same identifying address are duplicates. If a
[deleted](#delete-a-pv) PV has the identifying address, that PV comes back,
with its id and created date; everything else comes from the request.

| Status | Code | Condition |
|--------|------|-----------|
| 400 | `bad_request` | A tag id does not exist |
| 409 | `pv_already_exists` | A PV that isn't deleted has the same identifying address |
| 422 | — | No address given |

### Create PVs in bulk

```
POST /squirrel/v1/pvs/multi
```

Takes a list of PVs shaped like [Create a PV](#create-a-pv) and returns the
created PVs in the same order. Requires `write`. The import is all-or-nothing:
if any row is rejected, no PV is created.

Unlike [Create a PV](#create-a-pv), a tag id that doesn't exist isn't an
error here: it is ignored, and the PV is created without that tag. As there,
a row with a deleted PV's identifying address brings that PV back.

| Status | Code | Condition |
|--------|------|-----------|
| 400 | `bad_request` | Two rows share an identifying address |
| 409 | `pv_already_exists` | A row's identifying address belongs to a PV that isn't deleted |
| 422 | — | A row has no address |

### Update a PV

```
PUT /squirrel/v1/pvs/{pvId}
```

Changes `device`, `description`, `absTolerance`, `relTolerance`, and/or
`tags`. Omitted fields are left as they are, and `tags` replaces the whole
list. Addresses cannot be changed; create a new PV instead. Returns the
updated PV. Requires `write`.

| Status | Code | Condition |
|--------|------|-----------|
| 400 | `bad_request` | A tag id does not exist |
| 404 | `pv_not_found` | No PV with this id |

### Delete a PV

```
DELETE /squirrel/v1/pvs/{pvId}
```

Deletes the PV. It leaves the searches, and getting or updating it returns
`404`. Its saved values stay in past snapshots, which still show them with
the PV's addresses, tags, and tolerances, but restoring a snapshot never
writes a deleted PV. Registering its identifying address again brings it
back (see [Create a PV](#create-a-pv)). Returns `true`. Requires `write`. A
meta PV must be removed as a meta PV first.

| Status | Code | Condition |
|--------|------|-----------|
| 400 | `bad_request` | The PV is still a meta PV |
| 404 | `pv_not_found` | No PV with this id |

---

## Meta PVs

A meta PV is an ordinary PV marked to be shown alongside every snapshot in a
list. Meta PVs are identified by their PV's id.

### Add a meta PV

```
POST /squirrel/v1/pvs/meta
```

Requires `write`.

??? example "Request body"
    ```json
    {
      "pvName": "SIM:BEND:01:BDES",
      "displayName": "DMP Energy"
    }
    ```

`pvName` can be any of the PV's addresses. `displayName` is optional, up to 32
characters; only the database enforces the limit, so a longer name fails with a
`500` instead of a validation error. New meta PVs are active.

??? example "Response"
    ```json
    {
      "pvId": "9b2e…",
      "displayName": "DMP Energy",
      "isActive": true
    }
    ```

| Status | Code | Condition |
|--------|------|-----------|
| 404 | `pv_not_found` | No PV has this address |
| 409 | `pv_already_exists` | The PV is already a meta PV |

### List meta PVs

```
GET /squirrel/v1/pvs/meta
```

| Parameter | In | Type | Required | Description |
|-----------|----|------|----------|-------------|
| `isActive` | query | bool | no | Only active (`true`) or inactive (`false`) meta PVs |

Returns a list of `{pvId, displayName, isActive, pvName}`, where `pvName` is
the setpoint address, or the readback for a readback-only PV.

### Get a meta PV

```
GET /squirrel/v1/pvs/meta/{pvId}
```

Returns `{pvId, displayName, isActive}`.

| Status | Code | Condition |
|--------|------|-----------|
| 404 | `pv_not_found` | `pvId` is not a valid id |
| 404 | `meta_pv_not_found` | The PV is not a meta PV |

### Update a meta PV

```
PATCH /squirrel/v1/pvs/meta/{pvId}
```

Changes `displayName` and/or `active`; omitted fields are left as they are.
The same 32-character limit applies to `displayName`. An inactive meta PV
keeps its display name but is left out of snapshot lists.
Returns `{pvId, displayName, isActive}`. Requires `write`.

??? example "Request body"
    ```json
    {
      "active": false
    }
    ```

| Status | Code | Condition |
|--------|------|-----------|
| 404 | `pv_not_found` | `pvId` is not a valid id |
| 404 | `meta_pv_not_found` | The PV is not a meta PV |

### Remove a meta PV

```
DELETE /squirrel/v1/pvs/meta/{pvId}
```

Turns the meta PV back into an ordinary PV; the PV itself is kept. Returns
`204 No Content`. Requires `write`.

| Status | Code | Condition |
|--------|------|-----------|
| 404 | `pv_not_found` | `pvId` is not a valid id |
| 404 | `meta_pv_not_found` | The PV is not a meta PV |

---

## Tags

Tags belong to tag groups. Group names are unique, and tag names are unique
within a group.

Endpoints that return a full tag group, with its tags, use the same shape.

??? example "Example tag group"
    ```json
    {
      "id": "a1d4…",
      "name": "Area",
      "description": "Machine area",
      "tags": [{"id": "c3f1…", "name": "S01", "description": null}],
      "createdDate": "2026-10-29T17:04:12Z",
      "lastModifiedDate": "2026-10-29T17:04:12Z"
    }
    ```

### List tag groups

```
GET /squirrel/v1/tags
```

Returns a list of `{id, name, description, tagCount}`, without the tags
themselves.

### Get a tag group

```
GET /squirrel/v1/tags/{groupId}
```

Returns the full group, inside a one-element list.

| Status | Code | Condition |
|--------|------|-----------|
| 404 | `tag_group_not_found` | No group with this id |

### Create a tag group

```
POST /squirrel/v1/tags
```

Requires `write`. Body `{"name": "Area", "description": "Machine area"}`;
`description` is optional. Returns the full group.

| Status | Code | Condition |
|--------|------|-----------|
| 409 | `tag_group_already_exists` | A group with this name exists |

### Update a tag group

```
PUT /squirrel/v1/tags/{groupId}
```

Changes `name` and/or `description`. Returns the full group. Requires
`write`.

| Status | Code | Condition |
|--------|------|-----------|
| 404 | `tag_group_not_found` | No group with this id |
| 409 | `tag_group_already_exists` | Another group has the new name |

### Delete a tag group

```
DELETE /squirrel/v1/tags/{groupId}
```

Deletes the group and all its tags, removing them from every PV. Returns
`true`. Requires `write`. A `force` parameter is accepted but has no effect
yet.

| Status | Code | Condition |
|--------|------|-----------|
| 404 | `tag_group_not_found` | No group with this id |

### Add a tag

```
POST /squirrel/v1/tags/{groupId}/tags
```

Requires `write`. Body `{"name": "S01", "description": null}`.

| Parameter | In | Type | Required | Description |
|-----------|----|------|----------|-------------|
| `skip_duplicates` | query | bool | no | If the tag already exists, succeed without changing anything instead of returning `409` |

Returns `{group, wasCreated}`: the full group, and whether a tag was added.

| Status | Code | Condition |
|--------|------|-----------|
| 404 | `tag_group_not_found` | No group with this id |
| 409 | `tag_already_exists` | The group already has this tag, and `skip_duplicates` is not set |

### Update a tag

```
PUT /squirrel/v1/tags/{groupId}/tags/{tagId}
```

Changes `name` and/or `description`. Returns the full group. Requires
`write`.

| Status | Code | Condition |
|--------|------|-----------|
| 404 | `tag_not_found` | No such tag in this group |
| 409 | `tag_already_exists` | Another tag in the group has the new name |

### Remove a tag

```
DELETE /squirrel/v1/tags/{groupId}/tags/{tagId}
```

Deletes the tag, removing it from every PV. Returns the full group. Requires
`write`.

| Status | Code | Condition |
|--------|------|-----------|
| 404 | `tag_not_found` | No such tag in this group |

### Import tags in bulk

```
POST /squirrel/v1/tags/bulk
```

Creates any missing groups and tags. Requires `write`.

??? example "Request body"
    ```json
    {
      "groups": {
        "Area": ["S01", "S02"],
        "Subsystem": ["Magnets"]
      }
    }
    ```

Existing groups are reused. Tags that already exist, and tags that can't be
added, are skipped with a warning instead of failing the import.

??? example "Response"
    ```json
    {
      "groupsCreated": 1,
      "tagsCreated": 2,
      "tagsSkipped": 1,
      "warnings": ["Tag 'S01' already exists in group 'Area'"]
    }
    ```

---

## Health

### Watchdog

```
GET /squirrel/v1/health/watchdog
```

Liveness heartbeat; needs no API key and touches no database.

??? example "Response"
    ```json
    {
      "module": "squirrel",
      "version": "1.2.0",
      "uptimeS": 3605.2,
      "nowTsNs": 1793293452000000000
    }
    ```

### Restore hooks

```
GET /squirrel/v1/health/restore-hooks
```

Reports which [restore hooks](restore_hooks.md) the worker and the API each
loaded, without running a restore. See
[Checking the configuration](restore_hooks.md#checking-the-configuration).

---

## Error codes

Errors use Canopy's standard body, `{"code": "...", "detail": "..."}`. A
request body or query parameter of the wrong shape instead gets `422` with
FastAPI's validation body, which lists each problem under `detail`.

| Code | HTTP | Meaning |
|------|------|---------|
| `invalid_api_key` | 401 | Missing or unknown API key |
| `insufficient_read_access` / `insufficient_write_access` | 403 | The key lacks the required scope |
| `insufficient_module_access` | 403 | The key is not scoped to `squirrel` |
| `bad_request` | 400 | See the endpoint |
| `snapshot_not_found` | 404 | No snapshot with this id |
| `pv_not_found` | 404 | No PV with this id or address |
| `meta_pv_not_found` | 404 | The PV is not a meta PV |
| `tag_group_not_found` | 404 | No tag group with this id |
| `tag_not_found` | 404 | No such tag in the group |
| `pv_already_exists` | 409 | The PV, or the meta PV, already exists |
| `tag_group_already_exists` | 409 | A group with this name exists |
| `tag_already_exists` | 409 | The group already has a tag with this name |
| `job_not_found` | 404 | From `/jobs/v1/{jobId}`: no such job, or its result has expired |
| `not_owner` | 403 | The row, or the facility or group a create goes into, belongs to another instance; the detail names it. On a history head, a write to a read-only arm's row. See [Row owners](../deployment/index.md#row-owners) |
| `owner_mismatch` | 409 | The request links rows of different owners, such as a PV with another owner's tag or facility |
| `facility_not_found` | 404 | No facility with this slug |
| `facility_slug_conflict` | 409 | A facility with this slug already exists, under any owner |
| `machine_read_only` | 403 | This instance has `CANOPY_MACHINE_ACCESS=read`: no restores or PV writes here |
| `forwarded_write_refused` | 403 | `X-Canopy-On-Behalf-Of` from a key that isn't a service key, or on an instance without `CANOPY_ACCEPT_FORWARDED_WRITES` |
| `nothing_to_import` | 400 | An import bundle with no value this key could import |
| `snapshot_id_conflict` | 409 | A captured import's id exists with a different time or facility |
| `import_would_skip` | 409 | A strict captured import would leave out values; the detail names the PVs |
| `snapshot_pending` | 409 | On a history head: the snapshot is still waiting for its arm, so it can't be edited, deleted, or restored with machine access `read` |
| `outbox_full` | 503 | On a history head: the arm's snapshot outbox is at its cap |
| `archived_immutable` | 409 | On a history head: the arm no longer holds this snapshot (it was pruned there) |
| `primary_unavailable` | 502, 504 | On a history head: the arm couldn't be reached (502, nothing sent) or didn't answer in time (504, the write may have landed) |
| `service_key_refused` | 502 | On a history head: the arm refused the head's service key for it, not the caller's key. See [When an arm is unreachable](../deployment/history/operations.md#when-an-arm-is-unreachable) |
