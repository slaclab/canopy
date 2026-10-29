# Squirrel REST API

All endpoints live under `/squirrel/v1`. Reads require a key with the `read`
scope and anything that changes data requires `write`, sent in the `X-API-Key`
header. The key must also be scoped to the `squirrel` module (see
[API keys](../deployment/api-keys.md#module-scoping)). With the API running,
`/squirrel/docs` serves the interactive reference generated from the code.

Snapshots, PVs, tag groups, and tags are identified by UUIDs. An id that is
not a valid UUID is treated like an unknown one and returns `404`.

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
  "startedAt": "2026-10-29T17:04:12Z",
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
          "createdDate": "2026-10-29T17:04:12Z",
          "createdBy": null,
          "pvCount": 1250,
          "metaPvs": [
            {
              "pvId": "9b2e…",
              "pvName": "BEND:DMPH:400:BDES",
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
[meta PV](#meta-pvs)'s saved value.

| Status | Code | Condition |
|--------|------|-----------|
| 400 | `bad_request` | `start` is after `end`, or `continuationToken` is malformed |
| 422 | — | `start` or `end` is not a date-time with a time of day |

### Capture a snapshot

```
POST /squirrel/v1/snapshots
```

Queues a capture of every PV Squirrel tracks and returns `{jobId, statusUrl}`
(see [Background jobs](#background-jobs)). Requires `write`.

??? example "Request body"
    ```json
    {
      "title": "Before ROD",
      "description": "Baseline before the optics change"
    }
    ```

`title` is required (1–255 characters); `description` is optional.

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
          "pvName": "QUAD:LI21:201:BDES",
          "device": "QUAD:LI21:201",
          "setpointName": "QUAD:LI21:201:BDES",
          "readbackName": "QUAD:LI21:201:BACT",
          "setpointValue": {"value": 12.5, "status": 0, "severity": 0, "timestamp": "2026-10-29T17:04:12Z", "units": null, "precision": null, "upperCtrlLimit": null, "lowerCtrlLimit": null},
          "readbackValue": {"value": 12.49, "status": 0, "severity": 0, "timestamp": "2026-10-29T17:04:12Z", "units": null, "precision": null, "upperCtrlLimit": null, "lowerCtrlLimit": null},
          "status": 0,
          "severity": 0,
          "timestamp": "2026-10-29T17:04:12Z",
          "tags": [{"id": "c3f1…", "name": "LI21", "groupName": "Area"}]
        }
      ]
    }
    ```

- `pvCount` is the total number of saved values, whatever `limit` is.
- `setpointValue` or `readbackValue` is `null` when the PV has no such address
  or its read failed during capture.
- The row-level `status` and `severity` are the setpoint's, or the readback's
  for a readback-only PV.
- `units`, `precision`, and the control limits are not captured yet and are
  always `null`.

| Status | Code | Condition |
|--------|------|-----------|
| 404 | `snapshot_not_found` | No snapshot with this id |

### Update a snapshot

```
PUT /squirrel/v1/snapshots/{snapshotId}
```

Changes a snapshot's title and/or description; saved values never change.
Omitted fields are left as they are. Returns the updated summary. Requires
`write`.

??? example "Request body"
    ```json
    {
      "title": "Before ROD (good)"
    }
    ```

| Status | Code | Condition |
|--------|------|-----------|
| 404 | `snapshot_not_found` | No snapshot with this id |

### Delete a snapshot

```
DELETE /squirrel/v1/snapshots/{snapshotId}
```

Deletes the snapshot and all its saved values. Returns `true`. Requires
`write`.

| Status | Code | Condition |
|--------|------|-----------|
| 404 | `snapshot_not_found` | No snapshot with this id |

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

Without a body, the whole snapshot is restored.

| Status | Code | Condition |
|--------|------|-----------|
| 404 | `snapshot_not_found` | No snapshot with this id |

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
          "pvName": "QUAD:LI21:201:BDES",
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
      "setpointAddress": "QUAD:LI21:201:BDES",
      "readbackAddress": "QUAD:LI21:201:BACT",
      "configAddress": null,
      "device": "QUAD:LI21:201",
      "description": "LI21 quad field",
      "absTolerance": 0.01,
      "relTolerance": 0.0,
      "tags": [{"id": "c3f1…", "name": "LI21", "description": null}],
      "createdDate": "2026-10-29T17:04:12Z",
      "lastModifiedDate": "2026-10-29T17:04:12Z",
      "createdBy": null,
      "lastModifiedBy": null
    }
    ```

A text search matches a case-insensitive substring of any address, the
device, or the description. `*` and `?` are matched literally, not as
wildcards.

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
      "setpointAddress": "QUAD:LI21:201:BDES",
      "readbackAddress": "QUAD:LI21:201:BACT",
      "device": "QUAD:LI21:201",
      "description": "LI21 quad field",
      "absTolerance": 0.01,
      "relTolerance": 0.0,
      "tags": ["c3f1…"]
    }
    ```

At least one of `setpointAddress`, `readbackAddress`, and `configAddress` is
required; the rest are optional. Tolerances default to `0`, which means
values must match exactly. `tags` takes tag ids.

A PV is identified by its first address in the order setpoint, readback,
config. Two PVs with the same identifying address are duplicates.

| Status | Code | Condition |
|--------|------|-----------|
| 400 | `bad_request` | A tag id does not exist |
| 409 | `pv_already_exists` | A PV with the same identifying address exists |
| 422 | — | No address given |

### Create PVs in bulk

```
POST /squirrel/v1/pvs/multi
```

Takes a list of PVs shaped like [Create a PV](#create-a-pv) and returns the
created PVs in the same order. Requires `write`. The import is all-or-nothing:
if any row is rejected, no PV is created.

| Status | Code | Condition |
|--------|------|-----------|
| 400 | `bad_request` | A tag id does not exist, or two rows share an identifying address |
| 409 | `pv_already_exists` | A row's identifying address is already registered |
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

Deletes the PV and its saved values in every snapshot. Returns `true`.
Requires `write`. A meta PV must be removed as a meta PV first.

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
      "pvName": "BEND:DMPH:400:BDES",
      "displayName": "DMP Energy"
    }
    ```

`pvName` can be any of the PV's addresses. `displayName` is optional, up to 32
characters. New meta PVs are active.

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
| 404 | `meta_pv_not_found` | The PV is not a meta PV |

### Update a meta PV

```
PATCH /squirrel/v1/pvs/meta/{pvId}
```

Changes `displayName` and/or `active`; omitted fields are left as they are.
An inactive meta PV keeps its display name but is left out of snapshot lists.
Returns `{pvId, displayName, isActive}`. Requires `write`.

??? example "Request body"
    ```json
    {
      "active": false
    }
    ```

| Status | Code | Condition |
|--------|------|-----------|
| 404 | `meta_pv_not_found` | The PV is not a meta PV |

### Remove a meta PV

```
DELETE /squirrel/v1/pvs/meta/{pvId}
```

Turns the meta PV back into an ordinary PV; the PV itself is kept. Returns
`204 No Content`. Requires `write`.

| Status | Code | Condition |
|--------|------|-----------|
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
      "tags": [{"id": "c3f1…", "name": "LI21", "description": null}],
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

Requires `write`. Body `{"name": "LI21", "description": null}`.

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
        "Area": ["LI21", "LI22"],
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
      "warnings": ["Tag 'LI21' already exists in group 'Area'"]
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
