# Operations

How to run a head and its arms: the order of a release, the hourly jobs, what to watch,
how the head handles writes and snapshots for its arms, prune on an arm, and every
setting. What each database holds is on the [overview](index.md); how to set up each
shape is under [Topologies](topologies.md); repairs are under [Runbooks](runbooks.md).

## Release order

The head always migrates first, so it never lacks a column or table an arm sends.

1. **Head**: `alembic upgrade head` (with `CANOPY_ROLE=mirror`), then `grants`, then
   `refresh`, as the table owner. Then roll out the head's API and worker.
2. **Each arm**: `alembic upgrade head`, then `publish`. Then roll out its API and
   worker.
3. The head's next hourly `refresh` copies any table first published in step 2.

A deploy pipeline can enforce the order: before step 2, compare `alembicHead` from the
head's `GET /v1/instance` with the head in the new image, and stop if the head is
behind.

**A release with `canopy_requires_primary_at`.** A revision that drops a column or
table on the head, or tightens a constraint after a backfill, names the earlier
revision every arm must already run (see "Database Migrations" in
`CONTRIBUTING.md`). Before step 1 of such a release, confirm that each arm runs that
revision or a later one (`alembic current` there), and that the head has caught up:
on each arm, `status` shows the head's slot `active` with `lag_bytes` at or near 0.
Otherwise the head could still be applying rows the migration would reject.

**First deployment.** Migrate the head and run `grants`. Then, for each arm: run
`publish` on it, and `subscribe --arm <arm>` once on the head, with that arm's
`CANOPY_ARMS__<arm>__PUBLISHER_DSN` from a secret. It copies every table the arm
publishes, then streams changes. `status` shows the copy's progress on either side.

**A second arm.** `reseed` works on one arm's rows. That must be in place on the head
before a second arm is connected: a re-seed that reconciled whole tables would delete
the other arms' rows.

## Hourly jobs

On the head, hourly, for each arm: `refresh --arm <arm>`, then `drift-check --arm
<arm>` (with that arm's `CANOPY_ARMS__<arm>__READER_DSN`). `drift-check` exits 0 only
when every table the arm publishes was compared and is in step, 1 when the copy has
drifted, and 2 when it couldn't check every table, so anything but 0 fails the Job.
One Job per arm keeps one arm's failure from hiding another's result; without
`--arm`, one run checks every arm and exits with the worst. For an arm that prunes,
its Job also runs `heartbeat --arm <arm>` last, as described under [Prune](#prune).

## Metrics and alerts

Each side's `/metrics` reports its half of replication: an arm's slot, and the head's
subscription to each arm (labelled `arm`). `/health/ready` checks the database. The
gauges, and the alert rules shipped in `docker/observability`, are listed under
[Replication](../../observability/metrics.md#replication). The lag to watch is each
arm's `canopy_replication_slot_lag_bytes`; the head can't measure its own. A head
that forwards to arms also reports its snapshot outbox per arm, under
[Snapshot outbox](../../observability/metrics.md#snapshot-outbox).

## Drift check

Replication skips an update or delete whose row the head doesn't have, without an
error, so the copy can drift with every gauge green. `drift-check` compares, for each
arm, the rows that arm owns, on the arm and on the head, in each table the arm's
subscription holds. Other arms' rows and the head's own are never compared:

- reference tables: row count and a hash of all rows;
- time-series tables: row counts per UTC day (snapshot values by their snapshot's
  day).

A table that differs is drift only when the comparison is conclusive: the arm's slot
has confirmed the position the arm was read at, the head still differs, and a second
read of the arm matches the first. Otherwise lag or a write landing between the reads
may explain it, so the table is tried again within `--settle` seconds (30 by default)
and then reported as inconclusive (exit 2). Tables still copying, or published but
not subscribed yet, are reported as not checked (also exit 2), and so is an arm that
can't be reached; the other arms are still checked.

Then the head checks its own references, once per run. Apply doesn't fire foreign
keys, so only the head can hold a row whose parent is gone (a snapshot value without
its snapshot, a PV's tag link without its tag). Each such row is put down to the arm
that owns it, through whichever of its parents survives, and is that arm's drift
(exit 1 for that arm only). A row nobody can be found to own is reported under `head`
and fails every run until it is fixed. A reference that is NULL is never flagged.

The JSON report gives `status`, then each arm's report under `arms` (drifting tables
and days) and the head's under `head`. To repair an arm's drift, find its cause, then
disable that arm's subscription (`ALTER SUBSCRIPTION canopy_history_<arm> DISABLE`;
`reseed` refuses while it's enabled) and run `reseed --arm <arm>` with `--since` no
later than the oldest drifting day (reference tables and snapshots are always
reconciled in full). Run `reseed` as a superuser, or as the table owner with `EXECUTE`
on `pg_replication_origin_advance(text, pg_lsn)`; it enables the subscription again
from an exact cut (see [Slot lost](runbooks.md#slot-lost)).

If `publish` on an arm ever reported tables it `cleared`, or `status` showed
`restricted` ones (a row filter or column list on the publication), a head that
subscribed while the restriction was in place holds only part of that arm's rows in
those tables, and `refresh` won't copy them again. Re-seed that arm.

## Writes on a head

A head writes only its own rows, like every instance. A write to an arm's row goes to
that arm, over HTTPS, when the head forwards to it: the arm saves it, and the change
comes back through replication. An arm forwards when the head has both of its forward
settings, `CANOPY_ARMS__<arm>__FORWARD_URL` and `__SERVICE_KEY`. Without them the
arm's rows are read-only on the head, and a write to one is refused with 403
`not_owner`, naming the arm. `GET /v1/instance` shows each arm's `forwards`.

### Where each write goes

`canopy.core.forwarding.routes` names, for every write route, how the head finds the
owner of what it writes, in its own copy. A test fails when a route is neither routed
nor named as local.

| Writes | Owner |
|---|---|
| Snapshot edit, retain and delete; PV edit and delete; meta-PV edit and delete; tag group and tag changes; Magpie config edit and delete; facility edit and delete | The row's |
| Snapshot restore | Here with machine access `write`; otherwise the snapshot's ([Machine access](#machine-access)) |
| Snapshot capture | Taken here; saved here or on the facility's arm ([Snapshots taken on the head](#snapshots-taken-on-the-head)) |
| Snapshot history import | The facility's, from the body |
| PV create and PV import (`/pvs/multi`) | The facilities' in the body, which must share one owner |
| Meta-PV create | The owner of the PV at that address: the head's own first, else the one arm that holds it |
| Facility create | The parent's |
| Tag group create, tag import, Magpie config create, and any create above with no facility or parent | The head's, unless `?owner=` names another instance |
| API keys, Vole buffer evicts, analytics events, e-log entries and attachments, Hare PV writes | Always the head's own: never forwarded |

The creates that take a target from nothing else also take `?owner=<instance>`: a
forwardable arm's name sends the create to that arm, and any other name but the head's
is refused with 403 `not_owner`. A create whose `owner` contradicts its facility or
parent is answered by the head, with `not_owner` or 409 `owner_mismatch`. A facility
create sent to an arm whose slug the head already holds, under any owner, is refused
with 409 `facility_slug_conflict` before it is sent: slugs are unique across every
instance, and the arm's new row would otherwise stop replication from that arm.

A row or facility the head doesn't hold is answered by the head (404). A captured
import (`?mode=captured`) is never forwarded: it is how a head saves a snapshot it took
for an arm, and only a service key sends one, to the arm.

A snapshot an arm has pruned is still on the head. A change to it that the arm answers
with 404 is refused with 409 `archived_immutable`. To bring such a snapshot back to its
arm, export it on the head (`GET /squirrel/v1/snapshots/{id}/export`) and post the
bundle to `POST /squirrel/v1/snapshots/import`, which goes to the facility's arm and
creates a new snapshot there.

A forwarded restore runs as a job on the arm. Clients poll it on the head as usual:
`GET /jobs/v1/{id}` asks that arm about a job the head started there, for a day after
it started.

### Who wrote it

The head checks the caller's own key against the same gates the route has (write
scope and module; for facilities, admin over every facility), then calls the arm with
the head's service key there and names the caller in `X-Canopy-On-Behalf-Of`, as
`<key name>@<head>`, percent-encoded UTF-8 so a non-ASCII key name survives the trip.
The arm trusts that header only from a service key and only with
`CANOPY_ACCEPT_FORWARDED_WRITES=true`; otherwise it refuses the request with 403
`forwarded_write_refused`. It records the name in `createdBy` (snapshot captures,
Magpie configs) and logs it for imports.

The caller's facility grant goes with the request too, in
`X-Canopy-On-Behalf-Of-Facilities`, and the arm requires it alongside
`X-Canopy-On-Behalf-Of`. The arm's facility checks then allow only what both the
service key and the caller reach, so a caller keeps its own facility limits through
the head. Everything a forwarded write may change is that arm's own: the arm applies
its own rules to it, as for one of its local users, and the service key bounds the
rest.

The head checks the caller's grant first, for a snapshot change or a PV edit or
delete. A caller whose grant shares no facility with the row gets the head's own
answer, the same as on any instance, and nothing is sent to the arm: 404
`snapshot_not_found` for a snapshot, as `GET` answers, and 403
`insufficient_facility_access` for a PV or an unassigned snapshot. So the answer
doesn't depend on the arm being up, and doesn't say who owns the row.

Mint one service key on each arm, for that arm only, with the modules the head
forwards to it (`-m squirrel`, and `-m magpie` for Magpie configs), every facility
(`--facility '*'`) or only those the head's users work in. Each caller's own grant
still limits its requests. A key limited to some facilities must cover every one the
head's users reach on that arm. Otherwise the arm hides a snapshot outside them from the
key with a 404, and the head reads that as a prune (`archived_immutable`). Add `-m core -a` only if the head should create, edit or
delete the arm's facilities: facility routes are in the `core` module and need admin,
and admin on the arm also manages that arm's API keys. Without `-m core` the arm
refuses facility writes with 403 `insufficient_module_access`, and without `-a` with
403 `insufficient_admin_access`. Only the CLI mints service keys:

```sh
python scripts/create_key.py head -r -w -m squirrel --facility '*' --service
```

### Reading your own write

For a forwarded request that committed a write, the arm returns
`X-Canopy-Commit-LSN`. The head waits up to `CANOPY_FORWARD__WAIT_S` (2 s) for its
subscription to that arm, `canopy_history_<arm>`, to reach that position
(`pg_replication_origin_status`). If it doesn't, the answer carries
`X-Canopy-Replica-Stale: true`: the write succeeded, but a read straight after it may
not show it yet. A capture commits later, in the arm's worker, so it carries no
position. Both headers are exposed through CORS, so a browser client on another
origin can read them. The wait includes getting a database connection, so a busy pool
marks the answer stale rather than delaying it.

The view is readable only by superusers by default. As a superuser on the head's
database, grant its app role both:

```sql
GRANT SELECT ON pg_replication_origin_status TO canopy_app;
GRANT EXECUTE ON FUNCTION pg_show_replication_origin_status() TO canopy_app;
```

Without them every forwarded write is marked stale. `status` reads the same view
for each subscription's applied position: run it as a superuser, or grant the table
owner the same two rights, or its `applied_lsn` comes back as an error.

### Forwarding settings

On the head, per arm it forwards to: `CANOPY_ARMS__<arm>__FORWARD_URL` (https) and
`CANOPY_ARMS__<arm>__SERVICE_KEY` from a secret, set together. A head with only one
of the two doesn't start. Optional, for every arm: `CANOPY_FORWARD__WAIT_S`,
`CANOPY_FORWARD__TIMEOUT_S` and `CANOPY_FORWARD__CA_BUNDLE`. On each arm:
`CANOPY_ACCEPT_FORWARDED_WRITES=true`. The full list is in the
[settings below](#settings). Deploy the head from the same release as its arms, so
what it forwards has the shapes they expect.

### Machine access

`CANOPY_MACHINE_ACCESS` says whether an instance writes to the machine: a Hare PV write
(`PUT /hare/v1/pvs/{pv}`) and a snapshot restore. It is `write` on a primary and `read`
on a head unless set. With `read` both are refused with 403 `machine_read_only`, and
the worker doesn't register the restore task; captures, which only read EPICS, run on
every instance.

With `write`, an instance restores from its own copy of the snapshot, a head an arm's
snapshot included, through its own EPICS gateway, which must then allow writes. With
`read`, a head sends a restore of a forwardable arm's snapshot to that arm, whose
worker runs it; a read-only arm's snapshot gets 403 `not_owner`, and the head's own
gets `machine_read_only`.

Nothing coordinates two instances writing the same IOCs. Give `write` to one instance
per facility, normally its arm. `GET /v1/instance` reports the setting as
`machineAccess`.

### Snapshots taken on the head

The head reads EPICS for every facility, so a capture runs on its own worker,
whoever owns the facility. A snapshot of the head's own facility is saved on the head.
One of a forwardable arm's facility (or with `?owner=` naming that arm) is read from
that arm's PVs, given an id and time on the head, and sent to the arm's captured
import (`POST /squirrel/v1/snapshots/import?mode=captured`) as the head's service key,
on the user's behalf. The arm saves it under that id, with the user as its author, and
replication brings it back to the head unchanged. A capture for a read-only arm's
facility is refused with 403 `not_owner` before anything is queued.

The capture job's result names the arm: `{"type": "snapshot", "id", "arm"}`, with the
values the arm left out under `skipped`, and its last message is "Saved on *arm*". It
waits up to `CANOPY_FORWARD__TIMEOUT_S` (30 s) for the snapshot to come back. One that
takes longer is saved on the arm all the same, and the head keeps its bundle in the
outbox (below) until it replicates: the result adds `delivery` (`pending`, "Saved on
*arm*; waiting for replication"), the message is "Saved on *arm*; waiting for it to
replicate here", and the head serves it as pending meanwhile instead of answering 404.
The captured import is idempotent on the id, so sending a bundle again saves nothing
twice. When the arm refuses the snapshot (its facility is gone there, say), the job
fails with the arm's reason. When it can't be reached, the snapshot waits in the head's
outbox instead (below).

### When an arm is unreachable

A forwarded write fails with 502 `primary_unavailable` when the arm couldn't be
reached; nothing was sent. It fails with 504 when the request went out and no answer
came back; the write may have been applied, so check before sending it again. Reads
keep working from the head's own copy, and so do writes to the head's own rows and to
other arms.

An arm that refuses the head's service key answers a forwarded write with 502
`service_key_refused`. That covers a 401, or a 403 for the key's scope, modules or
admin access (and on facility routes, for its facilities). The caller's own key is fine:
the head checked it first. The detail names the arm and relays its answer, for example
"Arm 'arm1' refused this head's service key for it (401 invalid_api_key: Invalid API
key)". `canopy_forwarded_requests_total` counts these with `outcome="key_refused"`.

To fix it, mint the arm's service key again with the modules and access the head
forwards (see [Who wrote it](#who-wrote-it)), and set it as
`CANOPY_ARMS__<arm>__SERVICE_KEY` on the head. A refusal that comes from the caller's
own facility grant, and `forwarded_write_refused`, reach the caller as the arm sent
them.

Snapshots are the one exception: the machine's state at 14:02 can't be taken again at
14:30, so a capture for an arm that can't take it is kept on the head and saved there
later. Every other write to that arm is refused as above.

**The outbox.** The bundle waits in `squirrel.snapshot_outbox`, a local table on the
head that never replicates. A capture queues when the arm can't be reached, gives no
answer, answers 5xx, or turns down the head's service key; and also when the arm
already has snapshots waiting, so the arm gets them in the order they were taken. The
capture job then completes with the snapshot's id and `delivery`
(`{"state": "pending", ...}`), and the message "Captured; waiting for *arm*".

**Pending and failed snapshots.** Until its arm has it, the head lists and serves a
queued snapshot from its bundle, under the same id, with `delivery` set: `pending`
while it waits (or is saved on the arm and not replicated back yet), `failed` when the
arm refused it, with the arm's reason. Compare and export work on it; the export gives
each value its PV's addresses as the head holds them, as a stored snapshot's does.
Edit and delete are refused with 409 `snapshot_pending`; so is a restore, except where
the head has `CANOPY_MACHINE_ACCESS=write`, which restores it from its bundle, a failed
one too.
Once the snapshot replicates back, its outbox row goes and it is an ordinary snapshot.

**Delivery.** The head's worker sends each arm's queue oldest first, every
`CANOPY_SQUIRREL__OUTBOX__INTERVAL_S` seconds and whenever a capture queues. Each
attempt uses the user's name and facility grant as they were at capture time, and is
strict: an arm that would save it without some values (a PV moved out of the facility
meanwhile) refuses it with 409 `import_would_skip`, naming the PVs. A failure that says
nothing about the snapshot (unreachable, 5xx, the head's key refused) keeps it pending
and backs off, from `RETRY_INITIAL_S` doubling to `RETRY_MAX_S`; a queued snapshot
never gives up. A refusal of the snapshot itself (facility gone, PVs moved, id clash)
marks it failed, and the arm's queue goes on without it.

**Caps.** Each arm's queue holds at most `CANOPY_SQUIRREL__OUTBOX__MAX_SNAPSHOTS`
snapshots and `MAX_BYTES` bytes, failed rows included, and so are snapshots saved on
the arm that haven't replicated back. A full snapshot is tens of MB, so size
`MAX_BYTES` from the head's free disk and its number of arms. While an arm has
snapshots waiting and its queue is full, a capture for it is refused at once with 503
`outbox_full`. The arm's API must accept request bodies that large (an ingress in
front of it often limits them by default). A capture the arm saved is never refused for
the caps: if keeping its copy would pass one, the head logs a warning and doesn't keep it.

**Failed snapshots.** On the head, `python -m canopy.squirrel.outbox status` prints
each arm's queue and every failed snapshot with its reason. Once the cause is fixed,
`retry <id>...` or `retry --arm <arm>` sends them again at the next run;
`--allow-skips` lets the arm save one refused with `import_would_skip` without the
moved PVs' values. A snapshot that can't be saved can be exported
(`GET /squirrel/v1/snapshots/<id>/export`) and then removed with `discard <id>`,
which deletes it from the head for good.

**Saved but not back.** A snapshot the arm saved that hasn't replicated back after 30
minutes (the `canopy-outbox-not-back` alert) means the head's copy of that arm isn't
moving. Check the arm's subscription with `python -m canopy.core.replication status`
on the head and fix replication; the next delivery run removes the outbox row once
the snapshot is here. If the subscription was rebuilt without a reseed, run
`reseed --arm <arm>` (see [Slot lost](runbooks.md#slot-lost)). `discard <id>` takes such a row
too, but it only drops the head's copy: the arm has the snapshot, and it reaches the
head once replication from that arm is healthy.

The gauges and alert rules are listed under
[Snapshot outbox](../../observability/metrics.md#snapshot-outbox): a snapshot waiting
over 15 minutes, any failed one, one saved but not back after 30 minutes, a queue over
80% full or full, and a status that can't be read.

## Prune

An arm whose disk can't hold all of its history keeps the last `CANOPY_WINDOW_DAYS`
days of snapshots and leaves older ones to the head. Turn this on only for an arm whose
sizing forecast says it's needed; arms may differ, one pruning on the split layout and
another on `canopy_history`. The head never prunes, its own rows included. Pruning takes
the three steps below, once per arm, then a worker setting on that arm. Prune deletes
must never reach the head, so the arm first moves to a second publication layout:
`canopy_ref` carries the reference tables with every operation, and `canopy_ts` carries
the time-series tables with inserts and updates only. User deletes still reach the head,
because deleting a snapshot or a PV only marks it deleted.

### Switch to the split layout

1. On the arm: `publish --split`. It creates `canopy_ref` and `canopy_ts` beside
   `canopy_history`. Deploys keep running plain `publish`, which keeps all three in
   step and never switches layouts.
2. On the head: `switch --arm <arm>`, with `CANOPY_ARMS__<arm>__HEARTBEAT_DSN` set to a
   URI to the arm as `canopy_heartbeat`. It waits (up to `--timeout`, 120 s) until the
   slot has confirmed a position taken after the pair existed, then moves that arm's
   subscription to the pair without copying. A subscription moved while its slot is
   still behind the pair's creation can't find the publications and retries forever.
   `switch` refuses if the subscription is disabled or the arm has no pair yet. Other
   arms' subscriptions are untouched.
3. On the arm: `publish --split` again. It drops `canopy_history` once the head's
   walsender is seen streaming exactly the pair, and exits 2 with the reason until
   then. Retry after a few seconds: the walsender restarts after the switch. Reading
   what a walsender streams needs a superuser or `pg_read_all_stats`.

From then on the head's subscription to that arm reads the pair. `reseed --arm <arm>`
creates it on the pair whenever the arm has it. To copy everything again instead, run
`subscribe --split --arm <arm>`.

### Heartbeat

The head reports its checks to each arm that prunes, hourly, last in that arm's Job:

```text
refresh --arm <arm>
drift-check --arm <arm> --days <the arm's CANOPY_WINDOW_DAYS>
heartbeat --arm <arm> [--backup-ok] [--wal-archive-ok] [--drift-ok]
```

Pass each flag only when that check passed: `--drift-ok` when that arm's `drift-check`
exited 0 (1, drift, and 2, couldn't check, both leave it out), and `--backup-ok` and
`--wal-archive-ok` from the backup system's own status. The backup and WAL archive are
the head's, so every arm's heartbeat gets the same two; only `--drift-ok` differs by
arm. A dangling row `drift-check` attributes to one arm fails only that arm's check; one
it can't attribute fails every arm's. Once an arm prunes, its `drift-check` needs
`--days`: days the arm has pruned would otherwise count as drift. `heartbeat` writes the
arm's `core.history_heartbeat` through `CANOPY_ARMS__<arm>__HEARTBEAT_DSN`, in a row named
after the head's slot there, timed by the arm's clock.

While that arm's subscription is disabled or any of its tables is still copying,
`heartbeat` records the drift check as failed whatever `--drift-ok` says: those tables
weren't compared, and rows pruned before a table's copy reads them would never reach
history. A heartbeat also counts only for the stream it was written in. After the head
reconnects to the arm, or its subscription is rebuilt, the arm's prune waits for the
next heartbeat.

### Turn pruning on

On the arm's worker, set `CANOPY_WINDOW_DAYS` and `CANOPY_PRUNE__ENABLED=true`. The job
runs nightly at `CANOPY_PRUNE__HOUR` (UTC); a head's worker never schedules it, and the
job refuses to run anywhere but on an arm (`CANOPY_ROLE=primary`). Until
`CANOPY_PRUNE__DRY_RUN=false` it only counts what it would remove. Run it dry for a month while the arm's `drift-check`
stays clean, then turn the dry run off.

Each run removes snapshots older than the window that aren't retained, deleted or
not, up to `CANOPY_PRUNE__MAX_SNAPSHOTS_PER_RUN`, one per transaction. Each facility
keeps its newest snapshot however old it is, so a facility history still refers to
stays in use and can't be deleted. Before every snapshot it checks a gate on the head's
slot `canopy_history_<CANOPY_INSTANCE>`, and it stops at the first failure, logging each
failed check by name:

| Check | Fails when |
|---|---|
| `window_set` | `CANOPY_WINDOW_DAYS` is unset |
| `no_delete_publication` | Any publication carrying a snapshot table publishes deletes (`canopy_history` still exists) |
| `slot_active` | The head's slot is missing or no walsender holds it |
| `streams_split` | The walsender doesn't stream both `canopy_ref` and `canopy_ts`, they don't carry the snapshot tables whole (no row filter or column list), or its command can't be read |
| `no_initial_copy` | A table's initial copy is running (a `pg_<oid>_sync_…` slot exists) |
| `wal_status_reserved` | The slot's WAL is at risk |
| `lag` | Flush lag or the head's last reply is older than `CANOPY_PRUNE__MAX_LAG_S` (60 s) |
| `heartbeat_fresh`, `heartbeat_ok` | No heartbeat within `CANOPY_PRUNE__HEARTBEAT_MAX_AGE_H` (3 h) and since the walsender started, or one reporting a failed check |

A snapshot older than the window can still be restored on its arm: export it from the
head (`GET /squirrel/v1/snapshots/{id}/export`) and import the bundle on the arm
(`POST /squirrel/v1/snapshots/import`), which creates a new, recent copy owned by the
arm. The copy keeps the snapshot's facility, which must be the arm's, so the import
needs a key that can write there. It takes only values of the arm's own PVs that the
key could restore and that are still assigned to that facility or one below it, as a
capture there would; a PV moved to another facility since the snapshot was taken is
left out and listed in the response's `skipped`. A snapshot with `retain` set is never
pruned. Prune can't see references held outside Canopy, so a service that refers to
snapshots, such as an optimization service's runs, sets `retain` on the ones it needs.

## Settings

These are the history settings beside `CANOPY_ROLE`, `CANOPY_INSTANCE` and
`CANOPY_MACHINE_ACCESS`, which every instance sets (see the
[environment table](../index.md#configuration)). The annotated `.env.example` at the
repo root lists them all with their defaults.

| Variable | Purpose |
|----------|---------|
| `CANOPY_SQUIRREL__OUTBOX__MAX_SNAPSHOTS` | A history head only: most snapshots its outbox holds for one arm it can't reach (default 200). Failed and delivered rows count. At the cap, new captures for that arm get 503 `outbox_full` |
| `CANOPY_SQUIRREL__OUTBOX__MAX_BYTES` | A history head only: most bundle bytes its outbox holds for one arm (default 4 GiB). A full snapshot is tens of MB; size this from the head's free disk and its number of arms |
| `CANOPY_SQUIRREL__OUTBOX__RETRY_INITIAL_S` | A history head only: seconds before the first retry of a delivery to an arm (default 30). The wait doubles after each failure |
| `CANOPY_SQUIRREL__OUTBOX__RETRY_MAX_S` | A history head only: longest wait between delivery retries (default 900). A queued snapshot never gives up |
| `CANOPY_SQUIRREL__OUTBOX__INTERVAL_S` | A history head only: how often the **worker** looks for deliveries due, 1–60 seconds (default 30) |
| `CANOPY_HISTORY_URL`, `CANOPY_WINDOW_DAYS` | Optional. Reported by `GET /v1/instance` with the role, the instance name and the Alembic head shipped in the image: the URL of the history head that holds the instance's full history, and how many days of history it keeps (a whole number of at least 1; unset means all). A blank value counts as unset |
| `CANOPY_ARMS__<arm>__PUBLISHER_DSN` | History head only (`CANOPY_ROLE=mirror`): one per arm, named after that arm's `CANOPY_INSTANCE` (lowercase letters, digits and underscores, at most 48). The libpq connection string (key=value or URI) the head's subscription `canopy_history_<arm>` uses to reach the arm as its replication role. `subscribe` and `refresh` also open it themselves before an initial copy, to refuse facility or Magpie slugs the head already holds, so the CLI's network must reach the arm at that address too. Supply it from a secret; the CLI never prints it. The arms can also be set as JSON in `CANOPY_ARMS` |
| `CANOPY_ARMS__<arm>__READER_DSN` | History head only, for `python -m canopy.core.replication drift-check` and `reseed`: a `postgresql://` URI to that arm as its read-only reader role (asyncpg needs the URI form). A secret, like the publisher DSN |
| `CANOPY_ARMS__<arm>__HEARTBEAT_DSN` | History head only, for an arm that prunes: a `postgresql://` URI to that arm as its heartbeat role. `heartbeat --arm <arm>` records the head's backup, WAL-archive and drift checks for that arm in the arm's `core.history_heartbeat`; `switch --arm <arm>` reads the arm's slot position. Supply it from a secret; the CLI never prints it |
| `CANOPY_ARMS__<arm>__FORWARD_URL`, `__SERVICE_KEY` | History head only, set together or not at all: where the head sends writes to that arm's rows (the arm's API base URL, https) and a service key minted on that arm (`scripts/create_key.py --service`). Without them the arm's rows are read-only on the head (403 `not_owner`). Supply the key from a secret. See [Writes on a head](#writes-on-a-head) |
| `CANOPY_FORWARD__WAIT_S`, `__TIMEOUT_S`, `__CA_BUNDLE`, `__ALLOW_INSECURE_HTTP` | History head only, for every arm it forwards to. `WAIT_S` (default 2) bounds the wait for replication to bring a forwarded write back before the answer carries `X-Canopy-Replica-Stale: true`; the wait reads `pg_replication_origin_status`, so a superuser must grant the app role SELECT on it and EXECUTE on `pg_show_replication_origin_status()`. `TIMEOUT_S` (default 30) is per request to an arm; `CA_BUNDLE` verifies the arms' certificates; `ALLOW_INSECURE_HTTP` (default `false`) allows an `http://` forward URL, for development only |
| `CANOPY_ACCEPT_FORWARDED_WRITES` | Arm (`CANOPY_ROLE=primary`) only, default `false`. When `true`, a service key may send `X-Canopy-On-Behalf-Of`: the write is recorded under that name (`createdBy`), and an answer that committed a write carries `X-Canopy-Commit-LSN`. Otherwise the header is refused with 403 `forwarded_write_refused` |
| `CANOPY_REPLICATION__REPLICATOR_ROLE`, `__READER_ROLE`, `__APP_ROLE` | Role names for the replication CLI: on a primary, `publish` grants the replication role (default `replicator`) and the reader role (default `canopy_reader`) SELECT on the published tables; on a history head, `grants` lets the app role (default `canopy_app`) read every row and write only the head's own: row-level security on the tables that name a row's owner (facilities, PVs, snapshots, tag groups, Magpie configs) limits its writes to rows whose `owner_instance` is the head's `CANOPY_INSTANCE`, and any other role but the tables' owner sees none of those tables' rows. It also gets read-write on API keys |
| `CANOPY_REPLICATION__HEARTBEAT_ROLE` | On an arm: the role the head's heartbeat connection logs in as (default `canopy_heartbeat`); `publish` lets it write `core.history_heartbeat` and nothing else |
| `CANOPY_PRUNE__ENABLED`, `__DRY_RUN` | Arm worker only (`CANOPY_ROLE=primary`; a head never prunes). With `ENABLED=true` a nightly job removes snapshots older than `CANOPY_WINDOW_DAYS` that aren't retained; it only counts them until `DRY_RUN=false`. Before each snapshot it checks that no publication sends their deletes to the head (the split layout), that the head's slot `canopy_history_<CANOPY_INSTANCE>` is streaming and caught up, and that the head's heartbeat for this arm is fresh and OK; it needs a superuser or `pg_read_all_stats` to see what the slot streams. Each run logs what it removed or which check stopped it |
| `CANOPY_PRUNE__MAX_LAG_S`, `__HEARTBEAT_MAX_AGE_H`, `__HOUR`, `__MAX_SNAPSHOTS_PER_RUN` | Prune limits: the head's largest flush lag and reply age (default 60 s), the oldest acceptable heartbeat (3 h), the UTC hour the job runs (3), and the most snapshots one run removes (500) |
