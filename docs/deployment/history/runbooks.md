# Runbooks

Run SQL on the head as its superuser, from the history cluster's Postgres pod, and on
an arm as that arm's superuser. The head's subscription to an arm is
`canopy_history_<arm>`, and its slot on the arm has the same name. Each runbook acts
on one arm; the other arms keep streaming.

## Slot lost

**Signs**: an arm's slot shows `wal_status = lost`, and the head's apply worker for
that arm logs that it can't start streaming. The arm dropped WAL the head still
needed, so this subscription can't continue.

1. On the head, detach and drop that arm's subscription:
   ```sql
   ALTER SUBSCRIPTION canopy_history_<arm> DISABLE;
   ALTER SUBSCRIPTION canopy_history_<arm> SET (slot_name = NONE);
   DROP SUBSCRIPTION canopy_history_<arm>;
   ```
2. On the arm, drop the invalidated slot:
   `SELECT pg_drop_replication_slot('canopy_history_<arm>');`
3. On the head, find the newest snapshot it holds from that arm:
   `SELECT max(created_at) FROM squirrel.snapshot WHERE owner_instance = '<arm>';`
4. Run `reseed --arm <arm> --since <that time>` on the head, with that arm's
   `CANOPY_ARMS__<arm>__PUBLISHER_DSN` and `CANOPY_ARMS__<arm>__READER_DSN` set. It
   creates the subscription disabled and without a copy, copies what the head is
   missing of that arm's rows, and enables it from an exact cut, so nothing is copied
   and replayed both. Only rows that arm owns are written or deleted. Snapshots the
   head lacks are found by id, whatever their time; `--since` bounds which snapshots
   both sides hold get their values re-read. An earlier time is safe, only slower; a
   later one is refused. For an arm on the split layout the new subscription reads
   `canopy_ref` and `canopy_ts`.
5. Run `drift-check --arm <arm>`.

The cut needs a moment when no transaction on the arm takes a new transaction id.
`reseed` keeps trying for `--cut-wait` seconds (60 by default), waiting out writes that
are already open. On an arm that never pauses that long it refuses and changes
nothing: pause writes on the arm briefly (stop its API and worker), or retry at a
quieter time.

If `reseed` fails part-way, the subscription stays disabled: run it again. Never enable
the subscription by hand after a failed run, since replication would resume from where
the old slot left off, not from the copy.

## Head PITR

When the head's database must be restored to an earlier point in time (corruption,
or a mistake made on the head itself):

1. Restore it to time *T*, starting the restored server with
   `max_logical_replication_workers = 0`. Each restored subscription still points at
   its arm's slot, which is past *T*, and would otherwise resume from there and skip
   everything in between.
2. On the head, detach and drop every arm's subscription (step 1 of "Slot lost"). On
   each arm, drop the old slot, which is still holding WAL.
3. Restart the head with its usual `max_logical_replication_workers`.
4. For each arm in turn, run `reseed --arm <arm> --since <T>` (or the newest snapshot
   the restored database holds from that arm, if that's earlier), then `drift-check`.

The head's own rows written after *T* are lost: no arm holds them, only the head's
backups (see [Backups](index.md#backups)). So is any snapshot queued in the head's outbox after
*T*, unless its arm already has it. A snapshot queued before *T* and delivered since
waits again in the restored outbox; sending it again is harmless, since the arm saves
nothing twice, and its row goes once the reseed brings the snapshot back.

## Arm wipe or rebuild

Wiping or rebuilding an arm's database drops its publication and slot, and the head's
subscription to it starts failing. The head still holds that arm's rows up to the
wipe.

1. Rebuild the arm's data. The head is normally newer than the arm's last dump, so
   rebuild from it as in "Arm host lost" below. If you restore a dump instead, also
   copy the head's rows that arm owns in the reference tables into the arm before
   step 3: `reseed` makes the head's copy of that arm's reference rows match the arm,
   and would delete whatever the dump lacks (and, through foreign keys, the snapshot
   values of a deleted PV).
2. On the arm: `alembic upgrade head`, then `publish`.
3. On the head: detach and drop that arm's old subscription (step 1 of "Slot lost"),
   then run `reseed --arm <arm> --since <time the rebuilt arm's data starts>` and
   `drift-check --arm <arm>`.

## Arm host lost

Rebuild the arm from the head, which holds every row that arm owns. API keys are
local, so they come from the arm's latest dump or are minted again.

1. On the new host, create the database and run `alembic upgrade head`.
2. From the head, copy the rows that arm owns, parents before children: the reference
   tables, then the time-series rows the arm should hold (all of them, or a recent
   window). Filter each root table on `owner_instance = '<arm>'` and each child table
   through its parent, for example
   `\copy (SELECT v.* FROM squirrel.snapshot_value v JOIN squirrel.snapshot s ON s.id = v.snapshot_id WHERE s.owner_instance = '<arm>') TO ...`
   on the head and `\copy ... FROM` on the arm. Never copy another owner's rows.
3. Restore or mint API keys, then run `publish`.
4. On the head: detach and drop that arm's old subscription, run
   `reseed --arm <arm> --since <newest snapshot on the head from that arm>`, then
   `drift-check --arm <arm>`.

## Bad update replicated

A bad write on an arm (a mass edit, a wrong delete) reaches the head within seconds.
Never repair an arm's rows on the head; repair them on that arm and let replication
carry the fix.

1. Restore the head's backup into a scratch database at a time before the bad write.
2. Extract the good rows that arm owns from it (`\copy (SELECT ...) TO`, filtered on
   the owner as in "Arm host lost").
3. Write them back on the arm, in one transaction, parents before children.
4. Run `drift-check --arm <arm>` on the head once replication has applied the fix.

## An arm's apply stopped on a duplicate key

**Signs**: the `canopy-replication-apply-stopped` alert fires for one arm. The head's
apply from that arm keeps failing on the same change and retrying, while the other
arms stream on. Most likely the arm created a facility slug or a Magpie config slug
that another owner, another arm or the head, already uses: those slugs are unique
across owners, and each arm only sees its own rows.

1. Find it: the head's Postgres log names the unique constraint and the value. Find
   the row on the head that holds the value, and its `owner_instance`.
2. Fix it: free the value on the *other* owner, the one whose row is already on the
   head: on its arm, or on the head itself if the head owns it. The change reaches the
   head, the slug is free, and the stuck change applies on the next retry. A facility's
   slug can't be changed through the API (an edit takes its name, description and
   parent): delete the facility there if nothing uses it, or else change its slug in SQL
   on its owner. Changing the stuck arm's own row doesn't help: that change queues
   behind the one that's stuck.
3. Run `drift-check --arm <arm>` once the apply error count stops rising.

Don't skip the failing transaction: the arm's row would be missing on the head, and
re-seeding that arm would only meet the same clash.
