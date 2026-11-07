"""A history head's outbox: snapshots it captured for arms it can't reach yet.

When the head takes a snapshot of a facility an arm owns and can't hand it to that
arm, the bundle waits in ``squirrel.snapshot_outbox``, a local table that never
replicates. ``delivery`` sends each arm's queue in capture order, with backoff, to
the arm's captured import, which saves it under the head's id; once the snapshot
replicates back, its row goes. Until then the snapshot API serves it from the
bundle as pending, or as failed when the arm refused it.

``python -m canopy.squirrel.outbox`` shows the queue and retries or discards failed
rows; ``metrics`` reports it to Prometheus.
"""
