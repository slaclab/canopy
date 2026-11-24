"""Where a table's trigger rows are stitched together: one row per 100 ms flush bucket, N rows when N exist.

The archive is the harness's ns-exact fake, so these pin the store's own
arithmetic at every stitch: across the archive walk's outer windows, between
the ring and the archive, and when growing N extends older rows. Trigger
samples sit 1 ns past fixed ms offsets inside every bucket, so a stitch that
lands mid-bucket splits one.
"""

from __future__ import annotations

import numpy as np

from canopy.contracts.formula import FormulaSpec
from tests.canopy.formula_engine.table_harness import MS, HOUR, S, Harness
from canopy.formula_engine.services.table_store import (
    TableStore,
    TableSettings,
    build_view,
)

BUCKET = 100 * MS
OFFSETS_MS = range(5, 100, 10)  # 100 Hz: ten samples per bucket


def _view(n: int):
    spec = FormulaSpec(
        kind="table",
        bindings={"trig": "TRIG"},
        columns={"t": "{trig}"},
        index="first_input",
        trigger="trig",
        max_points=n,
    )
    return build_view(spec, {"t": None}, HOUR)


def _trigger_samples(t_lo: int, t_hi: int) -> np.ndarray:
    starts = np.arange(t_lo // BUCKET * BUCKET, t_hi, BUCKET, dtype=np.int64)
    ts = (starts[:, None] + np.asarray(OFFSETS_MS, dtype=np.int64)[None, :] * MS + 1).ravel()
    return np.sort(ts[(ts >= t_lo) & (ts <= t_hi)])


def _coalesced(ts: np.ndarray) -> np.ndarray:
    """The last sample of every bucket: the rows a table makes of ``ts``."""
    bucket = ts // BUCKET
    keep = np.ones(ts.size, dtype=bool)
    keep[:-1] = bucket[1:] != bucket[:-1]
    return ts[keep]


def _rows(store: TableStore) -> np.ndarray:
    ts, _ = store.rows.ordered()
    return np.concatenate([ts, np.asarray(store.tail_ts, dtype=np.int64)])


def _one_per_bucket(ts: np.ndarray) -> bool:
    return ts.size == np.unique(ts // BUCKET).size


async def _seed(h: Harness, n: int) -> TableStore:
    store = TableStore(_view(n), TableSettings(), h.port())
    store.adopt(store.view)
    await store.wait_ready()
    return store


async def test_outer_archive_windows_count_a_straddling_bucket_once():
    """N rows when N exist, though the archive walk's 60 s windows split buckets."""
    h = Harness()
    h.now = h.now + 50 * MS + 123  # every outer window bound lands mid-bucket
    h.wm = h.now
    ts = _trigger_samples(h.now - HOUR, h.now - 1)
    h.archived("TRIG", [(int(t), float(i)) for i, t in enumerate(ts)])
    # After five 60 s windows a per-window bucket count reads 3005 while 3001 rows exist.
    store = await _seed(h, 3003)
    rows = _rows(store)
    assert rows.size == 3003
    assert store.inputs["TRIG"].history == "seeded"
    assert _one_per_bucket(rows)


async def test_ring_to_archive_stitch_makes_one_row_per_bucket():
    """The archive part stops below the ring's oldest bucket, not 1 ns below its oldest row."""
    h = Harness()
    h.wm = h.now
    ts = _trigger_samples(h.now - HOUR, h.now - 1)
    h.archived("TRIG", [(int(t), float(i)) for i, t in enumerate(ts)])
    ring_from = h.now - 20 * S + 40 * MS  # the ring starts mid-bucket
    for i, t in enumerate(ts):
        if t >= ring_from:
            await h.live("TRIG", int(t), float(i), recv=int(t))
    store = await _seed(h, 1000)
    rows = _rows(store)
    assert _one_per_bucket(rows)
    assert np.array_equal(rows, _coalesced(ts)[-1000:])


async def test_growing_n_extends_older_rows_with_one_row_per_bucket():
    """A larger N resumes below the oldest held row's bucket."""
    h = Harness()
    h.wm = h.now
    ts = _trigger_samples(h.now - HOUR, h.now - 1)
    h.archived("TRIG", [(int(t), float(i)) for i, t in enumerate(ts)])
    store = await _seed(h, 300)
    store.adopt(_view(600))
    await store.wait_ready()
    rows = _rows(store)
    assert _one_per_bucket(rows)
    assert np.array_equal(rows, _coalesced(ts)[-600:])
