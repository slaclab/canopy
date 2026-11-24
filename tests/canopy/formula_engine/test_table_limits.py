"""Server-wide bounds on live tables: the total eval cap and the per-input seed sample cap."""

from __future__ import annotations

import numpy as np
import pytest

from canopy.contracts.formula import FormulaSpec
from canopy.formula_engine.errors import TableLimit
from tests.canopy.formula_engine.table_harness import MS, HOUR, S, Harness, timer_spec
from canopy.formula_engine.services.table_store import (
    TableStore,
    TableSettings,
    build_view,
)
from tests.canopy.formula_engine.test_table_eval import Rig, Collector

# --------------------------------------------------------------------------- #
# Total live table evals
# --------------------------------------------------------------------------- #


class TestTotalTables:
    async def test_a_new_table_past_the_total_gets_table_limit(self):
        rig = Rig(max_total=2)
        await rig.sub("c1", "s", timer_spec(dt_ns=1 * S), Collector())
        await rig.sub("c2", "s", timer_spec(dt_ns=2 * S), Collector())
        with pytest.raises(TableLimit) as exc:
            await rig.sub("c3", "s", timer_spec(dt_ns=3 * S), Collector())
        assert exc.value.error_code.value == "table_limit"
        assert "server" in exc.value.detail
        await rig.mgr.shutdown()

    async def test_joining_a_running_table_is_not_counted(self):
        rig = Rig(max_total=2)
        await rig.sub("c1", "s", timer_spec(dt_ns=1 * S), Collector())
        await rig.sub("c2", "s", timer_spec(dt_ns=2 * S), Collector())
        await rig.sub("c3", "s", timer_spec(dt_ns=2 * S), Collector())  # same eval as c2
        assert sum(1 for sh in rig.mgr._shared_evals.values() if sh.spec.kind == "table") == 2
        await rig.mgr.shutdown()

    async def test_a_client_at_the_total_can_replace_its_own_table(self):
        rig = Rig(max_total=2)
        await rig.sub("c1", "s", timer_spec(dt_ns=1 * S), Collector())
        await rig.sub("c2", "s", timer_spec(dt_ns=2 * S), Collector())
        await rig.sub("c2", "s", timer_spec(dt_ns=5 * S), Collector())  # re-subscribe, new parameters
        tables = [sh for sh in rig.mgr._shared_evals.values() if sh.spec.kind == "table"]
        assert sorted(sh.spec.grid_dt_ns for sh in tables) == [1 * S, 5 * S]
        await rig.mgr.shutdown()

    async def test_scalar_evals_do_not_count(self):
        rig = Rig(max_total=1)
        scalar = FormulaSpec(kind="scalar", expression="{a} + 1", bindings={"a": "SIM:A"})
        await rig.mgr.add_subscription("c", "x", scalar, 0, 0, Collector().send)
        await rig.sub("c", "t", timer_spec(), Collector())
        await rig.mgr.shutdown()


# --------------------------------------------------------------------------- #
# Seed sample cap
# --------------------------------------------------------------------------- #

CAP = 10_000


def _store(h: Harness, spec: FormulaSpec, parsed: dict, **settings) -> TableStore:
    store = TableStore(build_view(spec, parsed, HOUR), TableSettings(seed_max_samples=CAP, **settings), h.port())
    store.adopt(store.view)
    return store


def _last_at_or_before(ts: np.ndarray, vals: np.ndarray, at: np.ndarray) -> np.ndarray:
    idx = np.searchsorted(ts, at, side="right") - 1
    out = np.where(idx >= 0, vals[np.clip(idx, 0, None)], np.nan)
    return out.astype(np.float64)


class TestSeedSampleCap:
    async def test_a_dense_input_past_the_cap_leaves_older_cells_null_and_never_wrong(self):
        """600 timer rows over a 100 Hz input: the cells fetch stops near the cap, older cells are null."""
        h = Harness()
        h.wm = h.now
        dense_ts = np.arange(h.now - 700 * S + 3 * MS, h.now, 10 * MS, dtype=np.int64)  # 100 Hz
        h.archived("Y", [(int(t), float(i)) for i, t in enumerate(dense_ts)])
        sparse_ts = np.arange(h.now - 700 * S, h.now, 10 * S, dtype=np.int64)  # never near the cap
        h.archived("Z", [(int(t), float(-i)) for i, t in enumerate(sparse_ts)])
        spec = FormulaSpec(
            kind="table",
            bindings={"y": "Y", "z": "Z"},
            columns={"y": "{y}", "z": "{z}"},
            index="grid",
            grid_dt_ns=S,
            max_points=600,
        )
        store = _store(h, spec, {"y": None, "z": None})
        await store.wait_ready()

        ts, vals = store.rows.ordered()  # the final rows; the newest few are the provisional tail
        y, z = vals["Y"], vals["Z"]
        assert ts.size + len(store.tail_ts) == 600  # Z keeps every row
        assert store.inputs["Y"].history == "truncated"
        assert store.inputs["Z"].history == "seeded"
        # Y's archive calls loaded the cap and at most one more request, not the 70k-sample span.
        y_calls = h.mole_calls("Y")
        loaded = sum(int(((dense_ts >= t0) & (dense_ts <= t1)).sum()) for _, _, t0, t1 in y_calls)
        assert CAP <= loaded <= CAP + 30_000
        # No cell is ever wrong: every Y cell is the last sample at or before its row, or null.
        truth = _last_at_or_before(dense_ts, np.arange(dense_ts.size, dtype=np.float64), ts)
        known = ~np.isnan(y)
        assert known.any() and (~known).any()
        assert np.array_equal(y[known], truth[known])
        # The nulls are exactly the oldest rows: one cut, no holes after it.
        first_known = int(np.argmax(known))
        assert known[first_known:].all() and not known[:first_known].any()
        assert np.array_equal(z, _last_at_or_before(sparse_ts, -np.arange(sparse_ts.size, dtype=np.float64), ts))

    async def test_a_dense_trigger_walk_stops_at_the_cap_and_is_not_walked_again(self):
        h = Harness()
        h.wm = h.now
        trig_ts = np.arange(h.now - 1200 * S + 7 * MS, h.now, 10 * MS, dtype=np.int64)  # 100 Hz, 120k samples
        h.archived("TRIG", [(int(t), float(i)) for i, t in enumerate(trig_ts)])
        spec = FormulaSpec(
            kind="table",
            bindings={"trig": "TRIG"},
            columns={"t": "{trig}"},
            index="first_input",
            trigger="trig",
            max_points=10_000,
        )
        store = _store(h, spec, {"t": None})
        await store.wait_ready()

        ts, _ = store.rows.ordered()
        rows = np.concatenate([ts, np.asarray(store.tail_ts, dtype=np.int64)])
        assert 0 < rows.size < 10_000
        assert store.inputs["TRIG"].history == "truncated"
        assert rows.size == np.unique(rows // (100 * MS)).size  # still one row per bucket
        calls = len(h.mole_calls("TRIG"))
        store.adopt(store.view)  # a re-subscribe of the same table
        await store.wait_ready()
        assert len(h.mole_calls("TRIG")) == calls  # the capped range is not fetched again


def test_the_caps_come_from_the_config():
    from canopy.formula_engine.config import FormulaEngineConfig

    cfg = FormulaEngineConfig(table_max_total=5, table_seed_max_samples=1234)
    settings = TableSettings.from_config(cfg)
    assert (settings.max_total, settings.seed_max_samples) == (5, 1234)
    defaults = TableSettings.from_config(FormulaEngineConfig())
    assert (defaults.max_total, defaults.seed_max_samples) == (64, 500_000)


def test_a_later_uncapped_fill_does_not_clear_truncated():
    import numpy as np

    from canopy.formula_engine.services.table_store import _Fill

    h = Harness()
    spec = timer_spec()
    store = TableStore(build_view(spec, {"x": None, "y1": None}, HOUR), TableSettings(), h.port())
    store.inputs["X"].history = "truncated"
    store._apply_history("X", _Fill(np.empty(0), "seeded", True), h.now)
    assert store.inputs["X"].history == "truncated"
    store._apply_history("X", _Fill(np.empty(0), "live-only", False), h.now)
    assert store.inputs["X"].history == "live-only"  # a failed fetch still takes over (and is retried)
