"""Unit tests for the EPICS adapter: PVValue->EpicsValue mapping + batch get/put.

A fake connector stands in for ``canopy.epics`` — no IOCs, no DB.
"""

import time
import asyncio
from typing import Any
from datetime import UTC, datetime

import pytest

from canopy.contracts.pv import PVValue
from canopy.contracts.sample import AlarmSeverity
from canopy.epics.backends.simulation import SimulationConnector
from canopy.squirrel.adapters.epics_io import EpicsIO, _to_epics_value


def _pv(
    addr: str,
    value: Any,
    severity=AlarmSeverity.NO_ALARM,
    ts_ns: int = 1_000_000_000,
    alarm_status: int | None = None,
) -> PVValue:
    return PVValue(pv=addr, value=value, severity=severity, ts_ns=ts_ns, alarm_status=alarm_status)


class FakeConnector:
    def __init__(self, values: dict[str, PVValue] | None = None, fail: set[str] | None = None):
        self.values = values or {}
        self.fail = fail or set()
        self.puts: list[tuple[str, Any]] = []

    async def get(self, addr: str, *, timeout=None) -> PVValue:
        if addr in self.fail:
            raise RuntimeError(f"read boom {addr}")
        return self.values[addr]

    async def put(self, addr: str, value: Any, *, timeout=None, wait: bool = True) -> None:
        if addr in self.fail:
            raise RuntimeError(f"write boom {addr}")
        self.puts.append((addr, value))


# --- mapping ---------------------------------------------------------------- #


def test_maps_severity_code_and_aware_timestamp():
    ev = _to_epics_value(_pv("X", 42.0, AlarmSeverity.MAJOR, ts_ns=1_500_000_000))
    assert ev.value == 42.0
    assert ev.severity == 2  # MAJOR -> integer code
    assert ev.timestamp == datetime.fromtimestamp(1.5, tz=UTC)
    assert ev.timestamp.tzinfo is not None
    assert ev.connected is True
    assert ev.units is None and ev.precision is None  # v1 leaves these unset


def test_maps_alarm_status():
    ev = _to_epics_value(_pv("X", 42.0, AlarmSeverity.MAJOR, alarm_status=3))
    assert ev.status == 3


def test_absent_alarm_status_stays_none():
    assert _to_epics_value(_pv("X", 42.0)).status is None


def test_zero_alarm_status_is_preserved():
    assert _to_epics_value(_pv("X", 42.0, alarm_status=0)).status == 0


def test_zero_timestamp_becomes_none():
    assert _to_epics_value(_pv("X", 1, ts_ns=0)).timestamp is None


def test_severity_codes():
    codes = {sev: _to_epics_value(_pv("X", 0, sev)).severity for sev in AlarmSeverity}
    assert codes == {
        AlarmSeverity.NO_ALARM: 0,
        AlarmSeverity.MINOR: 1,
        AlarmSeverity.MAJOR: 2,
        AlarmSeverity.INVALID: 3,
    }


# --- batch reads ------------------------------------------------------------ #


async def test_get_many_reads_and_normalizes_failures():
    conn = FakeConnector(values={"A": _pv("A", 1.0), "B": _pv("B", 2.0)}, fail={"C"})
    out = await EpicsIO(conn, concurrency=4, chunk_size=2).get_many(["A", "B", "C"])
    assert out["A"].value == 1.0 and out["B"].value == 2.0
    assert out["C"].connected is False and "read boom" in out["C"].error  # failure never breaks the batch


async def test_get_many_empty_is_empty():
    assert await EpicsIO(FakeConnector()).get_many([]) == {}


async def test_get_many_progress_is_chunked():
    conn = FakeConnector(values={f"P{i}": _pv(f"P{i}", i) for i in range(5)})
    calls: list[tuple[int, int]] = []

    async def cb(cur: int, total: int, _msg: str) -> None:
        calls.append((cur, total))

    await EpicsIO(conn, chunk_size=2).get_many_with_progress([f"P{i}" for i in range(5)], cb)
    assert calls == [(2, 5), (4, 5), (5, 5)]  # chunks of 2 over 5 PVs


class SlowConnector:
    """Reads that take `delay[addr]` seconds (0 by default), tracking how many are in flight.

    With `gate` set, every read waits on it first, so reads can be held open.
    """

    def __init__(self, delay: dict[str, float] | None = None, gate: asyncio.Event | None = None):
        self.delay = delay or {}
        self.gate = gate
        self.inflight = 0
        self.max_inflight = 0
        self.started = 0
        self.cancelled = 0

    async def get(self, addr: str, *, timeout=None) -> PVValue:
        self.inflight += 1
        self.started += 1
        self.max_inflight = max(self.max_inflight, self.inflight)
        try:
            if self.gate is not None:
                await self.gate.wait()
            await asyncio.sleep(self.delay.get(addr, 0))
            return _pv(addr, 1.0)
        except asyncio.CancelledError:
            self.cancelled += 1
            raise
        finally:
            self.inflight -= 1


async def test_get_many_slow_pvs_do_not_stall_the_rest():
    # One slow PV in each of what used to be three chunks. Chunk by chunk this took
    # three delays in a row; in one pool the slow reads overlap.
    addrs = [f"P{i}" for i in range(6)]
    conn = SlowConnector(delay={"P0": 0.3, "P2": 0.3, "P4": 0.3})
    start = time.perf_counter()
    out = await EpicsIO(conn, concurrency=4, chunk_size=2).get_many(addrs)
    assert time.perf_counter() - start < 0.6
    assert set(out) == set(addrs)


async def test_get_many_progress_counts_completions_in_order():
    # Later PVs finish first; progress still counts up by chunk_size and ends at total.
    addrs = [f"P{i}" for i in range(5)]
    conn = SlowConnector(delay={f"P{i}": 0.05 * (5 - i) for i in range(5)})
    calls: list[tuple[int, int]] = []

    async def cb(cur: int, total: int, _msg: str) -> None:
        calls.append((cur, total))

    await EpicsIO(conn, concurrency=5, chunk_size=2).get_many_with_progress(addrs, cb)
    assert calls == [(2, 5), (4, 5), (5, 5)]


async def test_get_many_keeps_concurrency_reads_in_flight():
    conn = SlowConnector(delay={f"P{i}": 0.01 for i in range(50)})
    await EpicsIO(conn, concurrency=8, chunk_size=3).get_many([f"P{i}" for i in range(50)])
    assert conn.max_inflight == 8  # saturated, never over


async def test_get_many_cancelled_cancels_reads_in_flight():
    gate = asyncio.Event()  # never set: every read stays open
    conn = SlowConnector(gate=gate)
    task = asyncio.create_task(EpicsIO(conn, concurrency=4).get_many([f"P{i}" for i in range(20)]))
    while conn.inflight < 4:
        await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert conn.inflight == 0
    assert conn.cancelled == conn.started == 4
    # Reads still queued on the semaphore never reach the connector, and none is left over.
    leftover = [t for t in asyncio.all_tasks() if t is not asyncio.current_task() and not t.done()]
    assert leftover == []


# --- batch writes ----------------------------------------------------------- #


async def test_put_many_reports_ok_and_failures():
    conn = FakeConnector(fail={"BAD"})
    out = await EpicsIO(conn, chunk_size=10).put_many({"X": 1, "Y": 2, "BAD": 3})
    assert out["X"] == (True, None)
    assert out["Y"] == (True, None)
    assert out["BAD"][0] is False and "write boom" in out["BAD"][1]
    assert ("X", 1) in conn.puts and ("Y", 2) in conn.puts
    assert ("BAD", 3) not in conn.puts


async def test_put_many_empty_is_empty():
    assert await EpicsIO(FakeConnector()).put_many({}) == {}


# --- batch writes over the simulation backend -------------------------------- #


async def test_put_many_reports_every_simulated_failure():
    conn = SimulationConnector(put_error_rate=1.0, seed=1)
    out = await EpicsIO(conn, chunk_size=2).put_many({"SIM:A": 1, "SIM:B": 2, "SIM:C": 3})
    assert set(out) == {"SIM:A", "SIM:B", "SIM:C"}
    assert all(ok is False and "simulated put failure" in err for ok, err in out.values())


async def test_put_many_isolates_a_named_failing_pv():
    conn = SimulationConnector(put_fail_pvs=frozenset({"SIM:B"}))
    out = await EpicsIO(conn).put_many({"SIM:A": 1, "SIM:B": 2})
    assert out["SIM:A"] == (True, None)
    assert out["SIM:B"][0] is False
