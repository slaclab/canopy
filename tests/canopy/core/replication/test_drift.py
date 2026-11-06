"""``drift-check`` without a database: declarations, comparison, and the command's exit status per arm."""

import json
import pkgutil
import importlib
from argparse import Namespace

import pytest
from sqlalchemy.sql import sqltypes

import canopy
from canopy.core.db import Base
from canopy.core.config import get_settings
from canopy.core.replication import drift
from canopy.core.replication.drift import (
    EXIT,
    REFERENCES,
    TIME_SERIES_BUCKETS,
    Check,
    status,
    compare,
    overall,
    dated_by,
)
from canopy.core.replication.__main__ import build_parser
from canopy.core.replication.registry import REGISTRY, TableClass

for module in pkgutil.walk_packages(canopy.__path__, "canopy."):
    if module.name.endswith(".models"):
        importlib.import_module(module.name)

TIME_SERIES = sorted(table for table, kind in REGISTRY.tables.items() if kind is TableClass.TIME_SERIES)


@pytest.mark.parametrize("table", TIME_SERIES)
def test_every_time_series_table_is_dated(table):
    assert dated_by(table) is not None, f"{table}: no TIME_SERIES_BUCKETS entry on it or up its owner path"


@pytest.mark.parametrize("table", sorted(TIME_SERIES_BUCKETS))
def test_buckets_are_owner_roots_and_name_real_columns(table):
    assert REGISTRY.tables[table] is TableClass.TIME_SERIES
    assert REGISTRY.owner_paths[table].column is not None, "a child is dated through its owner path"
    assert TIME_SERIES_BUCKETS[table].time_column in Base.metadata.tables[table].columns


def test_children_are_dated_through_their_owner_path():
    assert dated_by("squirrel.snapshot") == ("squirrel.snapshot", 0)
    assert dated_by("squirrel.snapshot_value") == ("squirrel.snapshot", 1)


# Registered tables whose models exist in this tree (facility scoping's arrive later).
@pytest.mark.parametrize("table", [t for t in REGISTRY.replicated if t in Base.metadata.tables])
def test_replicated_keys_sort_the_same_on_any_server(table):
    """Row hashes follow primary-key order; text would follow each server's collation."""
    for column in Base.metadata.tables[table].primary_key.columns:
        assert not isinstance(column.type, sqltypes.String), f"{table}.{column.name} is a text key"


REFERENCE = Check("squirrel.pv", TableClass.REFERENCE, columns=("id",), key=("id",))
VALUES = Check(
    "squirrel.snapshot_value",
    TableClass.TIME_SERIES,
    bucketing=TIME_SERIES_BUCKETS["squirrel.snapshot"],
    dated_alias="t_o1",
)


def test_reference_tables_compare_count_and_hash():
    same = {"rows": 2, "md5": "a"}
    assert compare(REFERENCE, same, dict(same)) is None
    assert compare(REFERENCE, same, {"rows": 2, "md5": "b"}) == {
        "kind": "reference",
        "primary": same,
        "mirror": {"rows": 2, "md5": "b"},
    }


def test_time_series_compare_days():
    primary = {"2026-10-01": 5, "2026-10-02": 3}
    assert compare(VALUES, primary, dict(primary)) is None
    assert compare(VALUES, primary, {"2026-10-01": 4, "2026-10-02": 3, "2026-10-03": 2}) == {
        "kind": "time_series",
        "days": [
            {"day": "2026-10-01", "primary": 5, "mirror": 4},
            {"day": "2026-10-03", "primary": 0, "mirror": 2},
        ],
        "missing_rows": 1,
        "extra_rows": 2,
    }


def test_parser_options():
    args = build_parser().parse_args(["drift-check", "--days", "7", "--settle", "0", "--arm", "arm1"])
    assert (args.command, args.days, args.settle, args.arm) == ("drift-check", 7, 0.0, "arm1")
    defaults = build_parser().parse_args(["drift-check"])
    assert (defaults.days, defaults.settle, defaults.arm) == (None, 30.0, None)
    with pytest.raises(SystemExit):
        build_parser().parse_args(["drift-check", "--days", "0"])


# Every uuid column of a replicated table that holds another row's id: each one is a
# foreign key, or declared in REFERENCES, so the head check can't miss one.
@pytest.mark.parametrize("table", [t for t in REGISTRY.replicated if t in Base.metadata.tables])
def test_every_reference_is_a_foreign_key_or_declared(table):
    for column in Base.metadata.tables[table].columns:
        if not column.name.endswith("_id") or not isinstance(column.type, sqltypes.Uuid):
            continue
        assert column.foreign_keys or (table, column.name) in REFERENCES, f"{table}.{column.name} references what?"


@pytest.mark.parametrize(("child", "parent"), sorted(REFERENCES.items()))
def test_declared_references_name_real_columns(child, parent):
    table, column = child
    parent_table, parent_column = parent
    assert column in Base.metadata.tables[table].columns
    assert Base.metadata.tables[parent_table].columns[parent_column].primary_key
    assert not Base.metadata.tables[table].columns[column].foreign_keys, "a foreign key is found in the catalog"


@pytest.fixture
def env(monkeypatch):
    def set_env(**values: str) -> None:
        for name, value in values.items():
            monkeypatch.setenv(name, value)
        get_settings.cache_clear()

    monkeypatch.delenv("CANOPY_ARMS", raising=False)
    yield set_env
    get_settings.cache_clear()


ARGS = Namespace(command="drift-check", days=None, settle=0.0, arm=None)


async def test_refused_on_an_arm(env, caplog):
    env(CANOPY_ROLE="primary", CANOPY_INSTANCE="arm1")
    assert await drift.run(ARGS) == 2
    assert "runs on a mirror" in caplog.text


async def test_refused_without_arms(env, caplog):
    env(CANOPY_ROLE="mirror", CANOPY_INSTANCE="head")
    assert await drift.run(ARGS) == 2
    assert "no arms are configured" in caplog.text


async def test_refused_for_an_arm_not_configured(env, caplog):
    env(CANOPY_ROLE="mirror", CANOPY_INSTANCE="head", CANOPY_ARMS__ARM1__READER_DSN="postgresql://r:pw@x/db")
    assert await drift.run(Namespace(**{**vars(ARGS), "arm": "arm9"})) == 2
    assert "no arm named 'arm9'" in caplog.text


async def test_an_unreachable_head_is_exit_2_not_drift(env, monkeypatch, caplog):
    env(CANOPY_ROLE="mirror", CANOPY_INSTANCE="head", CANOPY_ARMS__ARM1__READER_DSN="postgresql://r:pw@x/db")

    async def connect(*_args, **_kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(drift.asyncpg, "connect", connect)
    assert await drift.run(ARGS) == 2
    assert "boom" in caplog.text


class _Head:
    async def close(self) -> None:
        pass


@pytest.fixture
def head_only(env, monkeypatch):
    """The head connects; no arm does (each arm's ``drift_check`` is replaced)."""
    calls = []

    async def connect(dsn, **_kwargs):
        if "head-db" in dsn:
            return _Head()
        raise OSError(f"could not connect to {dsn}")

    async def no_dangling(_mirror, _registry=None):
        return []

    monkeypatch.setattr(drift.asyncpg, "connect", connect)
    monkeypatch.setattr(drift, "dangling", no_dangling)
    env(
        CANOPY_ROLE="mirror",
        CANOPY_INSTANCE="head",
        CANOPY_DATABASE_URL="postgresql+asyncpg://canopy:canopy@head-db/canopy",
    )
    return calls


async def test_an_arm_without_a_reader_connection_is_not_checked_and_the_others_still_are(
    head_only, env, monkeypatch, capsys
):
    env(
        CANOPY_ARMS__ARM1__PUBLISHER_DSN="postgresql://p:pw@arm1-db/db",
        CANOPY_ARMS__ARM2__READER_DSN="postgresql://r:pw@arm2-db/db",
    )

    async def drift_check(_primary, _mirror, *, arm, **_kwargs):
        raise AssertionError("arm2 can't be reached")

    monkeypatch.setattr(drift, "drift_check", drift_check)
    assert await drift.run(ARGS) == 2
    report = json.loads(capsys.readouterr().out)
    assert report["status"] == "not_checked"
    assert "CANOPY_ARMS__ARM1__READER_DSN" in report["arms"]["arm1"]["error"]
    assert "could not connect" in report["arms"]["arm2"]["error"]
    assert report["head"] == {"status": "in_step", "dangling": []}


async def test_a_failed_arm_never_shows_its_password(head_only, env, caplog, capsys):
    secret = "s3cret-reader-pw"
    env(CANOPY_ARMS__ARM1__READER_DSN=f"postgresql://reader:{secret}@arm1-db/canopy")
    assert await drift.run(ARGS) == 2
    assert secret not in caplog.text and secret not in capsys.readouterr().out


DANGLING = [{"table": "squirrel.meta_pv", "column": "pv_id", "parent": "squirrel.pv", "owner": None, "rows": 1}]


def _owned_by(owner: str) -> list[dict]:
    return [{"table": "squirrel.pv_tag", "column": "tag_id", "parent": "squirrel.tag", "owner": owner, "rows": 2}]


def _arm_report(status: str) -> dict:
    return {
        "status": status,
        "in_step": status == "in_step",
        "compared": ["squirrel.pv"],
        "skipped": {},
        "inconclusive": {},
        "drift": {},
    }


@pytest.mark.parametrize(
    ("arms", "dangling", "expected"),
    [
        ({"arm1": "in_step", "arm2": "in_step"}, [], 0),
        ({"arm1": "drift", "arm2": "in_step"}, [], 1),
        ({"arm1": "drift", "arm2": "not_checked"}, [], 1),
        ({"arm1": "in_step", "arm2": "not_checked"}, [], 2),
        ({"arm1": "in_step", "arm2": "in_step"}, DANGLING, 1),
        ({"arm1": "not_checked"}, DANGLING, 1),
    ],
)
async def test_the_run_exits_with_the_worst_of_every_arm_and_the_head(
    head_only, env, monkeypatch, capsys, arms, dangling, expected
):
    env(**{f"CANOPY_ARMS__{name.upper()}__READER_DSN": f"postgresql://r:pw@{name}-db/db" for name in arms})

    async def connect(dsn, **_kwargs):
        return _Head()

    async def drift_check(_primary, _mirror, *, arm, **_kwargs):
        return _arm_report(arms[arm])

    async def found(_mirror, _registry=None):
        return dangling

    monkeypatch.setattr(drift.asyncpg, "connect", connect)
    monkeypatch.setattr(drift, "drift_check", drift_check)
    monkeypatch.setattr(drift, "dangling", found)
    assert await drift.run(ARGS) == expected
    report = json.loads(capsys.readouterr().out)
    assert {name: r["status"] for name, r in report["arms"].items()} == arms
    assert report["head"]["status"] == ("drift" if dangling else "in_step")


@pytest.mark.parametrize(
    ("only", "owner", "expected", "arm_statuses", "head_status"),
    [
        # Owned by an arm: that arm's drift only.
        (None, "arm1", 1, {"arm1": "drift", "arm2": "in_step"}, "in_step"),
        ("arm2", "arm1", 0, {"arm2": "in_step"}, "in_step"),  # arm1's own run reports it
        ("arm1", "arm1", 1, {"arm1": "drift"}, "in_step"),
        # Owned by the head, or by an instance that isn't an arm: the head's, on every run.
        ("arm2", "head", 1, {"arm2": "in_step"}, "drift"),
        ("arm2", "elsewhere", 1, {"arm2": "in_step"}, "drift"),
    ],
)
async def test_a_dangling_row_fails_the_arm_that_owns_it(
    head_only, env, monkeypatch, capsys, only, owner, expected, arm_statuses, head_status
):
    env(
        CANOPY_ARMS__ARM1__READER_DSN="postgresql://r:pw@arm1-db/db",
        CANOPY_ARMS__ARM2__READER_DSN="postgresql://r:pw@arm2-db/db",
    )

    async def connect(dsn, **_kwargs):
        return _Head()

    async def drift_check(_primary, _mirror, *, arm, **_kwargs):
        return _arm_report("in_step")

    async def found(_mirror, _registry=None):
        return _owned_by(owner)

    monkeypatch.setattr(drift.asyncpg, "connect", connect)
    monkeypatch.setattr(drift, "drift_check", drift_check)
    monkeypatch.setattr(drift, "dangling", found)
    assert await drift.run(Namespace(**{**vars(ARGS), "arm": only})) == expected
    report = json.loads(capsys.readouterr().out)
    assert {name: r["status"] for name, r in report["arms"].items()} == arm_statuses
    assert report["head"]["status"] == head_status
    if owner in report["arms"]:
        assert report["arms"][owner]["dangling"] == _owned_by(owner)


async def test_invalid_arms_are_refused_without_their_values(env, caplog):
    env(
        CANOPY_ROLE="mirror",
        CANOPY_INSTANCE="head",
        CANOPY_ARMS='{"Bad-Name": {"reader_dsn": "postgresql://r:s3cret@x/db"}}',
    )
    assert await drift.run(ARGS) == 2
    assert "CANOPY_ARMS is invalid" in caplog.text and "s3cret" not in caplog.text


async def test_arm_checks_one_arm(head_only, env, monkeypatch, capsys):
    env(
        CANOPY_ARMS__ARM1__READER_DSN="postgresql://r:pw@arm1-db/db",
        CANOPY_ARMS__ARM2__READER_DSN="postgresql://r:pw@arm2-db/db",
    )
    checked = []

    async def connect(dsn, **_kwargs):
        return _Head()

    async def drift_check(_primary, _mirror, *, arm, **_kwargs):
        checked.append(arm)
        return _arm_report("in_step")

    monkeypatch.setattr(drift.asyncpg, "connect", connect)
    monkeypatch.setattr(drift, "drift_check", drift_check)
    assert await drift.run(Namespace(**{**vars(ARGS), "arm": "arm2"})) == 0
    assert checked == ["arm2"] and list(json.loads(capsys.readouterr().out)["arms"]) == ["arm2"]


@pytest.mark.parametrize(
    ("statuses", "expected"),
    [
        ([], "in_step"),
        (["in_step"], "in_step"),
        (["not_checked", "in_step"], "not_checked"),
        (["not_checked", "drift"], "drift"),
    ],
)
def test_overall(statuses, expected):
    assert overall(statuses) == expected


def _report(**fields) -> dict:
    return {"compared": ["squirrel.pv"], "skipped": {}, "inconclusive": {}, "drift": {}} | fields


@pytest.mark.parametrize(
    ("fields", "expected"),
    [
        ({}, "in_step"),
        ({"drift": {"squirrel.pv": {}}}, "drift"),
        ({"drift": {"squirrel.pv": {}}, "skipped": {"squirrel.tag": "copying"}}, "drift"),
        ({"skipped": {"squirrel.tag": "copying"}}, "not_checked"),
        ({"inconclusive": {"squirrel.pv": "the primary changed while it was being compared"}}, "not_checked"),
        ({"compared": []}, "not_checked"),
    ],
)
def test_only_a_complete_comparison_is_in_step(fields, expected):
    assert status(_report(**fields)) == expected
    assert EXIT[expected] == {"in_step": 0, "drift": 1, "not_checked": 2}[expected]
