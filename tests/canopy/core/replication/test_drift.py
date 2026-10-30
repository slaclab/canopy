"""``drift-check`` without a database: bucket declarations, comparison, and the command's exit status."""

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
    ORPHANS,
    TIME_SERIES_BUCKETS,
    Check,
    Bucketing,
    compare,
)
from canopy.core.replication.__main__ import build_parser
from canopy.core.replication.registry import REGISTRY, TableClass

for module in pkgutil.walk_packages(canopy.__path__, "canopy."):
    if module.name.endswith(".models"):
        importlib.import_module(module.name)

TIME_SERIES = sorted(table for table, kind in REGISTRY.tables.items() if kind is TableClass.TIME_SERIES)


def test_every_time_series_table_declares_its_buckets():
    assert sorted(TIME_SERIES_BUCKETS) == TIME_SERIES


@pytest.mark.parametrize("table", sorted(TIME_SERIES_BUCKETS))
def test_buckets_name_real_columns(table):
    bucketing = TIME_SERIES_BUCKETS[table]
    model = Base.metadata.tables[table]
    if bucketing.parent is None:
        assert bucketing.time_column in model.columns
        return
    parent = Base.metadata.tables[bucketing.parent]
    assert TIME_SERIES_BUCKETS[bucketing.parent].parent is None, "a parent is dated by its own column"
    assert bucketing.time_column in parent.columns
    (foreign_key,) = model.columns[bucketing.parent_key].foreign_keys
    assert foreign_key.column.table is parent and foreign_key.column.primary_key


# Registered tables whose models exist in this tree (facility scoping's arrive later).
@pytest.mark.parametrize("table", [t for t in REGISTRY.replicated if t in Base.metadata.tables])
def test_replicated_keys_sort_the_same_on_any_server(table):
    """Row hashes follow primary-key order; text would follow each server's collation."""
    for column in Base.metadata.tables[table].primary_key.columns:
        assert not isinstance(column.type, sqltypes.String), f"{table}.{column.name} is a text key"


REFERENCE = Check("squirrel.pv", TableClass.REFERENCE, columns=("id",), key=("id",))
VALUES = Check(
    "squirrel.snapshot_value", TableClass.TIME_SERIES, bucketing=TIME_SERIES_BUCKETS["squirrel.snapshot_value"]
)


def test_reference_tables_compare_count_and_hash():
    same = {"rows": 2, "md5": "a"}
    assert compare(REFERENCE, same, dict(same)) is None
    assert compare(REFERENCE, same, {"rows": 2, "md5": "b"}) == {
        "kind": "reference",
        "primary": same,
        "mirror": {"rows": 2, "md5": "b"},
    }


def test_time_series_compare_days_and_rows_without_a_parent():
    primary = {"2026-10-01": 5, "2026-10-02": 3}
    assert compare(VALUES, primary, dict(primary)) is None
    assert compare(VALUES, primary, {"2026-10-01": 4, "2026-10-02": 3, "2026-10-03": 2, ORPHANS: 1}) == {
        "kind": "time_series",
        "days": [
            {"day": "2026-10-01", "primary": 5, "mirror": 4},
            {"day": "2026-10-03", "primary": 0, "mirror": 2},
        ],
        "orphans": {"primary": 0, "mirror": 1},
        "missing_rows": 1,
        "extra_rows": 2,
    }


def test_rows_without_a_parent_alone_are_drift():
    difference = compare(VALUES, {"2026-10-01": 1}, {"2026-10-01": 1, ORPHANS: 3})
    assert difference["days"] == [] and difference["orphans"] == {"primary": 0, "mirror": 3}


def test_bucketing_defaults_to_the_tables_own_column():
    assert Bucketing(time_column="created_at").parent is None


def test_parser_options():
    args = build_parser().parse_args(["drift-check", "--days", "7", "--settle", "0"])
    assert (args.command, args.days, args.settle) == ("drift-check", 7, 0.0)
    defaults = build_parser().parse_args(["drift-check"])
    assert (defaults.days, defaults.settle) == (None, 30.0)
    with pytest.raises(SystemExit):
        build_parser().parse_args(["drift-check", "--days", "0"])


@pytest.fixture
def env(monkeypatch):
    def set_env(**values: str) -> None:
        for name, value in values.items():
            monkeypatch.setenv(name, value)
        get_settings.cache_clear()

    yield set_env
    get_settings.cache_clear()


ARGS = Namespace(command="drift-check", days=None, settle=0.0)


async def test_refused_on_a_primary(env, caplog):
    env(CANOPY_ROLE="primary")
    assert await drift.run(ARGS) == 2
    assert "runs on a mirror" in caplog.text


async def test_refused_without_the_readers_connection(env, monkeypatch, caplog):
    env(CANOPY_ROLE="mirror", CANOPY_INSTANCE="site_a")
    monkeypatch.delenv("CANOPY_REPLICATION__READER_DSN", raising=False)
    assert await drift.run(ARGS) == 2
    assert "CANOPY_REPLICATION__READER_DSN" in caplog.text


async def test_an_unexpected_failure_is_exit_2_not_drift(env, monkeypatch, caplog):
    env(CANOPY_ROLE="mirror", CANOPY_INSTANCE="site_a", CANOPY_REPLICATION__READER_DSN="postgresql://r:pw@x/db")

    async def connect(*_args, **_kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(drift.asyncpg, "connect", connect)
    assert await drift.run(ARGS) == 2
    assert "boom" in caplog.text


async def test_a_failed_connection_never_shows_the_password(env, caplog, capsys):
    secret = "s3cret-reader-pw"
    env(
        CANOPY_ROLE="mirror",
        CANOPY_INSTANCE="site_a",
        CANOPY_DATABASE_URL="postgresql+asyncpg://canopy:canopy@127.0.0.1:1/canopy",
        CANOPY_REPLICATION__READER_DSN=f"postgresql://reader:{secret}@127.0.0.1:1/canopy",
    )
    assert await drift.run(ARGS) == 2
    assert secret not in caplog.text and secret not in capsys.readouterr().out
