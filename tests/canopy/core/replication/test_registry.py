"""The replication registry classifies every table, and its publications are well-formed.

``test_every_table_is_classified`` is the guard the registry exists for: a PR that
adds a table fails here until it says whether the history mirror receives it.
"""

import pkgutil
import importlib

import canopy
from canopy.core.db import Base
from canopy.core.replication.registry import EXCLUDED, REGISTRY, TableClass


def _raise(name: str) -> None:
    raise ImportError(f"could not import {name} while looking for models")


def _import_every_models_module() -> None:
    """Import every ``canopy.**.models`` module, so a new module's tables are on the metadata too."""
    for module in pkgutil.walk_packages(canopy.__path__, "canopy.", onerror=_raise):
        if module.name.rsplit(".", 1)[-1] == "models":
            importlib.import_module(module.name)


_import_every_models_module()
METADATA_TABLES = {
    table.fullname if table.schema else f"public.{table.name}": table for table in Base.metadata.sorted_tables
}


def test_the_walk_found_every_module_with_tables():
    assert {"core.api_key", "squirrel.pv", "magpie.config"} <= set(METADATA_TABLES)


def test_every_table_is_classified():
    unclassified = sorted(set(METADATA_TABLES) - set(REGISTRY.tables) - EXCLUDED)
    assert not unclassified, (
        f"classify {unclassified} in canopy.core.replication.registry: reference (edited in place, "
        "replicated with deletes), time_series (only grows) or local (never leaves this database)"
    )


def test_every_registered_table_has_a_model():
    assert set(REGISTRY.tables) <= set(METADATA_TABLES)


def test_excluded_tables_are_not_classified():
    assert not EXCLUDED & set(REGISTRY.tables)


def test_api_keys_never_replicate():
    assert REGISTRY.tables["core.api_key"] is TableClass.LOCAL
    assert "core.api_key" not in REGISTRY.replicated


def test_replicated_tables_have_primary_keys():
    # Logical replication applies updates and deletes by replica identity, the primary key here.
    for name in set(REGISTRY.replicated) & set(METADATA_TABLES):
        assert METADATA_TABLES[name].primary_key.columns, f"{name} has no primary key"


def test_every_non_local_table_is_in_exactly_one_publication():
    for name, cls in REGISTRY.tables.items():
        carriers = [p.name for p in REGISTRY.publications if name in REGISTRY.published(p)]
        if cls is TableClass.LOCAL:
            assert carriers == [], f"local table {name} is published by {carriers}"
        else:
            assert len(carriers) == 1, f"{name} is published by {carriers}"


def test_publications_are_well_formed():
    names = [p.name for p in REGISTRY.publications]
    assert len(names) == len(set(names))
    for publication in REGISTRY.publications:
        assert publication.operations
        assert set(publication.operations) <= {"insert", "update", "delete", "truncate"}
        assert TableClass.LOCAL not in publication.classes


def test_derived_views():
    assert REGISTRY.local == ["core.api_key"]
    assert REGISTRY.schemas == ["core", "magpie", "squirrel"]
    assert set(REGISTRY.replicated) == {name for name, cls in REGISTRY.tables.items() if cls is not TableClass.LOCAL}
