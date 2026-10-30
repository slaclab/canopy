"""Which tables a history mirror receives, and through which publication.

A history mirror is a read-only copy of a primary's database, fed by Postgres
logical replication. Every table on ``Base.metadata`` is classified here, and a
test fails on one that isn't, so a new table can't ship before someone decides
whether the mirror gets it:

``reference``
    Rows edited in place and shared across the instance (PVs, tags, configs).
    Replicated with every operation, so an edit or a delete on the primary is
    applied on the mirror too.
``time_series``
    Rows that only accumulate (snapshots and their values). Replicated with every
    operation in the single layout; in the split layout their publication carries
    no deletes, so pruning the primary never removes history.
``local``
    Rows that belong to one database and never replicate. API keys are local: a
    mirror mints and checks its own.

Two publication layouts carry the same tables. ``publications`` is the single
``canopy_history`` every primary starts with; ``split_publications`` is the pair a
primary that prunes old rows switches to (``publish --split``): ``canopy_ref`` with
every operation and ``canopy_ts`` with inserts and updates only.

Keys are ``"schema.table"`` strings rather than model classes, so ``canopy.core``
imports no module's models.
"""

from __future__ import annotations

from enum import StrEnum
from types import MappingProxyType
from dataclasses import dataclass
from collections.abc import Mapping


class TableClass(StrEnum):
    """How a table replicates; see the module docstring."""

    REFERENCE = "reference"
    TIME_SERIES = "time_series"
    LOCAL = "local"


@dataclass(frozen=True)
class Publication:
    """A Postgres publication built from the registry.

    Attributes
    ----------
    name : str
        Publication name on the primary; the mirror's subscription names it too.
    classes : frozenset[TableClass]
        Table classes it carries. Every table outside ``LOCAL`` is in exactly one publication.
    operations : tuple[str, ...]
        Changes it publishes (``publish = '...'``): any of insert, update, delete, truncate.
    """

    name: str
    classes: frozenset[TableClass]
    operations: tuple[str, ...]


@dataclass(frozen=True)
class ReplicationRegistry:
    """Table classes plus the publications that carry them.

    The commands take one of these (``REGISTRY`` by default), so a test can add a
    table without patching module state.
    """

    tables: Mapping[str, TableClass]
    publications: tuple[Publication, ...]
    split_publications: tuple[Publication, ...] = ()

    def layout(self, split: bool) -> tuple[Publication, ...]:
        """The publications of one layout: the split pair when ``split``, else the single one."""
        return self.split_publications if split else self.publications

    @property
    def all_publications(self) -> tuple[Publication, ...]:
        """Every publication either layout uses."""
        return self.publications + self.split_publications

    def published(self, publication: Publication) -> list[str]:
        """Tables ``publication`` carries, sorted."""
        return sorted(name for name, cls in self.tables.items() if cls in publication.classes)

    @property
    def replicated(self) -> list[str]:
        """Tables carried by any publication, sorted."""
        return sorted({name for publication in self.publications for name in self.published(publication)})

    @property
    def local(self) -> list[str]:
        """Tables that never replicate, sorted."""
        return sorted(name for name, cls in self.tables.items() if cls is TableClass.LOCAL)

    @property
    def schemas(self) -> list[str]:
        """Schemas holding at least one registered table, sorted."""
        return sorted({split_name(name)[0] for name in self.tables})


def split_name(qualified: str) -> tuple[str, str]:
    """Split ``"schema.table"`` into ``("schema", "table")``."""
    schema, _, table = qualified.partition(".")
    return schema, table


# Tables that belong to no module and are never classified. Commands only look at
# the registry's own schemas, so these are listed for the completeness test.
EXCLUDED = frozenset({"public.alembic_version"})

_ALL_OPERATIONS = ("insert", "update", "delete")
_NO_DELETES = ("insert", "update")

REGISTRY = ReplicationRegistry(
    tables=MappingProxyType(
        {
            "squirrel.pv": TableClass.REFERENCE,
            "squirrel.tag_group": TableClass.REFERENCE,
            "squirrel.tag": TableClass.REFERENCE,
            "squirrel.pv_tag": TableClass.REFERENCE,
            "squirrel.meta_pv": TableClass.REFERENCE,
            "magpie.config": TableClass.REFERENCE,
            "core.facility": TableClass.REFERENCE,
            "squirrel.pv_facility": TableClass.REFERENCE,
            "squirrel.snapshot": TableClass.TIME_SERIES,
            "squirrel.snapshot_value": TableClass.TIME_SERIES,
            "core.api_key": TableClass.LOCAL,
            "core.history_heartbeat": TableClass.LOCAL,
        }
    ),
    # One publication until the primary prunes old rows. TRUNCATE is left out on
    # purpose: no Canopy code truncates, so one typed on the primary by hand is a
    # mistake the history copy shouldn't repeat.
    publications=(
        Publication(
            name="canopy_history",
            classes=frozenset({TableClass.REFERENCE, TableClass.TIME_SERIES}),
            operations=_ALL_OPERATIONS,
        ),
    ),
    # Reference tables keep their deletes: replacing a PV's tags deletes and re-inserts
    # link rows, and without the delete the re-insert would collide on the mirror and
    # stop replication. Time-series deletes come only from pruning (user deletes are
    # soft), and those must never reach history.
    split_publications=(
        Publication(name="canopy_ref", classes=frozenset({TableClass.REFERENCE}), operations=_ALL_OPERATIONS),
        Publication(name="canopy_ts", classes=frozenset({TableClass.TIME_SERIES}), operations=_NO_DELETES),
    ),
)
