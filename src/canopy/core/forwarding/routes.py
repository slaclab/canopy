"""Which instance each write belongs to, as a history head routes it.

An instance writes only rows it owns (``canopy.core.ownership``). On a head every
write route below names how to find the owner of what it changes or creates in: the
row in its path, or the facility, facilities or PV address in its body. The head
forwards a write owned by a forwardable arm to that arm, and hands everything else
to the app here, whose services write the head's own rows and refuse an arm's with
``not_owner``.

``LOCAL`` names the writes that are never routed, each with the reason. Routes gated
only by a read scope compute, validate or subscribe and are never routed either.
``tests/canopy/core/forwarding/test_routes.py`` fails until every other route in the
app is in exactly one of ``ROUTES`` and ``LOCAL``, so a write route added later is
never forwarded, or left out, without someone deciding it.

Paths are full path templates as the route table prints them (mount prefix included).
"""

from __future__ import annotations

import re
from dataclasses import field, dataclass
from collections.abc import Callable

from starlette.routing import compile_path

from canopy.core.security import (
    APIKey,
    require_admin_access,
    require_write_access,
    require_wildcard_facility_access,
)


@dataclass(frozen=True)
class Row:
    """The owner of an existing row: the ``table`` row whose ``column`` is the path parameter ``param``."""

    table: str
    param: str
    column: str = "id"


@dataclass(frozen=True)
class Facility:
    """The owner of the facility whose slug is the body's ``field``; no target when it's absent."""

    field: str


@dataclass(frozen=True)
class Facilities:
    """The one owner of the facilities whose slugs are listed in the body's ``field``.

    ``rows`` when the body is a list of such objects, all of which then share one
    owner. Facilities of two owners are refused with ``owner_mismatch``.
    """

    field: str
    rows: bool = False


@dataclass(frozen=True)
class Address:
    """The owners of the live PVs holding the body's ``field`` as any of their addresses."""

    field: str


@dataclass(frozen=True)
class NoTarget:
    """A create with nothing to take an owner from: this instance's, unless ``?owner=`` names one."""


Owner = Row | Facility | Facilities | Address | NoTarget
Gate = Callable[[APIKey], APIKey]

_WRITE: tuple[Gate, ...] = (require_write_access,)
_HIERARCHY: tuple[Gate, ...] = (require_admin_access, require_wildcard_facility_access)


@dataclass(frozen=True)
class WriteRoute:
    """One routed write.

    ``gates`` are the key checks the app's route makes, which the head repeats before
    forwarding: an arm checks the head's service key, not the caller's. ``owner_param``
    marks a create that takes ``?owner=``. ``unique`` names a body field whose value
    must be unique across every owner (``ReplicationRegistry.shared_keys``), as
    ``(table, column)``: a forwarded create may not reuse one the head already holds,
    or the arm's new row would stop replication from that arm. ``local_when`` is a
    query parameter value that keeps the request here. ``machine`` marks a write to
    the machine, which runs here when this instance has machine access, whoever owns
    the row it reads.
    """

    method: str
    path: str
    owner: Owner
    gates: tuple[Gate, ...] = _WRITE
    owner_param: bool = False
    unique: tuple[str, str] | None = None
    local_when: tuple[str, str] | None = None
    machine: bool = False
    regex: re.Pattern = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "regex", compile_path(self.path)[0])

    @property
    def reads_body(self) -> bool:
        """Whether the owner, or a value checked before forwarding, is in the request body."""
        return isinstance(self.owner, Facility | Facilities | Address) or self.unique is not None

    @property
    def module(self) -> str:
        """The module the route belongs to, which the caller's key must be scoped to."""
        mount = self.path.split("/", 2)[1]
        return "core" if mount == "v1" else mount


_SNAPSHOT = Row("squirrel.snapshot", "snapshot_id")
_PV = Row("squirrel.pv", "pv_id")
_TAG_GROUP = Row("squirrel.tag_group", "group_id")
_CONFIG = Row("magpie.config", "slug", column="slug")
_FACILITY = Row("core.facility", "facility_slug", column="slug")

ROUTES: tuple[WriteRoute, ...] = (
    # A history import makes a new snapshot in the bundle's facility. A captured import is
    # how this head saves a snapshot it took for an arm: only a service key sends one, to
    # the arm directly, so one arriving here is this instance's.
    WriteRoute("POST", "/squirrel/v1/snapshots/import", Facility("facility"), local_when=("mode", "captured")),
    WriteRoute("PUT", "/squirrel/v1/snapshots/{snapshot_id}", _SNAPSHOT),
    WriteRoute("PATCH", "/squirrel/v1/snapshots/{snapshot_id}", _SNAPSHOT),
    WriteRoute("DELETE", "/squirrel/v1/snapshots/{snapshot_id}", _SNAPSHOT),
    # A restore writes the machine from this instance's copy when it has machine access;
    # otherwise the owner's worker restores, and GET /jobs/v1/{job_id} here asks that arm.
    WriteRoute("POST", "/squirrel/v1/snapshots/{snapshot_id}/restore", _SNAPSHOT, machine=True),
    WriteRoute("POST", "/squirrel/v1/pvs", Facilities("facilities"), owner_param=True),
    WriteRoute("POST", "/squirrel/v1/pvs/multi", Facilities("facilities", rows=True), owner_param=True),
    WriteRoute("PUT", "/squirrel/v1/pvs/{pv_id}", _PV),
    WriteRoute("DELETE", "/squirrel/v1/pvs/{pv_id}", _PV),
    # A meta-PV marker is keyed by its PV's id.
    WriteRoute("POST", "/squirrel/v1/pvs/meta", Address("pvName"), owner_param=True),
    WriteRoute("PATCH", "/squirrel/v1/pvs/meta/{id}", Row("squirrel.pv", "id")),
    WriteRoute("DELETE", "/squirrel/v1/pvs/meta/{id}", Row("squirrel.pv", "id")),
    WriteRoute("POST", "/squirrel/v1/tags", NoTarget(), owner_param=True),
    WriteRoute("PUT", "/squirrel/v1/tags/{group_id}", _TAG_GROUP),
    WriteRoute("DELETE", "/squirrel/v1/tags/{group_id}", _TAG_GROUP),
    WriteRoute("POST", "/squirrel/v1/tags/{group_id}/tags", _TAG_GROUP),
    WriteRoute("PUT", "/squirrel/v1/tags/{group_id}/tags/{tag_id}", _TAG_GROUP),
    WriteRoute("DELETE", "/squirrel/v1/tags/{group_id}/tags/{tag_id}", _TAG_GROUP),
    WriteRoute("POST", "/squirrel/v1/tags/bulk", NoTarget(), owner_param=True),
    WriteRoute("POST", "/magpie/v1/configs/{app}", NoTarget(), owner_param=True),
    WriteRoute("PUT", "/magpie/v1/configs/{app}/{slug}", _CONFIG),
    WriteRoute("DELETE", "/magpie/v1/configs/{app}/{slug}", _CONFIG),
    # A root facility takes ?owner=; a child is its parent's owner's.
    WriteRoute(
        "POST",
        "/v1/facilities",
        Facility("parent"),
        gates=_HIERARCHY,
        owner_param=True,
        unique=("core.facility", "slug"),
    ),
    WriteRoute("PUT", "/v1/facilities/{facility_slug}", _FACILITY, gates=_HIERARCHY),
    WriteRoute("DELETE", "/v1/facilities/{facility_slug}", _FACILITY, gates=_HIERARCHY),
)

LOCAL: dict[tuple[str, str], str] = {
    # The head reads EPICS for every facility; an arm's snapshot is saved on that arm
    # through its captured import (canopy.squirrel.capture).
    ("POST", "/squirrel/v1/snapshots"): "captured here, and saved here or on the facility's arm",
    ("POST", "/v1/api-keys"): "API keys are each instance's own and never replicate",
    ("DELETE", "/v1/api-keys/{key_id}"): "API keys are each instance's own and never replicate",
    ("PUT", "/hare/v1/pvs/{pv:path}"): "writes the machine from this instance; machine access decides",
    ("POST", "/vole/v1/_debug/evict/{pv:path}"): "drops this process's own buffer",
    ("POST", "/analytics/v1/events"): "records this instance's own usage events",
    # Posts go to the external e-log with this instance's own e-log app tokens.
    ("POST", "/elog/v1/apps/{app}/entries"): "posts to the external e-log from this instance",
    ("POST", "/elog/v1/apps/{app}/attachments"): "posts to the external e-log from this instance",
}


def match(method: str, path: str) -> tuple[WriteRoute, dict[str, str]] | None:
    """The routed write ``method path`` resolves to here, with its path parameters.

    ``path`` is the route path (any ``root_path`` already removed). ``None`` for any
    request that isn't routed.
    """
    for route in ROUTES:
        if route.method == method and (found := route.regex.match(path)):
            return route, found.groupdict()
    return None
