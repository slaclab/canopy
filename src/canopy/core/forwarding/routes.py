"""The writes a forward instance may send to its primary, and the ones it never does.

An explicit list, not a rule: a write route added later is refused on a forward
instance (``read_only_instance``, like a mirror) until someone adds it here, and
``tests/canopy/core/forwarding/test_routes.py`` fails until every write or admin
route is in exactly one of ``FORWARDABLE`` and ``NOT_FORWARDED``.

Paths are full path templates as the route table prints them (mount prefix
included). Facility-scoped writes are forwarded by default; writes to tables every
facility shares only with ``CANOPY_FORWARD__ALLOW_SHARED``, since a facility key
grant can't limit those rows. Facility enforcement itself is the primary's: it
applies its gates to what both the service key and the caller reach, since the
caller's grant goes with the request (``X-Canopy-On-Behalf-Of-Facilities``).
"""

from __future__ import annotations

import re
from enum import StrEnum
from dataclasses import field, dataclass

from starlette.routing import compile_path


class RouteClass(StrEnum):
    FACILITY = "facility"
    """Rows that belong to one facility (snapshots, PVs): forwarded by default."""
    SHARED = "shared"
    """Tables every facility shares (tag groups and tags, meta-PVs, Magpie configs)."""


@dataclass(frozen=True)
class ForwardRoute:
    """One forwardable route.

    ``snapshot_param`` names the path parameter holding a snapshot id whose age is
    checked against the primary's window before forwarding.
    """

    method: str
    path: str
    route_class: RouteClass
    snapshot_param: str | None = None
    regex: re.Pattern = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "regex", compile_path(self.path)[0])

    @property
    def module(self) -> str:
        """The mount the route lives under, which the caller's key must be scoped to."""
        return self.path.split("/", 2)[1]


FACILITY, SHARED = RouteClass.FACILITY, RouteClass.SHARED

FORWARDABLE: tuple[ForwardRoute, ...] = (
    # Snapshot capture is a machine read by the primary's worker; the response is a
    # job on the primary, which GET /jobs/v1/{job_id} here asks the primary about.
    ForwardRoute("POST", "/squirrel/v1/snapshots", FACILITY),
    # How an old snapshot comes back to the primary: export it here (a read), import it
    # there as a new snapshot. No age check; the import is new.
    ForwardRoute("POST", "/squirrel/v1/snapshots/import", FACILITY),
    # Title, description and retain. A snapshot already past the window can't be retained
    # from here: the primary may have pruned it.
    ForwardRoute("PATCH", "/squirrel/v1/snapshots/{snapshot_id}", FACILITY, snapshot_param="snapshot_id"),
    ForwardRoute("PUT", "/squirrel/v1/snapshots/{snapshot_id}", FACILITY, snapshot_param="snapshot_id"),
    ForwardRoute("DELETE", "/squirrel/v1/snapshots/{snapshot_id}", FACILITY, snapshot_param="snapshot_id"),
    ForwardRoute("POST", "/squirrel/v1/pvs", FACILITY),
    ForwardRoute("POST", "/squirrel/v1/pvs/multi", FACILITY),
    ForwardRoute("PUT", "/squirrel/v1/pvs/{pv_id}", FACILITY),
    ForwardRoute("DELETE", "/squirrel/v1/pvs/{pv_id}", FACILITY),
    ForwardRoute("POST", "/squirrel/v1/pvs/meta", SHARED),
    ForwardRoute("PATCH", "/squirrel/v1/pvs/meta/{id}", SHARED),
    ForwardRoute("DELETE", "/squirrel/v1/pvs/meta/{id}", SHARED),
    ForwardRoute("POST", "/squirrel/v1/tags", SHARED),
    ForwardRoute("PUT", "/squirrel/v1/tags/{group_id}", SHARED),
    ForwardRoute("DELETE", "/squirrel/v1/tags/{group_id}", SHARED),
    ForwardRoute("POST", "/squirrel/v1/tags/{group_id}/tags", SHARED),
    ForwardRoute("PUT", "/squirrel/v1/tags/{group_id}/tags/{tag_id}", SHARED),
    ForwardRoute("DELETE", "/squirrel/v1/tags/{group_id}/tags/{tag_id}", SHARED),
    ForwardRoute("POST", "/squirrel/v1/tags/bulk", SHARED),
    ForwardRoute("POST", "/magpie/v1/configs/{app}", SHARED),
    ForwardRoute("PUT", "/magpie/v1/configs/{app}/{slug}", SHARED),
    ForwardRoute("DELETE", "/magpie/v1/configs/{app}/{slug}", SHARED),
)

NOT_FORWARDED: dict[tuple[str, str], str] = {
    ("PUT", "/hare/v1/pvs/{pv:path}"): "writes the machine; machine access decides where it runs",
    ("POST", "/squirrel/v1/snapshots/{snapshot_id}/restore"): "writes the machine; only the primary restores",
    ("POST", "/vole/v1/_debug/evict/{pv:path}"): "drops this process's own buffer; nothing to forward",
    ("POST", "/v1/facilities"): "edits the facility hierarchy; done on the primary",
    ("PUT", "/v1/facilities/{facility_slug}"): "edits the facility hierarchy; done on the primary",
    ("DELETE", "/v1/facilities/{facility_slug}"): "edits the facility hierarchy; done on the primary",
}


def match(method: str, path: str, *, allow_shared: bool) -> tuple[ForwardRoute, dict[str, str]] | None:
    """The forwardable route ``method path`` resolves to here, with its path parameters.

    ``path`` is the route path (any ``root_path`` already removed). ``None`` when the
    request isn't forwarded, including shared-table writes without ``allow_shared``.
    """
    for route in FORWARDABLE:
        if route.method != method or (route.route_class is SHARED and not allow_shared):
            continue
        if found := route.regex.match(path):
            return route, found.groupdict()
    return None
