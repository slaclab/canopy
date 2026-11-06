"""Per-request state shared by the arm's half of forwarding.

Kept free of other Canopy imports, so ``core.security`` and ``core.db`` can use it
without an import cycle through the middleware.
"""

from __future__ import annotations

from contextvars import ContextVar
from dataclasses import dataclass

ON_BEHALF_OF_HEADER = "X-Canopy-On-Behalf-Of"
ON_BEHALF_OF_FACILITIES_HEADER = "X-Canopy-On-Behalf-Of-Facilities"
COMMIT_LSN_HEADER = "X-Canopy-Commit-LSN"
REPLICA_STALE_HEADER = "X-Canopy-Replica-Stale"


@dataclass
class ForwardedWrite:
    """A request a history head sent on someone's behalf, as the arm sees it.

    ``on_behalf_of`` and ``facilities`` (the caller's facility grant, as slugs) are set
    once the service key's headers have been accepted; ``commit_lsn`` once the request's
    session committed a write. A route sets ``report_position`` when its answer must
    carry a position even if it wrote nothing (``core.db._commit_forwarded``).
    """

    on_behalf_of: str | None = None
    facilities: frozenset[str] | None = None
    commit_lsn: str | None = None
    report_position: bool = False


forwarded_write: ContextVar[ForwardedWrite | None] = ContextVar("canopy_forwarded_write", default=None)
