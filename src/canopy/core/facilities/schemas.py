"""Facility DTOs."""

from pydantic import Field, field_validator

from canopy.contracts.base import CanopyModel


class FacilityDTO(CanopyModel):
    """A facility node. Flat, with ``parent`` — the client assembles the tree.

    Facilities are referenced by slug everywhere on the wire; ``id`` is included for
    debugging only.
    """

    id: str
    slug: str
    name: str
    description: str | None = None
    parent: str | None = None


class FacilityAccessDTO(CanopyModel):
    """The facilities the calling key can reach.

    ``unrestricted`` is true for a key granted every facility; ``facilities`` is then
    empty and means nothing. Otherwise ``facilities`` lists every reachable slug, with a
    parent grant expanded to its descendants, so a client can test membership directly.
    An empty list with ``unrestricted`` false is a key granted no facility at all.
    """

    unrestricted: bool
    facilities: list[str] = []


# Lowercase alphanumerics and single internal hyphens, not leading or trailing with
# a hyphen, and no doubled hyphen. Rejected rather than normalized: silently
# lowercasing `LCLS` would hand back a slug that doesn't match what the caller then
# types into a key's facility grant.
SLUG_PATTERN = r"^[a-z0-9]+(-[a-z0-9]+)*$"

# Slugs react-squirrel's router already claims as static `/{facility}/...` page
# segments (slaclab/react-squirrel#208) — a facility with one of these names would
# be unreachable through the frontend, shadowed by the page route of the same name.
RESERVED_SLUGS = frozenset(
    {"snapshots", "tags", "pv-browser", "api-keys", "meta-pvs", "comparison", "snapshot-details"}
)


class FacilityCreate(CanopyModel):
    slug: str = Field(..., min_length=1, max_length=64, pattern=SLUG_PATTERN)
    name: str = Field(..., min_length=1, max_length=255)
    description: str | None = None
    # A parent *slug*, matching the slug-keyed routes and ``FacilityDTO.parent``.
    parent: str | None = None

    @field_validator("slug")
    @classmethod
    def _reject_reserved_slug(cls, slug: str) -> str:
        if slug in RESERVED_SLUGS:
            raise ValueError(f"'{slug}' is a reserved slug and cannot be used for a facility")
        return slug


class FacilityUpdate(CanopyModel):
    """Partial update. ``slug`` is not editable — URLs and key grants reference it."""

    name: str | None = Field(None, min_length=1, max_length=255)
    description: str | None = None
    parent: str | None = None
