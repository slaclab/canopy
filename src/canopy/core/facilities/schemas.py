"""Facility DTOs."""

from pydantic import Field

from canopy.contracts.base import CanopyModel


class FacilityDTO(CanopyModel):
    """A facility node. Flat, with ``parent_id`` — the client assembles the tree."""

    id: str
    slug: str
    name: str
    description: str | None = None
    parent_id: str | None = None


# Lowercase alphanumerics and internal hyphens, not leading with a hyphen. Rejected
# rather than normalized: silently lowercasing `LCLS` would hand back a slug that
# doesn't match what the caller then types into a key's facility grant.
SLUG_PATTERN = r"^[a-z0-9][a-z0-9-]*$"


class FacilityCreate(CanopyModel):
    slug: str = Field(..., min_length=1, max_length=64, pattern=SLUG_PATTERN)
    name: str = Field(..., min_length=1, max_length=255)
    description: str | None = None
    # A parent *slug*, not an id, matching the slug-keyed routes. ``FacilityDTO``
    # still returns ``parent_id``, since that's what a client walks to build the tree.
    parent: str | None = None


class FacilityUpdate(CanopyModel):
    """Partial update. ``slug`` is not editable — URLs and key grants reference it."""

    name: str | None = Field(None, min_length=1, max_length=255)
    description: str | None = None
    parent: str | None = None
