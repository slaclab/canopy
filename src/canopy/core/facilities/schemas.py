"""Facility DTOs."""

from pydantic import Field

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


# Lowercase alphanumerics and internal hyphens, not leading with a hyphen. Rejected
# rather than normalized: silently lowercasing `LCLS` would hand back a slug that
# doesn't match what the caller then types into a key's facility grant.
SLUG_PATTERN = r"^[a-z0-9][a-z0-9-]*$"


class FacilityCreate(CanopyModel):
    slug: str = Field(..., min_length=1, max_length=64, pattern=SLUG_PATTERN)
    name: str = Field(..., min_length=1, max_length=255)
    description: str | None = None
    # A parent *slug*, matching the slug-keyed routes and ``FacilityDTO.parent``.
    parent: str | None = None


class FacilityUpdate(CanopyModel):
    """Partial update. ``slug`` is not editable — URLs and key grants reference it."""

    name: str | None = Field(None, min_length=1, max_length=255)
    description: str | None = None
    parent: str | None = None
