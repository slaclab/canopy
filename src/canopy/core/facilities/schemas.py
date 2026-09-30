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


class FacilityCreate(CanopyModel):
    slug: str = Field(..., min_length=1, max_length=64)
    name: str = Field(..., min_length=1, max_length=255)
    description: str | None = None
    parent_id: str | None = None


class FacilityUpdate(CanopyModel):
    """Partial update. ``slug`` is intentionally not editable — it's immutable once
    created (used in URLs and API key facility grants)."""

    name: str | None = Field(None, min_length=1, max_length=255)
    description: str | None = None
    parent_id: str | None = None
