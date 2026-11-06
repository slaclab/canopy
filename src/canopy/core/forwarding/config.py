"""Settings every forwarded write shares (``CANOPY_FORWARD__*``), read on a history head.

Where each arm's writes go is that arm's own setting (``CANOPY_ARMS__<arm>__FORWARD_URL``
and ``__SERVICE_KEY``, in ``canopy.core.arms``); these apply to all of them.

A fresh instance per ``get_forward_config()`` call (no caching), like the module
configs, so tests can vary the environment freely.
"""

from __future__ import annotations

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class ForwardConfig(BaseSettings):
    """How a head forwards writes to its arms, and how long it waits for them to come back."""

    model_config = SettingsConfigDict(
        env_prefix="CANOPY_FORWARD__",
        env_nested_delimiter="__",
        env_file=".env",
        extra="ignore",
    )

    wait_s: float = Field(
        default=2.0,
        ge=0,
        description=(
            "How long to wait for replication to bring a forwarded write back from its arm before answering "
            "with X-Canopy-Replica-Stale."
        ),
    )
    timeout_s: float = Field(default=30.0, gt=0, description="Timeout for one request to an arm.")
    ca_bundle: str | None = Field(
        default=None, description="CA bundle that verifies the arms' certificates; the system store when unset."
    )
    allow_insecure_http: bool = Field(
        default=False, description="Allow an http:// arm forward URL. For local development and tests only."
    )

    @field_validator("ca_bundle", mode="before")
    @classmethod
    def _blank_is_unset(cls, value: object) -> object:
        """Read a blank value, as Helm and compose render an unset one, as unset (like ``Settings``)."""
        if isinstance(value, str) and not value.strip():
            return None
        return value


def get_forward_config() -> ForwardConfig:
    return ForwardConfig()
