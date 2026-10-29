"""Forward-mode configuration (``CANOPY_FORWARD__*``), read only when ``CANOPY_ROLE=forward``.

A fresh instance per ``get_forward_config()`` call (no caching), like the module
configs, so tests can vary the environment freely.
"""

from __future__ import annotations

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class ForwardConfig(BaseSettings):
    """Where a forward instance sends writes, and which ones."""

    model_config = SettingsConfigDict(
        env_prefix="CANOPY_FORWARD__",
        env_nested_delimiter="__",
        env_file=".env",
        extra="ignore",
    )

    primary_url: str | None = Field(
        default=None,
        description=(
            "Base URL of the primary's API (e.g. https://canopy.example.org), without a trailing slash. "
            "Forwarded routes keep their paths under it. Must be https unless ALLOW_INSECURE_HTTP is set."
        ),
    )
    service_key: SecretStr | None = Field(
        default=None,
        description=(
            "A service key on the primary (scripts/create_key.py --service), with write scope on the "
            "forwarded modules. Supply it from a secret."
        ),
    )
    allow_shared: bool = Field(
        default=False,
        description=(
            "Also forward writes to tables every facility shares (tag groups and tags, meta-PVs, Magpie "
            "configs). Off by default: a facility key grant can't limit those rows."
        ),
    )
    primary_window_days: int | None = Field(
        default=None,
        ge=1,
        description=(
            "The primary's window (its CANOPY_WINDOW_DAYS). A change to a snapshot older than this is "
            "refused here with 409 archived_immutable instead of being forwarded. Unset: no age check."
        ),
    )
    wait_s: float = Field(
        default=2.0,
        ge=0,
        description=(
            "How long to wait for replication to apply a forwarded write before answering with X-Canopy-Replica-Stale."
        ),
    )
    timeout_s: float = Field(default=30.0, gt=0, description="Timeout for one request to the primary.")
    ca_bundle: str | None = Field(
        default=None, description="CA bundle that verifies the primary's certificate; the system store when unset."
    )
    allow_insecure_http: bool = Field(
        default=False, description="Allow an http:// primary_url. For local development and tests only."
    )

    @field_validator("primary_url")
    @classmethod
    def _strip_trailing_slash(cls, value: str | None) -> str | None:
        return value.rstrip("/") if value else value


def get_forward_config() -> ForwardConfig:
    return ForwardConfig()
