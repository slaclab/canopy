"""The arms a history head subscribes to.

An arm is a primary that publishes its database; a head (``CANOPY_ROLE=mirror``)
keeps one subscription per arm and holds every arm's rows. The arms are one
setting, keyed by arm name::

    CANOPY_ARMS__ARM1__PUBLISHER_DSN="host=<arm1-host> dbname=canopy user=replicator password=<secret>"
    CANOPY_ARMS__ARM1__READER_DSN="postgresql://canopy_reader:<secret>@<arm1-host>/canopy"
    CANOPY_ARMS__ARM1__HEARTBEAT_DSN="postgresql://canopy_heartbeat:<secret>@<arm1-host>/canopy"
    CANOPY_ARMS__ARM1__FORWARD_URL="https://<arm1-host>"
    CANOPY_ARMS__ARM1__SERVICE_KEY="<a service key minted on arm1>"
    CANOPY_ARMS__ARM2__PUBLISHER_DSN="..."

or as JSON, ``CANOPY_ARMS='{"arm1": {"publisher_dsn": "..."}}'``. An arm's name
is that arm's own ``CANOPY_INSTANCE``, and the head's subscription to it, and the
slot on the arm, are named ``canopy_history_<arm>``. An arm with a forward URL
and a service key is *forwardable*: the head sends writes to that arm's rows there
(``canopy.core.forwarding``). Without them its rows are read-only on the head.

A fresh instance per ``get_arms_config()`` call (no caching) so tests can vary the
environment freely.
"""

from __future__ import annotations

import re

from pydantic import Field, BaseModel, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_SUBSCRIPTION_PREFIX = "canopy_history_"
_ARM_NAME = re.compile(r"[a-z0-9_]{1,48}")


class Arm(BaseModel):
    """One arm's settings, as the head sees them."""

    publisher_dsn: SecretStr | None = Field(
        default=None,
        description=(
            "libpq connection string (key=value or URI) the head's subscription uses to reach this arm, "
            "as its replication role. Only `subscribe` reads it. Supply it from a secret; it never goes "
            "on the command line or into the CLI's output."
        ),
    )

    reader_dsn: SecretStr | None = Field(
        default=None,
        description=(
            "postgresql:// URI the head's `drift-check` uses to read this arm, as its read-only reader "
            "role. Supply it from a secret, like the publisher DSN."
        ),
    )

    heartbeat_dsn: SecretStr | None = Field(
        default=None,
        description=(
            "postgresql:// URI to this arm as its heartbeat role, which may write only "
            "core.history_heartbeat. `heartbeat` records the head's checks for this arm through it, and "
            "`switch` reads the arm's slot position. Needed only for an arm that prunes. Supply it from a "
            "secret, like the publisher DSN."
        ),
    )

    forward_url: str | None = Field(
        default=None,
        description=(
            "Base URL of this arm's API (e.g. https://canopy.example.org), where the head forwards writes "
            "to the arm's rows. https, unless CANOPY_FORWARD__ALLOW_INSECURE_HTTP is set. Set it with "
            "SERVICE_KEY or not at all."
        ),
    )

    service_key: SecretStr | None = Field(
        default=None,
        description=(
            "A service key minted on this arm (scripts/create_key.py --service) that the head forwards "
            "writes with. Supply it from a secret. Set it with FORWARD_URL or not at all."
        ),
    )

    @field_validator("forward_url", "service_key", mode="before")
    @classmethod
    def _blank_is_unset(cls, value: object) -> object:
        """Read a blank value, as Helm and compose render an unset one, as unset."""
        if isinstance(value, str) and not value.strip():
            return None
        return value

    @field_validator("forward_url")
    @classmethod
    def _strip_trailing_slash(cls, value: str | None) -> str | None:
        return value.rstrip("/") if value else value

    @model_validator(mode="after")
    def _forward_url_and_key_together(self) -> Arm:
        if (self.forward_url is None) != (self.service_key is None):
            raise ValueError("forward_url and service_key are set together or not at all")
        return self

    @property
    def forwards(self) -> bool:
        """Whether the head forwards writes to this arm's rows (both forward settings are set)."""
        return self.forward_url is not None


class ArmsConfig(BaseSettings):
    """The head's arms, keyed by arm name (each arm's ``CANOPY_INSTANCE``)."""

    model_config = SettingsConfigDict(
        env_prefix="CANOPY_",
        env_nested_delimiter="__",
        env_file=".env",
        extra="ignore",
    )

    arms: dict[str, Arm] = Field(default_factory=dict)

    @field_validator("arms")
    @classmethod
    def _names_are_identifier_safe(cls, arms: dict[str, Arm]) -> dict[str, Arm]:
        # The name becomes part of the subscription and slot name: at most 63 bytes
        # with the 15-byte prefix, and lowercase so it reads the same in SQL.
        if bad := sorted(name for name in arms if not _ARM_NAME.fullmatch(name)):
            raise ValueError(f"arm names must be 1-48 lowercase letters, digits or underscores: {', '.join(bad)}")
        return arms


def get_arms_config() -> ArmsConfig:
    return ArmsConfig()


def subscription_name(arm: str) -> str:
    """``canopy_history_<arm>``: the head's subscription to ``arm``, and its slot on that arm."""
    return _SUBSCRIPTION_PREFIX + arm
