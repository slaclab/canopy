"""The arms a history head subscribes to.

An arm is a primary that publishes its database; a head (``CANOPY_ROLE=mirror``)
keeps one subscription per arm and holds every arm's rows. The arms are one
setting, keyed by arm name::

    CANOPY_ARMS__ARM1__PUBLISHER_DSN="host=<arm1-host> dbname=canopy user=replicator password=<secret>"
    CANOPY_ARMS__ARM2__PUBLISHER_DSN="..."

or as JSON, ``CANOPY_ARMS='{"arm1": {"publisher_dsn": "..."}}'``. An arm's name
is that arm's own ``CANOPY_INSTANCE``, and the head's subscription to it, and the
slot on the arm, are named ``canopy_history_<arm>``.

A fresh instance per ``get_arms_config()`` call (no caching) so tests can vary the
environment freely.
"""

from __future__ import annotations

import re

from pydantic import Field, BaseModel, SecretStr, ValidationError, field_validator
from pydantic_settings import BaseSettings, SettingsError, SettingsConfigDict

from canopy.core.config import INSTANCE_NAME_PATTERN, describe_settings_error

_SUBSCRIPTION_PREFIX = "canopy_history_"


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
        # The name is the arm's own CANOPY_INSTANCE and becomes part of the subscription
        # and slot name; the instance pattern keeps that within 63 bytes with the
        # 15-byte prefix, and lowercase so it reads the same in SQL.
        if bad := sorted(name for name in arms if not re.fullmatch(INSTANCE_NAME_PATTERN, name)):
            raise ValueError(f"arm names must match {INSTANCE_NAME_PATTERN}, as CANOPY_INSTANCE does: {', '.join(bad)}")
        return arms


class ArmsConfigError(ValueError):
    """``CANOPY_ARMS`` is invalid. The message names each wrong setting and why, never a value."""


def get_arms_config() -> ArmsConfig:
    """The head's arms, from the environment.

    Raises
    ------
    ArmsConfigError
        When ``CANOPY_ARMS`` doesn't parse. pydantic's own error would echo the whole
        arms setting, every arm's DSN with it, so it is replaced, not chained.
    """
    try:
        return ArmsConfig()
    except (ValidationError, SettingsError) as exc:
        raise ArmsConfigError(describe_settings_error(exc)) from None


def subscription_name(arm: str) -> str:
    """``canopy_history_<arm>``: the head's subscription to ``arm``, and its slot on that arm."""
    return _SUBSCRIPTION_PREFIX + arm
