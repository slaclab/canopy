"""Replication CLI configuration.

Sourced from env vars with prefix ``CANOPY_REPLICATION__``, like the module
configs. A fresh instance per ``get_replication_config()`` call (no caching) so
tests can vary the environment freely. The CLI's own database connection is the
app's ``CANOPY_DATABASE_URL``; the side it runs on is ``CANOPY_ROLE``.
"""

from __future__ import annotations

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class ReplicationConfig(BaseSettings):
    """Settings for ``python -m canopy.core.replication``."""

    model_config = SettingsConfigDict(
        env_prefix="CANOPY_REPLICATION__",
        env_nested_delimiter="__",
        env_file=".env",
        extra="ignore",
    )

    publisher_dsn: SecretStr | None = Field(
        default=None,
        description=(
            "libpq connection string (key=value or URI) the mirror's subscription uses to reach the "
            "primary, as the replication role. Only `subscribe` reads it. Supply it from a secret; it "
            "never goes on the command line or into the CLI's output."
        ),
    )
    reader_dsn: SecretStr | None = Field(
        default=None,
        description=(
            "postgresql:// URI the mirror's `drift-check` uses to read the primary, as the read-only "
            "reader role. Supply it from a secret, like the publisher DSN."
        ),
    )
    replicator_role: str = Field(
        default="replicator",
        description="Role the subscription connects as; `publish` grants it SELECT on the published tables.",
    )
    reader_role: str = Field(
        default="canopy_reader",
        description="Read-only role for checks run against the primary; `publish` grants it the same SELECT.",
    )
    app_role: str = Field(
        default="canopy_app",
        description="Role the mirror's API connects as; `grants` gives it read access to replicated tables.",
    )


def get_replication_config() -> ReplicationConfig:
    return ReplicationConfig()
