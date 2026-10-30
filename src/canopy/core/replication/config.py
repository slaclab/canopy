"""Replication CLI and prune configuration.

Sourced from env vars with prefixes ``CANOPY_REPLICATION__`` and ``CANOPY_PRUNE__``,
like the module configs. A fresh instance per ``get_*_config()`` call (no caching)
so tests can vary the environment freely. The CLI's own database connection is the
app's ``CANOPY_DATABASE_URL``; the side it runs on is ``CANOPY_ROLE``. How many days
a pruning primary keeps is ``CANOPY_WINDOW_DAYS``, the setting ``GET /v1/instance``
already reports.
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
    heartbeat_dsn: SecretStr | None = Field(
        default=None,
        description=(
            "Mirror only: a postgresql:// URI from the mirror to its primary, as the heartbeat role. "
            "`heartbeat` writes the mirror's check results through it, and `switch` reads the slot's "
            "position through it. Supply it from a secret."
        ),
    )
    heartbeat_role: str = Field(
        default="canopy_heartbeat",
        description=(
            "Role `heartbeat_dsn` logs in as; `publish` grants it write access to core.history_heartbeat "
            "and nothing else, once the role exists."
        ),
    )


class PruneConfig(BaseSettings):
    """Settings for the job that removes old snapshots from a primary whose history mirror keeps them.

    Off by default, and a dry run until ``dry_run`` is turned off as well. The job runs
    on a primary's worker only, and deletes nothing unless every check of the gate in
    ``canopy.core.replication.gate`` passes before each snapshot.
    """

    model_config = SettingsConfigDict(
        env_prefix="CANOPY_PRUNE__",
        env_nested_delimiter="__",
        env_file=".env",
        extra="ignore",
    )

    enabled: bool = Field(default=False, description="Run the prune job at all.")
    dry_run: bool = Field(
        default=True,
        description="Check the gate and count what would go, deleting nothing. Turn off only after dry runs look right.",
    )
    max_lag_s: float = Field(
        default=60.0,
        gt=0,
        description=(
            "Largest acceptable flush lag of the mirror's subscription, and age of its last reply, in seconds. "
            "A healthy mirror replies about every 10 s (wal_receiver_status_interval) and the primary pings it "
            "at half of wal_sender_timeout (30 s by default); raise this if you raise those."
        ),
    )
    heartbeat_max_age_h: float = Field(
        default=3.0,
        gt=0,
        description="Oldest acceptable heartbeat from the mirror, in hours; it should write one hourly.",
    )
    hour: int = Field(default=3, ge=0, le=23, description="Hour of the day (UTC) the job runs.")
    max_snapshots_per_run: int = Field(
        default=500, gt=0, description="Most snapshots one run removes; the next run continues."
    )


def get_replication_config() -> ReplicationConfig:
    return ReplicationConfig()


def get_prune_config() -> PruneConfig:
    return PruneConfig()
