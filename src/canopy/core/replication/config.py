"""Replication CLI and prune configuration.

Sourced from env vars with prefixes ``CANOPY_REPLICATION__`` and ``CANOPY_PRUNE__``,
like the module configs. A fresh instance per ``get_*_config()`` call (no caching)
so tests can vary the environment freely. The CLI's own database connection is the
app's ``CANOPY_DATABASE_URL``; the side it runs on is ``CANOPY_ROLE``; a head's
arms, and how it reaches them, are ``canopy.core.arms``. How many days a pruning
arm keeps is ``CANOPY_WINDOW_DAYS``, the setting ``GET /v1/instance`` already reports.
"""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ReplicationConfig(BaseSettings):
    """Settings for ``python -m canopy.core.replication``."""

    model_config = SettingsConfigDict(
        env_prefix="CANOPY_REPLICATION__",
        env_nested_delimiter="__",
        env_file=".env",
        extra="ignore",
    )

    replicator_role: str = Field(
        default="replicator",
        description="Role the subscription connects as; `publish` grants it SELECT on the published tables.",
    )
    reader_role: str = Field(
        default="canopy_reader",
        description="Read-only role for a head's checks on an arm; `publish` grants it the same SELECT.",
    )
    app_role: str = Field(
        default="canopy_app",
        description=(
            "Role the head's API connects as; `grants` lets it read every replicated row and write only "
            "the head's own (row-level security on the root tables)."
        ),
    )
    heartbeat_role: str = Field(
        default="canopy_heartbeat",
        description=(
            "Arm only: the role the head's heartbeat connection logs in as (its "
            "CANOPY_ARMS__<arm>__HEARTBEAT_DSN); `publish` grants it write access to "
            "core.history_heartbeat and nothing else, once the role exists."
        ),
    )


class PruneConfig(BaseSettings):
    """Settings for the job that removes old snapshots from an arm whose history head keeps them.

    Off by default, and a dry run until ``dry_run`` is turned off as well. The job runs
    on an arm's worker only (``CANOPY_ROLE=primary``), and deletes nothing unless every check of the gate in
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
            "Largest acceptable flush lag of the head's subscription to this arm, and age of its last reply, in seconds. "
            "A healthy head replies about every 10 s (wal_receiver_status_interval) and the arm pings it "
            "at half of wal_sender_timeout (30 s by default); raise this if you raise those."
        ),
    )
    heartbeat_max_age_h: float = Field(
        default=3.0,
        gt=0,
        description="Oldest acceptable heartbeat from the head, in hours; it should write one hourly.",
    )
    hour: int = Field(default=3, ge=0, le=23, description="Hour of the day (UTC) the job runs.")
    max_snapshots_per_run: int = Field(
        default=500, gt=0, description="Most snapshots one run removes; the next run continues."
    )


def get_replication_config() -> ReplicationConfig:
    return ReplicationConfig()


def get_prune_config() -> PruneConfig:
    return PruneConfig()
