"""Squirrel module configuration.

Sourced from env vars with prefix ``CANOPY_SQUIRREL__`` (nested delimiter ``__``),
mirroring the other Canopy modules. A fresh instance per ``get_squirrel_config()``
call (no caching) so tests can vary the environment freely.
"""

from __future__ import annotations

from typing import Self

from pydantic import Field, BaseModel, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class PVCacheConfig(BaseModel):
    """The worker-side PV cache that ``POST /snapshots?useCache=true`` reads from.

    When enabled, one ``canopy-worker`` (elected through a Redis lease) monitors every
    registered setpoint and readback address and keeps its latest value in Redis.
    Snapshot create can then read the cache instead of issuing a live read per PV;
    anything the cache can't vouch for is still read live.

    Cached values are monitor-resolution: alarm changes are posted at once, but a
    value change within the record's monitor deadband (MDEL) isn't, so a cached value
    can trail a live read by up to that deadband, with its own (older) timestamp.
    """

    enabled: bool = False
    # How often the leader re-reads the registered PVs from the database and
    # subscribes to new addresses / drops removed ones.
    reconcile_interval_s: float = Field(60.0, gt=0)
    # How often buffered monitor updates are written to Redis (and the heartbeat bumped).
    flush_interval_s: float = Field(1.0, gt=0)
    # A cache whose heartbeat is older than this is ignored entirely by snapshot create.
    heartbeat_max_age_s: float = Field(30.0, gt=0)
    # Leader lease length; the leader renews it every third of this.
    lease_ttl_s: float = Field(30.0, gt=0)
    # Subscriptions opened concurrently while (re)building the monitor set.
    subscribe_concurrency: int = Field(200, ge=1)

    @model_validator(mode="after")
    def _heartbeat_outlasts_a_flush(self) -> Self:
        # Otherwise every heartbeat is stale by the next flush and snapshots never use the cache.
        if self.heartbeat_max_age_s <= self.flush_interval_s:
            raise ValueError(
                f"heartbeat_max_age_s ({self.heartbeat_max_age_s:g}) must be longer than "
                f"flush_interval_s ({self.flush_interval_s:g})"
            )
        return self


class OutboxConfig(BaseModel):
    """A history head's queue of snapshots for arms it can't reach (``CANOPY_SQUIRREL__OUTBOX__*``).

    The caps hold per arm and count every row the arm has in the queue, failed and
    delivered ones included, since each keeps its bundle on the head's disk. A full
    snapshot is tens of MB, so size ``max_bytes`` from the head's free space and the
    number of arms. At either cap, new captures for that arm are refused.
    """

    max_snapshots: int = Field(200, ge=1, description="Most snapshots queued for one arm.")
    max_bytes: int = Field(4 * 1024**3, ge=1, description="Most bundle bytes queued for one arm.")
    retry_initial_s: float = Field(30.0, gt=0, description="Wait before retrying an arm after its first failure.")
    retry_max_s: float = Field(900.0, gt=0, description="Longest wait between retries; the wait doubles up to it.")
    interval_s: int = Field(30, ge=1, le=60, description="How often the worker checks for deliveries due, in seconds.")

    @model_validator(mode="after")
    def _initial_within_max(self) -> Self:
        if self.retry_initial_s > self.retry_max_s:
            raise ValueError(
                f"retry_initial_s ({self.retry_initial_s:g}) must not be longer than retry_max_s ({self.retry_max_s:g})"
            )
        return self


class SquirrelConfig(BaseSettings):
    """Squirrel's environment-sourced settings."""

    model_config = SettingsConfigDict(
        env_prefix="CANOPY_SQUIRREL__",
        env_nested_delimiter="__",
        env_file=".env",
        extra="ignore",
    )

    restore_hooks: str = Field(
        default="",
        description=(
            'Comma-separated restore-hook provider keys (e.g. "magnet_trim,write_to_con"), '
            "applied in the order given; empty means no hook runs."
        ),
    )
    restore_hook: str = Field(
        default="",
        description=(
            "Retired singular form of restore_hooks. Never applied: it is read only so a "
            "leftover CANOPY_SQUIRREL__RESTORE_HOOK is reported instead of silently ignored."
        ),
    )

    # EPICS batch I/O for snapshot create (reads) / restore (writes) over the
    # canopy.epics connector: in-flight concurrency, progress-chunk size, per-op
    # timeout in seconds (None = connector default / no timeout).
    epics_concurrency: int = 64
    epics_chunk_size: int = 500
    epics_timeout_s: float | None = None

    # Worker-side PV cache for cached snapshots (CANOPY_SQUIRREL__PV_CACHE__*).
    pv_cache: PVCacheConfig = PVCacheConfig()

    # A history head's snapshot outbox (CANOPY_SQUIRREL__OUTBOX__*).
    outbox: OutboxConfig = OutboxConfig()

    @property
    def restore_hooks_list(self) -> list[str]:
        """Split ``restore_hooks`` into provider keys.

        Returns
        -------
        list[str]
            The comma-separated entries, stripped of surrounding whitespace, with
            empty entries dropped and configured order preserved.
        """
        return [name.strip() for name in self.restore_hooks.split(",") if name.strip()]


def get_squirrel_config() -> SquirrelConfig:
    return SquirrelConfig()
