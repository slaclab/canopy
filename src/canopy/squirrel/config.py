"""Squirrel module configuration.

Sourced from env vars with prefix ``CANOPY_SQUIRREL__`` (nested delimiter ``__``),
mirroring the other Canopy modules. A fresh instance per ``get_squirrel_config()``
call (no caching) so tests can vary the environment freely.
"""

from __future__ import annotations

from pydantic import Field, BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict


class PVCacheConfig(BaseModel):
    """The worker-side PV cache that ``POST /snapshots?useCache=true`` reads from.

    When enabled, one ``canopy-worker`` (elected through a Redis lease) monitors every
    registered setpoint and readback address and keeps its latest value in Redis.
    Snapshot create can then read the cache instead of issuing a live read per PV;
    anything the cache can't vouch for is still read live.
    """

    enabled: bool = False
    # How often the leader re-reads the registered PVs from the database and
    # subscribes to new addresses / drops removed ones.
    reconcile_interval_s: float = 60.0
    # How often buffered monitor updates are written to Redis (and the heartbeat bumped).
    flush_interval_s: float = 1.0
    # A cache whose heartbeat is older than this is ignored entirely by snapshot create.
    heartbeat_max_age_s: float = 30.0
    # Leader lease length; the leader renews it every third of this.
    lease_ttl_s: float = 30.0
    # Subscriptions opened concurrently while (re)building the monitor set.
    subscribe_concurrency: int = 200


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
