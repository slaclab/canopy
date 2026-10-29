import logging
from typing import Literal
from functools import lru_cache

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    """Core application settings, read from the environment with the ``CANOPY_`` prefix.

    Attributes
    ----------
    debug : bool
        Include verbose error detail in responses.
    api_keys : str
        API-key registry string, ``"<token>:<scope>[+<scope>][,...]"``.
    log_format : {"text", "json"}
        Line format for every log handler; ``"json"`` is what the Promtail scraper parses.
    log_path : str | None
        Optional file sink alongside stdout, rotated by size; ``None`` means stdout only.
    log_level : str
        Root log level name for the API and worker processes, matched case-insensitively.
        An unknown name falls back to ``INFO`` with a warning.
    database_url : str
        Postgres DSN using the asyncpg driver.
    database_pool_size : int
        Connections kept in the SQLAlchemy pool.
    database_max_overflow : int
        Connections the pool may open beyond ``database_pool_size``.
    redis_url : str
        Redis DSN backing the Arq job queue and per-job progress.
    role : {"primary", "mirror", "forward"}
        ``"mirror"`` makes this instance a read-only copy of a primary's data: write
        and admin routes refuse with ``read_only_instance`` (API-key management
        excepted) and the worker registers only stateless tasks. ``"forward"`` is a
        mirror that passes an allowlisted set of writes to its primary
        (``canopy.core.forwarding``) and refuses the rest the same way.
    instance : str | None
        Name of the deployment this process belongs to (e.g. ``"site-a"``), reported
        by ``GET /v1/instance``.
    history_url : str | None
        Base URL of the read-only history instance holding this instance's full
        history, reported by ``GET /v1/instance`` so clients can link to it.
    window_days : int | None
        Days of history this instance keeps; ``None`` means it keeps everything.
    accept_forwarded_writes : bool
        On a primary, trust ``X-Canopy-On-Behalf-Of`` from service keys, so a forward
        instance's writes are recorded under the user it sent them for. Off by
        default: the header is then refused from every key.
    machine_access : {"read", "write"}
        On a forward instance, ``"write"`` lets it change the machine: a Hare PV
        write runs here, through this instance's own EPICS gateway, and a snapshot
        restore is forwarded to the primary. With ``"read"`` (the default) both are
        refused with ``machine_read_only``. Ignored on other roles: a primary always
        controls the machine and a mirror never does.
    """

    model_config = SettingsConfigDict(
        env_prefix="CANOPY_",
        env_file=".env",
        extra="ignore",
    )

    debug: bool = False
    api_keys: str = ""

    # Logging — shared by canopy-api and canopy-worker (both call
    # core.logging.configure_logging at import time). "json" emits one JSON
    # object per line, which the Promtail scraper in docker/observability/
    # parses without regex extraction. log_path adds a file handler alongside
    # stdout so Promtail can tail a file; unset means stdout only.
    log_format: Literal["text", "json"] = "text"
    log_path: str | None = None
    log_level: str = "INFO"

    # API key cache. `api_key_cache_ttl_s` is also the revocation latency: a key
    # deactivated in the database keeps working until its cached entry expires.
    # `api_key_max_stale_s` bounds how long a cached key may be served while
    # Postgres is unreachable, so an outage can't keep a revoked key alive forever.
    api_key_cache_ttl_s: float = 60.0
    api_key_negative_cache_ttl_s: float = 5.0
    api_key_max_stale_s: float = 3600.0

    database_url: str = "postgresql+asyncpg://canopy:canopy@localhost:5432/canopy"
    database_pool_size: int = 5
    database_max_overflow: int = 10

    # Redis — Arq job queue + per-job progress; used by canopy-worker and the jobs module.
    redis_url: str = "redis://localhost:6379/0"

    # Instance role. A "mirror" serves a read-only copy of a primary's database
    # (fed by Postgres logical replication), so nothing here may write the
    # replicated tables; a "forward" instance is a mirror that sends selected
    # writes on to its primary over HTTPS. Reported, with the fields below, by
    # GET /v1/instance.
    role: Literal["primary", "mirror", "forward"] = "primary"
    instance: str | None = None
    history_url: str | None = None
    window_days: int | None = None

    # Primary only: accept writes a forward instance sends on a user's behalf.
    accept_forwarded_writes: bool = False
    # Forward only: whether machine writes (Hare PUT here, restore on the primary) are allowed.
    machine_access: Literal["read", "write"] = "read"

    @property
    def is_replica(self) -> bool:
        """Whether this instance's database is a subscriber's copy (``mirror`` or ``forward``)."""
        return self.role != "primary"

    @property
    def machine_writes(self) -> bool:
        """Whether this instance may change the machine, by itself or through its primary."""
        return self.role == "primary" or (self.role == "forward" and self.machine_access == "write")

    @model_validator(mode="after")
    def _warn_on_inert_machine_access(self) -> "Settings":
        if self.machine_access == "write" and self.role != "forward":
            logger.warning("CANOPY_MACHINE_ACCESS=write has no effect with CANOPY_ROLE=%s", self.role)
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
