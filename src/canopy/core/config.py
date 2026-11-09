from typing import Literal
from functools import lru_cache

from pydantic import Field, ValidationError, field_validator
from pydantic_settings import BaseSettings, SettingsError, SettingsConfigDict

INSTANCE_NAME_PATTERN = r"^[a-z0-9_]{1,48}$"
"""An instance name: ``CANOPY_INSTANCE``, an arm's name and every owner stamp."""


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
    role : {"primary", "mirror"}
        Which side of history replication this instance's database is on. A
        ``"primary"`` holds only rows it owns, publishes them and may prune. A
        ``"mirror"`` (the head) subscribes to its arms and holds their rows beside
        its own. Either way an instance writes only rows it owns
        (``canopy.core.ownership``).
    instance : str
        This instance's name: lowercase letters, digits and underscores, at most 48
        (``canopy_history_<name>`` must fit a Postgres identifier). Required, and
        permanent once the instance holds data. Reported by ``GET /v1/instance``.
    history_url : str | None
        Base URL of the history head holding this instance's full history,
        reported by ``GET /v1/instance`` so clients can link to it.
    window_days : int | None
        Days of history this instance keeps, at least 1; ``None`` means it keeps
        everything. A blank value, as Helm and compose render an unset one, is ``None``
        here and for ``history_url``.
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

    # Instance identity. ``instance`` is stamped on every row this instance
    # creates (``owner_instance``), so it can't change once the instance holds
    # data. Reported, with the fields below, by GET /v1/instance.
    role: Literal["primary", "mirror"] = "primary"
    instance: str = Field(pattern=INSTANCE_NAME_PATTERN)
    history_url: str | None = None
    window_days: int | None = Field(None, ge=1)

    @field_validator("history_url", "window_days", mode="before")
    @classmethod
    def _blank_is_unset(cls, value: object) -> object:
        """Read a blank value as unset rather than as ``""`` or an unparseable int."""
        if isinstance(value, str) and not value.strip():
            return None
        return value


@lru_cache
def get_settings() -> Settings:
    return Settings()


def describe_settings_error(exc: ValidationError | SettingsError, prefix: str = "CANOPY_") -> str:
    """Which settings are wrong and why, never their values.

    A ``ValidationError``'s own text (``str`` and ``repr``) echoes its input, which
    may hold a password, so only each error's variable and message are kept. A
    ``SettingsError`` names only the field and its source (a value that isn't JSON).
    """
    if isinstance(exc, SettingsError):
        return str(exc)
    return "; ".join(
        f"{prefix}{'__'.join(str(part) for part in error['loc']).upper()}: {error['msg']}"
        for error in exc.errors(include_input=False, include_url=False, include_context=False)
    )
