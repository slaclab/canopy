from typing import Any, Literal
from functools import lru_cache
from collections.abc import Mapping, Iterable

from pydantic import (
    Field,
    BaseModel,
    ValidationInfo,
    ValidationError,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, SettingsError, SettingsConfigDict

INSTANCE_NAME_PATTERN = r"^[a-z0-9_]{1,48}$"
"""An instance name: ``CANOPY_INSTANCE``, an arm's name and every owner stamp."""

# Every mountable module, in composition order. The order is load-bearing: vole
# uses the hare gateway and formula_engine_lifespan reads the service vole_lifespan binds,
# so a selected subset always keeps this relative order.
ALL_MODULES: tuple[str, ...] = (
    "mole",
    "hare",
    "raccoon",
    "vole",
    "formula-engine",
    "squirrel",
    "magpie",
    "jobs",
    "converter",
    "analytics",
    "elog",
)

# ASGI scope key under which the composed app publishes its selected modules.
# Sub-apps are shared singletons that cannot see the app they are mounted in,
# so the selection travels with the request instead of living in a global.
MODULES_SCOPE_KEY = "canopy.modules"


def resolve_modules(names: Iterable[str]) -> tuple[str, ...]:
    """Validate module names and return them deduplicated, in composition order.

    An empty selection means every module. An unknown name raises ``ValueError``
    so a typo stops startup instead of silently serving fewer routes.
    """
    wanted = {name.strip() for name in names if name.strip()}
    unknown = sorted(wanted - set(ALL_MODULES))
    if unknown:
        raise ValueError(f"Unknown CANOPY module(s): {', '.join(unknown)}. Known modules: {', '.join(ALL_MODULES)}")
    if not wanted:
        return ALL_MODULES
    return tuple(name for name in ALL_MODULES if name in wanted)


def parse_modules(value: str) -> tuple[str, ...]:
    """Resolve a comma-separated ``CANOPY_MODULES`` value (empty: every module)."""
    return resolve_modules(value.split(","))


def module_selected(scope: Mapping[str, Any], name: str) -> bool:
    """True when ``name`` is mounted in the app serving this ASGI scope.

    A scope that carries no selection (a sub-app served on its own, outside the
    composed app) counts every module as selected.
    """
    selected = scope.get(MODULES_SCOPE_KEY)
    return selected is None or name in selected


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
    accept_forwarded_writes : bool
        On a primary (an arm), trust ``X-Canopy-On-Behalf-Of`` from service keys, so
        writes a history head forwards are recorded under the user it sent them for.
        Off by default: the header is then refused from every key.
    machine_access : {"read", "write"}
        Whether this instance changes the machine: ``"write"`` lets it run Hare PV
        writes and snapshot restores, through its own EPICS gateway; with ``"read"``
        both are refused with ``machine_read_only``, and it only reads EPICS (a
        capture). Unset (or blank) means ``"write"`` on a primary and ``"read"`` on a
        mirror, so an arm keeps restoring and a history head never writes to the
        machine unless told to.
    """

    model_config = SettingsConfigDict(
        env_prefix="CANOPY_",
        env_file=".env",
        extra="ignore",
    )

    debug: bool = False
    api_keys: str = ""

    # Path of a file holding the key registry, in the same grammar as api_keys.
    # When set it is the only source: api_keys is ignored, and a file that is
    # missing, unreadable or holds no key stops startup instead of leaving an
    # app that answers with the wrong registry. Empty counts as unset.
    api_keys_file: str | None = None

    # Require a registry key on every route of the composed app except the probes,
    # GET /health and (on the database tier) GET /health/ready -- the routes that
    # carry no key dependency of their own (/, the docs and OpenAPI pages, the
    # per-module watchdogs) included.
    closed_meta: bool = False

    # Comma-separated names of the modules to mount (see ALL_MODULES); empty
    # means all of them. Kept a plain string because pydantic-settings would
    # JSON-decode a list-typed field. Modules left out are never imported, so
    # an install without the `data` extra can serve the ones that need no database.
    modules: str = ""

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

    @field_validator("history_url", "window_days", "accept_forwarded_writes", "machine_access", mode="before")
    @classmethod
    def _blank_is_unset(cls, value: object, info: ValidationInfo) -> object:
        """Read a blank value as unset rather than as ``""``, an unparseable int or bool."""
        return blank_keeps_default(cls, value, info)

    # Primary (arm) only: accept writes a history head forwards on a user's behalf.
    accept_forwarded_writes: bool = False
    # Whether this instance writes to the machine (Hare PUT, restores); by role when unset.
    machine_access: Literal["read", "write"] | None = None

    @model_validator(mode="after")
    def _machine_access_by_role(self) -> "Settings":
        if self.machine_access is None:
            self.machine_access = "write" if self.role == "primary" else "read"
        return self

    @property
    def machine_writes(self) -> bool:
        """Whether this instance may change the machine (``CANOPY_MACHINE_ACCESS=write``)."""
        return self.machine_access == "write"


def blank_keeps_default(model: type[BaseModel], value: object, info: ValidationInfo) -> object:
    """Read a blank string, as Helm and compose render an unset variable, as the field's default.

    Settings classes call it from a ``mode="before"`` field validator on each field
    where a blank value means unset.

    Parameters
    ----------
    model : type[BaseModel]
        The settings class being validated.
    value : object
        The raw value, a string when it comes from the environment.
    info : ValidationInfo
        Names the field being validated.

    Returns
    -------
    object
        The field's default (its factory's value, for a ``default_factory``) when
        ``value`` is a blank string, otherwise ``value``.
    """
    if isinstance(value, str) and not value.strip():
        return model.model_fields[info.field_name].get_default(call_default_factory=True)
    return value


@lru_cache
def get_settings() -> Settings:
    return Settings()


def describe_settings_error(exc: ValidationError | SettingsError) -> str:
    """Which settings are wrong and why, never their values.

    A ``ValidationError``'s own text (``str`` and ``repr``) echoes its input, which
    may hold a password, so only each error's variable and message are kept. A
    ``SettingsError`` names only the field and its source (a value that isn't JSON).
    """
    if isinstance(exc, SettingsError):
        return str(exc)
    return "; ".join(
        f"CANOPY_{'__'.join(str(part) for part in error['loc']).upper()}: {error['msg']}"
        for error in exc.errors(include_input=False, include_url=False, include_context=False)
    )
