"""Provider resolution + the per-app adapter registry.

One provider per replica, one adapter instance per configured app: each app
posts with its own token, so each gets its own client and cached identity.
The type-resolution mechanism mirrors Raccoon's and Mole's, so a
`module:Class` dotted path in ``CANOPY_ELOG__PROVIDER`` loads an out-of-tree
adapter that has been pip-installed alongside Canopy.

A misconfigured provider leaves the module disabled (with the reason logged
and reported by every app's config route) instead of failing startup: e-log
posting is an optional integration and must not take the rest of Canopy down.
"""

import time
import asyncio
import logging
import importlib

from canopy.elog.config import ElogConfig
from canopy.elog.validation import is_app
from canopy.elog.adapters.base import (
    ElogAdapter,
    AdapterHealth,
    get_registered_provider,
)

logger = logging.getLogger(__name__)


class ElogRegistry:
    """The configured provider's adapters, keyed by app name.

    ``provider`` is ``None`` when the module is disabled; ``disabled_reason``
    then says why.
    """

    def __init__(
        self,
        provider: str | None,
        adapters: dict[str, ElogAdapter] | None = None,
        *,
        disabled_reason: str | None = None,
    ) -> None:
        self.provider = provider
        self.disabled_reason = disabled_reason
        self._adapters = dict(adapters or {})

    @property
    def enabled(self) -> bool:
        return self.provider is not None

    @property
    def apps(self) -> list[str]:
        return sorted(self._adapters)

    def get(self, app: str) -> ElogAdapter | None:
        return self._adapters.get(app)

    async def list_with_health(self, timeout_s: float = 2.0) -> dict[str, AdapterHealth]:
        """Probe every adapter concurrently. Per-probe failures don't fail the call."""

        async def _probe(app: str, adapter: ElogAdapter) -> tuple[str, AdapterHealth]:
            start = time.monotonic()
            try:
                return app, await asyncio.wait_for(adapter.health(), timeout=timeout_s)
            except TimeoutError:
                last_error = "health probe timed out"
            except Exception as exc:
                last_error = str(exc)
            return app, AdapterHealth(
                healthy=False, latency_ms=(time.monotonic() - start) * 1000, last_error=last_error
            )

        results = await asyncio.gather(*(_probe(app, a) for app, a in sorted(self._adapters.items())))
        return dict(results)

    async def close_all(self) -> None:
        """Close every adapter; log but don't propagate errors (shutdown path)."""
        for app, adapter in self._adapters.items():
            try:
                await adapter.close()
            except Exception:
                logger.exception("Error closing e-log adapter for app %s", app)


def _resolve_provider_class(provider: str) -> type[ElogAdapter]:
    """Resolve ``CANOPY_ELOG__PROVIDER`` to an adapter class.

    Two forms supported:
      - ``"<short_name>"`` -- a ``@register_elog_provider``-decorated class.
      - ``"module.path:ClassName"`` -- dynamic import for out-of-tree adapters.
    """
    if ":" in provider:
        module_path, _, class_name = provider.partition(":")
        try:
            module = importlib.import_module(module_path)
        except ImportError as exc:
            raise ValueError(f"Cannot import e-log adapter module '{module_path}': {exc}") from exc
        cls = getattr(module, class_name, None)
        if cls is None:
            raise ValueError(f"E-log adapter class '{class_name}' not found in '{module_path}'")
        if not isinstance(cls, type) or not callable(getattr(cls, "from_config", None)):
            raise ValueError(f"'{provider}' is not an e-log adapter class (it has no from_config classmethod)")
        return cls

    cls = get_registered_provider(provider)
    if cls is None:
        raise ValueError(
            f"Unknown e-log provider '{provider}'. Use a registered short name or a 'module:Class' dotted path."
        )
    return cls


def build_registry(config: ElogConfig) -> ElogRegistry:
    """Construct the registry from `ElogConfig` at app startup.

    Apps with no token (or a name the ``{app}`` route segment could never
    match) are skipped with a warning, so their config route reports
    ``enabled=false`` rather than the whole module going dark.
    """
    if not config.provider:
        return ElogRegistry(None, disabled_reason="No e-log provider is configured (CANOPY_ELOG__PROVIDER is empty)")
    try:
        cls = _resolve_provider_class(config.provider)
    except ValueError as exc:
        logger.error("E-log disabled: %s", exc)
        return ElogRegistry(None, disabled_reason=str(exc))

    adapters: dict[str, ElogAdapter] = {}
    for app, app_config in sorted(config.apps.items()):
        if not is_app(app):
            logger.warning("Skipping e-log app %r: not a valid app name", app)
            continue
        if app_config.token is None or not app_config.token.get_secret_value():
            logger.warning("Skipping e-log app '%s': no token configured (CANOPY_ELOG__APPS__<APP>__TOKEN)", app)
            continue
        try:
            adapters[app] = cls.from_config(app, app_config, config)
        except Exception as exc:  # an out-of-tree adapter may raise anything; none of it may abort startup
            # Provider-level config (base URL, ...) is shared by every app, so
            # one app failing here means they all would: disable the module.
            # Adapters built so far have sent nothing, so dropping them is enough.
            logger.error("E-log disabled: building the '%s' adapter for app '%s' failed: %s", config.provider, app, exc)
            return ElogRegistry(None, disabled_reason=str(exc))
    return ElogRegistry(config.provider, adapters)
