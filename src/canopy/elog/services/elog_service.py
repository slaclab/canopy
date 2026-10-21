"""E-log service: resolves ``{app}`` to its adapter and fronts every operation.

Each client app posts through its own adapter (its own token), so every call
starts by resolving the app: a malformed name is 400, a disabled module 503,
an app without a token 404. The config lookup is the exception: it answers
200 for any well-formed name so a client can simply hide its "Post to e-log"
button.
"""

import logging
from typing import Literal

from canopy.elog.config import ElogConfig
from canopy.elog.errors import InvalidAppName, ElogAppNotFound, ElogNotConfigured
from canopy.elog.validation import APP_NAME_RULE, is_app
from canopy.elog.schemas.elog import (
    ElogTagDTO,
    ElogUserDTO,
    ElogConfigDTO,
    ElogLogbookDTO,
    ElogAttachmentDTO,
    ElogEntryCreateDTO,
    ElogEntryResultDTO,
    ElogRecentEntryDTO,
)
from canopy.elog.adapters.base import ElogAdapter
from canopy.elog.schemas.health import ElogAppHealthDTO, ElogHealthSummaryDTO
from canopy.elog.adapters.registry import ElogRegistry

logger = logging.getLogger(__name__)


def _check_app_name(app: str) -> None:
    if not is_app(app):
        raise InvalidAppName(f"Invalid app name '{app}' ({APP_NAME_RULE})")


def _no_token_reason(app: str) -> str:
    return f"No e-log token is configured for app '{app}'"


class ElogService:
    def __init__(self, config: ElogConfig, registry: ElogRegistry) -> None:
        self._config = config
        self._registry = registry

    @property
    def registry(self) -> ElogRegistry:
        return self._registry

    def _adapter_for(self, app: str) -> ElogAdapter:
        _check_app_name(app)
        if not self._registry.enabled:
            raise ElogNotConfigured(self._registry.disabled_reason or "E-log is not configured")
        adapter = self._registry.get(app)
        if adapter is None:
            raise ElogAppNotFound(_no_token_reason(app))
        return adapter

    def app_config(self, app: str) -> ElogConfigDTO:
        _check_app_name(app)
        if not self._registry.enabled:
            return ElogConfigDTO(enabled=False, reason=self._registry.disabled_reason)
        if self._registry.get(app) is None:
            return ElogConfigDTO(enabled=False, provider=self._registry.provider, reason=_no_token_reason(app))
        return ElogConfigDTO(
            enabled=True,
            provider=self._registry.provider,
            default_logbook_ids=list(self._config.apps[app].default_logbook_ids),
        )

    async def list_logbooks(self, app: str) -> list[ElogLogbookDTO]:
        return await self._adapter_for(app).list_logbooks()

    async def list_tags(self, app: str, logbook_id: str) -> list[ElogTagDTO]:
        return await self._adapter_for(app).list_tags(logbook_id)

    async def search_users(self, app: str, search: str, *, limit: int = 20) -> list[ElogUserDTO]:
        return await self._adapter_for(app).search_users(search, limit)

    async def list_recent_entries(
        self, app: str, logbook_ids: list[str], *, limit: int = 5
    ) -> list[ElogRecentEntryDTO]:
        return await self._adapter_for(app).list_recent_entries(logbook_ids, limit)

    async def upload_attachment(
        self, app: str, *, filename: str, content_type: str | None, data: bytes
    ) -> ElogAttachmentDTO:
        return await self._adapter_for(app).upload_attachment(filename, content_type, data)

    async def create_entry(self, app: str, entry: ElogEntryCreateDTO, *, requested_by: str) -> ElogEntryResultDTO:
        """Post ``entry``, as a follow-up when ``entry.follows_up_entry_id`` is set.

        ``requested_by`` is the API key's name, recorded in the log line only:
        the e-log itself credits the app's token. The token and the body are
        never logged.
        """
        adapter = self._adapter_for(app)
        if entry.follows_up_entry_id:
            result = await adapter.create_follow_up(entry.follows_up_entry_id, entry)
        else:
            result = await adapter.create_entry(entry)
        logger.info(
            "E-log entry posted: app=%s api_key=%s logbooks=%s follows_up=%s entry=%s",
            app,
            requested_by,
            ",".join(entry.logbook_ids),
            entry.follows_up_entry_id,
            result.id,
        )
        return result

    async def health_summary(self) -> ElogHealthSummaryDTO:
        """Per-app rollup. See `ElogHealthSummaryDTO.status` for semantics."""
        health_map = await self._registry.list_with_health()
        apps = [
            ElogAppHealthDTO(
                app=app,
                healthy=h.healthy,
                identity=h.identity,
                latency_ms=h.latency_ms,
                last_error=h.last_error,
            )
            for app, h in health_map.items()
        ]
        if not apps:
            status: Literal["ok", "degraded", "down", "disabled"] = "disabled"
        elif all(a.healthy for a in apps):
            status = "ok"
        elif not any(a.healthy for a in apps):
            status = "down"
        else:
            status = "degraded"
        return ElogHealthSummaryDTO(status=status, provider=self._registry.provider, apps=apps)

    async def close(self) -> None:
        await self._registry.close_all()
