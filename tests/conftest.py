"""Shared test fixtures for the Canopy backend test suite."""

import os
import asyncio
from unittest.mock import patch

import pytest

# Prevent pydantic-settings from reading the developer's local .env file.
# Tests must be deterministic regardless of .env contents. Env-var-based tests
# (monkeypatch.setenv) still work because EnvSettingsSource is a separate path.
patch("pydantic_settings.sources.providers.dotenv.dotenv_values", return_value={}).start()

# Seed an API-key registry before canopy.main imports cache settings.
os.environ["CANOPY_API_KEYS"] = "test-read:read@*,test-write:read+write@*,test-admin:admin@*"


READ_HEADERS = {"X-API-Key": "test-read"}
WRITE_HEADERS = {"X-API-Key": "test-write"}
ADMIN_HEADERS = {"X-API-Key": "test-admin"}


async def wait_for(predicate, timeout: float = 5.0, interval: float = 0.01) -> bool:
    """Poll ``predicate`` until it is truthy or ``timeout`` elapses.

    Use instead of a fixed ``asyncio.sleep`` when waiting on background tasks,
    so slow CI runners get more time without slowing down fast ones.
    """
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval)
    return False


@pytest.fixture
def forward_role(monkeypatch):
    """Run the test as a forward instance (``CANOPY_ROLE=forward``) named ``site_a``.

    Points ``CANOPY_FORWARD__*`` at a placeholder primary; tests mock it with
    ``httpx_mock``. Clears the cached ``Settings`` like ``mirror_role``.
    """
    from canopy.core.config import get_settings

    monkeypatch.setenv("CANOPY_ROLE", "forward")
    monkeypatch.setenv("CANOPY_INSTANCE", "site_a")
    monkeypatch.setenv("CANOPY_FORWARD__PRIMARY_URL", "https://primary.example.org")
    monkeypatch.setenv("CANOPY_FORWARD__SERVICE_KEY", "service-token")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
def mirror_role(monkeypatch):
    """Run the test as a read-only mirror (``CANOPY_ROLE=mirror``).

    Clears the cached ``Settings`` on both sides: ``monkeypatch`` puts the env var
    back but not the object ``get_settings`` already built from it, and a leaked
    mirror role would refuse every write in whatever test runs next.
    """
    from canopy.core.config import get_settings

    monkeypatch.setenv("CANOPY_ROLE", "mirror")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()
