"""Shared test fixtures for the Canopy backend test suite."""

import os
import asyncio
from contextlib import contextmanager
from unittest.mock import patch

import pytest

# Prevent pydantic-settings from reading the developer's local .env file.
# Tests must be deterministic regardless of .env contents. Env-var-based tests
# (monkeypatch.setenv) still work because EnvSettingsSource is a separate path.
patch("pydantic_settings.sources.providers.dotenv.dotenv_values", return_value={}).start()

# Seed an API-key registry before canopy.main imports cache settings.
os.environ["CANOPY_API_KEYS"] = "test-read:read@*,test-write:read+write@*,test-admin:admin@*"
# CANOPY_INSTANCE is required; the suite runs as an arm named arm1 unless a test says otherwise.
os.environ.setdefault("CANOPY_INSTANCE", "arm1")


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
def mirror_role(monkeypatch):
    """Run the test as a mirror (``CANOPY_ROLE=mirror``).

    Clears the cached ``Settings`` on both sides: ``monkeypatch`` puts the env var
    back but not the object ``get_settings`` already built from it, and a leaked
    role would change whatever test runs next.
    """
    from canopy.core.config import get_settings

    monkeypatch.setenv("CANOPY_ROLE", "mirror")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
def as_instance(monkeypatch):
    """Run a block as another instance: ``with as_instance("arm2"): ...``.

    Rows created inside are stamped with that name, the way a mirror receives an
    arm's rows through replication; outside it the suite is ``arm1`` again. Clears
    the cached ``Settings`` on the way in and out.
    """
    from canopy.core.config import get_settings

    @contextmanager
    def _as(name: str):
        previous = os.environ["CANOPY_INSTANCE"]
        monkeypatch.setenv("CANOPY_INSTANCE", name)
        get_settings.cache_clear()
        try:
            yield
        finally:
            monkeypatch.setenv("CANOPY_INSTANCE", previous)
            get_settings.cache_clear()

    yield _as
    get_settings.cache_clear()
