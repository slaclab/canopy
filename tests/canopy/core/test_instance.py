"""``GET /v1/instance``: the role and history fields from settings, and the shipped Alembic head."""

import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory
from fastapi.testclient import TestClient

from canopy.core import security
from canopy.main import app
from canopy.core.config import get_settings
from canopy.core.modules import WILDCARD
from canopy.core.instance import shipped_alembic_head
from canopy.core.security import APIKey

READ_KEY = APIKey(token="ro", read=True, write=False, modules=frozenset({WILDCARD}), name="ro")


class FakeRegistry:
    async def get(self, token: str) -> APIKey | None:
        return READ_KEY if token == READ_KEY.token else None


@pytest.fixture
def client():
    previous = security._registry
    security.bind_key_registry(FakeRegistry())
    yield TestClient(app)
    security._registry = previous


@pytest.fixture
def history_settings(monkeypatch, mirror_role):
    monkeypatch.setenv("CANOPY_INSTANCE", "lcls")
    monkeypatch.setenv("CANOPY_HISTORY_URL", "https://canopy-history-lcls.example.org")
    get_settings.cache_clear()


def _repo_head() -> str:
    heads = ScriptDirectory.from_config(Config("alembic.ini")).get_heads()
    assert len(heads) == 1, heads
    return heads[0]


def test_primary_defaults(client):
    resp = client.get("/v1/instance", headers={"X-API-Key": "ro"})

    assert resp.status_code == 200, resp.text
    assert resp.json() == {
        "role": "primary",
        "instance": None,
        "historyUrl": None,
        "windowDays": None,
        "alembicHead": _repo_head(),
    }


def test_mirror_reports_its_role_and_history_fields(client, history_settings):
    resp = client.get("/v1/instance", headers={"X-API-Key": "ro"})

    assert resp.status_code == 200, resp.text
    assert resp.json() == {
        "role": "mirror",
        "instance": "lcls",
        "historyUrl": "https://canopy-history-lcls.example.org",
        "windowDays": None,
        "alembicHead": _repo_head(),
    }


def test_requires_a_key(client):
    assert client.get("/v1/instance").status_code == 401


class TestShippedAlembicHead:
    @pytest.fixture(autouse=True)
    def _uncached(self):
        shipped_alembic_head.cache_clear()
        yield
        shipped_alembic_head.cache_clear()

    def test_reads_the_migrations_tree_not_the_database(self):
        # No database is reachable here; the head comes from the version files.
        assert shipped_alembic_head() == _repo_head()

    def test_honors_alembic_config(self, monkeypatch, tmp_path):
        ini = tmp_path / "other.ini"
        ini.write_text("[alembic]\nscript_location = migrations\n")
        monkeypatch.setenv("ALEMBIC_CONFIG", str(ini))

        assert shipped_alembic_head() == _repo_head()

    def test_missing_tree_is_none_not_an_error(self, monkeypatch, tmp_path):
        monkeypatch.setenv("ALEMBIC_CONFIG", str(tmp_path / "absent.ini"))

        assert shipped_alembic_head() is None
