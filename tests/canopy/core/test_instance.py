"""``GET /v1/instance``: the role and history fields from settings, and the shipped Alembic head."""

import sys

import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory
from fastapi.testclient import TestClient

from canopy.core import instance, security
from canopy.main import app
from canopy.core.config import get_settings
from canopy.core.modules import WILDCARD
from canopy.core.instance import shipped_alembic_head
from canopy.core.security import APIKey

READ_KEY = APIKey(token="ro", read=True, write=False, modules=frozenset({WILDCARD}), name="ro")
SHIPPED_HEAD = "0123456789ab"


class FakeRegistry:
    async def get(self, token: str) -> APIKey | None:
        return READ_KEY if token == READ_KEY.token else None


@pytest.fixture
def client(monkeypatch):
    # The endpoint reports whatever the shipped tree says; TestShippedAlembicHead covers reading it.
    monkeypatch.setattr(instance, "shipped_alembic_head", lambda: SHIPPED_HEAD)
    previous = security._registry
    security.bind_key_registry(FakeRegistry())
    yield TestClient(app)
    security._registry = previous


@pytest.fixture
def history_settings(monkeypatch, mirror_role):
    monkeypatch.setenv("CANOPY_INSTANCE", "site-a")
    monkeypatch.setenv("CANOPY_HISTORY_URL", "https://history.example.org")
    get_settings.cache_clear()


def test_primary_defaults(client):
    resp = client.get("/v1/instance", headers={"X-API-Key": "ro"})

    assert resp.status_code == 200, resp.text
    assert resp.json() == {
        "role": "primary",
        "instance": None,
        "historyUrl": None,
        "windowDays": None,
        "alembicHead": SHIPPED_HEAD,
        "arms": [],
    }


def test_mirror_reports_its_role_and_history_fields(client, history_settings):
    resp = client.get("/v1/instance", headers={"X-API-Key": "ro"})

    assert resp.status_code == 200, resp.text
    assert resp.json() == {
        "role": "mirror",
        "instance": "site-a",
        "historyUrl": "https://history.example.org",
        "windowDays": None,
        "alembicHead": SHIPPED_HEAD,
        "arms": [],
    }


def test_a_head_lists_its_arms_and_their_subscriptions(client, history_settings, monkeypatch):
    monkeypatch.setenv("CANOPY_ARMS__ARM2__PUBLISHER_DSN", "host=arm2.example.org")
    monkeypatch.setenv("CANOPY_ARMS__ARM1__PUBLISHER_DSN", "host=arm1.example.org")

    async def states(session, arms):
        assert arms == ["arm1", "arm2"]
        return {"arm1": "streaming", "arm2": "missing"}

    monkeypatch.setattr(instance, "arm_states", states)
    resp = client.get("/v1/instance", headers={"X-API-Key": "ro"})

    assert resp.status_code == 200, resp.text
    assert resp.json()["arms"] == [
        {"name": "arm1", "subscription": "streaming"},
        {"name": "arm2", "subscription": "missing"},
    ]
    assert "arm1.example.org" not in resp.text


def test_a_primary_lists_no_arms_even_if_some_are_set(client, monkeypatch):
    monkeypatch.setenv("CANOPY_ARMS__ARM1__PUBLISHER_DSN", "host=arm1.example.org")
    assert client.get("/v1/instance", headers={"X-API-Key": "ro"}).json()["arms"] == []


def test_requires_a_key(client):
    assert client.get("/v1/instance").status_code == 401


def _write_tree(root, revisions: dict[str, str | None], script_location: str, ini_extra: str = "") -> None:
    """An ``alembic.ini`` in ``root`` and one empty migration per ``{revision: down_revision}``."""
    versions = root / "migrations" / "versions"
    versions.mkdir(parents=True)
    for revision, down_revision in revisions.items():
        (versions / f"{revision}_step.py").write_text(
            f"revision = {revision!r}\ndown_revision = {down_revision!r}\nbranch_labels = None\ndepends_on = None\n"
        )
    (root / "alembic.ini").write_text(f"[alembic]\nscript_location = {script_location}\n{ini_extra}")


class TestShippedAlembicHead:
    @pytest.fixture(autouse=True)
    def _uncached(self, monkeypatch):
        monkeypatch.delenv("ALEMBIC_CONFIG", raising=False)
        shipped_alembic_head.cache_clear()
        yield
        shipped_alembic_head.cache_clear()

    def test_reads_alembic_ini_in_the_working_directory(self, monkeypatch, tmp_path):
        # The prod image's layout: /app/alembic.ini with a relative script_location.
        _write_tree(tmp_path, {"aaaa00000001": None, "aaaa00000002": "aaaa00000001"}, "migrations")
        monkeypatch.chdir(tmp_path)

        assert shipped_alembic_head() == "aaaa00000002"

    def test_honors_alembic_config(self, monkeypatch, tmp_path):
        _write_tree(tmp_path, {"aaaa00000001": None}, str(tmp_path / "migrations"))
        monkeypatch.setenv("ALEMBIC_CONFIG", str(tmp_path / "alembic.ini"))

        assert shipped_alembic_head() == "aaaa00000001"

    def test_several_heads_are_none(self, monkeypatch, tmp_path):
        _write_tree(
            tmp_path,
            {"aaaa00000001": None, "aaaa00000002": "aaaa00000001", "aaaa00000003": "aaaa00000001"},
            str(tmp_path / "migrations"),
        )
        monkeypatch.setenv("ALEMBIC_CONFIG", str(tmp_path / "alembic.ini"))

        assert shipped_alembic_head() is None

    def test_a_broken_revision_file_is_none_not_an_error(self, monkeypatch, tmp_path):
        # Alembic raises the file's own SyntaxError here, not a CommandError.
        _write_tree(tmp_path, {"aaaa00000001": None}, str(tmp_path / "migrations"))
        (tmp_path / "migrations" / "versions" / "broken_step.py").write_text("revision = (\n")
        monkeypatch.setenv("ALEMBIC_CONFIG", str(tmp_path / "alembic.ini"))

        assert shipped_alembic_head() is None

    def test_leaves_sys_path_as_it_was(self, monkeypatch, tmp_path):
        # The repo's alembic.ini sets prepend_sys_path, which Alembic applies to sys.path.
        _write_tree(tmp_path, {"aaaa00000001": None}, "migrations", ini_extra="prepend_sys_path = .\n")
        monkeypatch.chdir(tmp_path)
        before = sys.path[:]

        assert shipped_alembic_head() == "aaaa00000001"
        assert sys.path == before

    def test_missing_tree_is_none_not_an_error(self, monkeypatch, tmp_path):
        monkeypatch.setenv("ALEMBIC_CONFIG", str(tmp_path / "absent.ini"))

        assert shipped_alembic_head() is None

    def test_the_repo_tree_is_readable(self):
        # Without a database: the head comes from the version files, which must parse.
        assert ScriptDirectory.from_config(Config("alembic.ini")).get_heads()
