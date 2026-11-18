"""Service keys: minted only from the CLI, and resolved with ``is_service`` set."""

import sys
import importlib.util
from pathlib import Path

from canopy.core.modules import WILDCARD
from canopy.core.keys.service import ApiKeyService
from canopy.core.keys.registry import DbKeyRegistry
from tests.canopy.core.keys.conftest import db_backed_registry

ADMIN_HEADERS = {"X-API-Key": "test-admin"}
SCRIPT = Path(__file__).parents[4] / "scripts" / "create_key.py"


async def test_a_service_key_resolves_as_one(session, session_maker):
    created = await ApiKeyService(session).create_key(
        "history-site-a", read=True, write=True, service=True, modules=["squirrel"]
    )
    await session.commit()

    resolved = await DbKeyRegistry().get(created.token)

    assert created.is_service
    assert resolved is not None and resolved.is_service


async def test_keys_are_client_keys_by_default(session, session_maker):
    created = await ApiKeyService(session).create_key("trace", read=True, modules=[WILDCARD])
    await session.commit()

    assert not created.is_service
    assert not (await DbKeyRegistry().get(created.token)).is_service


def test_the_http_route_cannot_mint_one(keys_client, session_maker):
    with db_backed_registry(env="test-admin:admin@*"):
        resp = keys_client.post(
            "/v1/api-keys",
            json={"appName": "sneaky", "read": True, "write": True, "isService": True, "service": True},
            headers=ADMIN_HEADERS,
        )

    assert resp.status_code == 200, resp.text
    assert resp.json()["isService"] is False


async def test_the_cli_mints_one(session_maker, monkeypatch, capsys):
    spec = importlib.util.spec_from_file_location("create_key", SCRIPT)
    script = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(script)
    monkeypatch.setattr(sys, "argv", ["create_key.py", "history-site-a", "-r", "-w", "-m", "squirrel", "--service"])

    assert await script.main() == 0

    out = capsys.readouterr().out
    assert "Service:   yes" in out
    token = next(line.split()[-1] for line in out.splitlines() if line.strip().startswith("Token:"))
    assert (await DbKeyRegistry().get(token)).is_service
