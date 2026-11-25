"""The migration side helpers, and ``migrations/env.py`` recording the side from ``CANOPY_ROLE``."""

import io
import shutil
import logging
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from alembic.operations import Operations
from alembic.runtime.migration import MigrationContext

from canopy.core.config import get_settings
from canopy.core.replication.migration import (
    OPTION,
    REQUIRES_PRIMARY_AT,
    on_publisher,
    on_subscriber,
    side_for_role,
    migration_side,
)

ROOT = Path(__file__).resolve().parents[4]

# A one-revision tree whose upgrade emits different SQL on each side.
PROBE = '''"""probe

Revision ID: probe
Revises:
"""

from alembic import op

from canopy.core.replication.migration import on_publisher

revision = "probe"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SELECT 'publisher step'" if on_publisher() else "SELECT 'subscriber step'")


def downgrade() -> None:
    pass
'''


@pytest.mark.parametrize(("role", "side"), [("primary", "publisher"), ("mirror", "subscriber")])
def test_an_arm_publishes_and_a_head_subscribes(role, side):
    assert side_for_role(role) == side


@pytest.mark.parametrize("side", ["publisher", "subscriber"])
def test_helpers_read_the_side_from_the_migration_context(side):
    context = MigrationContext.configure(dialect_name="postgresql", opts={OPTION: side})
    with Operations.context(context):
        assert migration_side() == side
        assert on_publisher() is (side == "publisher")
        assert on_subscriber() is (side == "subscriber")


def test_a_context_without_a_side_refuses_to_guess():
    context = MigrationContext.configure(dialect_name="postgresql")
    with Operations.context(context), pytest.raises(RuntimeError, match="no replication side"):
        on_publisher()


# A second revision on top of the probe.
PROBE_2 = PROBE.replace('revision = "probe"', 'revision = "probe_2"').replace(
    "down_revision = None", 'down_revision = "probe"'
)


def _offline_upgrade(tmp_path, monkeypatch, role: str, revisions: dict[str, str]) -> str:
    """Run the shipped ``env.py`` offline with ``CANOPY_ROLE=role`` over ``revisions``; return its SQL."""
    scripts = tmp_path / "migrations"
    (scripts / "versions").mkdir(parents=True)
    shutil.copy(ROOT / "migrations" / "env.py", scripts / "env.py")
    shutil.copy(ROOT / "migrations" / "script.py.mako", scripts / "script.py.mako")
    for name, source in revisions.items():
        (scripts / "versions" / f"{name}.py").write_text(source)
    monkeypatch.setenv("CANOPY_ROLE", role)
    get_settings.cache_clear()
    sql = io.StringIO()
    config = Config(output_buffer=sql)
    config.set_main_option("script_location", str(scripts))
    try:
        command.upgrade(config, "head", sql=True)
    finally:
        get_settings.cache_clear()
    return sql.getvalue()


@pytest.mark.parametrize(
    ("role", "side"), [("primary", "publisher"), ("mirror", "subscriber")], ids=["primary", "mirror"]
)
def test_env_takes_the_side_from_canopy_role(tmp_path, monkeypatch, caplog, role, side):
    """The shipped ``env.py``, run offline over two probe revisions, migrates the side ``CANOPY_ROLE`` names."""
    caplog.set_level(logging.INFO, logger="alembic.env")
    sql = _offline_upgrade(tmp_path, monkeypatch, role, {"probe": PROBE, "probe_2": PROBE_2})
    assert sql.count(f"SELECT '{side} step'") == 2
    assert caplog.text.count(f"CANOPY_ROLE={role}: migrated as the replication {side}") == 1  # once a run


def test_env_names_no_side_when_no_step_applies(tmp_path, monkeypatch, caplog):
    # As for `alembic check`, or an upgrade with nothing to do: env.py runs, no step does.
    caplog.set_level(logging.INFO, logger="alembic.env")
    _offline_upgrade(tmp_path, monkeypatch, "mirror", {})
    assert "migrated as the replication" not in caplog.text


def test_a_required_primary_revision_comes_earlier_in_the_tree():
    """``canopy_requires_primary_at`` must name a revision before the one that declares it."""
    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(ROOT / "migrations"))
    ordered = [r.revision for r in reversed(list(ScriptDirectory.from_config(config).walk_revisions()))]
    script = ScriptDirectory.from_config(config)
    for revision in ordered:
        required = getattr(script.get_revision(revision).module, REQUIRES_PRIMARY_AT, None)
        if required is not None:
            assert required in ordered[: ordered.index(revision)], f"{revision} requires {required}"
