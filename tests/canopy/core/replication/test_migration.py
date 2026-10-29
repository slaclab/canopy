"""The migration side helpers, and ``migrations/env.py`` recording the side from ``CANOPY_ROLE``."""

import io
import shutil
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from alembic.operations import Operations
from alembic.runtime.migration import MigrationContext

from canopy.core.config import get_settings
from canopy.core.replication.migration import (
    OPTION,
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


@pytest.mark.parametrize(
    ("role", "side"), [("primary", "publisher"), ("mirror", "subscriber"), ("forward", "subscriber")]
)
def test_only_a_primary_is_the_publisher(role, side):
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


@pytest.mark.parametrize(("role", "step"), [("primary", "publisher step"), ("mirror", "subscriber step")])
def test_env_takes_the_side_from_canopy_role(tmp_path, monkeypatch, role, step):
    """The shipped ``env.py``, run offline over a probe revision, migrates the side ``CANOPY_ROLE`` names."""
    scripts = tmp_path / "migrations"
    (scripts / "versions").mkdir(parents=True)
    shutil.copy(ROOT / "migrations" / "env.py", scripts / "env.py")
    shutil.copy(ROOT / "migrations" / "script.py.mako", scripts / "script.py.mako")
    (scripts / "versions" / "probe.py").write_text(PROBE)
    monkeypatch.setenv("CANOPY_ROLE", role)
    get_settings.cache_clear()
    sql = io.StringIO()
    config = Config(output_buffer=sql)
    config.set_main_option("script_location", str(scripts))
    try:
        command.upgrade(config, "head", sql=True)
    finally:
        get_settings.cache_clear()
    assert f"SELECT '{step}'" in sql.getvalue()
