"""api_key: add is_service

Revision ID: 10fec83070b9
Revises: c3c52ce256d6
Create Date: 2026-10-28 18:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "10fec83070b9"
down_revision: str | None = "c3c52ce256d6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Every existing key is a client key; only scripts/create_key.py --service mints
    # a service key. core.api_key is local to each instance and never replicated,
    # so this column needs no subscriber-first ordering.
    op.add_column(
        "api_key",
        sa.Column("is_service", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        schema="core",
    )


def downgrade() -> None:
    op.drop_column("api_key", "is_service", schema="core")
