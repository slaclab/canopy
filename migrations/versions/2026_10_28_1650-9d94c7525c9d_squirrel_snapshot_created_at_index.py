"""squirrel: index snapshot (created_at, id)

Revision ID: 9d94c7525c9d
Revises: d4f8a1c6b209
Create Date: 2026-10-28 16:50:47.726175

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "9d94c7525c9d"
down_revision: str | None = "d4f8a1c6b209"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # The snapshot listing pages newest first on (created_at, id), and a time cutoff
    # scans the same key. A plain CREATE INDEX holds off snapshot inserts while it
    # builds, which is brief: the table has one row per save, not one per value.
    op.create_index(
        "ix_squirrel_snapshot_created_at_id", "snapshot", ["created_at", "id"], unique=False, schema="squirrel"
    )


def downgrade() -> None:
    op.drop_index("ix_squirrel_snapshot_created_at_id", table_name="snapshot", schema="squirrel")
