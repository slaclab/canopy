"""core: history_heartbeat

Revision ID: c3c52ce256d6
Revises: 3890b0b28e94
Create Date: 2026-10-28 17:27:08.462281

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "c3c52ce256d6"
down_revision: str | None = "3890b0b28e94"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # A history mirror reports its backup, WAL-archive and drift checks here, on the
    # primary; the prune job reads it. Local to each database: never published.
    op.create_table(
        "history_heartbeat",
        sa.Column("name", sa.String(length=63), nullable=False),
        sa.Column("backup_ok", sa.Boolean(), nullable=False),
        sa.Column("wal_archive_ok", sa.Boolean(), nullable=False),
        sa.Column("drift_ok", sa.Boolean(), nullable=False),
        sa.Column("applied_lsn", sa.Text(), nullable=True),
        sa.Column("written_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("name"),
        schema="core",
    )


def downgrade() -> None:
    op.drop_table("history_heartbeat", schema="core")
