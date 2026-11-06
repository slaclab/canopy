"""squirrel: snapshot_outbox

Revision ID: 2fac58bd6437
Revises: 10fec83070b9
Create Date: 2026-11-06 12:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "2fac58bd6437"
down_revision: str | None = "10fec83070b9"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # A head's queue of snapshots for arms it can't reach. Local to each database and
    # never published, so it needs no subscriber-first ordering; only a head writes it.
    # No foreign key to core.facility: that table is a replica on a head.
    op.create_table(
        "snapshot_outbox",
        sa.Column("id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("seq", sa.BigInteger(), sa.Identity(always=False), nullable=False),
        sa.Column("arm", sa.String(length=48), nullable=False),
        sa.Column("facility_id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("created_by", sa.String(length=255), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("bundle", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("value_count", sa.Integer(), nullable=False),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("caller", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("strict", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("state", sa.String(length=16), server_default=sa.text("'pending'"), nullable=False),
        sa.Column("attempts", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("queued_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("last_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("failed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.CheckConstraint("state IN ('pending', 'delivered', 'failed')", name="ck_snapshot_outbox_state"),
        sa.PrimaryKeyConstraint("id"),
        schema="squirrel",
    )
    op.create_index(
        "ix_squirrel_snapshot_outbox_arm_state_seq",
        "snapshot_outbox",
        ["arm", "state", "seq"],
        unique=False,
        schema="squirrel",
    )


def downgrade() -> None:
    op.drop_index("ix_squirrel_snapshot_outbox_arm_state_seq", table_name="snapshot_outbox", schema="squirrel")
    op.drop_table("snapshot_outbox", schema="squirrel")
