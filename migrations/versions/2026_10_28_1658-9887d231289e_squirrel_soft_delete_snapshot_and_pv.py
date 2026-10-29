"""squirrel: soft delete snapshot and pv

Revision ID: 9887d231289e
Revises: 9d94c7525c9d
Create Date: 2026-10-28 16:58:02.501085

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "9887d231289e"
down_revision: str | None = "9d94c7525c9d"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Deleting a snapshot or a PV sets deleted_at instead of removing the row. Nullable
    # with no default, so existing rows stay live and the ALTER doesn't rewrite the table.
    # With a history mirror, run this there before the primary: an added column has to
    # exist on the subscriber before the primary starts sending it.
    op.add_column("pv", sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True), schema="squirrel")
    op.add_column("snapshot", sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True), schema="squirrel")


def downgrade() -> None:
    # Soft-deleted rows come back as live ones.
    op.drop_column("snapshot", "deleted_at", schema="squirrel")
    op.drop_column("pv", "deleted_at", schema="squirrel")
