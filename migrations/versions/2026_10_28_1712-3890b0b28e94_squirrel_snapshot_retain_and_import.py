"""squirrel: snapshot retain and imported_from

Revision ID: 3890b0b28e94
Revises: 9887d231289e
Create Date: 2026-10-28 17:12:22.926090

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "3890b0b28e94"
down_revision: str | None = "9887d231289e"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # retain exempts a snapshot from pruning; imported_from is the id of the snapshot an
    # import copied, with no foreign key because that snapshot may exist only on a
    # history mirror. A constant default needs no table rewrite on Postgres 11+. With a
    # history mirror, run this there before the primary.
    op.add_column(
        "snapshot",
        sa.Column("retain", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        schema="squirrel",
    )
    op.add_column("snapshot", sa.Column("imported_from", sa.UUID(as_uuid=False), nullable=True), schema="squirrel")


def downgrade() -> None:
    op.drop_column("snapshot", "imported_from", schema="squirrel")
    op.drop_column("snapshot", "retain", schema="squirrel")
