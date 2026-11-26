"""api_key: add facility_ids

Revision ID: ddc02220891f
Revises: 2fac58bd6437
Create Date: 2026-11-23 14:07:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "ddc02220891f"
down_revision: str | None = "2fac58bd6437"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # A key's grant becomes the ids of the facilities it was minted with; `facilities`
    # keeps the slugs it was minted with, for display, and the wildcard. core.api_key is
    # local to each instance and never replicated, so this needs no subscriber-first
    # ordering, and the backfill fills every key here.
    op.add_column(
        "api_key",
        sa.Column(
            "facility_ids",
            postgresql.ARRAY(postgresql.UUID(as_uuid=False)),
            server_default=sa.text("'{}'::uuid[]"),
            nullable=False,
        ),
        schema="core",
    )
    # Each key's slugs, resolved to the facilities holding them now. That is the
    # facility each was minted with: an instance has refused to delete a facility one
    # of its active keys grants since key grants existed, and no history head has run
    # before this revision, so no active key's slug can have moved to another facility.
    # A deactivated key's slug can have (deactivating releases it), but nothing
    # reactivates a key, so what it resolves to here never grants anything. A slug that
    # names no facility stays unresolved, and matches nothing, as it did before.
    op.execute(
        """
        UPDATE core.api_key AS k
        SET facility_ids = ARRAY(
            SELECT f.id FROM core.facility AS f WHERE f.slug = ANY (k.facilities) ORDER BY f.id
        )
        WHERE k.facilities <> '{}' AND NOT ('*' = ANY (k.facilities))
        """
    )


def downgrade() -> None:
    op.drop_column("api_key", "facility_ids", schema="core")
