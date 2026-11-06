"""merge the Squirrel facility and API key rename heads

#26's d4f8a1c6b209 and #48's ca592f659279 both revise aecc42bb60da, so the tree
had two heads and ``alembic upgrade head`` refused to run. This revision joins
them without changing either, so every existing database upgrades from where it
is: one at either head gets the other one applied, and one that ran
``upgrade heads`` gets only this revision.

Revision ID: 18ac64c44669
Revises: ca592f659279, d4f8a1c6b209
Create Date: 2026-11-06 11:47:32.388342

"""

from collections.abc import Sequence

# revision identifiers, used by Alembic.
revision: str = "18ac64c44669"
down_revision: str | tuple[str, ...] | None = ("ca592f659279", "d4f8a1c6b209")
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
