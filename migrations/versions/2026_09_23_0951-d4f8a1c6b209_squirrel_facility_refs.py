"""squirrel facility references

Split out of ``aecc42bb60da`` so the ``core.facility`` table lands independently
of the module that consumes it. Both columns reference ``core.facility.id`` by
opaque id with **no cross-schema foreign key** — deliberate, matching the pattern
in ``squirrel/data/models.py``: a module refers to a core row by id and resolves
it through ``core.facilities.gateway``, so neither schema's migrations depend on
the other's.

Revision ID: d4f8a1c6b209
Revises: aecc42bb60da
Create Date: 2026-09-23 09:51:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "d4f8a1c6b209"
down_revision: str | None = "aecc42bb60da"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "pv_facility",
        sa.Column("pv_id", sa.UUID(as_uuid=False), nullable=False),
        sa.Column("facility_id", sa.UUID(as_uuid=False), nullable=False),
        sa.ForeignKeyConstraint(["pv_id"], ["squirrel.pv.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("pv_id", "facility_id"),
        schema="squirrel",
    )
    op.create_index(
        op.f("ix_squirrel_pv_facility_facility_id"), "pv_facility", ["facility_id"], unique=False, schema="squirrel"
    )
    op.add_column("snapshot", sa.Column("facility_id", sa.UUID(as_uuid=False), nullable=True), schema="squirrel")
    op.create_index(
        op.f("ix_squirrel_snapshot_facility_id"), "snapshot", ["facility_id"], unique=False, schema="squirrel"
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_squirrel_snapshot_facility_id"), table_name="snapshot", schema="squirrel")
    op.drop_column("snapshot", "facility_id", schema="squirrel")
    op.drop_index(op.f("ix_squirrel_pv_facility_facility_id"), table_name="pv_facility", schema="squirrel")
    op.drop_table("pv_facility", schema="squirrel")
