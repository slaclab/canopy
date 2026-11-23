"""row owners

Stamp every row of the five owned tables with the instance that wrote it
(``owner_instance``), and make PV setpoint addresses and tag-group names unique
per owner instead of everywhere. See ``canopy.core.ownership``.

Every existing row is stamped with this instance's ``CANOPY_INSTANCE``. That is
exact: a mirror can only subscribe from this revision on, so a database that
reaches it holds only rows it wrote. The column has no server default, because
the app stamps every insert, and replication carries the stamp to a mirror.

Later data steps on owned tables run on every instance, filtered to
``owner_instance = local_instance()``.

An older image can't insert into the owned tables once this has run, so stop its
API and workers first, and downgrade before rolling back to it (see "Upgrade" in
``docs/deployment/production.md``). The downgrade refuses while any owned table
holds another instance's rows, since it would drop their stamps and a later upgrade
would stamp them with this instance's name.

Revision ID: 89e21441c63c
Revises: 18ac64c44669
Create Date: 2026-11-06 12:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from canopy.core.config import get_settings

# revision identifiers, used by Alembic.
revision: str = "89e21441c63c"
down_revision: str | None = "18ac64c44669"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

OWNED_TABLES = [
    ("core", "facility"),
    ("squirrel", "pv"),
    ("squirrel", "snapshot"),
    ("squirrel", "tag_group"),
    ("magpie", "config"),
]


def upgrade() -> None:
    instance = get_settings().instance
    for schema, table in OWNED_TABLES:
        op.add_column(table, sa.Column("owner_instance", sa.String(48), nullable=True), schema=schema)
        op.execute(sa.text(f"UPDATE {schema}.{table} SET owner_instance = :instance").bindparams(instance=instance))
        op.alter_column(table, "owner_instance", nullable=False, schema=schema)

    op.drop_index("ix_squirrel_pv_setpoint_address", table_name="pv", schema="squirrel")
    op.create_index("ix_squirrel_pv_setpoint_address", "pv", ["setpoint_address"], unique=False, schema="squirrel")
    op.create_unique_constraint(
        "uq_pv_owner_instance_setpoint_address", "pv", ["owner_instance", "setpoint_address"], schema="squirrel"
    )

    op.drop_index("ix_squirrel_tag_group_name", table_name="tag_group", schema="squirrel")
    op.create_index("ix_squirrel_tag_group_name", "tag_group", ["name"], unique=False, schema="squirrel")
    op.create_unique_constraint(
        "uq_tag_group_owner_instance_name", "tag_group", ["owner_instance", "name"], schema="squirrel"
    )

    op.create_index("ix_squirrel_snapshot_owner_instance", "snapshot", ["owner_instance"], schema="squirrel")


def _refuse_other_owners() -> None:
    """Downgrading drops every stamp, so it can't run while another instance's rows are here."""
    bind, instance = op.get_bind(), get_settings().instance
    for schema, table in OWNED_TABLES:
        owners = (
            bind.execute(
                sa.text(
                    f"SELECT DISTINCT owner_instance FROM {schema}.{table} "
                    "WHERE owner_instance <> :instance ORDER BY owner_instance LIMIT 5"
                ).bindparams(instance=instance)
            )
            .scalars()
            .all()
        )
        if owners:
            raise RuntimeError(
                f"cannot downgrade: {schema}.{table} holds rows owned by {', '.join(owners)}, not "
                f"'{instance}'; downgrading would drop their owner, and an upgrade would stamp them "
                f"'{instance}'"
            )


def downgrade() -> None:
    # Every row is this instance's, so no address or name is shared by two owners,
    # and the unique indexes below can return.
    _refuse_other_owners()

    op.drop_index("ix_squirrel_snapshot_owner_instance", table_name="snapshot", schema="squirrel")

    op.drop_constraint("uq_tag_group_owner_instance_name", "tag_group", type_="unique", schema="squirrel")
    op.drop_index("ix_squirrel_tag_group_name", table_name="tag_group", schema="squirrel")
    op.create_index("ix_squirrel_tag_group_name", "tag_group", ["name"], unique=True, schema="squirrel")

    op.drop_constraint("uq_pv_owner_instance_setpoint_address", "pv", type_="unique", schema="squirrel")
    op.drop_index("ix_squirrel_pv_setpoint_address", table_name="pv", schema="squirrel")
    op.create_index("ix_squirrel_pv_setpoint_address", "pv", ["setpoint_address"], unique=True, schema="squirrel")

    for schema, table in OWNED_TABLES:
        op.drop_column(table, "owner_instance", schema=schema)
