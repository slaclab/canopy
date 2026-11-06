"""Alembic environment for Canopy's single, multi-module migration tree.

One Postgres database, one schema per persistent module. All modules' ORM
metadata lives on the shared ``canopy.core.db.Base``, so a single autogenerate
pass sees every module's tables; ``include_schemas=True`` makes the comparison
schema-aware, and the lone ``alembic_version`` table is pinned to ``public``.

A history head and each of its arms run this same tree against their own
databases, so the side being migrated (an arm publishes, the head subscribes; from
``CANOPY_ROLE``) goes into the migration context, where
``canopy.core.replication.migration.on_publisher`` and ``on_subscriber`` read it for
the few steps that belong to one side only.
"""

import asyncio
import logging
from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool, text
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

# Register every persistent module's models on Base.metadata so --autogenerate
# sees them. Add an import here when a module gains a data tier.
import canopy.core.keys.models  # noqa: F401, E402
import canopy.magpie.data.models  # noqa: F401, E402
import canopy.squirrel.data.models  # noqa: F401, E402
import canopy.core.facilities.models  # noqa: F401, E402
import canopy.core.replication.models  # noqa: F401, E402
from canopy.core.db import Base
from canopy.core.config import get_settings
from canopy.core.replication.migration import side_for_role

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata

# Pull the DB URL from Canopy settings, but deliberately do NOT round-trip it
# through config.set_main_option(): that stores into ConfigParser, whose
# %-interpolation would mangle (or reject) a password containing a literal '%'.
# We inject it straight into the engine config / migration context below instead.
settings = get_settings()
database_url = settings.database_url
replication_side = side_for_role(settings.role)
logging.getLogger("alembic.env").info("CANOPY_ROLE=%s: migrating the replication %s", settings.role, replication_side)


def _include_object(obj, name, type_, reflected, compare_to) -> bool:
    """Exclude Alembic's own version table from autogenerate comparison.

    ``alembic_version`` lives in ``public`` and belongs to no module's metadata;
    under ``include_schemas=True`` autogenerate would otherwise propose dropping it
    on every run (and ``alembic check`` would always fail). Never manage it.
    """
    if type_ == "table" and name == "alembic_version":
        return False
    return True


def run_migrations_offline() -> None:
    """Emit SQL without a live connection (alembic upgrade --sql)."""
    context.configure(
        url=database_url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_schemas=True,
        version_table_schema="public",
        include_object=_include_object,
        canopy_replication_side=replication_side,
    )
    with context.begin_transaction():
        context.run_migrations()


def _refuse_a_renamed_primary(connection: Connection) -> None:
    """Stop when a primary's rows carry another instance's name.

    A primary holds only rows it wrote, so a stamp other than ``CANOPY_INSTANCE``
    means the name was changed after the instance held data. Its rows would then
    all belong to an instance that no longer exists, and every write to them would
    answer ``not_owner``. An instance's name is permanent, so a rename fails here,
    at the deploy's migration step. A mirror holds its arms' rows beside its own,
    so it isn't checked here. Tables without ``owner_instance`` yet (a database
    below ``89e21441c63c``) are skipped.
    """
    settings = get_settings()
    if settings.role != "primary":
        return
    for table in target_metadata.sorted_tables:
        if "owner_instance" not in table.c:
            continue
        has_column = connection.execute(
            text(
                "SELECT 1 FROM information_schema.columns "
                "WHERE table_schema = :schema AND table_name = :table AND column_name = 'owner_instance'"
            ),
            {"schema": table.schema, "table": table.name},
        ).first()
        if has_column is None:
            continue
        other = connection.execute(
            text(f"SELECT owner_instance FROM {table.fullname} WHERE owner_instance <> :instance LIMIT 1"),
            {"instance": settings.instance},
        ).scalar()
        if other is not None:
            raise RuntimeError(
                f"CANOPY_INSTANCE is {settings.instance!r}, but {table.fullname} holds rows stamped {other!r}. "
                f"An instance's name is permanent: set CANOPY_INSTANCE back to {other!r}."
            )


def do_run_migrations(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        include_schemas=True,
        version_table_schema="public",
        include_object=_include_object,
        canopy_replication_side=replication_side,
    )
    with context.begin_transaction():
        # Inside the migration transaction: a query run before it would open one
        # of its own that Alembic then never commits.
        _refuse_a_renamed_primary(connection)
        context.run_migrations()


async def run_async_migrations() -> None:
    configuration = config.get_section(config.config_ini_section, {})
    configuration["sqlalchemy.url"] = database_url
    connectable = async_engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await connectable.dispose()


def run_migrations_online() -> None:
    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
