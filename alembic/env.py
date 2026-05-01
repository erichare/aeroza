"""Alembic environment for Aeroza.

Reads the DSN from :class:`aeroza.config.Settings` so the migration tool
shares one source of truth with the application. Both online (live engine)
and offline (SQL-only emit) modes are supported. All ORM model modules must
be imported here so that ``Base.metadata`` is fully populated before autogen
or upgrade runs.
"""

from __future__ import annotations

import asyncio
from logging.config import fileConfig

from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

# Importing the model modules registers their tables on Base.metadata.
import aeroza.ingest.mrms_grids_models
import aeroza.ingest.mrms_models
import aeroza.ingest.nws_alerts_models  # noqa: F401  (side-effect import)
from aeroza.config import get_settings
from aeroza.shared.base import Base
from alembic import context

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Allow callers (alembic CLI, integration tests, ad-hoc scripts) to pre-set
# sqlalchemy.url on the Config object before invoking the env. Only fall back
# to the application settings when no URL was supplied.
if not config.get_main_option("sqlalchemy.url"):
    config.set_main_option("sqlalchemy.url", get_settings().database_url)

target_metadata = Base.metadata


def _include_object(_obj: object, name: str | None, type_: str, *_args: object) -> bool:
    """Skip PostGIS-managed objects so autogenerate doesn't try to drop them."""
    if type_ == "table" and name == "spatial_ref_sys":
        return False
    return not (type_ == "index" and name and "spatial_ref_sys" in name)


def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_object=_include_object,
        compare_type=True,
        compare_server_default=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def _do_run_migrations(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        include_object=_include_object,
        compare_type=True,
        compare_server_default=True,
    )
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    section = config.get_section(config.config_ini_section) or {}
    connectable = async_engine_from_config(
        section,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    async with connectable.connect() as connection:
        await connection.run_sync(_do_run_migrations)
    await connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())
