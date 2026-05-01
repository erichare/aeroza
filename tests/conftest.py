"""Shared pytest fixtures.

The unit-only ``client`` fixture has no infrastructure dependencies. Database
fixtures (``integration_db``, ``db_session``) consume ``AEROZA_TEST_DATABASE_URL``
and skip the test when it's missing or the database is unreachable, so a
plain ``pytest`` invocation without docker compose still runs the unit suite.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
import pytest_asyncio
from alembic.config import Config
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from aeroza.main import create_app
from aeroza.shared.db import Database, create_engine_and_session
from alembic import command

INTEGRATION_DB_ENV: str = "AEROZA_TEST_DATABASE_URL"
ALEMBIC_INI: Path = Path(__file__).resolve().parent.parent / "alembic.ini"


def _alembic_config(dsn: str) -> Config:
    cfg = Config(str(ALEMBIC_INI))
    cfg.set_main_option("sqlalchemy.url", dsn)
    return cfg


@pytest.fixture
async def client() -> AsyncIterator[AsyncClient]:
    """ASGI client for the FastAPI app. Pure unit fixture — no DB."""
    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


@pytest_asyncio.fixture(scope="session")
async def integration_db() -> AsyncIterator[Database]:
    """Session-scoped Database against ``AEROZA_TEST_DATABASE_URL``.

    Runs alembic ``upgrade head`` once at session start. Skips integration
    tests entirely when the env var is unset or the database is unreachable.
    """
    dsn = os.environ.get(INTEGRATION_DB_ENV)
    if not dsn:
        pytest.skip(f"{INTEGRATION_DB_ENV} not set; integration tests skipped")

    db = create_engine_and_session(dsn)
    try:
        async with db.engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
    except SQLAlchemyError as exc:
        await db.dispose()
        pytest.skip(f"database unreachable at {dsn}: {exc}")

    # Alembic env.py uses asyncio.run() internally; running it in a worker
    # thread avoids "asyncio.run() cannot be called from a running event loop".
    await asyncio.to_thread(command.upgrade, _alembic_config(dsn), "head")

    try:
        yield db
    finally:
        await db.dispose()


@pytest_asyncio.fixture
async def db_session(integration_db: Database) -> AsyncIterator[AsyncSession]:
    """Per-test session that TRUNCATEs ``nws_alerts`` after the test for isolation."""
    async with integration_db.sessionmaker() as session:
        try:
            yield session
        finally:
            await session.rollback()
            await session.execute(text("TRUNCATE TABLE nws_alerts"))
            await session.commit()
