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
from aeroza.stream.subscriber import InMemoryAlertSubscriber
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


@pytest_asyncio.fixture
async def alert_subscriber() -> AsyncIterator[InMemoryAlertSubscriber]:
    """In-memory ``AlertSubscriber`` used by SSE-route tests."""
    subscriber = InMemoryAlertSubscriber()
    yield subscriber
    await subscriber.close()


@pytest_asyncio.fixture(scope="session")
async def grib_payload() -> AsyncIterator[bytes]:
    """Real MRMS GRIB2 payload, fetched once per session for ``@pytest.mark.grib``.

    Skips the test when:
    - cfgrib / eccodes aren't installed (``import cfgrib`` fails);
    - we can't reach the public ``noaa-mrms-pds`` bucket;
    - the bucket has no fresh ``PrecipRate`` files (very rare — published every
      ~2 minutes, but a fallback to "yesterday" is included for the off-hour
      case where today's prefix is empty).

    PrecipRate files are intentionally chosen — they're small (~50-200 KB
    compressed) so the per-session fetch is fast and doesn't dominate CI time.
    """
    try:
        # Importing cfgrib transitively imports gribapi, which raises a plain
        # ``RuntimeError("Cannot find the ecCodes library")`` (not ImportError)
        # when ``libeccodes`` isn't installed system-wide. Catching all three
        # covers every contributor-laptop failure mode we've seen.
        import cfgrib  # noqa: F401 — sentinel for "is eccodes available?"
    except (ImportError, OSError, RuntimeError) as exc:
        pytest.skip(f"cfgrib/eccodes not available: {exc}")

    try:
        from datetime import UTC, datetime, timedelta

        from aeroza.ingest._aws import open_data_s3_client
        from aeroza.ingest.mrms import MrmsFile, list_mrms_files
        from aeroza.ingest.mrms_decode import download_grib2_payload

        s3 = open_data_s3_client()
        now = datetime.now(UTC)
        files: tuple[MrmsFile, ...] = ()
        for offset in (0, 1):  # today, then yesterday
            files = await list_mrms_files(
                product="PrecipRate",
                level="00.00",
                day=now - timedelta(days=offset),
                s3_client=s3,
            )
            if files:
                break
        if not files:
            pytest.skip("No PrecipRate files in noaa-mrms-pds for today or yesterday")

        payload = download_grib2_payload(s3, key=files[-1].key)
    except Exception as exc:
        pytest.skip(f"could not fetch MRMS GRIB fixture: {exc}")

    yield payload


@pytest_asyncio.fixture
async def api_client(
    integration_db: Database,
    alert_subscriber: InMemoryAlertSubscriber,
) -> AsyncIterator[AsyncClient]:
    """ASGI client wired against the test database and an in-memory subscriber.

    httpx's ``ASGITransport`` doesn't run the FastAPI lifespan, so we set
    ``app.state.db`` and ``app.state.subscriber`` directly. After the test we
    TRUNCATE ``nws_alerts`` to keep tests isolated without paying for a full
    drop+recreate between cases.
    """
    app = create_app()
    app.state.db = integration_db
    app.state.subscriber = alert_subscriber
    transport = ASGITransport(app=app)
    try:
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            yield ac
    finally:
        async with integration_db.sessionmaker() as session:
            await session.execute(text("TRUNCATE TABLE nws_alerts"))
            await session.commit()
