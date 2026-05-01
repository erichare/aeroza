"""Async SQLAlchemy engine + session factory.

Two-layer split:

- :func:`create_engine_and_session` builds a fresh engine + sessionmaker pair
  from a DSN. It's pure (no module state) so tests can construct one against
  any database without touching the global API engine.
- :func:`session_scope` is a small async-context-manager helper that yields a
  session and rolls back on exception, commits on clean exit. Most callers
  (ingest workers, scripts, CLI commands) want this; HTTP request handlers
  should use FastAPI's ``Depends`` with a per-request override instead.

The FastAPI app does **not** open a global engine at import time — wiring of
an app-scoped engine into ``aeroza.main.lifespan`` is deferred until the API
actually serves DB-backed routes.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)


@dataclass(frozen=True, slots=True)
class Database:
    """An engine + session factory pair, owned by the caller."""

    engine: AsyncEngine
    sessionmaker: async_sessionmaker[AsyncSession]

    async def dispose(self) -> None:
        await self.engine.dispose()


def create_engine_and_session(
    dsn: str,
    *,
    echo: bool = False,
    pool_size: int = 5,
    max_overflow: int = 5,
) -> Database:
    """Build a fresh async engine + sessionmaker for ``dsn``.

    The DSN must be an asyncpg URL (``postgresql+asyncpg://…``); other drivers
    are rejected at engine creation by SQLAlchemy.
    """
    engine = create_async_engine(
        dsn,
        echo=echo,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_pre_ping=True,
        future=True,
    )
    sm = async_sessionmaker(
        bind=engine,
        expire_on_commit=False,
        class_=AsyncSession,
    )
    return Database(engine=engine, sessionmaker=sm)


@asynccontextmanager
async def session_scope(db: Database) -> AsyncIterator[AsyncSession]:
    """Yield a session; commit on clean exit, rollback on exception."""
    async with db.sessionmaker() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
