"""FastAPI dependencies for query routes."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import Depends, HTTPException, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from aeroza.shared.db import Database


def get_db(request: Request) -> Database:
    """Return the app-scoped :class:`Database` set up in the lifespan handler.

    Raises 503 when the application is not properly initialised — that
    indicates either the app is shutting down or, in tests, the fixture
    forgot to populate ``app.state.db``.
    """
    db: Database | None = getattr(request.app.state, "db", None)
    if db is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="database not available",
        )
    return db


DatabaseDep = Annotated[Database, Depends(get_db)]


async def get_session(db: DatabaseDep) -> AsyncIterator[AsyncSession]:
    """Yield a fresh :class:`AsyncSession` per request.

    Read endpoints don't need an explicit transaction boundary; the session
    closes (releasing the connection) on exit.
    """
    async with db.sessionmaker() as session:
        yield session
