"""FastAPI application entrypoint."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Final

import structlog
from fastapi import FastAPI

from aeroza import __version__
from aeroza.config import Settings, get_settings
from aeroza.query.v1 import router as v1_router
from aeroza.shared.db import create_engine_and_session

log = structlog.get_logger(__name__)

API_TITLE: Final = "Aeroza"
API_DESCRIPTION: Final = (
    "Programmable weather intelligence: streaming APIs, geospatial queries, "
    "and probabilistic nowcasting."
)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Open a single Database for the app's lifetime, dispose on shutdown.

    Tests that drive the ASGI app via ``httpx.ASGITransport`` skip this hook
    (httpx does not run lifespan by default), so they must set
    ``app.state.db`` themselves.
    """
    settings: Settings = get_settings()
    db = create_engine_and_session(settings.database_url)
    app.state.db = db
    log.info("startup", env=settings.env, version=__version__)
    try:
        yield
    finally:
        await db.dispose()
        log.info("shutdown")


def create_app() -> FastAPI:
    app = FastAPI(
        title=API_TITLE,
        description=API_DESCRIPTION,
        version=__version__,
        lifespan=lifespan,
    )

    @app.get("/health", tags=["meta"])
    async def health() -> dict[str, str]:
        return {"status": "ok", "version": __version__}

    @app.get("/", tags=["meta"])
    async def root() -> dict[str, str]:
        return {
            "name": API_TITLE,
            "version": __version__,
            "docs": "/docs",
        }

    app.include_router(v1_router)
    return app


app = create_app()
