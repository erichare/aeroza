"""FastAPI application entrypoint."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Final

import structlog
from fastapi import FastAPI

from aeroza import __version__
from aeroza.config import Settings, get_settings

log = structlog.get_logger(__name__)

API_TITLE: Final = "Aeroza"
API_DESCRIPTION: Final = (
    "Programmable weather intelligence: streaming APIs, geospatial queries, "
    "and probabilistic nowcasting."
)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings: Settings = get_settings()
    log.info("startup", env=settings.env, version=__version__)
    yield
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

    return app


app = create_app()
