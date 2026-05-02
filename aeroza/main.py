"""FastAPI application entrypoint."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from typing import Final

import structlog
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from aeroza import __version__
from aeroza.auth.routes import router as auth_router
from aeroza.config import Settings, get_settings
from aeroza.query.v1 import router as v1_router
from aeroza.shared.db import create_engine_and_session
from aeroza.stream.nats import NatsAlertSubscriber, nats_connection
from aeroza.webhooks.routes import router as webhooks_router
from aeroza.webhooks.rule_routes import router as alert_rules_router

log = structlog.get_logger(__name__)

API_TITLE: Final = "Aeroza"
API_DESCRIPTION: Final = (
    "Programmable weather intelligence: streaming APIs, geospatial queries, "
    "and probabilistic nowcasting."
)

# Origins the local Next.js dev console is served from. Only applied when
# ``settings.env == "development"`` — production gets no permissive CORS
# until we have real auth + a deployed marketing site to whitelist.
DEV_CONSOLE_ORIGINS: Final[tuple[str, ...]] = (
    "http://localhost:3000",
    "http://127.0.0.1:3000",
)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Open shared resources for the app's lifetime.

    - ``app.state.db`` is mandatory; failure to open it crashes startup.
    - ``app.state.subscriber`` is best-effort: if NATS is unreachable, the
      API still serves DB-backed routes; the streaming endpoint surfaces
      503. Tests bypass this hook entirely (httpx ASGITransport doesn't run
      lifespan) and set both attributes on ``app.state`` themselves.
    """
    settings: Settings = get_settings()
    async with AsyncExitStack() as stack:
        db = create_engine_and_session(settings.database_url)
        stack.push_async_callback(db.dispose)
        app.state.db = db

        subscriber: NatsAlertSubscriber | None
        try:
            nats_client = await stack.enter_async_context(nats_connection(settings.nats_url))
            subscriber = NatsAlertSubscriber(nats_client)
        except Exception as exc:
            log.warning(
                "startup.nats_unavailable",
                url=settings.nats_url,
                error=str(exc),
            )
            subscriber = None

        app.state.subscriber = subscriber
        log.info(
            "startup",
            env=settings.env,
            version=__version__,
            streaming=subscriber is not None,
        )
        yield
        log.info("shutdown")


def create_app() -> FastAPI:
    app = FastAPI(
        title=API_TITLE,
        description=API_DESCRIPTION,
        version=__version__,
        lifespan=lifespan,
    )

    if get_settings().env == "development":
        app.add_middleware(
            CORSMiddleware,
            allow_origins=list(DEV_CONSOLE_ORIGINS),
            allow_credentials=False,
            # The webhook CRUD surface needs the write verbs the dev console
            # uses for its operator UI (PR #5+). Keep CORS dev-only.
            allow_methods=["GET", "POST", "PATCH", "DELETE", "OPTIONS"],
            allow_headers=["*"],
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
    app.include_router(webhooks_router)
    app.include_router(alert_rules_router)
    app.include_router(auth_router)
    return app


app = create_app()
