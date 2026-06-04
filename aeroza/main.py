"""FastAPI application entrypoint."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager, suppress
from typing import Final

import structlog
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from aeroza import __version__
from aeroza.admin.routes import router as admin_router
from aeroza.auth.routes import router as auth_router
from aeroza.config import Settings, get_settings
from aeroza.query.v1 import router as v1_router
from aeroza.shared.db import create_engine_and_session
from aeroza.stream.nats import (
    NatsAlertSubscriber,
    NatsMrmsGridSubscriber,
    nats_connection,
)
from aeroza.tiles.cache import get_default_cache
from aeroza.tiles.prewarm import run_prewarm_consumer
from aeroza.tiles.r2 import get_default_r2_client
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
    - When NATS is reachable *and* R2 is not configured (local dev),
      also spin up a background consumer that prewarms the in-process
      tile LRU on every newly-materialised MRMS grid. In production R2
      IS configured and the dedicated ``prewarm`` worker process owns
      CDN population, so the API process skips it to avoid doubling the
      render load. Best-effort either way: if NATS drops mid-run we log
      and let the consumer wind down; the task is cancelled on shutdown.
    """
    settings: Settings = get_settings()
    async with AsyncExitStack() as stack:
        db = create_engine_and_session(settings.database_url)
        stack.push_async_callback(db.dispose)
        app.state.db = db

        subscriber: NatsAlertSubscriber | None
        prewarm_task: asyncio.Task[None] | None = None
        try:
            nats_client = await stack.enter_async_context(nats_connection(settings.nats_url))
            subscriber = NatsAlertSubscriber(nats_client)
            # ``get_default_r2_client`` returns None when any of the
            # AEROZA_R2_* env vars is blank. When R2 *is* configured
            # (production), the dedicated ``prewarm`` worker process
            # owns CDN population — running a second consumer in the API
            # process would only double the render load and steal CPU
            # from request serving, so we skip it here. When R2 is not
            # configured (local dev) there is no separate worker, so we
            # warm the in-process LRU instead, keeping the on-demand tile
            # route from being cold-on-first-hit.
            r2_client = get_default_r2_client()
            if r2_client is None:
                grid_subscriber = NatsMrmsGridSubscriber(nats_client)
                prewarm_task = asyncio.create_task(
                    run_prewarm_consumer(
                        subscriber=grid_subscriber,
                        r2_client=None,
                        lru_cache=get_default_cache(),
                    ),
                    name="tiles.prewarm.consumer",
                )
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
            prewarm=prewarm_task is not None,
        )
        try:
            yield
        finally:
            if prewarm_task is not None:
                prewarm_task.cancel()
                with suppress(asyncio.CancelledError):
                    await prewarm_task
            log.info("shutdown")


def create_app() -> FastAPI:
    app = FastAPI(
        title=API_TITLE,
        description=API_DESCRIPTION,
        version=__version__,
        lifespan=lifespan,
    )

    settings = get_settings()

    # Added before CORS so CORS stays the outermost layer (preflights and 429s
    # still get CORS headers). Per-IP token bucket; see aeroza.shared.ratelimit.
    if settings.rate_limit_enabled:
        from aeroza.shared.ratelimit import InMemoryRateLimiter, RateLimitMiddleware

        app.add_middleware(
            RateLimitMiddleware,
            limiter=InMemoryRateLimiter(
                capacity=settings.rate_limit_burst,
                refill_per_second=settings.rate_limit_requests_per_minute / 60.0,
            ),
        )

    cors_origins: list[str] = []
    if settings.env == "development":
        cors_origins.extend(DEV_CONSOLE_ORIGINS)
    # Always honour AEROZA_CORS_ALLOW_ORIGINS so production deployments
    # (Vercel web ↔ Railway API, etc.) can authorise the front-end host
    # without having to relax env to development.
    cors_origins.extend(
        origin.strip() for origin in settings.cors_allow_origins.split(",") if origin.strip()
    )
    if cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=cors_origins,
            allow_credentials=False,
            # The webhook CRUD surface needs the write verbs the dev
            # console uses for its operator UI (PR #5+). Same shape applies
            # to a deployed dashboard talking back to the API.
            allow_methods=["GET", "POST", "PATCH", "DELETE", "OPTIONS"],
            allow_headers=["*"],
            # Browsers strip non-CORS-safelisted response headers from
            # cross-origin reads unless the server lists them here. The
            # tile route's ``X-Aeroza-*`` family is what the radar loop
            # uses to verify cache state and grid pinning from the
            # browser devtools — without this, ``hit`` / ``miss`` is
            # invisible from the deployed UI even though the server is
            # setting it correctly.
            expose_headers=[
                "X-Aeroza-Tile-Cache",
                "X-Aeroza-Grid-Key",
                "X-Aeroza-Grid-Valid-At",
            ],
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
    app.include_router(admin_router)

    from aeroza.admin.push_test import router as admin_push_router

    app.include_router(admin_push_router)
    return app


app = create_app()
