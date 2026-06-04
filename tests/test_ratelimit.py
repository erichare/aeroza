"""Unit tests for the per-IP rate limiter (no DB, no network beyond ASGI)."""

from __future__ import annotations

import httpx
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import PlainTextResponse
from starlette.routing import Route

from aeroza.shared.ratelimit import (
    InMemoryRateLimiter,
    RateLimitMiddleware,
    TokenBucket,
    _is_limited,
)


def test_token_bucket_burst_then_deny_then_refill() -> None:
    bucket = TokenBucket(capacity=3, refill_per_second=1.0, now=0.0)
    assert bucket.allow(0.0)
    assert bucket.allow(0.0)
    assert bucket.allow(0.0)
    assert not bucket.allow(0.0)
    assert bucket.allow(1.0)  # +1s → one token refilled
    assert not bucket.allow(1.0)


def test_is_limited_paths() -> None:
    assert _is_limited("/v1/alerts")
    assert _is_limited("/v1/push/devices")
    assert not _is_limited("/v1/mrms/tiles/5/1/2.png")
    assert not _is_limited("/health")
    assert not _is_limited("/")


def test_limiter_isolates_keys() -> None:
    limiter = InMemoryRateLimiter(capacity=1, refill_per_second=0.0)
    assert limiter.allow("a")
    assert not limiter.allow("a")
    assert limiter.allow("b")  # different key, fresh bucket


def _app() -> Starlette:
    async def ok(_request: Request) -> PlainTextResponse:
        return PlainTextResponse("ok")

    app = Starlette(
        routes=[
            Route("/v1/x", ok),
            Route("/v1/mrms/tiles/0/0/0.png", ok),
            Route("/health", ok),
        ]
    )
    app.add_middleware(
        RateLimitMiddleware,
        limiter=InMemoryRateLimiter(capacity=2, refill_per_second=0.0),
    )
    return app


async def test_middleware_limits_v1_after_burst() -> None:
    transport = httpx.ASGITransport(app=_app())
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        assert (await client.get("/v1/x")).status_code == 200
        assert (await client.get("/v1/x")).status_code == 200
        assert (await client.get("/v1/x")).status_code == 429


async def test_middleware_exempts_tiles_and_health() -> None:
    transport = httpx.ASGITransport(app=_app())
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        for _ in range(5):
            assert (await client.get("/v1/mrms/tiles/0/0/0.png")).status_code == 200
            assert (await client.get("/health")).status_code == 200
