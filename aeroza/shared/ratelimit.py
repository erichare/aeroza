"""In-memory per-IP token-bucket rate limiting for the anonymous public API.

Single-process (per-instance) — fine for the current single-instance Railway
deployment. To scale horizontally, swap :class:`InMemoryRateLimiter` for a
Redis-backed bucket (the redis client is already configured) keyed the same way.

The radar tile endpoint (``/v1/mrms/tiles/*``) is **exempt**: one map view loads
dozens of CDN-cached tiles, so limiting it would throttle legitimate use. The
expensive JSON endpoints and device registration are what this protects.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Final

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.types import ASGIApp

_LIMITED_PREFIX: Final[str] = "/v1"
_EXEMPT_PREFIXES: Final[tuple[str, ...]] = ("/v1/mrms/tiles",)
_MAX_TRACKED_KEYS: Final[int] = 50_000


class TokenBucket:
    """A refilling token bucket. Not thread-safe; the limiter serialises access."""

    __slots__ = ("_tokens", "_updated", "capacity", "refill_per_second")

    def __init__(self, *, capacity: float, refill_per_second: float, now: float) -> None:
        self.capacity = capacity
        self.refill_per_second = refill_per_second
        self._tokens = capacity
        self._updated = now

    def allow(self, now: float, cost: float = 1.0) -> bool:
        elapsed = max(0.0, now - self._updated)
        self._tokens = min(self.capacity, self._tokens + elapsed * self.refill_per_second)
        self._updated = now
        if self._tokens >= cost:
            self._tokens -= cost
            return True
        return False


class InMemoryRateLimiter:
    """Per-key token buckets with bounded memory."""

    def __init__(
        self,
        *,
        capacity: float,
        refill_per_second: float,
        now: Callable[[], float] = time.monotonic,
    ) -> None:
        self._capacity = capacity
        self._refill = refill_per_second
        self._now = now
        self._buckets: dict[str, TokenBucket] = {}

    def allow(self, key: str) -> bool:
        now = self._now()
        bucket = self._buckets.get(key)
        if bucket is None:
            if len(self._buckets) >= _MAX_TRACKED_KEYS:
                self._evict_idle(now)
            bucket = TokenBucket(capacity=self._capacity, refill_per_second=self._refill, now=now)
            self._buckets[key] = bucket
        return bucket.allow(now)

    def _evict_idle(self, now: float) -> None:
        """Drop buckets idle long enough to have fully refilled — no state lost."""
        if self._refill <= 0:
            self._buckets.clear()
            return
        full_after = self._capacity / self._refill
        stale = [k for k, b in self._buckets.items() if (now - b._updated) >= full_after]
        for key in stale:
            del self._buckets[key]


def _is_limited(path: str) -> bool:
    if not path.startswith(_LIMITED_PREFIX):
        return False
    return not path.startswith(_EXEMPT_PREFIXES)


def _client_ip(request: Request) -> str:
    """Best-effort real client IP behind Cloudflare / Railway proxies."""
    cf = request.headers.get("cf-connecting-ip")
    if cf:
        return cf.strip()
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Reject over-limit requests to ``/v1/*`` (tiles exempt) with 429."""

    def __init__(self, app: ASGIApp, *, limiter: InMemoryRateLimiter) -> None:
        super().__init__(app)
        self._limiter = limiter

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        if _is_limited(request.url.path) and not self._limiter.allow(_client_ip(request)):
            return JSONResponse(
                {"detail": "rate limit exceeded"},
                status_code=429,
                headers={"Retry-After": "1"},
            )
        return await call_next(request)
