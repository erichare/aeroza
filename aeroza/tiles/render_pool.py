"""Shared concurrency limiter for tile rendering.

The MRMS tile route hands the actual render off to a worker thread via
:func:`asyncio.to_thread` (Pillow + numpy are CPU-bound). Without a
limit, a burst of map-pan / radar-loop tiles fans out into N concurrent
worker threads, each opening Zarr + sampling + encoding in parallel —
enough work to push the API into HTTP 503s on a small Railway box.

A semaphore in front of every render call back-pressures the burst:
the next tile waits for an in-flight render to finish before starting
its own. Cache hits skip the semaphore entirely (they're cheap and
shouldn't queue behind cold renders), so a hot loop is unaffected.

The same semaphore is also acquired by the prewarm subscriber when
it pre-renders CONUS-wide tiles after a new grid materialises, so
prewarm cannot starve live request handling.
"""

from __future__ import annotations

import asyncio
import os
from typing import Final

# Default parallelism. Picked for a single-vCPU Railway box: enough
# concurrency to overlap S3/Zarr I/O with the next render's CPU work,
# small enough that the GIL + numpy threads don't thrash. Override
# via the ``AEROZA_TILE_RENDER_CONCURRENCY`` env var when scaling
# vertically.
DEFAULT_RENDER_CONCURRENCY: Final[int] = 4

_render_semaphore: asyncio.Semaphore | None = None


def get_render_semaphore() -> asyncio.Semaphore:
    """Lazy module-level singleton.

    Built lazily so the underlying ``asyncio.Semaphore`` binds to the
    running event loop on first use rather than at import time —
    matters for tests that spin up an isolated loop per case.
    """
    global _render_semaphore
    if _render_semaphore is None:
        _render_semaphore = asyncio.Semaphore(_concurrency_from_env())
    return _render_semaphore


def set_render_semaphore(semaphore: asyncio.Semaphore) -> None:
    """Test seam — install a fresh semaphore (small N is typical)."""
    global _render_semaphore
    _render_semaphore = semaphore


def _concurrency_from_env() -> int:
    raw = os.environ.get("AEROZA_TILE_RENDER_CONCURRENCY", "").strip()
    if not raw:
        return DEFAULT_RENDER_CONCURRENCY
    try:
        parsed = int(raw)
    except ValueError:
        return DEFAULT_RENDER_CONCURRENCY
    return parsed if parsed >= 1 else DEFAULT_RENDER_CONCURRENCY


__all__ = [
    "DEFAULT_RENDER_CONCURRENCY",
    "get_render_semaphore",
    "set_render_semaphore",
]
