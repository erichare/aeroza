"""Bounded in-process LRU cache for rendered tile PNGs.

Per-grid, per-(z, x, y), keyed by ``file_key``. A pinned tile (one with
``?fileKey=…``) is forever-immutable — the source grid never changes
once materialised — so caching the rendered bytes by key avoids
re-opening the Zarr, re-sampling, and re-encoding PNG on every
hit. Live-mode (latest-grid) tiles are *not* cached here: the latest
grid moves with new ingests, so the cache key would race the
materialiser and serve stale frames.

Sizing: bounded by total bytes, not entry count. A typical CONUS
tile at z=5 is ~6 KB; 200 MB holds ~30k tiles, comfortably more
than a 12-frame loop × the visible-viewport tile count even on a
4K display.

The cache is process-local. A second uvicorn worker has its own
copy. That's fine for the dev console (single-worker reload) and
acceptable for any production rollout where each worker pays the
first-render cost once. A shared Redis-backed cache is a future
addition, but the LRU here is so cheap it's the right v1.

Thread-safety: protected by a single `threading.Lock`. The route
handler hands rendering off to a worker thread via
:func:`asyncio.to_thread`, so multiple concurrent renders into and
out of the cache are realistic.
"""

from __future__ import annotations

import threading
from collections import OrderedDict
from dataclasses import dataclass
from typing import Final

import structlog

log = structlog.get_logger(__name__)

# Default cache budget. Tunable via :class:`TilePngCache` for tests
# (small) and per-deployment overrides (huge). 200 MB is the sweet
# spot for a single dev worker — still leaves plenty for FastAPI's
# own request buffers and asyncio's tasks.
DEFAULT_MAX_BYTES: Final[int] = 200 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class CacheKey:
    """Identity for a cached tile PNG.

    ``file_key`` pins the source grid; ``z, x, y`` pin the tile coords.
    A live-mode tile (no ``file_key``) deliberately can't be expressed
    as a CacheKey — the cache module *only* speaks about pinned
    tiles, so the type system pushes that invariant up to the caller.
    """

    file_key: str
    z: int
    x: int
    y: int


@dataclass(frozen=True, slots=True)
class CacheStats:
    """Snapshot for ``X-Aeroza-Tile-Cache-*`` headers + tests.

    Hits and misses are absolute counters since process start;
    ``current_bytes`` and ``current_entries`` reflect live state.
    """

    hits: int
    misses: int
    evictions: int
    current_bytes: int
    current_entries: int
    max_bytes: int


class TilePngCache:
    """Thread-safe LRU keyed by :class:`CacheKey`.

    Insert is ``put``; lookup is ``get``. Both move the entry to the
    most-recent slot. Inserts trim oldest entries until the total
    bytes is within :attr:`max_bytes`.
    """

    def __init__(self, *, max_bytes: int = DEFAULT_MAX_BYTES) -> None:
        if max_bytes <= 0:
            raise ValueError(f"max_bytes must be positive, got {max_bytes}")
        self._max_bytes = max_bytes
        self._entries: OrderedDict[CacheKey, bytes] = OrderedDict()
        self._current_bytes = 0
        self._hits = 0
        self._misses = 0
        self._evictions = 0
        self._lock = threading.Lock()

    @property
    def max_bytes(self) -> int:
        return self._max_bytes

    def get(self, key: CacheKey) -> bytes | None:
        """Return cached bytes, or ``None``. Bumps recency on hit."""
        with self._lock:
            png = self._entries.get(key)
            if png is None:
                self._misses += 1
                return None
            self._entries.move_to_end(key)
            self._hits += 1
            return png

    def put(self, key: CacheKey, png: bytes) -> None:
        """Insert ``png`` under ``key``, evicting LRU entries to fit.

        A single PNG larger than :attr:`max_bytes` is silently dropped
        rather than wiping the whole cache — defensive guard for an
        unexpectedly huge tile (zero in practice; transparent PNGs
        are <500 bytes, painted ones <50 KB).
        """
        size = len(png)
        if size > self._max_bytes:
            log.warning(
                "tiles.cache.oversize_drop",
                size=size,
                max=self._max_bytes,
            )
            return
        with self._lock:
            existing = self._entries.pop(key, None)
            if existing is not None:
                self._current_bytes -= len(existing)
            self._entries[key] = png
            self._current_bytes += size
            while self._current_bytes > self._max_bytes and self._entries:
                _, evicted = self._entries.popitem(last=False)
                self._current_bytes -= len(evicted)
                self._evictions += 1

    def clear(self) -> None:
        """Drop every entry. Intended for tests; the route doesn't call this."""
        with self._lock:
            self._entries.clear()
            self._current_bytes = 0

    def stats(self) -> CacheStats:
        with self._lock:
            return CacheStats(
                hits=self._hits,
                misses=self._misses,
                evictions=self._evictions,
                current_bytes=self._current_bytes,
                current_entries=len(self._entries),
                max_bytes=self._max_bytes,
            )


_default_cache: TilePngCache | None = None


def get_default_cache() -> TilePngCache:
    """Lazy module-level singleton.

    Tests bind a fresh instance via :func:`set_default_cache` to keep
    state from bleeding between cases.
    """
    global _default_cache
    if _default_cache is None:
        _default_cache = TilePngCache()
    return _default_cache


def set_default_cache(cache: TilePngCache) -> None:
    """Test seam — install a fresh cache (small max_bytes is typical)."""
    global _default_cache
    _default_cache = cache


__all__ = [
    "DEFAULT_MAX_BYTES",
    "CacheKey",
    "CacheStats",
    "TilePngCache",
    "get_default_cache",
    "set_default_cache",
]
