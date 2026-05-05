"""Prewarm the tile LRU after a new MRMS grid materialises.

The tile route renders on-demand, which means the *first* user lands a
cold cache and pays full Zarr-open + colormap + encode latency on every
visible tile. Worse during a radar replay: every loop frame is a fresh
file_key so each iteration is a cold render even if the user keeps the
same viewport. Prewarming flips that so the first viewer of a fresh
grid hits a hot cache.

Strategy:

* Listen for ``aeroza.mrms.grids.new`` events on NATS.
* For each event, render the CONUS tile pyramid at z=4..6 (~315
  tiles) and put the bytes into the in-process LRU keyed by the new
  ``file_key``.
* Acquire the same render semaphore the live tile route uses, so
  prewarm cannot starve real traffic.

Process placement: this runs *inside the API process* (wired into the
FastAPI lifespan), not inside the materialiser worker, because the LRU
is process-local and only the API serves tile requests. The materialiser
publishing the event is enough — every API replica that subscribes
will warm its own cache.

Failure mode: best-effort. Any per-tile render error is logged and
skipped; the consumer keeps draining the subscription. If NATS is
unreachable at startup, the API serves cold renders forever — same
graceful degradation as the existing alerts subscriber.
"""

from __future__ import annotations

import asyncio
import math
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Final

import structlog

from aeroza.ingest.mrms_zarr import MrmsGridLocator
from aeroza.stream.subscriber import MrmsGridSubscriber
from aeroza.tiles.cache import CacheKey, TilePngCache
from aeroza.tiles.raster import TileFormat, render_tile_image
from aeroza.tiles.render_pool import get_render_semaphore

log = structlog.get_logger(__name__)

# CONUS bounding box (lng_w, lat_s, lng_e, lat_n). Mirrors the
# ``DEFAULT_BOUNDS`` MapLibre uses on the live map page so the prewarm
# covers exactly the viewport every cold visitor lands on.
CONUS_BBOX: Final[tuple[float, float, float, float]] = (-125.0, 24.0, -66.0, 50.0)

# Zoom levels to prewarm. z=4..6 covers the page's default fit-bounds
# camera + the first couple of zoom-in steps; that's the 90th percentile
# of tile requests and the only band cheap enough to render in bulk
# on every grid event. z=7+ is left to lazy on-demand rendering — if
# the user zooms in, the semaphore + LRU still keep them honest.
DEFAULT_PREWARM_ZOOMS: Final[tuple[int, ...]] = (4, 5, 6)

# Formats to prewarm. WebP is what every modern browser asks for; PNG
# is the curl-and-Safari-old-tabs fallback. Both share the same RGBA
# render and only the encode differs, so prewarming both is cheap.
DEFAULT_PREWARM_FORMATS: Final[tuple[TileFormat, ...]] = ("webp", "png")


@dataclass(frozen=True, slots=True)
class PrewarmStats:
    """Tally returned from :func:`prewarm_tiles_for_grid`. Useful for tests
    and for the structured log line the consumer emits per grid."""

    rendered: int
    failed: int
    skipped_cached: int


def conus_tile_coords(zoom: int) -> tuple[tuple[int, int], ...]:
    """Return the (x, y) tile coords covering CONUS at ``zoom``.

    Uses the standard slippy-map tile math: ``2^z`` total tiles per
    axis, x uniform in longitude, y the Mercator latitude formula.
    Returns an immutable tuple so callers can freely cache it.
    """
    if zoom < 0:
        raise ValueError(f"zoom must be non-negative, got {zoom}")
    n = 1 << zoom
    lng_w, lat_s, lng_e, lat_n = CONUS_BBOX

    def lng_to_x(lng: float) -> int:
        return int((lng + 180.0) / 360.0 * n)

    def lat_to_y(lat: float) -> int:
        clamped = max(-85.0511, min(85.0511, lat))
        rad = math.radians(clamped)
        return int((1.0 - math.log(math.tan(rad) + 1.0 / math.cos(rad)) / math.pi) / 2.0 * n)

    x_min = max(0, min(n - 1, lng_to_x(lng_w)))
    x_max = max(0, min(n - 1, lng_to_x(lng_e)))
    # Mercator y grows southward; north is the smaller index.
    y_min = max(0, min(n - 1, lat_to_y(lat_n)))
    y_max = max(0, min(n - 1, lat_to_y(lat_s)))

    coords: list[tuple[int, int]] = []
    for x in range(x_min, x_max + 1):
        for y in range(y_min, y_max + 1):
            coords.append((x, y))
    return tuple(coords)


async def prewarm_tiles_for_grid(
    locator: MrmsGridLocator,
    *,
    cache: TilePngCache,
    zooms: Iterable[int] = DEFAULT_PREWARM_ZOOMS,
    formats: Iterable[TileFormat] = DEFAULT_PREWARM_FORMATS,
) -> PrewarmStats:
    """Render and cache every CONUS tile in ``zooms`` × ``formats`` for
    ``locator``. Returns counters for the structured log line.

    Each render goes through :func:`asyncio.to_thread` (the encode is
    GIL-bound) and acquires the shared render semaphore so prewarm
    bursts back-pressure the same way live request handling does.
    Already-cached entries are skipped — re-prewarming is a no-op when
    the same grid event fires twice (cf. NATS at-least-once delivery).
    """
    semaphore = get_render_semaphore()

    rendered = 0
    failed = 0
    skipped = 0

    for zoom in zooms:
        for x, y in conus_tile_coords(zoom):
            for fmt in formats:
                key = CacheKey(file_key=locator.file_key, z=zoom, x=x, y=y, format=fmt)
                if cache.get(key) is not None:
                    skipped += 1
                    continue
                try:
                    async with semaphore:
                        body = await asyncio.to_thread(
                            render_tile_image,
                            zarr_uri=locator.zarr_uri,
                            variable=locator.variable,
                            z=zoom,
                            x=x,
                            y=y,
                            format=fmt,
                        )
                except Exception as exc:
                    failed += 1
                    log.warning(
                        "tiles.prewarm.render_failed",
                        file_key=locator.file_key,
                        z=zoom,
                        x=x,
                        y=y,
                        format=fmt,
                        error=str(exc),
                    )
                    continue
                cache.put(key, body)
                rendered += 1

    return PrewarmStats(rendered=rendered, failed=failed, skipped_cached=skipped)


async def run_prewarm_consumer(
    *,
    subscriber: MrmsGridSubscriber,
    cache: TilePngCache,
    zooms: Iterable[int] = DEFAULT_PREWARM_ZOOMS,
    formats: Iterable[TileFormat] = DEFAULT_PREWARM_FORMATS,
) -> None:
    """Long-lived consumer: drain ``aeroza.mrms.grids.new`` and prewarm.

    Runs forever until cancelled. Per-event errors are logged and
    swallowed so a single bad grid doesn't tear down the subscription.
    Cancellation propagates cleanly via the underlying async generator.
    """
    log.info(
        "tiles.prewarm.consumer.start",
        zooms=tuple(zooms),
        formats=tuple(formats),
    )
    try:
        async for locator in subscriber.subscribe_new_grids():
            try:
                stats = await prewarm_tiles_for_grid(
                    locator,
                    cache=cache,
                    zooms=zooms,
                    formats=formats,
                )
            except Exception as exc:
                log.exception(
                    "tiles.prewarm.consumer.event_failed",
                    file_key=locator.file_key,
                    error=str(exc),
                )
                continue
            log.info(
                "tiles.prewarm.consumer.event_done",
                file_key=locator.file_key,
                rendered=stats.rendered,
                failed=stats.failed,
                skipped_cached=stats.skipped_cached,
            )
    except asyncio.CancelledError:
        log.info("tiles.prewarm.consumer.cancelled")
        raise
    finally:
        log.info("tiles.prewarm.consumer.stop")


__all__ = [
    "CONUS_BBOX",
    "DEFAULT_PREWARM_FORMATS",
    "DEFAULT_PREWARM_ZOOMS",
    "PrewarmStats",
    "conus_tile_coords",
    "prewarm_tiles_for_grid",
    "run_prewarm_consumer",
]
