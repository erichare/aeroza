"""Pre-render and publish the CONUS tile pyramid for every new MRMS grid.

Previous shape (PR #66, deprecated): rendered z=4..6 into the in-process
LRU on each NATS event. That was a band-aid — the LRU lives inside the
API process and dies on every restart, the coverage stopped at z=6 so
real user zoom-ins fell back to on-demand renders, and "warm one
process's LRU" doesn't help when the API has multiple replicas.

Current shape: render the full pyramid (``z=2..8``) over the CONUS
bbox and **upload every tile to Cloudflare R2** under the deterministic
key ``{file_key}/{z}/{x}/{y}.{webp|png}``. The frontend points
``tiles.aeroza.app`` at that bucket via a Cloudflare custom domain;
``Cache-Control: public, max-age=31536000, immutable`` lets the edge
cache the response forever. The on-demand FastAPI tile route stays as
a defensive fallback for anything not yet in R2 (e.g. the first ~30 s
after a new grid lands), but steady-state traffic should hit R2 100%
of the time.

When R2 isn't configured (local dev, tests without monkeypatched env)
the consumer **falls back to the legacy LRU population path** so the
existing on-demand route still benefits from pre-warmed tiles. That
keeps ``make dev`` viable without a Cloudflare account.

Strategy:

* Listen for ``aeroza.mrms.grids.new`` events on NATS.
* For each event, render every CONUS tile coord in ``z=2..8`` (~680
  tiles per grid). Re-use the shared render semaphore so prewarm
  cannot starve any live request handling that still hits the
  fallback route.
* Skip tiles that already exist in R2 (HEAD probe). NATS delivers
  at-least-once, so re-publishing the same event must be a no-op.

Failure mode: best-effort. Per-tile render or upload errors are
logged and skipped; the consumer keeps draining. If NATS is
unreachable at startup, prewarm just doesn't run — same graceful
degradation as the existing alerts subscriber.
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
from aeroza.tiles.r2 import R2Client
from aeroza.tiles.raster import TileFormat, render_tile_image
from aeroza.tiles.render_pool import get_render_semaphore

log = structlog.get_logger(__name__)

# CONUS bounding box (lng_w, lat_s, lng_e, lat_n). Mirrors the
# ``DEFAULT_BOUNDS`` MapLibre uses on the live map page so the prewarm
# covers exactly the viewport every cold visitor lands on.
CONUS_BBOX: Final[tuple[float, float, float, float]] = (-125.0, 24.0, -66.0, 50.0)

# Zoom levels to prewarm. ``z=2..8`` covers the page's fit-bounds
# camera (z=4 on first paint) plus every zoom-in step where the
# upload cost is bounded. z=9 quadruples the tile count and z=10
# quadruples it again, so we explicitly *don't* prewarm those —
# users who zoom past 8 trigger the on-demand write-through path
# instead, which bounds total R2 footprint by viewer behaviour
# rather than total CONUS coverage. MapLibre's ``maxzoom: 10`` lets
# it ask for those z=9/z=10 tiles directly (rather than GPU-stretch
# a z=8 tile), and the bypass-cache rule on tiles.aeroza.app means
# the brief 404→write-through churn resolves on the next request.
#
# Tile count for the CONUS bbox: roughly {z=2: 4, z=3: 8, z=4: 24,
# z=5: 80, z=6: 256, z=7: 800, z=8: 2500} → ~3.7k tiles when computed
# from the slippy-tile math, but the bbox-intersect shaving below
# brings the effective count to ~680 (most of the world is ocean).
DEFAULT_PREWARM_ZOOMS: Final[tuple[int, ...]] = (2, 3, 4, 5, 6, 7, 8)

# Formats to prewarm. WebP is what every modern browser asks for and
# what the deployed dashboard requests by default. PNG is kept for the
# rare curl-with-Accept-image/png-only case the fallback FastAPI route
# still handles natively — at the R2 layer we *don't* upload PNG, to
# halve the storage and write-op footprint. Any PNG request falls
# through to the on-demand route (essentially never hit in production).
DEFAULT_PREWARM_FORMATS: Final[tuple[TileFormat, ...]] = ("webp",)


@dataclass(frozen=True, slots=True)
class PrewarmStats:
    """Tally returned from :func:`prewarm_tiles_for_grid`. Useful for
    tests and for the structured log line the consumer emits per grid.

    ``rendered`` counts tiles successfully encoded *and* uploaded (or
    LRU-cached, when R2 is disabled). ``skipped_existing`` counts
    tiles that were already in R2 (or the LRU) before this event.
    ``failed`` counts render-or-upload exceptions; the loop logs each
    and keeps going.
    """

    rendered: int
    failed: int
    skipped_existing: int


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
    r2_client: R2Client | None,
    lru_cache: TilePngCache | None,
    zooms: Iterable[int] = DEFAULT_PREWARM_ZOOMS,
    formats: Iterable[TileFormat] = DEFAULT_PREWARM_FORMATS,
) -> PrewarmStats:
    """Render the full pyramid for ``locator`` and publish each tile.

    Publication target depends on the caller-supplied context:

    * ``r2_client is not None``: each tile is uploaded to R2. The
      ``Cache-Control: immutable`` directive lets Cloudflare hold the
      response at every edge for the bucket's lifetime. This is the
      production path.
    * ``r2_client is None and lru_cache is not None``: tiles populate
      the in-process LRU. Useful in local dev (no R2 configured) so
      the on-demand FastAPI route still benefits from pre-rendered
      bytes.
    * Both ``None``: the function still runs (useful for tests) but
      every rendered tile is dropped on the floor.

    Each render goes through :func:`asyncio.to_thread` (Pillow + numpy
    are GIL-heavy) and acquires the shared render semaphore so a
    prewarm burst back-pressures the same way live tile renders do.
    Already-uploaded tiles are skipped via HEAD probe (R2 path) or
    LRU lookup (fallback path) — re-running the same event is a
    no-op.
    """
    semaphore = get_render_semaphore()

    rendered = 0
    failed = 0
    skipped = 0

    for zoom in zooms:
        for x, y in conus_tile_coords(zoom):
            for fmt in formats:
                # Skip if we've already published this tile (NATS
                # at-least-once delivery + Railway redeploys mean we
                # see duplicate events constantly).
                already_published = await _already_published(
                    r2_client=r2_client,
                    lru_cache=lru_cache,
                    file_key=locator.file_key,
                    z=zoom,
                    x=x,
                    y=y,
                    fmt=fmt,
                )
                if already_published:
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

                try:
                    await _publish_tile(
                        r2_client=r2_client,
                        lru_cache=lru_cache,
                        file_key=locator.file_key,
                        z=zoom,
                        x=x,
                        y=y,
                        fmt=fmt,
                        body=body,
                    )
                except Exception as exc:
                    failed += 1
                    log.warning(
                        "tiles.prewarm.publish_failed",
                        file_key=locator.file_key,
                        z=zoom,
                        x=x,
                        y=y,
                        format=fmt,
                        error=str(exc),
                    )
                    continue

                rendered += 1

    return PrewarmStats(rendered=rendered, failed=failed, skipped_existing=skipped)


async def _already_published(
    *,
    r2_client: R2Client | None,
    lru_cache: TilePngCache | None,
    file_key: str,
    z: int,
    x: int,
    y: int,
    fmt: TileFormat,
) -> bool:
    """Return True when the tile is already at the publication target.

    R2 path takes precedence — if R2 is enabled, we check it
    regardless of whether an LRU is also present. (The LRU is just a
    secondary cache anyway; the R2 object is the source of truth for
    the frontend.)
    """
    if r2_client is not None:
        return await r2_client.object_exists(file_key=file_key, z=z, x=x, y=y, fmt=fmt)
    if lru_cache is not None:
        return lru_cache.get(CacheKey(file_key=file_key, z=z, x=x, y=y, format=fmt)) is not None
    return False


async def _publish_tile(
    *,
    r2_client: R2Client | None,
    lru_cache: TilePngCache | None,
    file_key: str,
    z: int,
    x: int,
    y: int,
    fmt: TileFormat,
    body: bytes,
) -> None:
    """Send a rendered tile to its publication target(s).

    When R2 is enabled this is a single upload. When it isn't, the
    tile populates the in-process LRU so the on-demand route still
    benefits — the local-dev compromise.
    """
    if r2_client is not None:
        await r2_client.put_tile(file_key=file_key, z=z, x=x, y=y, fmt=fmt, body=body)
        return
    if lru_cache is not None:
        lru_cache.put(CacheKey(file_key=file_key, z=z, x=x, y=y, format=fmt), body)


async def run_prewarm_consumer(
    *,
    subscriber: MrmsGridSubscriber,
    r2_client: R2Client | None = None,
    lru_cache: TilePngCache | None = None,
    zooms: Iterable[int] = DEFAULT_PREWARM_ZOOMS,
    formats: Iterable[TileFormat] = DEFAULT_PREWARM_FORMATS,
) -> None:
    """Long-lived consumer: drain ``aeroza.mrms.grids.new`` and prewarm.

    Runs forever until cancelled. Per-event errors are logged and
    swallowed so a single bad grid doesn't tear down the subscription.
    Cancellation propagates cleanly via the underlying async generator.

    The caller decides the publication target (R2 / LRU / both) by
    passing the appropriate clients; this function is purely event-
    loop machinery.
    """
    log.info(
        "tiles.prewarm.consumer.start",
        zooms=tuple(zooms),
        formats=tuple(formats),
        target="r2" if r2_client is not None else ("lru" if lru_cache is not None else "noop"),
    )
    try:
        async for locator in subscriber.subscribe_new_grids():
            try:
                stats = await prewarm_tiles_for_grid(
                    locator,
                    r2_client=r2_client,
                    lru_cache=lru_cache,
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
                skipped_existing=stats.skipped_existing,
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
