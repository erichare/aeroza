"""``/v1/mrms/*`` routes — file catalog, materialised grids, raster tiles,
point sample, polygon reduce, single-grid detail.

Route registration order matters: ``/mrms/grids/sample`` and
``/mrms/grids/polygon`` register before ``/mrms/grids/{file_key:path}`` so
the literal paths win over the path-parameter matcher.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Annotated

import structlog
from fastapi import APIRouter, Depends, Header, HTTPException, Path, Query, status
from fastapi.responses import Response
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.ext.asyncio import AsyncSession

from aeroza.query.dependencies import DatabaseDep, get_session
from aeroza.query.mrms import (
    DEFAULT_LIMIT as MRMS_DEFAULT_LIMIT,
)
from aeroza.query.mrms import (
    MAX_LIMIT as MRMS_MAX_LIMIT,
)
from aeroza.query.mrms import (
    MrmsFileList,
    find_mrms_files,
    mrms_view_to_item,
)
from aeroza.query.mrms_grids import (
    DEFAULT_LIMIT as GRIDS_DEFAULT_LIMIT,
)
from aeroza.query.mrms_grids import (
    MAX_LIMIT as GRIDS_MAX_LIMIT,
)
from aeroza.query.mrms_grids import (
    MrmsGridItem,
    MrmsGridList,
    find_latest_mrms_grid,
    find_mrms_grid_by_key,
    find_mrms_grids,
    mrms_grid_view_to_item,
)
from aeroza.query.mrms_sample import (
    ALL_REDUCERS,
    DEFAULT_TOLERANCE_DEG,
    MAX_TOLERANCE_DEG,
    MrmsGridPolygonResponse,
    MrmsGridSampleResponse,
    OutOfDomainError,
    PolygonReducer,
    sample_grid_at_point,
    sample_grid_in_polygon,
)
from aeroza.query.parsers import parse_polygon
from aeroza.tiles.cache import CacheKey, get_default_cache
from aeroza.tiles.raster import (
    TILE_FORMAT_CONTENT_TYPE,
    TileFormat,
    render_tile_image,
    transparent_tile_bytes,
)
from aeroza.tiles.render_pool import get_render_semaphore

log = structlog.get_logger(__name__)

router = APIRouter(tags=["mrms"])


# Cap zoom so a single misbehaving client can't ask for billions of
# unique tiles. The MRMS grid is ~1 km cell size; rendering past z=10
# starts oversampling without any data benefit.
_TILE_MAX_ZOOM: int = 10

# Cache TTL for *live-mode* tiles (no fileKey query param). Grids
# refresh every ~2 minutes upstream, so 60 s keeps the map pleasantly
# fresh while still letting CDNs/browsers coalesce zoom-pan storms.
_TILE_CACHE_SECONDS_LIVE: int = 60

# ``stale-while-revalidate`` window (RFC 5861) layered on top of the
# live freshness window. While a tile is within this grace period after
# its max-age expires, conforming caches MAY return the stale bytes
# immediately and asynchronously revalidate against the origin. iOS 17+
# URLCache and Cloudflare both honour this directive — together they
# make pan/zoom feel instant during the gap between MRMS refreshes.
_TILE_SWR_SECONDS_LIVE: int = 120

# Pinned tiles (``?fileKey=…``) are forever-immutable: the bytes for
# (file_key, z, x, y) are deterministic and the source grid never
# changes once materialised. Emitting the immutable directive lets
# browsers and CDNs cache aggressively across the radar replay loop —
# the second loop iteration becomes a series of 304s / cache hits.
_TILE_CACHE_HEADER_PINNED: str = "public, max-age=31536000, immutable"

# Live-mode tile header. The ``stale-while-revalidate`` window is what
# unblocks the "radar vanishes mid-session" experience: when max-age
# expires, conforming caches hand back the stale tile immediately and
# refresh in the background, so users never see a blank gap during the
# 60–180 s window after the previous MRMS frame ages out.
_TILE_CACHE_HEADER_LIVE: str = (
    f"public, max-age={_TILE_CACHE_SECONDS_LIVE}, stale-while-revalidate={_TILE_SWR_SECONDS_LIVE}"
)


def _transparent_tile_response(tile_format: TileFormat) -> Response:
    """Return a fully-transparent tile in the negotiated format.

    Used as a graceful fallback for three different failure modes:

    1. No grid materialised yet (``grid is None``).
    2. The catalog row resolves but its ``zarr_uri`` is gone from
       disk (retention race, volume wipe, or partial deploy).
    3. The catalog row resolves but the Zarr store throws an IO
       error mid-read.

    All three share the same live-mode Cache-Control so the client
    doesn't memoise the absence forever — the next materialiser pass
    or deploy can fix the underlying storage, and a 60s freshness
    window keeps the radar loop responsive when the fix lands.
    """
    return Response(
        content=transparent_tile_bytes(format=tile_format),
        media_type=TILE_FORMAT_CONTENT_TYPE[tile_format],
        headers={
            "Cache-Control": _TILE_CACHE_HEADER_LIVE,
            # Same body across both formats; declaring the negotiated
            # dimension lets shared caches partition by Accept
            # correctly when CDNs eventually sit in front.
            "Vary": "Accept",
        },
    )


def _negotiate_tile_format(accept: str | None) -> TileFormat:
    """Pick PNG or WebP based on the request's Accept header.

    WebP is the default because:

    * Bytes are ~30% smaller than lossless PNG for the discrete dBZ
      colour ramp, and the radar loop pulls ~28 tiles per frame
      swap — that's ~700 KB saved per frame on a typical viewport.
    * Pillow encodes lossless WebP ~2x faster than PNG.
    * Browser support is essentially universal (>97%).

    PNG is only returned when the client *explicitly* prefers it and
    rejects WebP — the standard signal is ``Accept: image/png``
    without ``image/webp``. Common cases that hit this path:

    * ``curl`` smoke tests that happen to send ``Accept: image/png``.
    * Internal tools / older non-browser clients.

    A request with ``Accept: */*`` (the default for browser
    ``fetch()`` and MapLibre's raster source) lands on WebP — that's
    the change that unblocks the deployed radar UI from PNG-only.
    """
    if accept is None:
        return "webp"
    lowered = accept.lower()
    explicitly_png = "image/png" in lowered
    accepts_webp = "image/webp" in lowered or "*/*" in lowered or "image/*" in lowered
    if explicitly_png and not accepts_webp:
        return "png"
    return "webp"


@router.get(
    "/mrms/files",
    response_model=MrmsFileList,
    response_model_by_alias=True,
    response_model_exclude_none=False,
    summary="List MRMS files in the catalog",
    description=(
        "Returns the most-recent rows of the MRMS file catalog populated by "
        "the ``aeroza-ingest-mrms`` worker. Filter by ``product`` (e.g. "
        "``MergedReflectivityComposite``), ``level`` (e.g. ``00.50``), and a "
        "half-open ``[since, until)`` window on ``valid_at``. Results are "
        "ordered by ``valid_at`` descending (most recent first)."
    ),
)
async def list_mrms_files_route(
    session: Annotated[AsyncSession, Depends(get_session)],
    product: Annotated[
        str | None,
        Query(description="Filter to a single product (e.g. 'MergedReflectivityComposite')"),
    ] = None,
    level: Annotated[
        str | None,
        Query(description="Filter to a single product level (e.g. '00.50')"),
    ] = None,
    since: Annotated[
        datetime | None,
        Query(description="Inclusive lower bound on valid_at (ISO-8601 timestamp)"),
    ] = None,
    until: Annotated[
        datetime | None,
        Query(description="Exclusive upper bound on valid_at (ISO-8601 timestamp)"),
    ] = None,
    limit: Annotated[
        int,
        Query(
            ge=1,
            le=MRMS_MAX_LIMIT,
            description=f"Max results to return (default {MRMS_DEFAULT_LIMIT})",
        ),
    ] = MRMS_DEFAULT_LIMIT,
) -> MrmsFileList:
    if since is not None and until is not None and since >= until:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="since must be strictly before until",
        )
    views = await find_mrms_files(
        session,
        product=product,
        level=level,
        since=since,
        until=until,
        limit=limit,
    )
    return MrmsFileList(items=[mrms_view_to_item(v) for v in views])


@router.get(
    "/mrms/grids",
    response_model=MrmsGridList,
    response_model_by_alias=True,
    response_model_exclude_none=False,
    summary="List materialised MRMS grids",
    description=(
        "Returns the most-recent rows of the materialised-grid catalog "
        "populated by the ``aeroza-materialise-mrms`` worker. Each item "
        "carries the locator (``zarrUri``, ``shape``, ``dtype``, …) and "
        "the source file's product/level/valid_at. Same filters as "
        "``/v1/mrms/files``."
    ),
)
async def list_mrms_grids_route(
    session: Annotated[AsyncSession, Depends(get_session)],
    product: Annotated[
        str | None,
        Query(description="Filter to a single product (e.g. 'MergedReflectivityComposite')"),
    ] = None,
    level: Annotated[
        str | None,
        Query(description="Filter to a single product level (e.g. '00.50')"),
    ] = None,
    since: Annotated[
        datetime | None,
        Query(description="Inclusive lower bound on valid_at (ISO-8601 timestamp)"),
    ] = None,
    until: Annotated[
        datetime | None,
        Query(description="Exclusive upper bound on valid_at (ISO-8601 timestamp)"),
    ] = None,
    limit: Annotated[
        int,
        Query(
            ge=1,
            le=GRIDS_MAX_LIMIT,
            description=f"Max results to return (default {GRIDS_DEFAULT_LIMIT})",
        ),
    ] = GRIDS_DEFAULT_LIMIT,
) -> MrmsGridList:
    if since is not None and until is not None and since >= until:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="since must be strictly before until",
        )
    views = await find_mrms_grids(
        session,
        product=product,
        level=level,
        since=since,
        until=until,
        limit=limit,
    )
    return MrmsGridList(items=[mrms_grid_view_to_item(v) for v in views])


class MrmsLatestResponse(BaseModel):
    """Compact pointer at the latest materialised grid for a product/level.

    The radar UI polls this every 30s to pin its tile source at the
    newest available ``file_key``, then loads tiles from
    ``tiles.aeroza.app/{file_key}/{z}/{x}/{y}.webp`` — the CDN-backed
    static origin. Returning just ``{file_key, valid_at}`` (instead of
    the full ``MrmsGridItem``) keeps the polled payload tiny — sub-100
    bytes — which is what allows aggressive ``stale-while-revalidate``
    caching at the edge without bloating the dashboard's network
    panel.
    """

    model_config = ConfigDict(populate_by_name=True, frozen=True)

    file_key: str = Field(serialization_alias="fileKey")
    valid_at: datetime = Field(serialization_alias="validAt")
    product: str
    level: str


@router.get(
    "/mrms/latest",
    response_model=MrmsLatestResponse,
    response_model_by_alias=True,
    response_model_exclude_none=False,
    summary="Latest-grid pointer for the radar dashboard",
    description=(
        "Returns ``{fileKey, validAt, product, level}`` for the most "
        "recent materialised MRMS grid matching the requested "
        "product/level. The radar UI polls this endpoint every 30s as "
        "its live-mode pin — tile bytes come from the static "
        "``tiles.aeroza.app`` CDN origin, so this is the only API "
        "round-trip the live map needs.\n\n"
        "Caching: ``Cache-Control: public, max-age=15, "
        "stale-while-revalidate=30``. Short freshness because a new "
        "grid lands every ~2 minutes; the SWR window keeps the radar "
        "loop responsive when MRMS is briefly behind."
    ),
    responses={
        404: {"description": "No grid has been materialised for the requested product/level."},
    },
)
async def get_latest_mrms_grid_route(
    session: Annotated[AsyncSession, Depends(get_session)],
    product: Annotated[
        str,
        Query(description="MRMS product (e.g. 'MergedReflectivityComposite')."),
    ] = "MergedReflectivityComposite",
    level: Annotated[
        str,
        Query(description="MRMS product level (e.g. '00.50')."),
    ] = "00.50",
) -> Response:
    grid = await find_latest_mrms_grid(session, product=product, level=level)
    if grid is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"no materialised grid for product={product!r} level={level!r}",
        )
    payload = MrmsLatestResponse(
        file_key=grid.file_key,
        valid_at=grid.valid_at,
        product=grid.product,
        level=grid.level,
    )
    return Response(
        content=payload.model_dump_json(by_alias=True),
        media_type="application/json",
        headers={
            # Short freshness, generous stale-while-revalidate window —
            # same shape as the live tile route's header but applied to
            # the JSON pointer. Browsers + Cloudflare hand back the
            # stale value while asynchronously refreshing, so the
            # radar dashboard never blocks on this poll.
            "Cache-Control": "public, max-age=15, stale-while-revalidate=30",
        },
    )


@router.get(
    "/mrms/tiles/{z}/{x}/{y}.png",
    summary="Raster tile of an MRMS grid (Web Mercator, XYZ)",
    description=(
        "Returns a 256×256 raster tile suitable as a MapLibre / Leaflet "
        "raster source. By default renders the most recent "
        "``MergedReflectivityComposite`` grid; pass ``file_key`` to pin "
        "a specific grid (used by the timeline scrubber). Tiles are "
        "sampled nearest-neighbor from the Zarr store, coloured with "
        "the standard NWS dBZ ramp, and 86%-opaque so the basemap shows "
        "through where there's no echo.\n\n"
        "**Content negotiation**: the URL ends in ``.png`` for "
        "MapLibre's tile-template compatibility, but the response is "
        "WebP when the request's ``Accept`` header includes "
        "``image/webp`` (every modern browser does). WebP is ~30-40% "
        "smaller than PNG for the dBZ ramp's discrete colours and "
        "Pillow encodes it ~2x faster — both compound on top of the "
        "per-tile LRU."
    ),
    response_class=Response,
    responses={
        200: {
            "content": {"image/png": {}, "image/webp": {}},
            "description": "Tile PNG or WebP (per the request's Accept header).",
        },
        404: {"description": "No matching grid materialised yet."},
    },
)
async def get_mrms_tile_route(
    db: DatabaseDep,
    z: Annotated[int, Path(ge=0, le=_TILE_MAX_ZOOM, description="Zoom level (0–10).")],
    x: Annotated[int, Path(ge=0, description="Tile column.")],
    y: Annotated[int, Path(ge=0, description="Tile row.")],
    product: Annotated[
        str,
        Query(description="MRMS product (e.g. 'MergedReflectivityComposite')."),
    ] = "MergedReflectivityComposite",
    level: Annotated[
        str,
        Query(description="MRMS product level (e.g. '00.50')."),
    ] = "00.50",
    file_key: Annotated[
        str | None,
        Query(
            alias="fileKey",
            description=(
                "Optional pin to one specific source file_key. When omitted, "
                "the latest grid for the requested product/level is used."
            ),
        ),
    ] = None,
    accept: Annotated[
        str | None,
        Header(
            description=(
                "Browser-supplied content negotiation. Include "
                "``image/webp`` to opt into the WebP encoding."
            ),
        ),
    ] = None,
) -> Response:
    tile_format: TileFormat = _negotiate_tile_format(accept)

    # Cheap, sync coord validation BEFORE any DB / render work — these
    # are caller bugs (z is already bounded by FastAPI's Path
    # validator) and should propagate as 400, not be smoothed over.
    n = 1 << z
    if x >= n or y >= n:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"tile coords out of range for zoom {z} (max x/y = {n - 1})",
        )

    # Defense in depth: ANY unhandled exception from the lookup / render
    # / encode path falls back to a transparent tile rather than a 500.
    # Live tile requests are not user-facing API calls — they're a
    # MapLibre raster source repeatedly hammering for tiles to paint.
    # A 500 spike there taints the source and breaks the radar UI for
    # the rest of the session; a blank frame is recovered by the next
    # poll. Causes we've seen or anticipate:
    #
    #   * ``xr.open_zarr`` raising FileNotFoundError when the retention
    #     sweeper races with a live request (closed by PR #91, but the
    #     transparent-tile fallback survives any retention-order
    #     regression).
    #   * SQLAlchemy connection pool exhaustion under burst load
    #     (``pool_size=5, max_overflow=5`` total → 10 connections; a
    #     radar-loop salvo of 60+ concurrent tiles can saturate this
    #     during the 1-6s render window when each session is held for
    #     the duration of its render). Surfaces as ``TimeoutError`` /
    #     ``DBAPIError`` — neither in the IOError family.
    #   * numpy / Pillow raising under memory pressure on a small
    #     Railway dyno.
    #   * Anything else we haven't seen yet.
    #
    # ``HTTPException`` is re-raised inside the catch so intentional
    # 4xx/5xx returns (400 above, 502 for structural grid bugs below)
    # still flow through FastAPI's normal error response handling.
    try:
        # Acquire a session JUST for the catalog lookup, then release
        # it before the (potentially seconds-long) render. The
        # previous shape held the session for the full request
        # lifetime via ``Depends(get_session)``, which capped
        # throughput at ``pool_size + max_overflow`` concurrent
        # renders (10 on the default Railway config). With renders
        # taking 1-6s and the radar loop firing ~30 concurrent tile
        # requests per frame, the pool saturated under burst and the
        # waiters that timed out surfaced as 500s. Lookup-only
        # sessions are ~10ms each, so the pool now serves on the
        # order of hundreds of req/s.
        async with db.sessionmaker() as session:
            if file_key is not None:
                grid = await find_mrms_grid_by_key(session, file_key)
            else:
                grid = await find_latest_mrms_grid(session, product=product, level=level)

        if grid is None:
            # Return a transparent tile rather than a 404 — the MapLibre
            # raster source aggressively retries 404s, which would spam
            # the API with tiles outside the materialised grid's
            # coverage. The short live-mode TTL applies whether or not
            # the caller passed a fileKey: even a "missing pinned grid"
            # answer can flip when the materialiser catches up, so we
            # don't promise immutability.
            return _transparent_tile_response(tile_format)

        # Server-side LRU keyed by (file_key, z, x, y, format). The
        # route has already resolved the request to a concrete
        # ``grid.file_key`` — whether the caller asked for
        # ``?fileKey=…`` (pinned) or relied on the "latest" fallback —
        # so the cache key is identical in both cases. Live-mode used
        # to be excluded out of fear that "latest" could race the
        # materialiser; in practice every cached entry is keyed on a
        # specific file_key, and the entry is correct for that
        # file_key forever. The Cache-Control header on the response
        # is what differs (immutable vs short max-age) — the cache
        # itself is safe to populate either way.
        cache = get_default_cache()
        cache_key = CacheKey(file_key=grid.file_key, z=z, x=x, y=y, format=tile_format)
        cached_bytes: bytes | None = cache.get(cache_key)

        if cached_bytes is not None:
            body = cached_bytes
            cache_status = "hit"
        else:
            # Cap concurrent renders. Without this back-pressure a
            # radar-loop pan storm fans out into uncapped worker
            # threads and the API self-503s under its own load. The
            # semaphore only guards the cold-render path — cache hits
            # above skip it entirely.
            try:
                async with get_render_semaphore():
                    body = await asyncio.to_thread(
                        render_tile_image,
                        zarr_uri=grid.zarr_uri,
                        variable=grid.variable,
                        z=z,
                        x=x,
                        y=y,
                        format=tile_format,
                    )
            except (FileNotFoundError, OSError) as exc:
                # Specific case worth its own log event — storage gone
                # underneath us is the single most common cause we've
                # seen and is worth a dedicated counter for retention
                # tuning. The outer ``except Exception`` below would
                # also catch this, but with a less specific event name.
                log.warning(
                    "tile.storage_unavailable",
                    file_key=grid.file_key,
                    zarr_uri=grid.zarr_uri,
                    z=z,
                    x=x,
                    y=y,
                    exc_class=type(exc).__name__,
                    exc_message=str(exc),
                )
                return _transparent_tile_response(tile_format)
            except KeyError as exc:
                # Variable not in Zarr → grid in catalog is structurally
                # broken (schema mismatch with the renderer's
                # expectations, not a transient storage hiccup).
                # Surface as 502 so the caller knows this is
                # upstream-data wrong, not transient.
                raise HTTPException(
                    status_code=status.HTTP_502_BAD_GATEWAY,
                    detail=f"grid storage missing variable: {exc}",
                ) from exc
            cache.put(cache_key, body)
            cache_status = "miss"

        cache_control = (
            _TILE_CACHE_HEADER_PINNED if file_key is not None else _TILE_CACHE_HEADER_LIVE
        )
        return Response(
            content=body,
            media_type=TILE_FORMAT_CONTENT_TYPE[tile_format],
            headers={
                "Cache-Control": cache_control,
                # Negotiated on Accept — declare it so shared caches
                # partition correctly between the PNG and WebP variants.
                "Vary": "Accept",
                # Including the source key in headers lets clients
                # (and our smoke tests) verify which grid populated a
                # tile without decoding the PNG.
                "X-Aeroza-Grid-Key": grid.file_key,
                "X-Aeroza-Grid-Valid-At": grid.valid_at.isoformat(),
                # ``hit`` / ``miss`` for pinned tiles, ``bypass`` for
                # live mode (cache deliberately skipped). Useful for
                # the browser devtools-driven sanity check in the
                # radar loop.
                "X-Aeroza-Tile-Cache": cache_status,
            },
        )
    except HTTPException:
        # Intentional 4xx/5xx — let FastAPI handle response shaping.
        raise
    except Exception as exc:
        # Unknown failure on the tile path. Logged with full traceback
        # so we can fix the root cause; meanwhile the user sees a
        # blank frame instead of a 500-burst breaking the radar source.
        log.warning(
            "tile.unexpected_error",
            file_key=file_key,
            product=product,
            level=level,
            z=z,
            x=x,
            y=y,
            exc_class=type(exc).__name__,
            exc_message=str(exc),
            exc_info=True,
        )
        return _transparent_tile_response(tile_format)


@router.get(
    "/mrms/grids/sample",
    response_model=MrmsGridSampleResponse,
    response_model_by_alias=True,
    response_model_exclude_none=False,
    summary="Sample one MRMS grid value at a (latitude, longitude) point",
    description=(
        "Returns the nearest-cell value for ``(lat, lng)`` from a "
        "materialised MRMS grid. By default samples the *latest* grid for "
        "the given ``product``/``level``; pass ``at_time`` (ISO-8601 UTC) "
        "to sample the most-recent grid valid at-or-before that moment. "
        "Out-of-domain requests (no cell within ``tolerance_deg``) return "
        "404 rather than a misleading nearest-edge value."
    ),
    responses={
        404: {
            "description": (
                "No materialised grid satisfies the request, or the request "
                "point is outside the grid (farther than ``tolerance_deg`` "
                "from any cell)."
            ),
        },
    },
)
async def sample_mrms_grid_route(
    session: Annotated[AsyncSession, Depends(get_session)],
    lat: Annotated[
        float,
        Query(
            ge=-90.0,
            le=90.0,
            description="Latitude in degrees (WGS84).",
            examples=[29.76],
        ),
    ],
    lng: Annotated[
        float,
        Query(
            ge=-180.0,
            le=180.0,
            description="Longitude in degrees (WGS84, -180..180).",
            examples=[-95.37],
        ),
    ],
    product: Annotated[
        str,
        Query(description="MRMS product (e.g. 'MergedReflectivityComposite')."),
    ] = "MergedReflectivityComposite",
    level: Annotated[
        str,
        Query(description="MRMS product level (e.g. '00.50')."),
    ] = "00.50",
    at_time: Annotated[
        datetime | None,
        Query(
            description=(
                "Sample the most recent grid with valid_at <= this moment. "
                "Defaults to the overall latest grid."
            ),
        ),
    ] = None,
    tolerance_deg: Annotated[
        float,
        Query(
            gt=0.0,
            le=MAX_TOLERANCE_DEG,
            description=(
                f"Reject the sample if no cell is within this many degrees "
                f"(default {DEFAULT_TOLERANCE_DEG}; max {MAX_TOLERANCE_DEG})."
            ),
        ),
    ] = DEFAULT_TOLERANCE_DEG,
) -> MrmsGridSampleResponse:
    grid = await find_latest_mrms_grid(
        session,
        product=product,
        level=level,
        at_or_before=at_time,
    )
    if grid is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"no materialised grid for product={product!r} level={level!r}"
                + (f" at_or_before={at_time.isoformat()}" if at_time else "")
            ),
        )
    try:
        sample = await sample_grid_at_point(
            zarr_uri=grid.zarr_uri,
            variable=grid.variable,
            latitude=lat,
            longitude=lng,
            tolerance_deg=tolerance_deg,
        )
    except OutOfDomainError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc
    return MrmsGridSampleResponse(
        file_key=grid.file_key,
        product=grid.product,
        level=grid.level,
        valid_at=grid.valid_at,
        variable=sample.variable,
        value=sample.value,
        requested_latitude=lat,
        requested_longitude=lng,
        matched_latitude=sample.latitude,
        matched_longitude=sample.longitude,
        tolerance_deg=tolerance_deg,
    )


@router.get(
    "/mrms/grids/polygon",
    response_model=MrmsGridPolygonResponse,
    response_model_by_alias=True,
    response_model_exclude_none=False,
    summary="Reduce one MRMS grid over a polygon (max / mean / min / count_ge)",
    description=(
        "Reduces ``variable`` over the cells of a materialised MRMS grid "
        "whose centres fall inside ``polygon``. Polygon vertices are flat "
        "comma-separated ``lng,lat,lng,lat,...`` (GeoJSON / OGC order), "
        "minimum three vertices, implicitly closed. By default samples the "
        "*latest* grid for the given ``product``/``level``; pass ``at_time`` "
        "(ISO-8601 UTC) to reduce the most-recent grid valid at-or-before "
        "that moment.\n\n"
        "Reducers: ``max``, ``mean``, ``min``, ``count_ge`` (number of "
        "cells with value >= ``threshold``). ``count_ge`` requires "
        "``threshold``; the others ignore it."
    ),
    responses={
        404: {
            "description": (
                "No grid satisfies the request, or the polygon does not "
                "overlap the grid (no cell centres inside it)."
            )
        },
    },
)
async def reduce_mrms_grid_over_polygon_route(
    session: Annotated[AsyncSession, Depends(get_session)],
    polygon: Annotated[
        str,
        Query(
            description=(
                "Polygon vertices as ``lng,lat,lng,lat,...`` (GeoJSON / OGC "
                "order). Minimum 3 vertices; the ring is implicitly closed."
            ),
            examples=["-95.7,29.5,-95.0,29.5,-95.0,30.0,-95.7,30.0"],
        ),
    ],
    reducer: Annotated[
        PolygonReducer,
        Query(description=f"Reducer to apply. One of {list(ALL_REDUCERS)}."),
    ] = "max",
    threshold: Annotated[
        float | None,
        Query(
            description=(
                "Required when ``reducer == 'count_ge'``: counts cells with value >= ``threshold``."
            ),
        ),
    ] = None,
    product: Annotated[
        str,
        Query(description="MRMS product (e.g. 'MergedReflectivityComposite')."),
    ] = "MergedReflectivityComposite",
    level: Annotated[
        str,
        Query(description="MRMS product level (e.g. '00.50')."),
    ] = "00.50",
    at_time: Annotated[
        datetime | None,
        Query(
            description=(
                "Reduce the most recent grid with valid_at <= this moment. "
                "Defaults to the overall latest grid."
            ),
        ),
    ] = None,
) -> MrmsGridPolygonResponse:
    vertices = parse_polygon(polygon)
    # ``parse_polygon`` returns None only when ``raw is None``; FastAPI requires
    # the ``polygon`` query param so this branch is unreachable, but the cast
    # below keeps mypy honest about the non-None contract.
    if vertices is None:  # pragma: no cover - defensive
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="polygon query param is required",
        )
    if reducer == "count_ge" and threshold is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="reducer 'count_ge' requires a numeric 'threshold' query param",
        )

    grid = await find_latest_mrms_grid(
        session,
        product=product,
        level=level,
        at_or_before=at_time,
    )
    if grid is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"no materialised grid for product={product!r} level={level!r}"
                + (f" at_or_before={at_time.isoformat()}" if at_time else "")
            ),
        )

    try:
        sample = await sample_grid_in_polygon(
            zarr_uri=grid.zarr_uri,
            variable=grid.variable,
            polygon_lng_lat=vertices,
            reducer=reducer,
            threshold=threshold,
        )
    except OutOfDomainError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc

    return MrmsGridPolygonResponse(
        file_key=grid.file_key,
        product=grid.product,
        level=grid.level,
        valid_at=grid.valid_at,
        variable=sample.variable,
        reducer=sample.reducer,
        threshold=sample.threshold,
        value=sample.value,
        cell_count=sample.cell_count,
        vertex_count=len(vertices),
        bbox_min_latitude=sample.bbox_min_latitude,
        bbox_min_longitude=sample.bbox_min_longitude,
        bbox_max_latitude=sample.bbox_max_latitude,
        bbox_max_longitude=sample.bbox_max_longitude,
    )


@router.get(
    "/mrms/grids/{file_key:path}",
    response_model=MrmsGridItem,
    response_model_by_alias=True,
    response_model_exclude_none=False,
    summary="Get a single materialised MRMS grid by source S3 key",
    description=(
        "Returns the locator + product/level/valid_at for one materialised "
        "grid identified by its source S3 ``file_key`` (the same key "
        "returned by ``/v1/mrms/files``). The ``:path`` converter accepts "
        "the slash-bearing CONUS-prefixed key as a single parameter."
    ),
    responses={404: {"description": "No materialised grid for that file_key."}},
)
async def get_mrms_grid_route(
    session: Annotated[AsyncSession, Depends(get_session)],
    file_key: Annotated[
        str,
        Path(
            description="S3 key of the source MRMS file (e.g. 'CONUS/.../MRMS_..._120000.grib2.gz')",
            min_length=1,
        ),
    ],
) -> MrmsGridItem:
    view = await find_mrms_grid_by_key(session, file_key)
    if view is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"no materialised grid for file_key {file_key!r}",
        )
    return mrms_grid_view_to_item(view)
