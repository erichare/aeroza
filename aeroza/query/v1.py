"""Aeroza v1 query API.

Routes here are URL-versioned (``/v1/...``). The current surface:

- ``GET /v1/alerts`` — active NWS alerts as a GeoJSON ``FeatureCollection``,
  filterable by a single ``point``, a ``bbox``, and/or a minimum ``severity``.
- ``GET /v1/alerts/stream`` — Server-Sent Events feed re-emitting newly
  observed alerts published by the ingest worker on the ``aeroza.alerts.nws.new``
  NATS subject.
- ``GET /v1/alerts/{alert_id}`` — single-alert detail with the long-form
  ``description`` and ``instruction`` fields that the list endpoint omits.
- ``GET /v1/mrms/files`` — MRMS catalog ("what data is available right
  now") populated by the ``aeroza-ingest-mrms`` worker.
- ``GET /v1/mrms/grids`` — materialised-grid catalog ("what data is
  decoded and queryable right now") populated by the
  ``aeroza-materialise-mrms`` worker.
- ``GET /v1/mrms/grids/sample`` — point sample (``lat``, ``lng``) against
  the latest matching grid (or one ``at_time`` in the past). The first
  read-side primitive over the materialised grids — turns the catalog
  from "what is available" into "what is the value here, right now".
- ``GET /v1/mrms/grids/polygon`` — reduce one grid over the cells inside
  a ``polygon`` (``max`` / ``mean`` / ``min`` / ``count_ge``). Building
  block for "is anything intense enough inside this region right now?"
  alerting / geofencing.
- ``GET /v1/mrms/grids/{file_key}`` — single-grid detail by S3 key.
- ``GET /v1/stats`` — compact health-style snapshot of how much data the
  system currently knows about (alerts active/total, MRMS files,
  materialised grids, and freshness watermarks).

Route registration order matters: ``/alerts/stream`` is registered before
``/alerts/{alert_id}`` so the literal path wins over the path-parameter
matcher. Same for ``/mrms/grids/sample`` and ``/mrms/grids/polygon`` vs
``/mrms/grids/{file_key:path}``.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Annotated

import structlog
from fastapi import APIRouter, Depends, HTTPException, Path, Query, status
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from aeroza.ingest.nws_alerts import Severity
from aeroza.query.alerts import (
    DEFAULT_LIMIT as ALERTS_DEFAULT_LIMIT,
)
from aeroza.query.alerts import (
    MAX_LIMIT as ALERTS_MAX_LIMIT,
)
from aeroza.query.alerts import (
    find_active_alerts,
    find_alert_by_id,
)
from aeroza.query.dependencies import SubscriberDep, get_session
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
from aeroza.query.parsers import parse_bbox, parse_point, parse_polygon
from aeroza.query.schemas import (
    AlertDetailFeature,
    AlertFeatureCollection,
    alert_view_to_detail_feature,
    alert_view_to_feature,
)
from aeroza.query.stats import Stats, compute_stats, stats_view_to_model

log = structlog.get_logger(__name__)

router = APIRouter(prefix="/v1", tags=["alerts"])

SSE_MEDIA_TYPE: str = "text/event-stream"
SSE_HEADERS: dict[str, str] = {
    "Cache-Control": "no-cache",
    "X-Accel-Buffering": "no",  # nginx: don't buffer chunked output
    "Connection": "keep-alive",
}


@router.get(
    "/alerts",
    response_model=AlertFeatureCollection,
    response_model_exclude_none=True,
    summary="List active NWS alerts",
    description=(
        "Returns currently-active NWS alerts as a GeoJSON FeatureCollection. "
        "Filter by a single point (alerts whose polygon intersects), or by "
        "a bounding box (``min_lng,min_lat,max_lng,max_lat``), and/or by "
        "minimum severity. ``point`` and ``bbox`` are mutually exclusive — "
        "supplying both is an error. Results are ordered by severity "
        "descending then earliest expiry."
    ),
)
async def list_alerts(
    session: Annotated[AsyncSession, Depends(get_session)],
    point: Annotated[
        str | None,
        Query(
            description="Filter to alerts intersecting 'lat,lng'",
            examples=["29.76,-95.37"],
        ),
    ] = None,
    bbox: Annotated[
        str | None,
        Query(
            description="Filter to alerts intersecting 'min_lng,min_lat,max_lng,max_lat'",
            examples=["-95.7,29.5,-95.0,30.0"],
        ),
    ] = None,
    severity: Annotated[
        Severity | None,
        Query(description="Return only alerts at this severity level or higher"),
    ] = None,
    limit: Annotated[
        int,
        Query(
            ge=1,
            le=ALERTS_MAX_LIMIT,
            description=f"Max results to return (default {ALERTS_DEFAULT_LIMIT})",
        ),
    ] = ALERTS_DEFAULT_LIMIT,
) -> AlertFeatureCollection:
    if point is not None and bbox is not None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="point and bbox are mutually exclusive",
        )
    coord = parse_point(point)
    box = parse_bbox(bbox)
    views = await find_active_alerts(
        session,
        point=coord,
        bbox=box,
        severity_at_least=severity,
        limit=limit,
    )
    return AlertFeatureCollection(
        features=[alert_view_to_feature(view) for view in views],
    )


@router.get(
    "/alerts/stream",
    summary="Stream new NWS alerts (Server-Sent Events)",
    response_class=StreamingResponse,
    responses={
        200: {
            "content": {SSE_MEDIA_TYPE: {}},
            "description": (
                "An SSE stream where each ``event: alert`` carries an "
                "alert payload (NWS-aliased JSON) in ``data:``. "
                "The connection stays open; clients should reconnect with "
                "``Last-Event-ID`` if they want resume semantics (not yet "
                "honored on the server side)."
            ),
        },
        503: {"description": "Streaming is not available (NATS broker unreachable)."},
    },
)
async def stream_alerts(subscriber: SubscriberDep) -> StreamingResponse:
    return StreamingResponse(
        _alert_event_stream(subscriber),
        media_type=SSE_MEDIA_TYPE,
        headers=SSE_HEADERS,
    )


@router.get(
    "/alerts/{alert_id:path}",
    response_model=AlertDetailFeature,
    response_model_exclude_none=True,
    summary="Get a single NWS alert (full detail)",
    description=(
        "Returns one alert as a GeoJSON Feature including the long-form "
        "``description`` and ``instruction`` fields that the list endpoint "
        "omits. Includes alerts whose ``expires`` is in the past."
    ),
    responses={404: {"description": "Alert not found."}},
)
async def get_alert(
    session: Annotated[AsyncSession, Depends(get_session)],
    alert_id: Annotated[
        str,
        Path(
            description="Alert id (often a URN, e.g. 'urn:oid:2.49.0.1.840.0.…')",
            min_length=1,
        ),
    ],
) -> AlertDetailFeature:
    view = await find_alert_by_id(session, alert_id)
    if view is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"alert {alert_id!r} not found",
        )
    return alert_view_to_detail_feature(view)


async def _alert_event_stream(subscriber: SubscriberDep) -> AsyncIterator[bytes]:
    """Encode each :class:`Alert` from the subscriber as one SSE message."""
    yield b": connected\n\n"
    try:
        async for alert in subscriber.subscribe_new_alerts():
            payload = alert.model_dump_json(by_alias=True)
            # SSE event format: id (for Last-Event-ID), event name, payload, blank line.
            yield (f"event: alert\nid: {alert.id}\ndata: {payload}\n\n".encode())
    except Exception as exc:
        log.exception("stream.alerts.terminated", error=str(exc))
        raise


@router.get(
    "/mrms/files",
    response_model=MrmsFileList,
    response_model_by_alias=True,
    response_model_exclude_none=False,
    tags=["mrms"],
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
    tags=["mrms"],
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


@router.get(
    "/mrms/grids/sample",
    response_model=MrmsGridSampleResponse,
    response_model_by_alias=True,
    response_model_exclude_none=False,
    tags=["mrms"],
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
    tags=["mrms"],
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
    tags=["mrms"],
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


@router.get(
    "/stats",
    response_model=Stats,
    response_model_by_alias=True,
    response_model_exclude_none=False,
    tags=["meta"],
    summary="Live system stats: alerts + MRMS counts and freshness",
    description=(
        "Compact 'what does the system know right now?' snapshot. Cheap "
        "aggregate counts (alerts active/total, MRMS files vs grids "
        "materialised) plus the latest ``valid_at`` and ``materialised_at`` "
        "timestamps so callers can confirm data is flowing without scanning "
        "the catalogs themselves."
    ),
)
async def get_stats_route(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> Stats:
    now = datetime.now(UTC)
    view = await compute_stats(session, now=now)
    return stats_view_to_model(view, generated_at=now)
