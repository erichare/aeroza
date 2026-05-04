"""``/v1/alerts*`` routes — NWS active, SSE stream, IEM historical, detail.

Route registration order matters: ``/alerts/stream`` and
``/alerts/historical`` register before ``/alerts/{alert_id:path}`` so the
literal paths win over the path-parameter matcher.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime
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
from aeroza.query.historical_alerts import (
    HistoricalAlertQuery,
    fetch_historical_alerts,
    parse_wfo_list,
)
from aeroza.query.parsers import parse_bbox, parse_point
from aeroza.query.schemas import (
    AlertDetailFeature,
    AlertFeatureCollection,
    alert_view_to_detail_feature,
    alert_view_to_feature,
)

log = structlog.get_logger(__name__)

router = APIRouter(tags=["alerts"])

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
    "/alerts/historical",
    response_model=AlertFeatureCollection,
    response_model_exclude_none=True,
    summary="List historical NWS Storm-Based Warnings (IEM archive)",
    description=(
        "Returns NWS Storm-Based Warnings (tornado, severe thunderstorm, "
        "flash flood, marine, etc.) issued during the supplied UTC window, "
        "filtered to one or more NWS forecast offices (``wfos``). Powered "
        "by the Iowa Environmental Mesonet archive, which retains every "
        "warning polygon back to 2002 — well beyond NWS's own ~30-day "
        "retention. Returns the same GeoJSON ``FeatureCollection`` shape "
        "as ``GET /v1/alerts`` so the same map renderer works."
    ),
)
async def list_historical_alerts(
    since: Annotated[
        datetime,
        Query(
            description="Inclusive UTC window start (ISO-8601, with Z or offset)",
            examples=["2024-05-16T22:00:00Z"],
        ),
    ],
    until: Annotated[
        datetime,
        Query(
            description="Exclusive UTC window end (ISO-8601, with Z or offset)",
            examples=["2024-05-17T02:30:00Z"],
        ),
    ],
    wfos: Annotated[
        str,
        Query(
            description=(
                "Comma-separated NWS forecast office 3-letter codes (e.g. "
                "'HGX,LCH'). At least one is required so we don't ship "
                "CONUS-wide history through one process."
            ),
            min_length=3,
            examples=["HGX,LCH"],
        ),
    ],
) -> AlertFeatureCollection:
    if until <= since:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="until must be strictly after since",
        )
    parsed_wfos = parse_wfo_list(wfos)
    if not parsed_wfos:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="at least one valid WFO code is required",
        )
    return await fetch_historical_alerts(
        HistoricalAlertQuery(since=since, until=until, wfos=parsed_wfos),
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
