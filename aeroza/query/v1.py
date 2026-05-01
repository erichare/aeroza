"""Aeroza v1 query API.

Routes here are URL-versioned (``/v1/...``). The current surface:

- ``GET /v1/alerts`` — active NWS alerts as a GeoJSON ``FeatureCollection``,
  filterable by a single ``point``, a ``bbox``, and/or a minimum ``severity``.
- ``GET /v1/alerts/stream`` — Server-Sent Events feed re-emitting newly
  observed alerts published by the ingest worker on the ``aeroza.alerts.nws.new``
  NATS subject.
- ``GET /v1/alerts/{alert_id}`` — single-alert detail with the long-form
  ``description`` and ``instruction`` fields that the list endpoint omits.

Route registration order matters: ``/alerts/stream`` is registered before
``/alerts/{alert_id}`` so the literal path wins over the path-parameter
matcher.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Annotated

import structlog
from fastapi import APIRouter, Depends, HTTPException, Path, Query, status
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from aeroza.ingest.nws_alerts import Severity
from aeroza.query.alerts import DEFAULT_LIMIT, MAX_LIMIT, find_active_alerts, find_alert_by_id
from aeroza.query.dependencies import SubscriberDep, get_session
from aeroza.query.parsers import parse_bbox, parse_point
from aeroza.query.schemas import (
    AlertDetailFeature,
    AlertFeatureCollection,
    alert_view_to_detail_feature,
    alert_view_to_feature,
)

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
        Query(ge=1, le=MAX_LIMIT, description=f"Max results to return (default {DEFAULT_LIMIT})"),
    ] = DEFAULT_LIMIT,
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
