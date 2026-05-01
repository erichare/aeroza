"""Aeroza v1 query API.

Routes here are URL-versioned (``/v1/...``). The first endpoint, ``/v1/alerts``,
returns active NWS alerts as a GeoJSON ``FeatureCollection``, optionally
filtered to those intersecting a single point and/or to a minimum severity.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from aeroza.ingest.nws_alerts import Severity
from aeroza.query.alerts import DEFAULT_LIMIT, MAX_LIMIT, find_active_alerts
from aeroza.query.dependencies import get_session
from aeroza.query.parsers import parse_point
from aeroza.query.schemas import AlertFeatureCollection, alert_view_to_feature

router = APIRouter(prefix="/v1", tags=["alerts"])


@router.get(
    "/alerts",
    response_model=AlertFeatureCollection,
    response_model_exclude_none=True,
    summary="List active NWS alerts",
    description=(
        "Returns currently-active NWS alerts as a GeoJSON FeatureCollection. "
        "Filter by a single point (alerts whose polygon intersects), and/or "
        "by minimum severity. Results are ordered by severity descending then "
        "earliest expiry."
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
    severity: Annotated[
        Severity | None,
        Query(description="Return only alerts at this severity level or higher"),
    ] = None,
    limit: Annotated[
        int,
        Query(ge=1, le=MAX_LIMIT, description=f"Max results to return (default {DEFAULT_LIMIT})"),
    ] = DEFAULT_LIMIT,
) -> AlertFeatureCollection:
    coord = parse_point(point)
    views = await find_active_alerts(
        session,
        point=coord,
        severity_at_least=severity,
        limit=limit,
    )
    return AlertFeatureCollection(
        features=[alert_view_to_feature(view) for view in views],
    )
