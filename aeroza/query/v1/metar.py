"""``/v1/metar*`` routes — surface-station observations."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from aeroza.ingest.metar_store import find_latest_metar_for_station, list_metar_observations
from aeroza.query.dependencies import get_session
from aeroza.query.metar import (
    DEFAULT_LIMIT as METAR_DEFAULT_LIMIT,
)
from aeroza.query.metar import (
    MAX_LIMIT as METAR_MAX_LIMIT,
)
from aeroza.query.metar import (
    MetarObservationItem,
    MetarObservationList,
    metar_row_to_item,
)
from aeroza.query.parsers import parse_bbox

router = APIRouter(tags=["metar"])


@router.get(
    "/metar",
    response_model=MetarObservationList,
    response_model_by_alias=True,
)
async def list_metar_observations_route(
    session: Annotated[AsyncSession, Depends(get_session)],
    station: Annotated[
        str | None,
        Query(description="Filter to one ICAO station id (e.g. KIAH). Case-insensitive."),
    ] = None,
    since: Annotated[
        datetime | None,
        Query(description="ISO-8601 lower bound (inclusive) on observationTime."),
    ] = None,
    until: Annotated[
        datetime | None,
        Query(description="ISO-8601 upper bound (exclusive) on observationTime."),
    ] = None,
    bbox: Annotated[
        str | None,
        Query(description="min_lng,min_lat,max_lng,max_lat — same convention as /v1/alerts."),
    ] = None,
    limit: Annotated[
        int,
        Query(
            ge=1,
            le=METAR_MAX_LIMIT,
            description=f"Maximum rows (default {METAR_DEFAULT_LIMIT}, max {METAR_MAX_LIMIT}).",
        ),
    ] = METAR_DEFAULT_LIMIT,
) -> MetarObservationList:
    """List recent METAR observations, newest first."""
    parsed_bbox = parse_bbox(bbox)
    rows = await list_metar_observations(
        session,
        station_id=station.upper() if station else None,
        since=since,
        until=until,
        bbox=(
            (parsed_bbox.min_lng, parsed_bbox.min_lat, parsed_bbox.max_lng, parsed_bbox.max_lat)
            if parsed_bbox is not None
            else None
        ),
        limit=limit,
    )
    return MetarObservationList(items=[metar_row_to_item(r) for r in rows])


@router.get(
    "/metar/{station_id}/latest",
    response_model=MetarObservationItem,
    response_model_by_alias=True,
)
async def get_latest_metar_route(
    session: Annotated[AsyncSession, Depends(get_session)],
    station_id: Annotated[
        str,
        Path(
            description="ICAO 4-letter station id (e.g. KIAH). Case-insensitive.",
            min_length=3,
            max_length=8,
        ),
    ],
) -> MetarObservationItem:
    """Most recent observation for one station."""
    row = await find_latest_metar_for_station(session, station_id=station_id.upper())
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"no METAR observations for station {station_id.upper()!r}",
        )
    return metar_row_to_item(row)
