"""Query-param parsers shared by v1 routes."""

from __future__ import annotations

from fastapi import HTTPException, status

from aeroza.shared.types import BoundingBox, Coordinate

_BBOX_PART_COUNT: int = 4


def parse_point(raw: str | None) -> Coordinate | None:
    """Parse a ``"lat,lng"`` query parameter into a :class:`Coordinate`.

    Returns ``None`` for missing input. Raises ``HTTPException(400)`` with a
    consumer-friendly message for any malformed value or out-of-range
    latitude/longitude.
    """
    if raw is None:
        return None
    parts = raw.split(",")
    if len(parts) != 2:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"invalid point '{raw}': expected 'lat,lng'",
        )
    try:
        lat = float(parts[0])
        lng = float(parts[1])
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"invalid point '{raw}': lat and lng must be numeric",
        ) from None
    try:
        return Coordinate(lat=lat, lng=lng)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"invalid point '{raw}': {exc}",
        ) from None


def parse_bbox(raw: str | None) -> BoundingBox | None:
    """Parse a ``"min_lng,min_lat,max_lng,max_lat"`` query parameter.

    The ``lng,lat,lng,lat`` ordering matches GeoJSON / OGC convention so
    callers copying coordinates from a map widget don't have to reorder.
    Returns ``None`` for missing input; raises ``HTTPException(400)`` for
    any malformed or out-of-range value.
    """
    if raw is None:
        return None
    parts = raw.split(",")
    if len(parts) != _BBOX_PART_COUNT:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"invalid bbox '{raw}': expected 'min_lng,min_lat,max_lng,max_lat'",
        )
    try:
        min_lng, min_lat, max_lng, max_lat = (float(p) for p in parts)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"invalid bbox '{raw}': all four bounds must be numeric",
        ) from None
    try:
        return BoundingBox(min_lat=min_lat, min_lng=min_lng, max_lat=max_lat, max_lng=max_lng)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"invalid bbox '{raw}': {exc}",
        ) from None
