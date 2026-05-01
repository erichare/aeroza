"""Query-param parsers shared by v1 routes."""

from __future__ import annotations

from fastapi import HTTPException, status

from aeroza.shared.types import Coordinate


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
