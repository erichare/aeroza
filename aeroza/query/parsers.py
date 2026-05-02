"""Query-param parsers shared by v1 routes."""

from __future__ import annotations

from fastapi import HTTPException, status

from aeroza.shared.types import BoundingBox, Coordinate

_BBOX_PART_COUNT: int = 4
_MIN_POLYGON_VERTICES: int = 3


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


def parse_polygon(raw: str | None) -> tuple[tuple[float, float], ...] | None:
    """Parse a flat ``"lng,lat,lng,lat,..."`` query parameter into a polygon.

    Coordinate order matches :func:`parse_bbox` (GeoJSON / OGC ``lng lat``
    pairs) so a polygon copied from a map widget pastes in directly.
    Returns ``None`` for missing input. Raises ``HTTPException(400)`` for
    malformed values, an odd number of components, fewer than three
    vertices, or any out-of-range coordinate.

    The returned tuple is *not* automatically closed — consumers that
    need a closed ring (e.g. ray-casting point-in-polygon) should treat
    the last edge as connecting back to the first vertex on their own.
    """
    if raw is None:
        return None
    parts = [p for p in raw.split(",") if p.strip() != ""]
    if len(parts) % 2 != 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"invalid polygon '{raw}': expected an even number of values (lng,lat pairs)",
        )
    try:
        flat = [float(p) for p in parts]
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"invalid polygon '{raw}': all coordinates must be numeric",
        ) from None
    vertices = tuple((flat[i], flat[i + 1]) for i in range(0, len(flat), 2))
    if len(vertices) < _MIN_POLYGON_VERTICES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"invalid polygon '{raw}': need at least {_MIN_POLYGON_VERTICES} vertices, "
                f"got {len(vertices)}"
            ),
        )
    for lng, lat in vertices:
        if not -180.0 <= lng <= 180.0 or not -90.0 <= lat <= 90.0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    f"invalid polygon '{raw}': vertex (lng={lng}, lat={lat}) out of WGS84 range"
                ),
            )
    return vertices
