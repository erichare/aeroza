"""Cross-cutting value objects for geospatial and temporal queries.

These are intentionally framework-agnostic frozen dataclasses so they can be
constructed cheaply, hashed for caching, and shared across ingest, query,
nowcast, and verify modules without circular imports.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Self

LAT_MIN: float = -90.0
LAT_MAX: float = 90.0
LNG_MIN: float = -180.0
LNG_MAX: float = 180.0


@dataclass(frozen=True, slots=True)
class Coordinate:
    """A latitude/longitude pair in WGS-84."""

    lat: float
    lng: float

    def __post_init__(self) -> None:
        if not LAT_MIN <= self.lat <= LAT_MAX:
            raise ValueError(f"latitude {self.lat} outside [{LAT_MIN}, {LAT_MAX}]")
        if not LNG_MIN <= self.lng <= LNG_MAX:
            raise ValueError(f"longitude {self.lng} outside [{LNG_MIN}, {LNG_MAX}]")


@dataclass(frozen=True, slots=True)
class BoundingBox:
    """An axis-aligned WGS-84 bounding box.

    Antimeridian-crossing boxes (where ``min_lng > max_lng``) are not yet
    supported; callers must split them. This is intentional for v1 — global
    coverage is post-MVP.
    """

    min_lat: float
    min_lng: float
    max_lat: float
    max_lng: float

    def __post_init__(self) -> None:
        Coordinate(self.min_lat, self.min_lng)
        Coordinate(self.max_lat, self.max_lng)
        if self.min_lat > self.max_lat:
            raise ValueError(f"min_lat {self.min_lat} > max_lat {self.max_lat}")
        if self.min_lng > self.max_lng:
            raise ValueError(
                f"antimeridian-crossing bbox not supported: "
                f"min_lng {self.min_lng} > max_lng {self.max_lng}"
            )

    @classmethod
    def from_corners(cls, sw: Coordinate, ne: Coordinate) -> Self:
        return cls(sw.lat, sw.lng, ne.lat, ne.lng)

    def contains(self, point: Coordinate) -> bool:
        return (
            self.min_lat <= point.lat <= self.max_lat and self.min_lng <= point.lng <= self.max_lng
        )


@dataclass(frozen=True, slots=True)
class TimeWindow:
    """A half-open time interval ``[start, end)`` with timezone-aware bounds."""

    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        if self.start.tzinfo is None or self.end.tzinfo is None:
            raise ValueError("TimeWindow bounds must be timezone-aware")
        if self.start >= self.end:
            raise ValueError(
                f"start {self.start.isoformat()} must precede end {self.end.isoformat()}"
            )

    def contains(self, instant: datetime) -> bool:
        if instant.tzinfo is None:
            raise ValueError("instant must be timezone-aware")
        return self.start <= instant < self.end

    @classmethod
    def of(cls, *, start: datetime, end: datetime) -> Self:
        return cls(start=_ensure_aware(start), end=_ensure_aware(end))


def _ensure_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value
