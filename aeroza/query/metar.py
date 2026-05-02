"""Query helpers + wire schemas for METAR observations.

Mirrors the pattern in :mod:`aeroza.query.mrms`: row → pydantic model
mapping plus camelCase serialisation aliases. The list response uses
``MetarObservationList`` with a discriminator string for symmetry with
the rest of the v1 surface.
"""

from __future__ import annotations

from datetime import datetime
from typing import Final, Literal

from pydantic import BaseModel, ConfigDict, Field

from aeroza.ingest.metar_models import MetarObservationRow

DEFAULT_LIMIT: Final[int] = 100
MAX_LIMIT: Final[int] = 500


class MetarObservationItem(BaseModel):
    """One row of ``GET /v1/metar`` / ``GET /v1/metar/{station}/latest``."""

    model_config = ConfigDict(populate_by_name=True, frozen=True)

    type: Literal["MetarObservation"] = "MetarObservation"
    station_id: str = Field(serialization_alias="stationId")
    observation_time: datetime = Field(serialization_alias="observationTime")
    latitude: float
    longitude: float
    raw_text: str = Field(serialization_alias="rawText")
    temp_c: float | None = Field(default=None, serialization_alias="tempC")
    dewpoint_c: float | None = Field(default=None, serialization_alias="dewpointC")
    wind_speed_kt: float | None = Field(default=None, serialization_alias="windSpeedKt")
    wind_direction_deg: int | None = Field(default=None, serialization_alias="windDirectionDeg")
    wind_gust_kt: float | None = Field(default=None, serialization_alias="windGustKt")
    visibility_sm: float | None = Field(default=None, serialization_alias="visibilitySm")
    altimeter_hpa: float | None = Field(default=None, serialization_alias="altimeterHpa")
    flight_category: str | None = Field(default=None, serialization_alias="flightCategory")


class MetarObservationList(BaseModel):
    """Wire shape for ``GET /v1/metar``."""

    model_config = ConfigDict(populate_by_name=True, frozen=True)

    type: Literal["MetarObservationList"] = "MetarObservationList"
    items: list[MetarObservationItem]


def metar_row_to_item(row: MetarObservationRow) -> MetarObservationItem:
    return MetarObservationItem(
        station_id=row.station_id,
        observation_time=row.observation_time,
        latitude=row.latitude,
        longitude=row.longitude,
        raw_text=row.raw_text,
        temp_c=row.temp_c,
        dewpoint_c=row.dewpoint_c,
        wind_speed_kt=row.wind_speed_kt,
        wind_direction_deg=row.wind_direction_deg,
        wind_gust_kt=row.wind_gust_kt,
        visibility_sm=row.visibility_sm,
        altimeter_hpa=row.altimeter_hpa,
        flight_category=row.flight_category,
    )


__all__ = [
    "DEFAULT_LIMIT",
    "MAX_LIMIT",
    "MetarObservationItem",
    "MetarObservationList",
    "metar_row_to_item",
]
