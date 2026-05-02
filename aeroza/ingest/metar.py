"""Ingest METAR surface observations from aviationweather.gov.

METAR is the global standard for hourly surface weather reporting at
airports. The Aviation Weather Center (NOAA) publishes a JSON API
that returns *already-parsed* observations — so this module never has
to touch the legacy METAR text format. The raw text is preserved on
the row for callers who want their own parser, but the fields we
expose come straight from the JSON.

API: ``https://aviationweather.gov/api/data/metar?ids=<csv>&format=json``

Behaviour:

- All requested stations in one call (the API accepts CSV of IDs).
- Stations with no current observation come back as missing entries
  in the response, not as an error — the parser drops them.
- Returns immutable :class:`MetarObservation` tuples; persistence and
  publishing are caller concerns. (Same shape as
  :func:`aeroza.ingest.nws_alerts.fetch_active_alerts`.)
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Final

import httpx
import structlog
from pydantic import BaseModel, ConfigDict, ValidationError

from aeroza.shared.http import http_client

log = structlog.get_logger(__name__)

# AWC's METAR JSON endpoint. We pin to a single base URL and stop short
# of querying for the full list of CONUS stations on every tick — the
# caller passes the station list it wants, which keeps the ingest
# bounded and predictable.
AWC_METAR_BASE_URL: Final[str] = "https://aviationweather.gov"
AWC_METAR_PATH: Final[str] = "/api/data/metar"


@dataclass(frozen=True, slots=True)
class MetarObservation:
    """One parsed METAR row, ready to upsert.

    All measurement fields are nullable: AWC drops obs from a station
    whose sensors weren't reporting, but we still want the row (with
    the timestamp + raw_text) so a "last seen" question against the
    catalog has the right answer. Numeric fields use SI / aviation
    conventional units (hPa, °C, knots, statute miles, degrees) —
    consumers convert at the edge.
    """

    station_id: str
    observation_time: datetime
    latitude: float
    longitude: float
    raw_text: str
    temp_c: float | None = None
    dewpoint_c: float | None = None
    wind_speed_kt: float | None = None
    wind_direction_deg: int | None = None
    wind_gust_kt: float | None = None
    visibility_sm: float | None = None
    altimeter_hpa: float | None = None
    flight_category: str | None = None


class _AwcRecord(BaseModel):
    """The AWC JSON shape per record. Only fields we keep are typed.

    The API returns more (e.g. quality codes, cloud layers) — we ignore
    them with ``extra='ignore'`` so an upstream addition doesn't break
    parsing. ``icaoId`` is the canonical ICAO identifier (e.g. KIAH).
    ``reportTime`` is ISO-8601, *always UTC*.
    """

    model_config = ConfigDict(extra="ignore")

    icaoId: str
    reportTime: str
    lat: float
    lon: float
    rawOb: str
    temp: float | None = None
    dewp: float | None = None
    wspd: float | None = None
    wdir: int | str | None = None  # AWC sometimes returns "VRB"
    wgst: float | None = None
    visib: float | str | None = None  # "10+", "1/4", "M1/4", or float
    altim: float | None = None
    fltcat: str | None = None


def _coerce_wind_direction(value: int | str | None) -> int | None:
    """Convert AWC ``wdir`` (which can be "VRB" or numeric) to int.

    "VRB" — variable wind, common with light winds — flattens to None
    because there is no representable scalar. A truly absent value is
    also None on the wire.
    """
    if value is None or isinstance(value, str):
        return None
    return int(value)


def _coerce_visibility(value: float | str | None) -> float | None:
    """Convert AWC ``visib`` (mixed types) to a float in statute miles.

    AWC returns:
    - a float for normal visibility,
    - the string ``"10+"`` for visibility ≥ 10 SM (we floor to 10.0),
    - fractions ``"1/4"`` or ``"M1/4"`` for very low vis (we report
      ``0.25`` and ``0.0`` respectively — "M" is "less than", and we
      don't have a sub-quarter granularity in our schema).
    """
    if value is None:
        return None
    if isinstance(value, int | float):
        return float(value)
    if value == "10+":
        return 10.0
    if value == "M1/4":
        return 0.0
    if value == "1/4":
        return 0.25
    if value == "1/2":
        return 0.5
    if value == "3/4":
        return 0.75
    # Unknown shape — log and surface as None rather than raising.
    log.warning("metar.visibility.unparsed", value=value)
    return None


def _record_to_observation(record: _AwcRecord) -> MetarObservation | None:
    """Map AWC's record shape to our :class:`MetarObservation`.

    Returns None if the record is missing the load-bearing fields
    (id / timestamp / location / raw text). Anything else can be
    null and the row still ships.
    """
    if not record.icaoId or not record.rawOb:
        return None
    try:
        observation_time = datetime.fromisoformat(record.reportTime.replace("Z", "+00:00"))
    except ValueError:
        log.warning("metar.report_time.invalid", value=record.reportTime, station=record.icaoId)
        return None
    if observation_time.tzinfo is None:
        observation_time = observation_time.replace(tzinfo=UTC)
    return MetarObservation(
        station_id=record.icaoId,
        observation_time=observation_time,
        latitude=record.lat,
        longitude=record.lon,
        raw_text=record.rawOb,
        temp_c=record.temp,
        dewpoint_c=record.dewp,
        wind_speed_kt=record.wspd,
        wind_direction_deg=_coerce_wind_direction(record.wdir),
        wind_gust_kt=record.wgst,
        visibility_sm=_coerce_visibility(record.visib),
        altimeter_hpa=record.altim,
        flight_category=record.fltcat,
    )


def parse_awc_response(payload: list[dict[str, Any]]) -> tuple[MetarObservation, ...]:
    """Decode the AWC JSON response into typed observations.

    Pure function — exposed for tests.
    """
    out: list[MetarObservation] = []
    for raw in payload:
        try:
            record = _AwcRecord.model_validate(raw)
        except ValidationError as exc:
            log.warning("metar.record.invalid", error=str(exc))
            continue
        observation = _record_to_observation(record)
        if observation is not None:
            out.append(observation)
    return tuple(out)


async def fetch_metar_observations(
    *,
    station_ids: Sequence[str],
    client: httpx.AsyncClient | None = None,
) -> tuple[MetarObservation, ...]:
    """Fetch the latest METAR for each station in ``station_ids``.

    Pass an injected ``client`` from tests; otherwise the function
    opens a short-lived one against the AWC base URL.
    """
    if not station_ids:
        return ()
    params = {"ids": ",".join(station_ids), "format": "json"}

    if client is not None:
        response = await client.get(AWC_METAR_PATH, params=params)
        response.raise_for_status()
        return parse_awc_response(response.json())

    async with http_client(base_url=AWC_METAR_BASE_URL) as managed:
        response = await managed.get(AWC_METAR_PATH, params=params)
        response.raise_for_status()
        return parse_awc_response(response.json())


__all__ = [
    "AWC_METAR_BASE_URL",
    "AWC_METAR_PATH",
    "MetarObservation",
    "fetch_metar_observations",
    "parse_awc_response",
]
