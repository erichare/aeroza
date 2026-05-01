"""Ingest active alerts from the NOAA / National Weather Service public API.

The NWS API returns alerts as a GeoJSON FeatureCollection. We model only the
fields needed for downstream query and streaming; the raw geometry is kept as
an opaque GeoJSON dict so the geospatial layer can parse it once at storage
time. This module is pure: it issues one HTTPS GET, parses the response, and
returns an immutable tuple. Persistence and event publishing live in callers.

Reference: https://www.weather.gov/documentation/services-web-api
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any, Final

import httpx
import structlog
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from aeroza.shared.http import http_client

log = structlog.get_logger(__name__)

NWS_BASE_URL: Final[str] = "https://api.weather.gov"
ACTIVE_ALERTS_PATH: Final[str] = "/alerts/active"


class Severity(StrEnum):
    EXTREME = "Extreme"
    SEVERE = "Severe"
    MODERATE = "Moderate"
    MINOR = "Minor"
    UNKNOWN = "Unknown"


_SEVERITY_RANK: dict[str, int] = {
    Severity.EXTREME.value: 4,
    Severity.SEVERE.value: 3,
    Severity.MODERATE.value: 2,
    Severity.MINOR.value: 1,
    Severity.UNKNOWN.value: 0,
}


def severity_rank(severity: Severity | str) -> int:
    """Return the integer rank of ``severity`` (Extreme=4 … Unknown=0)."""
    key = severity.value if isinstance(severity, Severity) else severity
    return _SEVERITY_RANK.get(key, 0)


def severities_at_least(threshold: Severity) -> tuple[str, ...]:
    """Return the severity strings whose rank is ``>=`` ``threshold``'s rank.

    Useful for ``WHERE severity IN (...)`` filters where the column is stored
    as plain text rather than a ranked enum.
    """
    threshold_rank = severity_rank(threshold)
    return tuple(name for name, rank in _SEVERITY_RANK.items() if rank >= threshold_rank)


class Urgency(StrEnum):
    IMMEDIATE = "Immediate"
    EXPECTED = "Expected"
    FUTURE = "Future"
    PAST = "Past"
    UNKNOWN = "Unknown"


class Certainty(StrEnum):
    OBSERVED = "Observed"
    LIKELY = "Likely"
    POSSIBLE = "Possible"
    UNLIKELY = "Unlikely"
    UNKNOWN = "Unknown"


class Alert(BaseModel):
    """An active NWS alert, normalised for downstream consumption."""

    model_config = ConfigDict(
        frozen=True,
        str_strip_whitespace=True,
        extra="ignore",
        populate_by_name=True,  # accept both API aliases (areaDesc) and field names (area_desc)
    )

    id: str
    event: str
    headline: str | None = None
    description: str | None = None
    instruction: str | None = None
    severity: Severity = Severity.UNKNOWN
    urgency: Urgency = Urgency.UNKNOWN
    certainty: Certainty = Certainty.UNKNOWN
    sender_name: str | None = Field(default=None, alias="senderName")
    area_desc: str | None = Field(default=None, alias="areaDesc")
    effective: datetime | None = None
    onset: datetime | None = None
    expires: datetime | None = None
    ends: datetime | None = None
    geometry: dict[str, Any] | None = None


class NwsAlertsError(RuntimeError):
    """Raised when the NWS Alerts endpoint returns an error or malformed payload."""


async def fetch_active_alerts(
    *,
    area: str | None = None,
    point: tuple[float, float] | None = None,
    client: httpx.AsyncClient | None = None,
) -> tuple[Alert, ...]:
    """Fetch currently-active NWS alerts.

    Args:
        area: Two-letter US state/territory code (e.g. ``"TX"``). Optional.
        point: ``(lat, lng)`` to filter alerts intersecting a single point.
        client: Inject a pre-configured ``httpx.AsyncClient`` (e.g. for tests
            or shared connection pooling). When ``None``, a one-shot client is
            created via :func:`aeroza.shared.http.http_client`.
    """
    params: dict[str, str] = {}
    if area is not None:
        params["area"] = area
    if point is not None:
        params["point"] = f"{point[0]},{point[1]}"

    if client is not None:
        response = await _request(client, params)
    else:
        async with http_client(base_url=NWS_BASE_URL) as one_shot:
            response = await _request(one_shot, params)

    return _parse(response.json())


async def _request(client: httpx.AsyncClient, params: dict[str, str]) -> httpx.Response:
    url = ACTIVE_ALERTS_PATH if client.base_url else f"{NWS_BASE_URL}{ACTIVE_ALERTS_PATH}"
    log.debug("nws.alerts.fetch", url=url, params=params)
    try:
        response = await client.get(url, params=params)
        response.raise_for_status()
    except httpx.HTTPError as exc:
        raise NwsAlertsError(f"NWS alerts request failed: {exc}") from exc
    return response


def _parse(payload: Any) -> tuple[Alert, ...]:
    if not isinstance(payload, dict) or payload.get("type") != "FeatureCollection":
        raise NwsAlertsError(
            f"unexpected NWS payload: type={payload.get('type') if isinstance(payload, dict) else type(payload).__name__}"
        )

    raw_features = payload.get("features", [])
    if not isinstance(raw_features, list):
        raise NwsAlertsError("NWS payload features field is not a list")

    alerts: list[Alert] = []
    skipped = 0
    for feature in raw_features:
        if not isinstance(feature, dict):
            skipped += 1
            continue
        properties = feature.get("properties")
        if not isinstance(properties, dict):
            skipped += 1
            continue
        try:
            alerts.append(Alert.model_validate({**properties, "geometry": feature.get("geometry")}))
        except ValidationError as exc:
            log.warning("nws.alerts.skip_invalid", id=properties.get("id"), error=str(exc))
            skipped += 1

    if skipped:
        log.info("nws.alerts.parsed", kept=len(alerts), skipped=skipped)
    return tuple(alerts)
