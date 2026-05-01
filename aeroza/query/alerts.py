"""Read-side query repository for persisted NWS alerts.

Returns immutable :class:`AlertView` instances rather than ORM rows so callers
(route handlers, scheduled exporters, …) can rely on a stable shape and the
SQL drives geometry → GeoJSON conversion via PostGIS rather than parsing WKB
in Python on each request.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import case, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from aeroza.ingest.nws_alerts import Severity, severities_at_least
from aeroza.ingest.nws_alerts_models import NWS_ALERTS_SRID, NwsAlertRow
from aeroza.shared.types import Coordinate

DEFAULT_LIMIT: int = 100
MAX_LIMIT: int = 500


@dataclass(frozen=True, slots=True)
class AlertView:
    """A read-projection of one alert with geometry already as GeoJSON."""

    id: str
    event: str
    headline: str | None
    severity: str
    urgency: str
    certainty: str
    sender_name: str | None
    area_desc: str | None
    effective: datetime | None
    onset: datetime | None
    expires: datetime | None
    ends: datetime | None
    geometry: dict[str, Any] | None


# Severity ranks as a Postgres CASE expression — keep the order stable so the
# resulting SQL is identical across calls (helps with prepared statement caching).
_SEVERITY_ORDER = case(
    (NwsAlertRow.severity == Severity.EXTREME.value, 4),
    (NwsAlertRow.severity == Severity.SEVERE.value, 3),
    (NwsAlertRow.severity == Severity.MODERATE.value, 2),
    (NwsAlertRow.severity == Severity.MINOR.value, 1),
    else_=0,
)


async def find_active_alerts(
    session: AsyncSession,
    *,
    point: Coordinate | None = None,
    severity_at_least: Severity | None = None,
    limit: int = DEFAULT_LIMIT,
) -> tuple[AlertView, ...]:
    """Return active alerts matching the supplied filters.

    "Active" means ``expires IS NULL OR expires > NOW()``. Results are ordered
    by severity descending, then earliest expiry. ``limit`` is clamped to
    :data:`MAX_LIMIT`.
    """
    bounded_limit = min(max(limit, 1), MAX_LIMIT)

    stmt = (
        select(
            NwsAlertRow.id,
            NwsAlertRow.event,
            NwsAlertRow.headline,
            NwsAlertRow.severity,
            NwsAlertRow.urgency,
            NwsAlertRow.certainty,
            NwsAlertRow.sender_name,
            NwsAlertRow.area_desc,
            NwsAlertRow.effective,
            NwsAlertRow.onset,
            NwsAlertRow.expires,
            NwsAlertRow.ends,
            func.ST_AsGeoJSON(NwsAlertRow.geometry).label("geometry_json"),
        )
        .where(or_(NwsAlertRow.expires.is_(None), NwsAlertRow.expires > func.now()))
        .order_by(_SEVERITY_ORDER.desc(), NwsAlertRow.expires.asc().nulls_last())
        .limit(bounded_limit)
    )

    if point is not None:
        point_geom = func.ST_GeomFromText(f"POINT({point.lng} {point.lat})", NWS_ALERTS_SRID)
        stmt = stmt.where(func.ST_Intersects(NwsAlertRow.geometry, point_geom))

    if severity_at_least is not None:
        stmt = stmt.where(NwsAlertRow.severity.in_(severities_at_least(severity_at_least)))

    result = await session.execute(stmt)
    return tuple(_row_to_view(row) for row in result.mappings())


def _row_to_view(row: Any) -> AlertView:
    geom_json = row["geometry_json"]
    geometry = json.loads(geom_json) if isinstance(geom_json, str) else None
    return AlertView(
        id=row["id"],
        event=row["event"],
        headline=row["headline"],
        severity=row["severity"],
        urgency=row["urgency"],
        certainty=row["certainty"],
        sender_name=row["sender_name"],
        area_desc=row["area_desc"],
        effective=row["effective"],
        onset=row["onset"],
        expires=row["expires"],
        ends=row["ends"],
        geometry=geometry,
    )
