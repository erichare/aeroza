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
from aeroza.shared.types import BoundingBox, Coordinate

DEFAULT_LIMIT: int = 100
MAX_LIMIT: int = 500


@dataclass(frozen=True, slots=True)
class AlertView:
    """A read-projection of one alert with geometry already as GeoJSON.

    Includes the long-form ``description`` and ``instruction`` fields; callers
    that don't need them (the list endpoint) project them out at the schema
    boundary so the wire payload stays tight.
    """

    id: str
    event: str
    headline: str | None
    description: str | None
    instruction: str | None
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


_BASE_COLUMNS = (
    NwsAlertRow.id,
    NwsAlertRow.event,
    NwsAlertRow.headline,
    NwsAlertRow.description,
    NwsAlertRow.instruction,
    NwsAlertRow.severity,
    NwsAlertRow.urgency,
    NwsAlertRow.certainty,
    NwsAlertRow.sender_name,
    NwsAlertRow.area_desc,
    NwsAlertRow.effective,
    NwsAlertRow.onset,
    NwsAlertRow.expires,
    NwsAlertRow.ends,
)


async def find_active_alerts(
    session: AsyncSession,
    *,
    point: Coordinate | None = None,
    bbox: BoundingBox | None = None,
    severity_at_least: Severity | None = None,
    limit: int = DEFAULT_LIMIT,
) -> tuple[AlertView, ...]:
    """Return active alerts matching the supplied filters.

    "Active" means ``expires IS NULL OR expires > NOW()``. Results are ordered
    by severity descending, then earliest expiry. ``limit`` is clamped to
    :data:`MAX_LIMIT`.

    ``point`` and ``bbox`` are mutually compatible at this layer (both will be
    AND-ed); the route handler is what rejects callers passing both at once,
    since combining the two on the wire is almost always a bug.
    """
    bounded_limit = min(max(limit, 1), MAX_LIMIT)

    stmt = (
        select(
            *_BASE_COLUMNS,
            func.ST_AsGeoJSON(NwsAlertRow.geometry).label("geometry_json"),
        )
        .where(or_(NwsAlertRow.expires.is_(None), NwsAlertRow.expires > func.now()))
        .order_by(_SEVERITY_ORDER.desc(), NwsAlertRow.expires.asc().nulls_last())
        .limit(bounded_limit)
    )

    if point is not None:
        point_geom = func.ST_GeomFromText(f"POINT({point.lng} {point.lat})", NWS_ALERTS_SRID)
        stmt = stmt.where(func.ST_Intersects(NwsAlertRow.geometry, point_geom))

    if bbox is not None:
        envelope = func.ST_MakeEnvelope(
            bbox.min_lng, bbox.min_lat, bbox.max_lng, bbox.max_lat, NWS_ALERTS_SRID
        )
        stmt = stmt.where(func.ST_Intersects(NwsAlertRow.geometry, envelope))

    if severity_at_least is not None:
        stmt = stmt.where(NwsAlertRow.severity.in_(severities_at_least(severity_at_least)))

    result = await session.execute(stmt)
    return tuple(_row_to_view(row) for row in result.mappings())


async def find_alert_by_id(session: AsyncSession, alert_id: str) -> AlertView | None:
    """Return the alert with ``alert_id``, or ``None`` if absent.

    Includes alerts whose ``expires`` is in the past — the detail endpoint
    shows the full record regardless of activeness. Filtering by activeness
    belongs at the route layer if at all.
    """
    stmt = select(
        *_BASE_COLUMNS,
        func.ST_AsGeoJSON(NwsAlertRow.geometry).label("geometry_json"),
    ).where(NwsAlertRow.id == alert_id)
    result = await session.execute(stmt)
    row = result.mappings().first()
    return _row_to_view(row) if row is not None else None


def _row_to_view(row: Any) -> AlertView:
    geom_json = row["geometry_json"]
    geometry = json.loads(geom_json) if isinstance(geom_json, str) else None
    return AlertView(
        id=row["id"],
        event=row["event"],
        headline=row["headline"],
        description=row["description"],
        instruction=row["instruction"],
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
