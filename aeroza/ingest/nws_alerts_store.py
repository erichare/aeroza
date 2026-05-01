"""Persistence for NWS alerts: idempotent upsert on stable ``id``.

NWS reissues an alert with the same id when it updates — so the natural
operation is ``INSERT … ON CONFLICT (id) DO UPDATE``. We:

- skip no-op updates via a ``WHERE`` clause comparing each mutable column
  with ``IS DISTINCT FROM`` (so two NULLs match), preventing churn on
  ``updated_at``;
- distinguish inserted from updated rows using Postgres's ``xmax`` system
  column, which is 0 for inserts and non-zero for updates — letting the
  whole batch run in a single round trip.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import structlog
from geoalchemy2.shape import from_shape
from shapely.errors import GEOSException
from shapely.geometry import shape as shapely_shape
from sqlalchemy import func, literal_column, or_
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from aeroza.ingest.nws_alerts import Alert
from aeroza.ingest.nws_alerts_models import NWS_ALERTS_SRID, NwsAlertRow

log = structlog.get_logger(__name__)

_MUTABLE_COLUMNS: tuple[str, ...] = (
    "event",
    "headline",
    "description",
    "instruction",
    "severity",
    "urgency",
    "certainty",
    "sender_name",
    "area_desc",
    "effective",
    "onset",
    "expires",
    "ends",
    "geometry",
)


@dataclass(frozen=True, slots=True)
class UpsertResult:
    """Outcome of a batch upsert."""

    inserted: int
    updated: int

    @property
    def total(self) -> int:
        return self.inserted + self.updated


async def upsert_alerts(session: AsyncSession, alerts: Iterable[Alert]) -> UpsertResult:
    """Upsert ``alerts`` by id, returning insert/update counts.

    Empty input is a no-op. The session is **not** committed; callers own the
    transaction boundary (typically via :func:`aeroza.shared.db.session_scope`).
    """
    rows = [_to_row_dict(alert) for alert in alerts]
    if not rows:
        return UpsertResult(inserted=0, updated=0)

    insert_stmt = pg_insert(NwsAlertRow).values(rows)
    update_set: dict[str, Any] = {col: insert_stmt.excluded[col] for col in _MUTABLE_COLUMNS}
    update_set["updated_at"] = func.now()

    upsert_stmt: Any = insert_stmt.on_conflict_do_update(
        index_elements=[NwsAlertRow.id],
        set_=update_set,
        where=_changed_predicate(insert_stmt),
    ).returning(
        NwsAlertRow.id,
        literal_column("(xmax = 0)").label("inserted"),
    )

    result = await session.execute(upsert_stmt)
    affected_rows = result.all()
    inserted = sum(1 for row in affected_rows if row.inserted)
    updated = len(affected_rows) - inserted
    return UpsertResult(inserted=inserted, updated=updated)


def _changed_predicate(stmt: Any) -> Any:
    """Build ``col IS DISTINCT FROM excluded.col OR …`` for every mutable column."""
    excluded = stmt.excluded
    table = NwsAlertRow.__table__
    return or_(*(table.c[col].is_distinct_from(excluded[col]) for col in _MUTABLE_COLUMNS))


def _to_row_dict(alert: Alert) -> dict[str, Any]:
    return {
        "id": alert.id,
        "event": alert.event,
        "headline": alert.headline,
        "description": alert.description,
        "instruction": alert.instruction,
        "severity": str(alert.severity),
        "urgency": str(alert.urgency),
        "certainty": str(alert.certainty),
        "sender_name": alert.sender_name,
        "area_desc": alert.area_desc,
        "effective": alert.effective,
        "onset": alert.onset,
        "expires": alert.expires,
        "ends": alert.ends,
        "geometry": _geometry_to_wkb(alert),
    }


def _geometry_to_wkb(alert: Alert) -> Any:
    if alert.geometry is None:
        return None
    try:
        return from_shape(shapely_shape(alert.geometry), srid=NWS_ALERTS_SRID)
    except (GEOSException, ValueError, TypeError) as exc:
        log.warning("nws.alerts.geometry_skip", id=alert.id, error=str(exc))
        return None
