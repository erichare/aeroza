"""DB ops on ``metar_observations``: upsert + read.

Upsert is keyed on ``(station_id, observation_time)`` and updates
mutable measurement columns when a row already exists. AWC's API
sometimes re-publishes the same observation with a corrected reading
or a SPECI update at the same cycle minute; the upsert lets the
worker tick through repeatedly without the catalog drifting.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import literal_column, or_, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from aeroza.ingest.metar import MetarObservation
from aeroza.ingest.metar_models import MetarObservationRow

# Columns the upsert may overwrite when the row already exists.
# (id, station_id, observation_time, created_at) are immutable.
_MUTABLE_COLUMNS: tuple[str, ...] = (
    "latitude",
    "longitude",
    "raw_text",
    "temp_c",
    "dewpoint_c",
    "wind_speed_kt",
    "wind_direction_deg",
    "wind_gust_kt",
    "visibility_sm",
    "altimeter_hpa",
    "flight_category",
)


@dataclass(frozen=True, slots=True)
class MetarUpsertResult:
    """Per-call summary, useful for the worker's structured log."""

    inserted: int
    updated: int

    @property
    def total(self) -> int:
        return self.inserted + self.updated


_EMPTY_RESULT: MetarUpsertResult = MetarUpsertResult(inserted=0, updated=0)


def _to_row_dict(obs: MetarObservation) -> dict[str, Any]:
    return {
        "station_id": obs.station_id,
        "observation_time": obs.observation_time,
        "latitude": obs.latitude,
        "longitude": obs.longitude,
        "raw_text": obs.raw_text,
        "temp_c": obs.temp_c,
        "dewpoint_c": obs.dewpoint_c,
        "wind_speed_kt": obs.wind_speed_kt,
        "wind_direction_deg": obs.wind_direction_deg,
        "wind_gust_kt": obs.wind_gust_kt,
        "visibility_sm": obs.visibility_sm,
        "altimeter_hpa": obs.altimeter_hpa,
        "flight_category": obs.flight_category,
    }


async def upsert_metar_observations(
    session: AsyncSession,
    observations: Iterable[MetarObservation],
) -> MetarUpsertResult:
    """Insert or merge a batch of observations.

    Returns counts of newly-inserted vs updated rows. Caller commits.
    """
    rows = [_to_row_dict(o) for o in observations]
    if not rows:
        return _EMPTY_RESULT

    insert_stmt = pg_insert(MetarObservationRow).values(rows)

    update_set: dict[str, Any] = {col: insert_stmt.excluded[col] for col in _MUTABLE_COLUMNS}

    table = MetarObservationRow.__table__
    excluded = insert_stmt.excluded
    changed_predicate = or_(
        *(table.c[col].is_distinct_from(excluded[col]) for col in _MUTABLE_COLUMNS)
    )

    # `xmax = 0` is the canonical PG "row was inserted in this txn"
    # check: the deleted-by-xact id is zero only when no previous
    # version existed. Mirrors the same trick used in mrms_store.
    upsert_stmt: Any = insert_stmt.on_conflict_do_update(
        index_elements=[
            MetarObservationRow.station_id,
            MetarObservationRow.observation_time,
        ],
        set_=update_set,
        where=changed_predicate,
    ).returning(
        MetarObservationRow.station_id,
        literal_column("(xmax = 0)").label("inserted"),
    )

    result = await session.execute(upsert_stmt)
    affected = result.all()
    inserted = sum(1 for row in affected if row.inserted)
    updated = sum(1 for row in affected if not row.inserted)
    return MetarUpsertResult(inserted=inserted, updated=updated)


async def list_metar_observations(
    session: AsyncSession,
    *,
    station_id: str | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    bbox: tuple[float, float, float, float] | None = None,  # min_lng, min_lat, max_lng, max_lat
    limit: int = 100,
) -> Sequence[MetarObservationRow]:
    """Filtered list, newest first.

    The (station_id, observation_time DESC) index covers single-station
    queries; bbox queries fall back to a sequential scan over the
    observation_time range — fine at this scale (one row per station
    per cycle, a few thousand stations max).
    """
    stmt = select(MetarObservationRow).order_by(MetarObservationRow.observation_time.desc())
    if station_id:
        stmt = stmt.where(MetarObservationRow.station_id == station_id)
    if since:
        stmt = stmt.where(MetarObservationRow.observation_time >= since)
    if until:
        stmt = stmt.where(MetarObservationRow.observation_time < until)
    if bbox:
        min_lng, min_lat, max_lng, max_lat = bbox
        stmt = stmt.where(
            MetarObservationRow.longitude >= min_lng,
            MetarObservationRow.longitude <= max_lng,
            MetarObservationRow.latitude >= min_lat,
            MetarObservationRow.latitude <= max_lat,
        )
    stmt = stmt.limit(limit)
    result = await session.execute(stmt)
    return result.scalars().all()


async def find_latest_metar_for_station(
    session: AsyncSession,
    *,
    station_id: str,
) -> MetarObservationRow | None:
    """Return the most-recent observation for one station, or None."""
    stmt = (
        select(MetarObservationRow)
        .where(MetarObservationRow.station_id == station_id)
        .order_by(MetarObservationRow.observation_time.desc())
        .limit(1)
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


__all__ = [
    "MetarUpsertResult",
    "find_latest_metar_for_station",
    "list_metar_observations",
    "upsert_metar_observations",
]
