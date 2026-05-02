"""Persistence for the mrms_nowcasts catalog.

Upserts on ``(source_file_key, algorithm, horizon)`` so re-running the
worker against the same observation overwrites in place. The
``valid_at`` derives from the source observation's ``valid_at`` plus
the horizon, so it doesn't need to be in the natural key.

Same convention as the other stores: the session is **not** committed.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Final

import structlog
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from aeroza.nowcast.models import NowcastRow

log = structlog.get_logger(__name__)

DEFAULT_LIST_LIMIT: Final[int] = 100
MAX_LIST_LIMIT: Final[int] = 500

_MUTABLE_COLUMNS: Final[tuple[str, ...]] = (
    "valid_at",
    "zarr_uri",
    "variable",
    "dims_json",
    "shape_json",
    "dtype",
    "nbytes",
)


async def upsert_nowcast(
    session: AsyncSession,
    *,
    source_file_key: str,
    product: str,
    level: str,
    algorithm: str,
    horizon_minutes: int,
    valid_at: datetime,
    zarr_uri: str,
    variable: str,
    dims: tuple[str, ...],
    shape: tuple[int, ...],
    dtype: str,
    nbytes: int,
) -> NowcastRow:
    """Insert or update one nowcast row. Returns the persisted ORM row."""
    row = {
        "source_file_key": source_file_key,
        "product": product,
        "level": level,
        "algorithm": algorithm,
        "forecast_horizon_minutes": horizon_minutes,
        "valid_at": valid_at,
        "zarr_uri": zarr_uri,
        "variable": variable,
        "dims_json": json.dumps(list(dims)),
        "shape_json": json.dumps(list(shape)),
        "dtype": dtype,
        "nbytes": nbytes,
    }
    insert_stmt = pg_insert(NowcastRow).values(row)
    update_set: dict[str, Any] = {col: insert_stmt.excluded[col] for col in _MUTABLE_COLUMNS}
    upsert_stmt = insert_stmt.on_conflict_do_update(
        constraint="uq_mrms_nowcasts_source_algorithm_horizon",
        set_=update_set,
    ).returning(NowcastRow)
    result = await session.execute(upsert_stmt)
    persisted = result.scalar_one()
    await session.refresh(persisted)
    log.info(
        "nowcast.store.upsert",
        source_file_key=source_file_key,
        algorithm=algorithm,
        horizon_minutes=horizon_minutes,
        valid_at=valid_at.isoformat(),
        zarr_uri=zarr_uri,
    )
    return persisted


async def list_nowcasts(
    session: AsyncSession,
    *,
    product: str | None = None,
    level: str | None = None,
    algorithm: str | None = None,
    horizon_minutes: int | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    limit: int = DEFAULT_LIST_LIMIT,
) -> tuple[NowcastRow, ...]:
    """Return nowcasts ordered by ``valid_at`` descending (newest first)."""
    bounded_limit = min(max(limit, 1), MAX_LIST_LIMIT)
    stmt = select(NowcastRow).order_by(NowcastRow.valid_at.desc()).limit(bounded_limit)
    if product is not None:
        stmt = stmt.where(NowcastRow.product == product)
    if level is not None:
        stmt = stmt.where(NowcastRow.level == level)
    if algorithm is not None:
        stmt = stmt.where(NowcastRow.algorithm == algorithm)
    if horizon_minutes is not None:
        stmt = stmt.where(NowcastRow.forecast_horizon_minutes == horizon_minutes)
    if since is not None:
        stmt = stmt.where(NowcastRow.valid_at >= since)
    if until is not None:
        stmt = stmt.where(NowcastRow.valid_at < until)
    result = await session.execute(stmt)
    return tuple(result.scalars().all())


async def find_nowcasts_for_observation(
    session: AsyncSession,
    *,
    valid_at: datetime,
    product: str,
    level: str,
    valid_at_window_seconds: int = 60,
) -> tuple[NowcastRow, ...]:
    """Return nowcasts whose ``valid_at`` is within ``valid_at_window_seconds``
    of ``valid_at``, scoped to product + level.

    The verification worker calls this when an observation grid arrives
    at time T to score every previously-issued forecast that predicted
    that time. The ±window absorbs the inherent jitter in MRMS valid_at
    timestamps (publishes are every ~2 minutes but not on the dot).
    """
    from datetime import timedelta as _td

    delta = _td(seconds=valid_at_window_seconds)
    stmt = (
        select(NowcastRow)
        .where(NowcastRow.product == product)
        .where(NowcastRow.level == level)
        .where(NowcastRow.valid_at >= valid_at - delta)
        .where(NowcastRow.valid_at <= valid_at + delta)
    )
    result = await session.execute(stmt)
    return tuple(result.scalars().all())


__all__ = [
    "DEFAULT_LIST_LIMIT",
    "MAX_LIST_LIMIT",
    "find_nowcasts_for_observation",
    "list_nowcasts",
    "upsert_nowcast",
]
