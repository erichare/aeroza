"""Persistence + aggregate queries for nowcast_verifications."""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Final

import structlog
from sqlalchemy import case, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from aeroza.verify.metrics import DeterministicMetrics
from aeroza.verify.models import VerificationRow

log = structlog.get_logger(__name__)

DEFAULT_AGGREGATE_WINDOW_HOURS: Final[int] = 24

_MUTABLE_COLUMNS: Final[tuple[str, ...]] = (
    "mae",
    "bias",
    "rmse",
    "sample_count",
)


async def upsert_verification(
    session: AsyncSession,
    *,
    nowcast_id: uuid.UUID,
    observation_file_key: str,
    product: str,
    level: str,
    algorithm: str,
    horizon_minutes: int,
    valid_at: datetime,
    metrics: DeterministicMetrics,
) -> VerificationRow:
    """Insert or update one verification row.

    Re-running the verifier against the same (forecast, observation)
    pair is idempotent — useful when a nowcast is recomputed (e.g.
    algorithm tuning) and we want fresh scores without first deleting
    the old row.
    """
    row = {
        "nowcast_id": nowcast_id,
        "observation_file_key": observation_file_key,
        "product": product,
        "level": level,
        "algorithm": algorithm,
        "forecast_horizon_minutes": horizon_minutes,
        "valid_at": valid_at,
        "mae": metrics.mae,
        "bias": metrics.bias,
        "rmse": metrics.rmse,
        "sample_count": metrics.sample_count,
    }
    insert_stmt = pg_insert(VerificationRow).values(row)
    update_set: dict[str, Any] = {col: insert_stmt.excluded[col] for col in _MUTABLE_COLUMNS}
    update_set["verified_at"] = func.now()
    upsert_stmt = insert_stmt.on_conflict_do_update(
        constraint="uq_nowcast_verifications_nowcast_observation",
        set_=update_set,
    ).returning(VerificationRow)
    result = await session.execute(upsert_stmt)
    persisted = result.scalar_one()
    await session.refresh(persisted)
    log.info(
        "verify.store.upsert",
        nowcast_id=str(nowcast_id),
        observation_file_key=observation_file_key,
        algorithm=algorithm,
        horizon_minutes=horizon_minutes,
        mae=metrics.mae,
        sample_count=metrics.sample_count,
    )
    return persisted


@dataclass(frozen=True, slots=True)
class CalibrationBucket:
    """One row of the public ``/v1/calibration`` aggregate.

    Each bucket is the per-(algorithm × horizon) summary over the
    requested time window. ``mae_mean`` and friends are sample-weighted
    averages — a verification with 1M cells contributing counts more
    than one with 100 cells, which is what an honest dashboard wants.
    """

    algorithm: str
    forecast_horizon_minutes: int
    verification_count: int
    sample_count: int
    mae_mean: float
    bias_mean: float
    rmse_mean: float


async def aggregate_calibration(
    session: AsyncSession,
    *,
    since: datetime | None = None,
    product: str | None = None,
    level: str | None = None,
    algorithm: str | None = None,
    window_hours: int = DEFAULT_AGGREGATE_WINDOW_HOURS,
) -> Sequence[CalibrationBucket]:
    """Group ``nowcast_verifications`` by ``(algorithm, horizon)``.

    Sample-weighted averages: one verification with sample_count=N
    contributes N times to each metric. ``since`` defaults to
    ``now() - window_hours``; pass an explicit value for backfill
    queries.
    """
    if since is None:
        since = datetime.now().astimezone() - timedelta(hours=window_hours)

    weighted_mae = func.sum(VerificationRow.mae * VerificationRow.sample_count)
    weighted_bias = func.sum(VerificationRow.bias * VerificationRow.sample_count)
    weighted_rmse = func.sum(VerificationRow.rmse * VerificationRow.sample_count)
    total_samples = func.sum(VerificationRow.sample_count)

    def safe_div(numer: Any) -> Any:
        # CASE avoids the NULL Postgres returns for division by zero
        # when ``sample_count`` sums to zero (no data contributed).
        return case((total_samples > 0, numer / total_samples), else_=0.0)

    stmt = (
        select(
            VerificationRow.algorithm,
            VerificationRow.forecast_horizon_minutes,
            func.count(VerificationRow.id).label("verification_count"),
            total_samples.label("sample_count"),
            safe_div(weighted_mae).label("mae_mean"),
            safe_div(weighted_bias).label("bias_mean"),
            safe_div(weighted_rmse).label("rmse_mean"),
        )
        .where(VerificationRow.verified_at >= since)
        .group_by(
            VerificationRow.algorithm,
            VerificationRow.forecast_horizon_minutes,
        )
        .order_by(
            VerificationRow.algorithm,
            VerificationRow.forecast_horizon_minutes,
        )
    )
    if product is not None:
        stmt = stmt.where(VerificationRow.product == product)
    if level is not None:
        stmt = stmt.where(VerificationRow.level == level)
    if algorithm is not None:
        stmt = stmt.where(VerificationRow.algorithm == algorithm)

    result = await session.execute(stmt)
    buckets: list[CalibrationBucket] = []
    for row in result.mappings():
        buckets.append(
            CalibrationBucket(
                algorithm=row["algorithm"],
                forecast_horizon_minutes=int(row["forecast_horizon_minutes"]),
                verification_count=int(row["verification_count"]),
                sample_count=int(row["sample_count"] or 0),
                mae_mean=float(row["mae_mean"] or 0.0),
                bias_mean=float(row["bias_mean"] or 0.0),
                rmse_mean=float(row["rmse_mean"] or 0.0),
            )
        )
    return tuple(buckets)


# --------------------------------------------------------------------------- #
# Time-series aggregation                                                     #
# --------------------------------------------------------------------------- #

# Default time-bucket size for calibration sparklines. One hour is the
# sweet spot for a 24h window: 24 points per series gives a readable
# trend without looking pixel-noisy.
DEFAULT_BUCKET_SECONDS: Final[int] = 3600


@dataclass(frozen=True, slots=True)
class CalibrationSeriesPoint:
    """One time-bucket aggregate for the calibration sparkline.

    ``bucket_start`` is the inclusive lower edge of the bucket
    (``date_trunc(bucket, verified_at)``). ``bucket_seconds`` is fixed
    by the caller; rows in a bucket are aggregated the same way as
    :func:`aggregate_calibration` (sample-weighted means).
    """

    algorithm: str
    forecast_horizon_minutes: int
    bucket_start: datetime
    verification_count: int
    sample_count: int
    mae_mean: float
    bias_mean: float
    rmse_mean: float


async def aggregate_calibration_series(
    session: AsyncSession,
    *,
    since: datetime | None = None,
    bucket_seconds: int = DEFAULT_BUCKET_SECONDS,
    product: str | None = None,
    level: str | None = None,
    algorithm: str | None = None,
    window_hours: int = DEFAULT_AGGREGATE_WINDOW_HOURS,
) -> Sequence[CalibrationSeriesPoint]:
    """Time-bucketed sample-weighted aggregate of ``nowcast_verifications``.

    Grouped by ``(algorithm, forecast_horizon_minutes, bucket_start)``,
    where ``bucket_start = to_timestamp(floor(epoch / bucket_seconds) * bucket_seconds)``.
    Using floor-based bucketing rather than ``date_trunc`` lets callers
    request arbitrary bucket sizes (10 min, 6 h, etc.) without picking
    from a fixed Postgres-flavour list.

    The result is sorted by ``(algorithm, horizon, bucket_start)`` so the
    front-end can build sparklines without re-sorting.
    """
    if since is None:
        since = datetime.now().astimezone() - timedelta(hours=window_hours)
    if bucket_seconds <= 0:
        raise ValueError(f"bucket_seconds must be positive, got {bucket_seconds}")

    # `to_timestamp(floor(epoch / N) * N)` snaps every row to the start
    # of its bucket. Cast verified_at to epoch via extract().
    bucket_expr = func.to_timestamp(
        func.floor(func.extract("epoch", VerificationRow.verified_at) / bucket_seconds)
        * bucket_seconds
    ).label("bucket_start")

    weighted_mae = func.sum(VerificationRow.mae * VerificationRow.sample_count)
    weighted_bias = func.sum(VerificationRow.bias * VerificationRow.sample_count)
    weighted_rmse = func.sum(VerificationRow.rmse * VerificationRow.sample_count)
    total_samples = func.sum(VerificationRow.sample_count)

    def safe_div(numer: Any) -> Any:
        return case((total_samples > 0, numer / total_samples), else_=0.0)

    stmt = (
        select(
            VerificationRow.algorithm,
            VerificationRow.forecast_horizon_minutes,
            bucket_expr,
            func.count(VerificationRow.id).label("verification_count"),
            total_samples.label("sample_count"),
            safe_div(weighted_mae).label("mae_mean"),
            safe_div(weighted_bias).label("bias_mean"),
            safe_div(weighted_rmse).label("rmse_mean"),
        )
        .where(VerificationRow.verified_at >= since)
        .group_by(
            VerificationRow.algorithm,
            VerificationRow.forecast_horizon_minutes,
            bucket_expr,
        )
        .order_by(
            VerificationRow.algorithm,
            VerificationRow.forecast_horizon_minutes,
            bucket_expr,
        )
    )
    if product is not None:
        stmt = stmt.where(VerificationRow.product == product)
    if level is not None:
        stmt = stmt.where(VerificationRow.level == level)
    if algorithm is not None:
        stmt = stmt.where(VerificationRow.algorithm == algorithm)

    result = await session.execute(stmt)
    points: list[CalibrationSeriesPoint] = []
    for row in result.mappings():
        bucket_start = row["bucket_start"]
        # `to_timestamp` returns a tz-aware datetime when the input
        # column is TIMESTAMPTZ; defensively coerce naive values to UTC
        # so callers can rely on isoformat() with offset.
        if bucket_start.tzinfo is None:
            from datetime import UTC

            bucket_start = bucket_start.replace(tzinfo=UTC)
        points.append(
            CalibrationSeriesPoint(
                algorithm=row["algorithm"],
                forecast_horizon_minutes=int(row["forecast_horizon_minutes"]),
                bucket_start=bucket_start,
                verification_count=int(row["verification_count"]),
                sample_count=int(row["sample_count"] or 0),
                mae_mean=float(row["mae_mean"] or 0.0),
                bias_mean=float(row["bias_mean"] or 0.0),
                rmse_mean=float(row["rmse_mean"] or 0.0),
            )
        )
    return tuple(points)


__all__ = [
    "DEFAULT_AGGREGATE_WINDOW_HOURS",
    "DEFAULT_BUCKET_SECONDS",
    "CalibrationBucket",
    "CalibrationSeriesPoint",
    "aggregate_calibration",
    "aggregate_calibration_series",
    "upsert_verification",
]
