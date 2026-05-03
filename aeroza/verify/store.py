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

from aeroza.verify.metrics import DeterministicMetrics, ProbabilisticMetrics
from aeroza.verify.models import VerificationRow

log = structlog.get_logger(__name__)

DEFAULT_AGGREGATE_WINDOW_HOURS: Final[int] = 24

_MUTABLE_COLUMNS: Final[tuple[str, ...]] = (
    "mae",
    "bias",
    "rmse",
    "sample_count",
    "threshold_dbz",
    "hits",
    "misses",
    "false_alarms",
    "correct_negatives",
    "ensemble_size",
    "brier_score",
    "crps",
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
    probabilistic: ProbabilisticMetrics | None = None,
) -> VerificationRow:
    """Insert or update one verification row.

    Re-running the verifier against the same (forecast, observation)
    pair is idempotent — useful when a nowcast is recomputed (e.g.
    algorithm tuning) and we want fresh scores without first deleting
    the old row.

    ``probabilistic`` is set when the source nowcast was an ensemble.
    When passed, ``ensemble_size``, ``brier_score``, and ``crps`` are
    written; otherwise those columns stay NULL so the calibration
    aggregator can skip them.
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
        "threshold_dbz": metrics.threshold_dbz,
        "hits": metrics.hits,
        "misses": metrics.misses,
        "false_alarms": metrics.false_alarms,
        "correct_negatives": metrics.correct_negatives,
        "ensemble_size": probabilistic.ensemble_size if probabilistic is not None else None,
        "brier_score": probabilistic.brier_score if probabilistic is not None else None,
        "crps": probabilistic.crps if probabilistic is not None else None,
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

    Categorical fields (``hits_total`` etc.) are simple sums across the
    window so the route layer can compute POD/FAR/CSI as ratios on
    read. Sums survive aggregation; ratios don't (averaging ratios is
    not the same as the ratio of averages).

    Probabilistic fields (``brier_mean`` / ``crps_mean``) are
    sample-weighted across only those rows where the ensemble
    metrics were populated (``brier_score IS NOT NULL`` etc.). They
    are ``None`` when no ensemble rows contributed — surfaced as
    ``null`` rather than an invented 0 by the route layer.
    ``ensemble_size`` is set when every ensemble row in the bucket
    used the same ``M`` (None on mismatch, same convention as the
    threshold field).
    """

    algorithm: str
    forecast_horizon_minutes: int
    verification_count: int
    sample_count: int
    mae_mean: float
    bias_mean: float
    rmse_mean: float
    # Categorical aggregate. ``threshold_dbz`` is the threshold used
    # if every contributing row agrees (None when rows used different
    # thresholds or the column was NULL). The summed counts are 0 when
    # no row in the window had categorical metrics.
    threshold_dbz: float | None
    hits_total: int
    misses_total: int
    false_alarms_total: int
    correct_negatives_total: int
    # Probabilistic aggregate. ``brier_sample_count`` is the
    # cell-weighted denominator of the Brier/CRPS means; 0 means no
    # ensemble rows contributed.
    ensemble_size: int | None
    brier_sample_count: int
    brier_mean: float | None
    crps_mean: float | None


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

    # Distinct count of thresholds used across rows in this bucket. If
    # every row used the same threshold (count == 1) we surface it; if
    # rows used different thresholds (count > 1) the route returns None
    # rather than mix incompatible cells silently.
    distinct_thresholds = func.count(func.distinct(VerificationRow.threshold_dbz))
    any_threshold = func.max(VerificationRow.threshold_dbz)

    # Probabilistic aggregates. The weighted sums and the sample-count
    # denominator only count rows where the column is not NULL — so a
    # bucket containing both deterministic and ensemble rows surfaces a
    # Brier/CRPS mean weighted by the ensemble rows alone. Same for
    # ensemble_size: distinct-count > 1 → mixed M → return None.
    brier_weighted = func.sum(VerificationRow.brier_score * VerificationRow.sample_count)
    crps_weighted = func.sum(VerificationRow.crps * VerificationRow.sample_count)
    brier_total_samples = func.coalesce(
        func.sum(
            case(
                (VerificationRow.brier_score.is_not(None), VerificationRow.sample_count),
                else_=0,
            )
        ),
        0,
    )
    distinct_ensemble_sizes = func.count(func.distinct(VerificationRow.ensemble_size))
    any_ensemble_size = func.max(VerificationRow.ensemble_size)

    stmt = (
        select(
            VerificationRow.algorithm,
            VerificationRow.forecast_horizon_minutes,
            func.count(VerificationRow.id).label("verification_count"),
            total_samples.label("sample_count"),
            safe_div(weighted_mae).label("mae_mean"),
            safe_div(weighted_bias).label("bias_mean"),
            safe_div(weighted_rmse).label("rmse_mean"),
            func.coalesce(func.sum(VerificationRow.hits), 0).label("hits_total"),
            func.coalesce(func.sum(VerificationRow.misses), 0).label("misses_total"),
            func.coalesce(func.sum(VerificationRow.false_alarms), 0).label("false_alarms_total"),
            func.coalesce(func.sum(VerificationRow.correct_negatives), 0).label(
                "correct_negatives_total"
            ),
            any_threshold.label("any_threshold"),
            distinct_thresholds.label("distinct_thresholds"),
            brier_total_samples.label("brier_sample_count"),
            brier_weighted.label("brier_weighted"),
            crps_weighted.label("crps_weighted"),
            any_ensemble_size.label("any_ensemble_size"),
            distinct_ensemble_sizes.label("distinct_ensemble_sizes"),
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
        threshold = (
            float(row["any_threshold"])
            if row["any_threshold"] is not None and int(row["distinct_thresholds"]) == 1
            else None
        )
        brier_samples = int(row["brier_sample_count"] or 0)
        ensemble_size = (
            int(row["any_ensemble_size"])
            if row["any_ensemble_size"] is not None and int(row["distinct_ensemble_sizes"]) == 1
            else None
        )
        brier_mean = float(row["brier_weighted"]) / brier_samples if brier_samples > 0 else None
        crps_mean = float(row["crps_weighted"]) / brier_samples if brier_samples > 0 else None
        buckets.append(
            CalibrationBucket(
                algorithm=row["algorithm"],
                forecast_horizon_minutes=int(row["forecast_horizon_minutes"]),
                verification_count=int(row["verification_count"]),
                sample_count=int(row["sample_count"] or 0),
                mae_mean=float(row["mae_mean"] or 0.0),
                bias_mean=float(row["bias_mean"] or 0.0),
                rmse_mean=float(row["rmse_mean"] or 0.0),
                threshold_dbz=threshold,
                hits_total=int(row["hits_total"] or 0),
                misses_total=int(row["misses_total"] or 0),
                false_alarms_total=int(row["false_alarms_total"] or 0),
                correct_negatives_total=int(row["correct_negatives_total"] or 0),
                ensemble_size=ensemble_size,
                brier_sample_count=brier_samples,
                brier_mean=brier_mean,
                crps_mean=crps_mean,
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
    threshold_dbz: float | None
    hits_total: int
    misses_total: int
    false_alarms_total: int
    correct_negatives_total: int
    ensemble_size: int | None
    brier_sample_count: int
    brier_mean: float | None
    crps_mean: float | None


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

    distinct_thresholds = func.count(func.distinct(VerificationRow.threshold_dbz))
    any_threshold = func.max(VerificationRow.threshold_dbz)
    brier_weighted = func.sum(VerificationRow.brier_score * VerificationRow.sample_count)
    crps_weighted = func.sum(VerificationRow.crps * VerificationRow.sample_count)
    brier_total_samples = func.coalesce(
        func.sum(
            case(
                (VerificationRow.brier_score.is_not(None), VerificationRow.sample_count),
                else_=0,
            )
        ),
        0,
    )
    distinct_ensemble_sizes = func.count(func.distinct(VerificationRow.ensemble_size))
    any_ensemble_size = func.max(VerificationRow.ensemble_size)
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
            func.coalesce(func.sum(VerificationRow.hits), 0).label("hits_total"),
            func.coalesce(func.sum(VerificationRow.misses), 0).label("misses_total"),
            func.coalesce(func.sum(VerificationRow.false_alarms), 0).label("false_alarms_total"),
            func.coalesce(func.sum(VerificationRow.correct_negatives), 0).label(
                "correct_negatives_total"
            ),
            any_threshold.label("any_threshold"),
            distinct_thresholds.label("distinct_thresholds"),
            brier_total_samples.label("brier_sample_count"),
            brier_weighted.label("brier_weighted"),
            crps_weighted.label("crps_weighted"),
            any_ensemble_size.label("any_ensemble_size"),
            distinct_ensemble_sizes.label("distinct_ensemble_sizes"),
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
        threshold = (
            float(row["any_threshold"])
            if row["any_threshold"] is not None and int(row["distinct_thresholds"]) == 1
            else None
        )
        brier_samples = int(row["brier_sample_count"] or 0)
        ensemble_size = (
            int(row["any_ensemble_size"])
            if row["any_ensemble_size"] is not None and int(row["distinct_ensemble_sizes"]) == 1
            else None
        )
        brier_mean = float(row["brier_weighted"]) / brier_samples if brier_samples > 0 else None
        crps_mean = float(row["crps_weighted"]) / brier_samples if brier_samples > 0 else None
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
                threshold_dbz=threshold,
                hits_total=int(row["hits_total"] or 0),
                misses_total=int(row["misses_total"] or 0),
                false_alarms_total=int(row["false_alarms_total"] or 0),
                correct_negatives_total=int(row["correct_negatives_total"] or 0),
                ensemble_size=ensemble_size,
                brier_sample_count=brier_samples,
                brier_mean=brier_mean,
                crps_mean=crps_mean,
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
