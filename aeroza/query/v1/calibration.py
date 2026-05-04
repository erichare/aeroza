"""``/v1/calibration*`` routes — aggregate verification + time-bucketed series."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from aeroza.query.dependencies import get_session
from aeroza.verify.schemas import (
    CalibrationResponse,
    CalibrationSeriesResponse,
    calibration_buckets_to_response,
    calibration_points_to_series,
)
from aeroza.verify.store import (
    DEFAULT_AGGREGATE_WINDOW_HOURS as CALIBRATION_DEFAULT_WINDOW_HOURS,
)
from aeroza.verify.store import (
    DEFAULT_BUCKET_SECONDS as CALIBRATION_DEFAULT_BUCKET_SECONDS,
)
from aeroza.verify.store import (
    aggregate_calibration,
    aggregate_calibration_reliability,
    aggregate_calibration_series,
)

router = APIRouter(tags=["calibration"])


# Bucket bounds: between 5 min (rate-limit storm) and 1 day (any wider
# and a sparkline collapses to a single point per series).
_CALIBRATION_BUCKET_MIN_SECONDS: int = 300
_CALIBRATION_BUCKET_MAX_SECONDS: int = 86_400


@router.get(
    "/calibration",
    response_model=CalibrationResponse,
    response_model_by_alias=True,
    response_model_exclude_none=False,
    summary="Aggregate verification metrics, grouped by algorithm × horizon",
    description=(
        "Returns sample-weighted MAE / bias / RMSE for every "
        "(algorithm, forecastHorizonMinutes) pair that has scored "
        "verifications inside the requested window, alongside "
        "categorical POD / FAR / CSI from the summed contingency "
        "table and probabilistic Brier / CRPS over any ensemble "
        "rows in the bucket. The public face of the §3.3 "
        "calibration moat — point a chart at this and you can watch "
        "a real algorithm pull ahead of the persistence baseline.\n\n"
        "Probabilistic fields (``brierMean``, ``crpsMean``, "
        "``ensembleSize``) are ``null`` for buckets that contain no "
        "ensemble forecasts; ``brierSampleCount`` reports the cells "
        "behind the means so you can detect 'one tiny ensemble row'.\n\n"
        "Window defaults to the last 24h. Pass ``windowHours`` to widen "
        "or narrow it; pass ``algorithm`` / ``product`` / ``level`` to "
        "scope the aggregation."
    ),
)
async def get_calibration_route(
    session: Annotated[AsyncSession, Depends(get_session)],
    window_hours: Annotated[
        int,
        Query(
            alias="windowHours",
            ge=1,
            le=24 * 30,  # 30 days
            description=(f"Lookback window in hours (default {CALIBRATION_DEFAULT_WINDOW_HOURS})."),
        ),
    ] = CALIBRATION_DEFAULT_WINDOW_HOURS,
    algorithm: Annotated[
        str | None,
        Query(description="Filter to one algorithm tag (e.g. 'persistence')"),
    ] = None,
    product: Annotated[
        str | None,
        Query(description="Filter to one product (e.g. 'MergedReflectivityComposite')"),
    ] = None,
    level: Annotated[
        str | None,
        Query(description="Filter to one product level (e.g. '00.50')"),
    ] = None,
) -> CalibrationResponse:
    now = datetime.now(UTC)
    since = now - timedelta(hours=window_hours)
    buckets = await aggregate_calibration(
        session,
        since=since,
        algorithm=algorithm,
        product=product,
        level=level,
        window_hours=window_hours,
    )
    # Pull reliability bins in parallel-shaped logic but sequentially —
    # sticking to one shared session keeps the connection-pool story
    # simple. Reliability data is small (10 bins × ~60 bytes per
    # algo/horizon) so the extra round-trip is negligible.
    reliability = await aggregate_calibration_reliability(
        session,
        since=since,
        algorithm=algorithm,
        product=product,
        level=level,
        window_hours=window_hours,
    )
    return calibration_buckets_to_response(
        list(buckets),
        generated_at=now,
        window_hours=window_hours,
        reliability=reliability,
    )


@router.get(
    "/calibration/series",
    response_model=CalibrationSeriesResponse,
    response_model_by_alias=True,
    response_model_exclude_none=False,
    summary="Time-series calibration metrics, per (algorithm × horizon × bucket)",
    description=(
        "Sparkline-shaped companion to ``/v1/calibration``: same sample-"
        "weighted means and probabilistic Brier/CRPS aggregates, but "
        "each (algorithm, forecastHorizonMinutes) row carries an "
        "ordered list of bucketed points so the front-end can chart "
        "how every metric moves over the window.\n\n"
        "``bucketSeconds`` controls bucket width (default 1 h). Series "
        "are sorted ``(algorithm, horizon, bucketStart)`` so the wire "
        "shape is render-ready."
    ),
)
async def get_calibration_series_route(
    session: Annotated[AsyncSession, Depends(get_session)],
    window_hours: Annotated[
        int,
        Query(
            alias="windowHours",
            ge=1,
            le=24 * 30,
            description=f"Lookback window in hours (default {CALIBRATION_DEFAULT_WINDOW_HOURS}).",
        ),
    ] = CALIBRATION_DEFAULT_WINDOW_HOURS,
    bucket_seconds: Annotated[
        int,
        Query(
            alias="bucketSeconds",
            ge=_CALIBRATION_BUCKET_MIN_SECONDS,
            le=_CALIBRATION_BUCKET_MAX_SECONDS,
            description=(
                f"Bucket width in seconds (default {CALIBRATION_DEFAULT_BUCKET_SECONDS}, "
                f"min {_CALIBRATION_BUCKET_MIN_SECONDS}, max {_CALIBRATION_BUCKET_MAX_SECONDS})."
            ),
        ),
    ] = CALIBRATION_DEFAULT_BUCKET_SECONDS,
    algorithm: Annotated[
        str | None,
        Query(description="Filter to one algorithm tag (e.g. 'persistence')"),
    ] = None,
    product: Annotated[
        str | None,
        Query(description="Filter to one product (e.g. 'MergedReflectivityComposite')"),
    ] = None,
    level: Annotated[
        str | None,
        Query(description="Filter to one product level (e.g. '00.50')"),
    ] = None,
) -> CalibrationSeriesResponse:
    now = datetime.now(UTC)
    since = now - timedelta(hours=window_hours)
    points = await aggregate_calibration_series(
        session,
        since=since,
        bucket_seconds=bucket_seconds,
        algorithm=algorithm,
        product=product,
        level=level,
        window_hours=window_hours,
    )
    return calibration_points_to_series(
        points,
        generated_at=now,
        window_hours=window_hours,
        bucket_seconds=bucket_seconds,
    )
