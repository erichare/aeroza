"""Wire shapes for the public ``/v1/calibration`` and ``/v1/calibration/series`` routes."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from aeroza.verify.metrics import csi, far, pod
from aeroza.verify.store import CalibrationBucket, CalibrationSeriesPoint


class CalibrationRow(BaseModel):
    """One row of the calibration aggregate.

    Per (algorithm × forecast horizon) over the requested window.
    Sample-weighted means: a verification with N=1M cells contributes
    N times to ``maeMean`` / ``biasMean`` / ``rmseMean``.

    Categorical fields (``pod``, ``far``, ``csi``) are computed at
    serialization time from the summed contingency table — averaging
    POD/FAR/CSI across rows would be wrong (averaging ratios is not
    the same as the ratio of averages). They're nullable: when no
    contributing row had categorical metrics or the threshold was
    mixed, we surface ``null`` rather than a misleading 0.

    Probabilistic fields (``brierMean``, ``crpsMean``,
    ``ensembleSize``) are populated only when at least one ensemble
    nowcast contributed to the bucket. ``brierMean`` and ``crpsMean``
    are sample-weighted across only the ensemble rows, so a bucket
    that mixes deterministic and ensemble forecasts surfaces an
    apples-to-apples Brier/CRPS for the ensemble subset (with
    ``brierSampleCount`` reporting the cell count behind it).
    """

    model_config = ConfigDict(populate_by_name=True, frozen=True)

    algorithm: str
    forecast_horizon_minutes: int = Field(serialization_alias="forecastHorizonMinutes")
    verification_count: int = Field(serialization_alias="verificationCount")
    sample_count: int = Field(serialization_alias="sampleCount")
    mae_mean: float = Field(serialization_alias="maeMean")
    bias_mean: float = Field(serialization_alias="biasMean")
    rmse_mean: float = Field(serialization_alias="rmseMean")
    threshold_dbz: float | None = Field(default=None, serialization_alias="thresholdDbz")
    pod: float | None = None
    far: float | None = None
    csi: float | None = None
    ensemble_size: int | None = Field(default=None, serialization_alias="ensembleSize")
    brier_sample_count: int = Field(default=0, serialization_alias="brierSampleCount")
    brier_mean: float | None = Field(default=None, serialization_alias="brierMean")
    crps_mean: float | None = Field(default=None, serialization_alias="crpsMean")


class CalibrationResponse(BaseModel):
    """Wire shape for ``GET /v1/calibration``."""

    model_config = ConfigDict(populate_by_name=True, frozen=True)

    type: Literal["Calibration"] = "Calibration"
    generated_at: datetime = Field(serialization_alias="generatedAt")
    window_hours: int = Field(serialization_alias="windowHours")
    items: list[CalibrationRow]


def calibration_buckets_to_response(
    buckets: list[CalibrationBucket] | tuple[CalibrationBucket, ...],
    *,
    generated_at: datetime,
    window_hours: int,
) -> CalibrationResponse:
    return CalibrationResponse(
        generated_at=generated_at,
        window_hours=window_hours,
        items=[
            CalibrationRow(
                algorithm=b.algorithm,
                forecast_horizon_minutes=b.forecast_horizon_minutes,
                verification_count=b.verification_count,
                sample_count=b.sample_count,
                mae_mean=b.mae_mean,
                bias_mean=b.bias_mean,
                rmse_mean=b.rmse_mean,
                threshold_dbz=b.threshold_dbz,
                pod=pod(b.hits_total, b.misses_total),
                far=far(b.hits_total, b.false_alarms_total),
                csi=csi(b.hits_total, b.misses_total, b.false_alarms_total),
                ensemble_size=b.ensemble_size,
                brier_sample_count=b.brier_sample_count,
                brier_mean=b.brier_mean,
                crps_mean=b.crps_mean,
            )
            for b in buckets
        ],
    )


class CalibrationSeriesItemPoint(BaseModel):
    """One time-bucket on a calibration sparkline."""

    model_config = ConfigDict(populate_by_name=True, frozen=True)

    bucket_start: datetime = Field(serialization_alias="bucketStart")
    verification_count: int = Field(serialization_alias="verificationCount")
    sample_count: int = Field(serialization_alias="sampleCount")
    mae_mean: float = Field(serialization_alias="maeMean")
    bias_mean: float = Field(serialization_alias="biasMean")
    rmse_mean: float = Field(serialization_alias="rmseMean")
    threshold_dbz: float | None = Field(default=None, serialization_alias="thresholdDbz")
    pod: float | None = None
    far: float | None = None
    csi: float | None = None
    ensemble_size: int | None = Field(default=None, serialization_alias="ensembleSize")
    brier_sample_count: int = Field(default=0, serialization_alias="brierSampleCount")
    brier_mean: float | None = Field(default=None, serialization_alias="brierMean")
    crps_mean: float | None = Field(default=None, serialization_alias="crpsMean")


class CalibrationSeriesItem(BaseModel):
    """All buckets for one (algorithm, horizon) — the per-row sparkline data."""

    model_config = ConfigDict(populate_by_name=True, frozen=True)

    algorithm: str
    forecast_horizon_minutes: int = Field(serialization_alias="forecastHorizonMinutes")
    points: list[CalibrationSeriesItemPoint]


class CalibrationSeriesResponse(BaseModel):
    """Wire shape for ``GET /v1/calibration/series``.

    ``bucketSeconds`` is the width of each bucket (e.g. 3600 for hourly).
    ``items`` is one element per (algorithm × horizon); each carries an
    ordered list of points (oldest → newest).
    """

    model_config = ConfigDict(populate_by_name=True, frozen=True)

    type: Literal["CalibrationSeries"] = "CalibrationSeries"
    generated_at: datetime = Field(serialization_alias="generatedAt")
    window_hours: int = Field(serialization_alias="windowHours")
    bucket_seconds: int = Field(serialization_alias="bucketSeconds")
    items: list[CalibrationSeriesItem]


def calibration_points_to_series(
    points: Sequence[CalibrationSeriesPoint],
    *,
    generated_at: datetime,
    window_hours: int,
    bucket_seconds: int,
) -> CalibrationSeriesResponse:
    """Group flat (algorithm, horizon, bucket) rows into per-series points.

    Assumes the input is already sorted by ``(algorithm, horizon, bucket_start)``
    — :func:`aggregate_calibration_series` guarantees that, so we just walk
    the sequence once and start a new series whenever the (algorithm,
    horizon) key changes.
    """
    items: list[CalibrationSeriesItem] = []
    current_key: tuple[str, int] | None = None
    current_points: list[CalibrationSeriesItemPoint] = []
    for p in points:
        key = (p.algorithm, p.forecast_horizon_minutes)
        if current_key is not None and key != current_key:
            items.append(
                CalibrationSeriesItem(
                    algorithm=current_key[0],
                    forecast_horizon_minutes=current_key[1],
                    points=current_points,
                )
            )
            current_points = []
        current_key = key
        current_points.append(
            CalibrationSeriesItemPoint(
                bucket_start=p.bucket_start,
                verification_count=p.verification_count,
                sample_count=p.sample_count,
                mae_mean=p.mae_mean,
                bias_mean=p.bias_mean,
                rmse_mean=p.rmse_mean,
                threshold_dbz=p.threshold_dbz,
                pod=pod(p.hits_total, p.misses_total),
                far=far(p.hits_total, p.false_alarms_total),
                csi=csi(p.hits_total, p.misses_total, p.false_alarms_total),
                ensemble_size=p.ensemble_size,
                brier_sample_count=p.brier_sample_count,
                brier_mean=p.brier_mean,
                crps_mean=p.crps_mean,
            )
        )
    if current_key is not None:
        items.append(
            CalibrationSeriesItem(
                algorithm=current_key[0],
                forecast_horizon_minutes=current_key[1],
                points=current_points,
            )
        )
    return CalibrationSeriesResponse(
        generated_at=generated_at,
        window_hours=window_hours,
        bucket_seconds=bucket_seconds,
        items=items,
    )


__all__ = [
    "CalibrationResponse",
    "CalibrationRow",
    "CalibrationSeriesItem",
    "CalibrationSeriesItemPoint",
    "CalibrationSeriesResponse",
    "calibration_buckets_to_response",
    "calibration_points_to_series",
]
