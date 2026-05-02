"""Wire shapes for the public ``/v1/calibration`` and ``/v1/calibration/series`` routes."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from aeroza.verify.store import CalibrationBucket, CalibrationSeriesPoint


class CalibrationRow(BaseModel):
    """One row of the calibration aggregate.

    Per (algorithm × forecast horizon) over the requested window.
    Sample-weighted means: a verification with N=1M cells contributes
    N times to ``maeMean`` / ``biasMean`` / ``rmseMean``.
    """

    model_config = ConfigDict(populate_by_name=True, frozen=True)

    algorithm: str
    forecast_horizon_minutes: int = Field(serialization_alias="forecastHorizonMinutes")
    verification_count: int = Field(serialization_alias="verificationCount")
    sample_count: int = Field(serialization_alias="sampleCount")
    mae_mean: float = Field(serialization_alias="maeMean")
    bias_mean: float = Field(serialization_alias="biasMean")
    rmse_mean: float = Field(serialization_alias="rmseMean")


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
