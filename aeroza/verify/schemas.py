"""Wire shapes for the public ``/v1/calibration`` route."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from aeroza.verify.store import CalibrationBucket


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


__all__ = [
    "CalibrationResponse",
    "CalibrationRow",
    "calibration_buckets_to_response",
]
