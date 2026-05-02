"""Unit tests for the deterministic scoring metrics.

The functions are pure numpy — no DB, no Zarr. Tests pin every
property the calibration aggregator depends on (NaN handling, sample
count, shape mismatch raises).
"""

from __future__ import annotations

import numpy as np
import pytest

from aeroza.verify.metrics import (
    DeterministicMetrics,
    score_deterministic_grids,
)

pytestmark = pytest.mark.unit


def test_perfect_forecast_zero_error() -> None:
    arr = np.array([[1.0, 2.0], [3.0, 4.0]], dtype=np.float32)
    metrics = score_deterministic_grids(arr, arr)
    assert metrics.mae == 0.0
    assert metrics.bias == 0.0
    assert metrics.rmse == 0.0
    assert metrics.sample_count == 4


def test_uniform_offset_pure_bias() -> None:
    obs = np.zeros((2, 2), dtype=np.float32)
    forecast = obs + 5.0
    metrics = score_deterministic_grids(forecast, obs)
    assert metrics.bias == 5.0
    assert metrics.mae == 5.0
    assert metrics.rmse == 5.0
    assert metrics.sample_count == 4


def test_signed_errors_average_to_zero_bias() -> None:
    """MAE > 0 but bias = 0 when over- and under-shoots cancel."""
    obs = np.array([0.0, 0.0], dtype=np.float32)
    forecast = np.array([5.0, -5.0], dtype=np.float32)
    metrics = score_deterministic_grids(forecast, obs)
    assert metrics.mae == 5.0
    assert metrics.bias == 0.0
    assert metrics.rmse == 5.0


def test_rmse_penalises_large_errors_more_than_mae() -> None:
    obs = np.zeros(4, dtype=np.float32)
    forecast = np.array([1.0, 1.0, 1.0, 5.0], dtype=np.float32)
    metrics = score_deterministic_grids(forecast, obs)
    # MAE = 8/4 = 2.0; RMSE = sqrt(28/4) = sqrt(7) ≈ 2.646
    assert metrics.mae == pytest.approx(2.0)
    assert metrics.rmse == pytest.approx(2.6457513)


def test_nan_cells_excluded_from_sample_count_and_metrics() -> None:
    obs = np.array([1.0, 2.0, np.nan, 4.0], dtype=np.float32)
    forecast = np.array([1.0, 3.0, 100.0, 4.0], dtype=np.float32)
    metrics = score_deterministic_grids(forecast, obs)
    assert metrics.sample_count == 3  # NaN cell skipped
    assert metrics.mae == pytest.approx(1.0 / 3)  # only the (2 vs 3) miss


def test_all_nan_returns_zero_with_zero_sample_count() -> None:
    obs = np.full(3, np.nan, dtype=np.float32)
    forecast = np.array([1.0, 2.0, 3.0], dtype=np.float32)
    metrics = score_deterministic_grids(forecast, obs)
    assert metrics.sample_count == 0
    assert metrics.mae == 0.0
    assert metrics.bias == 0.0
    assert metrics.rmse == 0.0


def test_shape_mismatch_raises_valueerror() -> None:
    a = np.zeros((2, 2), dtype=np.float32)
    b = np.zeros((3, 3), dtype=np.float32)
    with pytest.raises(ValueError, match="shape mismatch"):
        score_deterministic_grids(a, b)


def test_metrics_dataclass_is_frozen() -> None:
    metrics = DeterministicMetrics(mae=1.0, bias=0.0, rmse=1.0, sample_count=1)
    with pytest.raises(AttributeError):
        metrics.mae = 2.0  # type: ignore[misc]
