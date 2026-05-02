"""Unit tests for ``PystepsForecaster``.

The pySTEPS optical-flow + extrapolation pipeline is a heavy native
dep we don't want every contributor to install, so these tests are
gated on the ``[nowcast]`` extra: when pysteps isn't importable the
whole module is skipped, and the persistence suite still gates the
non-pysteps code paths.

What we exercise:

- ``forecast()`` returns one prediction per requested horizon, with
  ``valid_at`` correctly bumped.
- The output coords + dims match the input observation (so the
  worker's Zarr writer doesn't have to re-coerce).
- A short history list (less than ``lookback - 1``) falls back to
  persistence rather than crashing.
- A synthetic moving blob actually moves in the predicted frames —
  the regression that catches "we accidentally implemented
  persistence in fancy clothing".
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import numpy as np
import pytest

pysteps = pytest.importorskip("pysteps")

from aeroza.nowcast.pysteps_forecaster import (  # noqa: E402
    BELOW_THRESHOLD_DBZ,
    DEFAULT_DT_MINUTES,
    PYSTEPS_ALGORITHM,
    PystepsForecaster,
)

pytestmark = pytest.mark.unit


def _frame(values: np.ndarray) -> object:
    """Build an xarray DataArray from a 2D numpy array. Lazy-imported so
    the bare unit-test sweep doesn't need xarray on the import path
    when this file is collected but skipped."""
    import xarray as xr

    h, w = values.shape
    return xr.DataArray(
        values,
        coords={
            "latitude": np.linspace(50.0, 25.0, h),
            "longitude": np.linspace(-110.0, -80.0, w),
        },
        dims=("latitude", "longitude"),
        name="reflectivity",
    )


def _moving_blob(t: int, *, height: int = 64, width: int = 64) -> np.ndarray:
    """Return a (height, width) array with a Gaussian blob centered at
    column ``20 + t * 4`` — a clean "moves east at 4 px / step" signal
    Lucas–Kanade can lock onto."""
    yy, xx = np.meshgrid(np.arange(height), np.arange(width), indexing="ij")
    cy = height // 2
    cx = 20 + t * 4
    sigma = 4.0
    blob = 40.0 * np.exp(-((yy - cy) ** 2 + (xx - cx) ** 2) / (2 * sigma**2))
    # Floor at the no-echo level so the forecaster's NaN-handling has
    # something to do.
    floor = BELOW_THRESHOLD_DBZ - 1.0
    return np.maximum(blob, floor).astype(np.float32)


def test_algorithm_tag() -> None:
    assert PystepsForecaster().algorithm == PYSTEPS_ALGORITHM


async def test_short_history_falls_back_to_persistence() -> None:
    """One past frame is shorter than the default lookback (3) → fall
    back to persistence so the worker doesn't crash on its first tick."""
    forecaster = PystepsForecaster()
    obs = _frame(_moving_blob(2))
    valid_at = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    history = (_frame(_moving_blob(1)),)

    predictions = await forecaster.forecast(
        observation=obs,
        observation_valid_at=valid_at,
        horizons_minutes=[10, 30, 60],
        history=history,
    )

    assert len(predictions) == 3
    # Persistence: every prediction equals the observation.
    import xarray as xr  # noqa: F401

    for p, h in zip(predictions, [10, 30, 60], strict=True):
        assert p.horizon_minutes == h
        assert p.valid_at == valid_at + timedelta(minutes=h)
        np.testing.assert_array_equal(p.data.values, obs.values)


async def test_forecast_returns_one_prediction_per_horizon() -> None:
    forecaster = PystepsForecaster()
    valid_at = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    history = tuple(_frame(_moving_blob(t)) for t in range(2))
    obs = _frame(_moving_blob(2))

    predictions = await forecaster.forecast(
        observation=obs,
        observation_valid_at=valid_at,
        horizons_minutes=[10, 30],
        history=history,
    )

    assert [p.horizon_minutes for p in predictions] == [10, 30]
    for p, h in zip(predictions, [10, 30], strict=True):
        assert p.valid_at == valid_at + timedelta(minutes=h)
        # Coords + dims survive intact.
        assert tuple(p.data.dims) == ("latitude", "longitude")
        assert p.data.shape == obs.shape


async def test_forecast_actually_advects_a_moving_blob() -> None:
    """The pySTEPS regression: with a clean east-moving blob, the
    predicted frame's center of mass should be EAST of the observation's.
    If we accidentally fell back to persistence the centers would
    coincide — which is exactly the bug this test is here to catch."""
    forecaster = PystepsForecaster(lookback=3, dt_minutes=DEFAULT_DT_MINUTES)
    valid_at = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    history = tuple(_frame(_moving_blob(t)) for t in range(2))
    obs = _frame(_moving_blob(2))

    predictions = await forecaster.forecast(
        observation=obs,
        observation_valid_at=valid_at,
        # 6 minutes ≈ 3 steps at dt=2 min, plenty of advection.
        horizons_minutes=[6],
        history=history,
    )

    [forecast] = predictions
    obs_cx = _center_of_mass_x(obs.values)
    pred_cx = _center_of_mass_x(np.nan_to_num(forecast.data.values, nan=0.0))
    # The blob travels east at +4 px / step, so 3 steps → +12 px. Allow
    # a generous slack for sub-pixel motion-solver smoothing; the test's
    # job is to catch "didn't move at all," not to validate the exact
    # number of pixels.
    assert pred_cx > obs_cx + 3.0, (
        f"forecast center-of-mass ({pred_cx:.1f}) did not move east of "
        f"observation ({obs_cx:.1f}) — pySTEPS may have fallen through "
        f"to persistence."
    )


def _center_of_mass_x(arr: np.ndarray) -> float:
    """Mass-weighted x-coordinate. Falls back to 0 on an empty image."""
    cols = np.arange(arr.shape[1], dtype=np.float64)
    weights = np.maximum(arr - BELOW_THRESHOLD_DBZ, 0.0)
    total = weights.sum()
    if total <= 0:
        return 0.0
    return float((weights.sum(axis=0) * cols).sum() / total)
