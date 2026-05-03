"""Unit tests for :class:`LaggedEnsembleForecaster`.

The forecaster is a pure stack-and-emit operation; tests pin the
output shape, member ordering, ensemble-size honoring, and the cold-
start fallback behaviour the worker depends on.
"""

from __future__ import annotations

from datetime import UTC, datetime

import numpy as np
import pytest
import xarray as xr

from aeroza.nowcast.engine import NowcastPrediction
from aeroza.nowcast.lagged_ensemble import (
    LAGGED_ENSEMBLE_ALGORITHM,
    LaggedEnsembleForecaster,
)

pytestmark = pytest.mark.unit


def _grid(value: float, shape: tuple[int, int] = (3, 4)) -> xr.DataArray:
    return xr.DataArray(
        np.full(shape, value, dtype=np.float32),
        dims=("y", "x"),
        coords={"y": np.arange(shape[0]), "x": np.arange(shape[1])},
        name="reflectivity",
    )


@pytest.mark.asyncio
async def test_full_history_yields_requested_ensemble_size() -> None:
    forecaster = LaggedEnsembleForecaster(ensemble_size=4)
    history = [_grid(10.0), _grid(20.0), _grid(30.0)]
    observation = _grid(40.0)
    predictions = await forecaster.forecast(
        observation=observation,
        observation_valid_at=datetime(2026, 5, 3, 9, 0, tzinfo=UTC),
        horizons_minutes=[10, 30],
        history=history,
    )
    assert len(predictions) == 2
    for prediction in predictions:
        assert isinstance(prediction, NowcastPrediction)
        assert prediction.ensemble_size == 4
        assert prediction.data.dims == ("member", "y", "x")
        assert prediction.data.shape == (4, 3, 4)
        # Members oldest → newest: 10, 20, 30, then 40 (the obs).
        assert prediction.data[0, 0, 0].item() == pytest.approx(10.0)
        assert prediction.data[-1, 0, 0].item() == pytest.approx(40.0)


@pytest.mark.asyncio
async def test_cold_start_with_no_history_returns_single_member() -> None:
    forecaster = LaggedEnsembleForecaster(ensemble_size=4)
    observation = _grid(7.0)
    predictions = await forecaster.forecast(
        observation=observation,
        observation_valid_at=datetime(2026, 5, 3, 9, 0, tzinfo=UTC),
        horizons_minutes=[10],
        history=None,
    )
    assert predictions[0].ensemble_size == 1
    assert predictions[0].data.shape == (1, 3, 4)
    assert predictions[0].data[0, 0, 0].item() == pytest.approx(7.0)


@pytest.mark.asyncio
async def test_partial_history_uses_what_is_available() -> None:
    forecaster = LaggedEnsembleForecaster(ensemble_size=8)
    history = [_grid(1.0), _grid(2.0)]
    observation = _grid(3.0)
    predictions = await forecaster.forecast(
        observation=observation,
        observation_valid_at=datetime(2026, 5, 3, 9, 0, tzinfo=UTC),
        horizons_minutes=[60],
        history=history,
    )
    # Three members total: history (2) + current (1).
    assert predictions[0].ensemble_size == 3
    assert predictions[0].data.shape == (3, 3, 4)


@pytest.mark.asyncio
async def test_history_window_truncates_to_requested_size() -> None:
    """If history exceeds ensemble_size - 1, only the most recent are used."""
    forecaster = LaggedEnsembleForecaster(ensemble_size=3)
    history = [_grid(0.0), _grid(1.0), _grid(2.0), _grid(3.0)]  # 4 frames available
    observation = _grid(4.0)
    predictions = await forecaster.forecast(
        observation=observation,
        observation_valid_at=datetime(2026, 5, 3, 9, 0, tzinfo=UTC),
        horizons_minutes=[10],
        history=history,
    )
    # Most recent 2 from history + current = 3 total.
    assert predictions[0].ensemble_size == 3
    members = predictions[0].data
    # Members are oldest → newest: 2.0, 3.0, 4.0.
    assert members[0, 0, 0].item() == pytest.approx(2.0)
    assert members[-1, 0, 0].item() == pytest.approx(4.0)


@pytest.mark.asyncio
async def test_shape_mismatch_in_history_falls_back_to_single_member() -> None:
    forecaster = LaggedEnsembleForecaster(ensemble_size=4)
    history = [_grid(1.0, shape=(3, 4)), _grid(2.0, shape=(2, 4))]  # second is wrong
    observation = _grid(3.0, shape=(3, 4))
    predictions = await forecaster.forecast(
        observation=observation,
        observation_valid_at=datetime(2026, 5, 3, 9, 0, tzinfo=UTC),
        horizons_minutes=[10],
        history=history,
    )
    assert predictions[0].ensemble_size == 1


def test_algorithm_tag_constant() -> None:
    assert LaggedEnsembleForecaster().algorithm == LAGGED_ENSEMBLE_ALGORITHM
