"""Unit tests for the persistence forecaster.

The Forecaster Protocol is one async method; the v1 implementation
is two-line copy-and-bump-timestamp logic. These tests pin the
contract so a future pySTEPS / NowcastNet implementation has clear
behaviour to mirror.
"""

from __future__ import annotations

from datetime import UTC, datetime

import numpy as np
import pytest
import xarray as xr

from aeroza.nowcast.engine import (
    DEFAULT_HORIZONS_MINUTES,
    PERSISTENCE_ALGORITHM,
    NowcastPrediction,
    PersistenceForecaster,
)

pytestmark = pytest.mark.unit


def _grid(value: float = 1.0) -> xr.DataArray:
    return xr.DataArray(
        np.full((3, 3), value, dtype=np.float32),
        dims=("latitude", "longitude"),
        name="reflectivity",
    )


async def test_persistence_algorithm_tag() -> None:
    assert PersistenceForecaster().algorithm == PERSISTENCE_ALGORITHM
    assert PERSISTENCE_ALGORITHM == "persistence"


async def test_default_horizons_match_plan_targets() -> None:
    """The plan §7 calls out 10/30/60-minute reliability targets."""
    assert DEFAULT_HORIZONS_MINUTES == (10, 30, 60)


async def test_persistence_yields_one_prediction_per_horizon() -> None:
    forecaster = PersistenceForecaster()
    obs = _grid(value=42.0)
    valid_at = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    predictions = await forecaster.forecast(
        obs, observation_valid_at=valid_at, horizons_minutes=(10, 30, 60)
    )
    assert len(predictions) == 3


async def test_persistence_data_unchanged() -> None:
    forecaster = PersistenceForecaster()
    obs = _grid(value=42.0)
    valid_at = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    predictions = await forecaster.forecast(
        obs, observation_valid_at=valid_at, horizons_minutes=(10,)
    )
    np.testing.assert_array_equal(predictions[0].data.values, obs.values)


async def test_persistence_valid_at_bumped_by_horizon() -> None:
    forecaster = PersistenceForecaster()
    obs = _grid()
    base = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    predictions = await forecaster.forecast(
        obs, observation_valid_at=base, horizons_minutes=(10, 30, 60)
    )
    by_horizon = {p.horizon_minutes: p for p in predictions}
    assert by_horizon[10].valid_at == datetime(2026, 5, 1, 12, 10, tzinfo=UTC)
    assert by_horizon[30].valid_at == datetime(2026, 5, 1, 12, 30, tzinfo=UTC)
    assert by_horizon[60].valid_at == datetime(2026, 5, 1, 13, 0, tzinfo=UTC)


async def test_persistence_returns_independent_copies() -> None:
    """Mutating a prediction's data must not corrupt the source observation."""
    forecaster = PersistenceForecaster()
    obs = _grid(value=10.0)
    base = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    predictions = await forecaster.forecast(obs, observation_valid_at=base, horizons_minutes=(10,))
    predictions[0].data.values[:] = 99.0
    assert obs.values[0, 0] == 10.0


async def test_persistence_handles_empty_horizon_list() -> None:
    forecaster = PersistenceForecaster()
    obs = _grid()
    base = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    predictions = await forecaster.forecast(obs, observation_valid_at=base, horizons_minutes=())
    assert predictions == []


def test_nowcast_prediction_is_frozen() -> None:
    p = NowcastPrediction(
        horizon_minutes=10,
        valid_at=datetime(2026, 5, 1, 12, 10, tzinfo=UTC),
        data=_grid(),
    )
    with pytest.raises(AttributeError):
        p.horizon_minutes = 20  # type: ignore[misc]
