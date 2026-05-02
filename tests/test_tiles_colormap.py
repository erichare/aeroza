"""Unit tests for the dBZ → RGBA colormap."""

from __future__ import annotations

import numpy as np
import pytest

from aeroza.tiles.colormap import (
    DBZ_STOPS,
    TRANSPARENT_BELOW_DBZ,
    reflectivity_to_rgba,
)


@pytest.mark.unit
def test_nan_cells_are_transparent() -> None:
    values = np.array([[np.nan, np.nan], [np.nan, np.nan]], dtype=np.float32)
    rgba = reflectivity_to_rgba(values)
    assert rgba.shape == (2, 2, 4)
    assert (rgba[..., 3] == 0).all()


@pytest.mark.unit
def test_below_threshold_is_transparent() -> None:
    values = np.array([[TRANSPARENT_BELOW_DBZ - 1.0]], dtype=np.float32)
    rgba = reflectivity_to_rgba(values)
    assert rgba[0, 0, 3] == 0


@pytest.mark.unit
def test_at_threshold_is_visible() -> None:
    values = np.array([[TRANSPARENT_BELOW_DBZ]], dtype=np.float32)
    rgba = reflectivity_to_rgba(values)
    assert rgba[0, 0, 3] > 0


@pytest.mark.unit
def test_above_top_stop_is_clamped_to_top_color() -> None:
    top_value = DBZ_STOPS[-1][0]
    rgba = reflectivity_to_rgba(np.array([[top_value + 50.0]], dtype=np.float32))
    expected_rgb = np.array(DBZ_STOPS[-1][1:], dtype=np.uint8)
    assert (rgba[0, 0, :3] == expected_rgb).all()


@pytest.mark.unit
def test_below_first_stop_uses_first_stop_color() -> None:
    # Threshold == 5 dBZ == first stop. Anything ≥ that and < the next stop
    # should interpolate between stops 0 and 1.
    rgba = reflectivity_to_rgba(np.array([[5.0]], dtype=np.float32))
    first_rgb = np.array(DBZ_STOPS[0][1:], dtype=np.uint8)
    # At t=0 we get exactly the first stop's RGB.
    assert (rgba[0, 0, :3] == first_rgb).all()


@pytest.mark.unit
def test_interpolation_picks_between_adjacent_stops() -> None:
    # Halfway between stop 0 (5 dBZ) and stop 1 (10 dBZ) should be the
    # average of the two RGBs.
    halfway = (DBZ_STOPS[0][0] + DBZ_STOPS[1][0]) / 2
    rgba = reflectivity_to_rgba(np.array([[halfway]], dtype=np.float32))
    a = np.array(DBZ_STOPS[0][1:], dtype=np.float64)
    b = np.array(DBZ_STOPS[1][1:], dtype=np.float64)
    expected = ((a + b) / 2).astype(np.uint8)
    assert np.allclose(rgba[0, 0, :3].astype(np.int16), expected.astype(np.int16), atol=1)


@pytest.mark.unit
def test_2d_shape_required() -> None:
    with pytest.raises(ValueError):
        reflectivity_to_rgba(np.array([1.0, 2.0]))


@pytest.mark.unit
def test_returns_uint8_rgba() -> None:
    rgba = reflectivity_to_rgba(np.array([[20.0, np.nan]], dtype=np.float32))
    assert rgba.dtype == np.uint8
    assert rgba.shape == (1, 2, 4)
