"""Unit tests for the Web Mercator tile-coordinate math."""

from __future__ import annotations

import math

import numpy as np
import pytest

from aeroza.tiles.web_mercator import (
    MAX_LATITUDE,
    TILE_SIZE,
    latlng_to_pixel_indices,
    pixel_lonlat_grid,
    tile_bounds,
)


@pytest.mark.unit
def test_z0_tile_covers_whole_world() -> None:
    b = tile_bounds(0, 0, 0)
    assert b.lng_min == pytest.approx(-180.0)
    assert b.lng_max == pytest.approx(180.0)
    # Web Mercator clips at ~85.05° N/S, not 90°.
    assert b.lat_max == pytest.approx(MAX_LATITUDE, abs=1e-6)
    assert b.lat_min == pytest.approx(-MAX_LATITUDE, abs=1e-6)


@pytest.mark.unit
def test_z1_quadrants_partition_the_world() -> None:
    # The four z=1 tiles must tile the world without overlap.
    nw = tile_bounds(1, 0, 0)
    ne = tile_bounds(1, 1, 0)
    sw = tile_bounds(1, 0, 1)
    se = tile_bounds(1, 1, 1)

    assert nw.lng_min == pytest.approx(-180) and nw.lng_max == pytest.approx(0)
    assert ne.lng_min == pytest.approx(0) and ne.lng_max == pytest.approx(180)
    assert nw.lat_min == pytest.approx(0, abs=1e-6) and nw.lat_max > 0
    assert sw.lat_max == pytest.approx(0, abs=1e-6) and sw.lat_min < 0
    assert se.lat_min == pytest.approx(-MAX_LATITUDE, abs=1e-6)


@pytest.mark.unit
def test_tile_bounds_rejects_out_of_range() -> None:
    with pytest.raises(ValueError):
        tile_bounds(2, 4, 0)  # n=4, x=4 is out of range
    with pytest.raises(ValueError):
        tile_bounds(2, 0, 4)
    with pytest.raises(ValueError):
        tile_bounds(2, -1, 0)


@pytest.mark.unit
def test_pixel_grid_spans_tile_bounds() -> None:
    b = tile_bounds(2, 1, 1)
    lng, lat = pixel_lonlat_grid(b, tile_size=TILE_SIZE)
    assert lng.shape == (TILE_SIZE, TILE_SIZE)
    assert lat.shape == (TILE_SIZE, TILE_SIZE)
    # Pixel centers, so the first/last pixels are half a step inside the
    # tile edge — direction matters (lat decreases top→bottom).
    assert lng[0, 0] > b.lng_min
    assert lng[0, -1] < b.lng_max
    assert lat[0, 0] < b.lat_max
    assert lat[-1, 0] > b.lat_min
    # Each row shares its latitude; each column shares its longitude.
    assert np.allclose(lng[0, :], lng[-1, :])
    assert np.allclose(lat[:, 0], lat[:, -1])


@pytest.mark.unit
def test_pixel_grid_rejects_nonpositive_size() -> None:
    b = tile_bounds(0, 0, 0)
    with pytest.raises(ValueError):
        pixel_lonlat_grid(b, tile_size=0)


@pytest.mark.unit
def test_latlng_to_pixel_indices_marks_out_of_bounds() -> None:
    grid_lats = np.linspace(40.0, 50.0, 11)  # 1° steps
    grid_lngs = np.linspace(-100.0, -90.0, 11)
    lats = np.array([[40.5, 60.0], [45.0, 49.0]])  # second pixel out of bounds
    lngs = np.array([[-100.0, -90.0], [-95.0, -89.0]])  # last pixel out of bounds
    rows, cols, in_bounds = latlng_to_pixel_indices(
        lats=lats, lngs=lngs, grid_lats=grid_lats, grid_lngs=grid_lngs
    )
    assert in_bounds[0, 0]
    assert not in_bounds[0, 1]  # 60° N out of grid
    assert in_bounds[1, 0]
    assert not in_bounds[1, 1]  # -89° E out of grid
    assert rows.shape == lats.shape
    assert cols.shape == lats.shape


@pytest.mark.unit
def test_latlng_to_pixel_indices_picks_nearest_cell() -> None:
    grid_lats = np.linspace(0.0, 10.0, 11)
    grid_lngs = np.linspace(0.0, 10.0, 11)
    # Request exactly on a grid point and slightly off-center.
    lats = np.array([[5.0, 5.4, 5.6]])
    lngs = np.array([[3.0, 3.4, 3.6]])
    rows, cols, _ = latlng_to_pixel_indices(
        lats=lats, lngs=lngs, grid_lats=grid_lats, grid_lngs=grid_lngs
    )
    assert rows.tolist() == [[5, 5, 6]]
    assert cols.tolist() == [[3, 3, 4]]


@pytest.mark.unit
def test_z0_pixel_lat_endpoints_clip_at_max_latitude() -> None:
    b = tile_bounds(0, 0, 0)
    _lng, lat = pixel_lonlat_grid(b, tile_size=4)
    # Top row latitude is north of equator and inside the Mercator clip.
    # We don't assert proximity to MAX_LATITUDE because the pixel center
    # at coarse tile sizes sits well inside the tile (≈79° at size 4).
    assert lat[0, 0] < MAX_LATITUDE
    assert lat[0, 0] > 60  # comfortably north
    assert lat[-1, 0] > -MAX_LATITUDE
    assert lat[-1, 0] < -60  # comfortably south
    # And rows are strictly monotonic top→bottom (north→south).
    assert all(lat[i, 0] > lat[i + 1, 0] for i in range(lat.shape[0] - 1))


@pytest.mark.unit
def test_inverse_mercator_against_known_value() -> None:
    # At z=2 / x=1 / y=1 the tile spans (-90,0) east-west and (0, ~66.51) NS.
    # 66.51° is `atan(sinh(π/2))·180/π`.
    b = tile_bounds(2, 1, 1)
    expected_top = math.degrees(math.atan(math.sinh(math.pi / 2)))
    assert b.lat_max == pytest.approx(expected_top, abs=1e-6)
