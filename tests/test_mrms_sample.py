"""Unit tests for the Zarr point sampler.

Builds a tiny synthetic DataArray with known coords, writes it to a
real Zarr store under ``tmp_path``, then exercises the sampler. No
NATS, no DB, no S3 — pure read-side logic.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest
import xarray as xr

from aeroza.query.mrms_sample import (
    DEFAULT_TOLERANCE_DEG,
    GridSample,
    OutOfDomainError,
    PolygonSample,
    sample_grid_at_point,
    sample_grid_in_polygon,
)

pytestmark = pytest.mark.unit


def _write_grid(
    *,
    target: Path,
    latitudes: list[float],
    longitudes: list[float],
    values: np.ndarray,
    variable: str = "reflectivity",
) -> str:
    """Write a 2D DataArray with explicit lat/lng coords. Returns the URI."""
    da = xr.DataArray(
        values,
        coords={"latitude": latitudes, "longitude": longitudes},
        dims=("latitude", "longitude"),
        name=variable,
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    da.to_zarr(str(target), mode="w")
    return str(target)


async def test_returns_value_at_exact_cell(tmp_path: Path) -> None:
    uri = _write_grid(
        target=tmp_path / "g.zarr",
        latitudes=[20.0, 20.5, 21.0, 21.5],
        longitudes=[-100.0, -99.5, -99.0, -98.5],
        values=np.arange(16, dtype=np.float32).reshape(4, 4),
    )
    sample = await sample_grid_at_point(
        zarr_uri=uri,
        variable="reflectivity",
        latitude=20.5,
        longitude=-99.5,
    )
    assert isinstance(sample, GridSample)
    assert sample.latitude == pytest.approx(20.5)
    assert sample.longitude == pytest.approx(-99.5)
    # arange(16).reshape(4,4)[1,1] = 5
    assert sample.value == pytest.approx(5.0)
    assert sample.variable == "reflectivity"


async def test_snaps_to_nearest_cell(tmp_path: Path) -> None:
    uri = _write_grid(
        target=tmp_path / "g.zarr",
        latitudes=[20.0, 20.5, 21.0],
        longitudes=[-100.0, -99.5, -99.0],
        values=np.arange(9, dtype=np.float32).reshape(3, 3),
    )
    # Closer to (20.5, -99.5) → cell [1,1] = 4
    sample = await sample_grid_at_point(
        zarr_uri=uri,
        variable="reflectivity",
        latitude=20.48,
        longitude=-99.49,
    )
    assert sample.latitude == pytest.approx(20.5)
    assert sample.longitude == pytest.approx(-99.5)
    assert sample.value == pytest.approx(4.0)


async def test_out_of_domain_raises(tmp_path: Path) -> None:
    uri = _write_grid(
        target=tmp_path / "g.zarr",
        latitudes=[20.0, 20.5, 21.0],
        longitudes=[-100.0, -99.5, -99.0],
        values=np.zeros((3, 3), dtype=np.float32),
    )
    # Far outside both axes → no cell within DEFAULT_TOLERANCE_DEG.
    with pytest.raises(OutOfDomainError):
        await sample_grid_at_point(
            zarr_uri=uri,
            variable="reflectivity",
            latitude=50.0,
            longitude=-50.0,
        )


async def test_unknown_variable_raises_keyerror(tmp_path: Path) -> None:
    uri = _write_grid(
        target=tmp_path / "g.zarr",
        latitudes=[20.0, 20.5],
        longitudes=[-100.0, -99.5],
        values=np.zeros((2, 2), dtype=np.float32),
    )
    with pytest.raises(KeyError):
        await sample_grid_at_point(
            zarr_uri=uri,
            variable="not-a-variable",
            latitude=20.0,
            longitude=-100.0,
        )


async def test_translates_signed_longitude_to_grid_convention(tmp_path: Path) -> None:
    """MRMS publishes on [0, 360); user input is on [-180, 180]. The
    sampler should translate transparently and report matched lng on
    the user's convention."""
    # Grid covers 260..262 (i.e. -100..-98 in the signed convention).
    uri = _write_grid(
        target=tmp_path / "g.zarr",
        latitudes=[20.0, 20.5],
        longitudes=[260.0, 260.5, 261.0],
        values=np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]], dtype=np.float32),
    )
    # Request -99.5 == grid 260.5 → (lat 20.0, lng 260.5) → value 2.0
    sample = await sample_grid_at_point(
        zarr_uri=uri,
        variable="reflectivity",
        latitude=20.0,
        longitude=-99.5,
    )
    assert sample.value == pytest.approx(2.0)
    # Reported on the signed convention.
    assert sample.longitude == pytest.approx(-99.5)
    assert sample.latitude == pytest.approx(20.0)


async def test_custom_tolerance_widens_match(tmp_path: Path) -> None:
    """A point ~0.4° away misses with the default tolerance but hits at 0.5°."""
    uri = _write_grid(
        target=tmp_path / "g.zarr",
        latitudes=[20.0, 21.0],
        longitudes=[-100.0, -99.0],
        values=np.array([[10.0, 20.0], [30.0, 40.0]], dtype=np.float32),
    )
    with pytest.raises(OutOfDomainError):
        await sample_grid_at_point(
            zarr_uri=uri,
            variable="reflectivity",
            latitude=20.4,
            longitude=-99.6,
            tolerance_deg=DEFAULT_TOLERANCE_DEG,
        )
    # Wider tolerance → matches the (20.0, -100.0) cell.
    sample = await sample_grid_at_point(
        zarr_uri=uri,
        variable="reflectivity",
        latitude=20.4,
        longitude=-99.6,
        tolerance_deg=0.5,
    )
    assert sample.value == pytest.approx(10.0)


# ---------------------------------------------------------------------------
# sample_grid_in_polygon — polygon reducer


def _write_5x5_grid(target: Path) -> str:
    """5x5 grid covering lat 20..21 and lng -100..-99 at 0.25° spacing.

    Values are sequential 0..24 row-major so each test can compute the
    expected reducer output by hand.
    """
    return _write_grid(
        target=target,
        latitudes=[20.0, 20.25, 20.5, 20.75, 21.0],
        longitudes=[-100.0, -99.75, -99.5, -99.25, -99.0],
        values=np.arange(25, dtype=np.float32).reshape(5, 5),
    )


async def test_polygon_max_over_known_cells(tmp_path: Path) -> None:
    uri = _write_5x5_grid(tmp_path / "g.zarr")
    # Polygon covering the top-right 2x2 sub-grid (lat 20.75..21.0, lng -99.25..-99.0).
    # Values there are 18, 19, 23, 24. Max = 24.
    polygon = (
        (-99.30, 20.70),
        (-98.95, 20.70),
        (-98.95, 21.05),
        (-99.30, 21.05),
    )
    sample = await sample_grid_in_polygon(
        zarr_uri=uri,
        variable="reflectivity",
        polygon_lng_lat=polygon,
        reducer="max",
    )
    assert isinstance(sample, PolygonSample)
    assert sample.reducer == "max"
    assert sample.value == pytest.approx(24.0)
    assert sample.cell_count == 4
    assert sample.threshold is None
    assert sample.bbox_min_latitude == pytest.approx(20.75)
    assert sample.bbox_max_latitude == pytest.approx(21.0)
    assert sample.bbox_min_longitude == pytest.approx(-99.25)
    assert sample.bbox_max_longitude == pytest.approx(-99.0)


async def test_polygon_mean_min(tmp_path: Path) -> None:
    uri = _write_5x5_grid(tmp_path / "g.zarr")
    polygon = (
        (-99.30, 20.70),
        (-98.95, 20.70),
        (-98.95, 21.05),
        (-99.30, 21.05),
    )
    mean = await sample_grid_in_polygon(
        zarr_uri=uri,
        variable="reflectivity",
        polygon_lng_lat=polygon,
        reducer="mean",
    )
    # mean(18, 19, 23, 24) = 21.0
    assert mean.value == pytest.approx(21.0)
    assert mean.reducer == "mean"

    minimum = await sample_grid_in_polygon(
        zarr_uri=uri,
        variable="reflectivity",
        polygon_lng_lat=polygon,
        reducer="min",
    )
    assert minimum.value == pytest.approx(18.0)


async def test_polygon_count_ge_with_threshold(tmp_path: Path) -> None:
    uri = _write_5x5_grid(tmp_path / "g.zarr")
    # Whole-grid polygon → all 25 cells. Cells with value >= 20 are {20..24} = 5.
    polygon = (
        (-100.05, 19.95),
        (-98.95, 19.95),
        (-98.95, 21.05),
        (-100.05, 21.05),
    )
    sample = await sample_grid_in_polygon(
        zarr_uri=uri,
        variable="reflectivity",
        polygon_lng_lat=polygon,
        reducer="count_ge",
        threshold=20.0,
    )
    assert sample.reducer == "count_ge"
    assert sample.threshold == 20.0
    assert sample.cell_count == 25
    assert sample.value == pytest.approx(5.0)


async def test_polygon_count_ge_requires_threshold(tmp_path: Path) -> None:
    uri = _write_5x5_grid(tmp_path / "g.zarr")
    with pytest.raises(ValueError, match="threshold"):
        await sample_grid_in_polygon(
            zarr_uri=uri,
            variable="reflectivity",
            polygon_lng_lat=((-100.0, 20.0), (-99.0, 20.0), (-99.0, 21.0)),
            reducer="count_ge",
        )


async def test_polygon_outside_grid_raises(tmp_path: Path) -> None:
    uri = _write_5x5_grid(tmp_path / "g.zarr")
    # Far away from the grid extent.
    polygon = (
        (10.0, 50.0),
        (11.0, 50.0),
        (11.0, 51.0),
        (10.0, 51.0),
    )
    with pytest.raises(OutOfDomainError):
        await sample_grid_in_polygon(
            zarr_uri=uri,
            variable="reflectivity",
            polygon_lng_lat=polygon,
            reducer="max",
        )


async def test_polygon_too_small_to_contain_any_cell_centre_raises(tmp_path: Path) -> None:
    """Tiny polygon that misses every cell centre → OutOfDomainError so
    the route can map cleanly to a 404. Either branch (empty bbox slice
    or non-zero bbox slice with empty mask) surfaces the same exception
    type — that's the load-bearing contract for the route."""
    uri = _write_5x5_grid(tmp_path / "g.zarr")
    # Cells are at 0.25° intervals; this polygon sits between cells.
    polygon = (
        (-99.62, 20.62),
        (-99.60, 20.62),
        (-99.60, 20.64),
        (-99.62, 20.64),
    )
    with pytest.raises(OutOfDomainError):
        await sample_grid_in_polygon(
            zarr_uri=uri,
            variable="reflectivity",
            polygon_lng_lat=polygon,
            reducer="max",
        )


async def test_polygon_unknown_variable_raises_keyerror(tmp_path: Path) -> None:
    uri = _write_5x5_grid(tmp_path / "g.zarr")
    polygon = ((-100.0, 20.0), (-99.0, 20.0), (-99.0, 21.0), (-100.0, 21.0))
    with pytest.raises(KeyError):
        await sample_grid_in_polygon(
            zarr_uri=uri,
            variable="not-a-variable",
            polygon_lng_lat=polygon,
            reducer="max",
        )


async def test_polygon_translates_signed_longitude(tmp_path: Path) -> None:
    """If the grid uses [0, 360) lngs, a polygon supplied on [-180, 180]
    should still match — same translation logic as the point sampler."""
    # Grid in the 260..262 (== -100..-98) convention.
    uri = _write_grid(
        target=tmp_path / "g.zarr",
        latitudes=[20.0, 20.5, 21.0],
        longitudes=[260.0, 260.5, 261.0],
        values=np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]], dtype=np.float32),
    )
    # Polygon on the signed convention covering exactly the centre cell.
    polygon = (
        (-99.6, 20.4),
        (-99.4, 20.4),
        (-99.4, 20.6),
        (-99.6, 20.6),
    )
    sample = await sample_grid_in_polygon(
        zarr_uri=uri,
        variable="reflectivity",
        polygon_lng_lat=polygon,
        reducer="max",
    )
    assert sample.cell_count == 1
    assert sample.value == pytest.approx(5.0)
    # Bbox reported back on the signed convention.
    assert sample.bbox_min_longitude == pytest.approx(-99.5)
    assert sample.bbox_max_longitude == pytest.approx(-99.5)
