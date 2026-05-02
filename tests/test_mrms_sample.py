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
    sample_grid_at_point,
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
