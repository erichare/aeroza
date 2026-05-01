"""Unit tests for MRMS Zarr materialisation primitives.

Uses synthetic ``xarray.DataArray`` instances built from numpy — no GRIB
involved. Phase 3.2b plugs in cfgrib to produce these DataArrays from
real MRMS files; the tests below stay valid because the Zarr writer's
contract is "any DataArray in, MrmsGridLocator out".
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest
import xarray as xr

from aeroza.ingest.mrms_zarr import (
    MrmsGridLocator,
    locator_to_row_dict,
    write_dataarray_to_zarr,
    zarr_path_for,
)

pytestmark = pytest.mark.unit


# --------------------------------------------------------------------------- #
# Path layout                                                                  #
# --------------------------------------------------------------------------- #


def test_zarr_path_strips_grib_gz_suffix(tmp_path: Path) -> None:
    file_key = (
        "CONUS/MergedReflectivityComposite_00.50/20260501/"
        "MRMS_MergedReflectivityComposite_00.50_20260501-120000.grib2.gz"
    )
    target = zarr_path_for(tmp_path, file_key)
    assert target == (
        tmp_path
        / "CONUS"
        / "MergedReflectivityComposite_00.50"
        / "20260501"
        / "MRMS_MergedReflectivityComposite_00.50_20260501-120000.zarr"
    )


def test_zarr_path_strips_uncompressed_grib_suffix(tmp_path: Path) -> None:
    file_key = "CONUS/PrecipRate_00.00/20260501/MRMS_PrecipRate_00.00_20260501-001500.grib2"
    target = zarr_path_for(tmp_path, file_key)
    assert target.name == "MRMS_PrecipRate_00.00_20260501-001500.zarr"


def test_zarr_path_handles_unexpected_suffix(tmp_path: Path) -> None:
    """Files without a recognised suffix still produce a sensible path."""
    file_key = "weird/path/no-suffix"
    target = zarr_path_for(tmp_path, file_key)
    assert target == tmp_path / "weird" / "path" / "no-suffix.zarr"


# --------------------------------------------------------------------------- #
# DataArray → Zarr round-trip                                                  #
# --------------------------------------------------------------------------- #


def _synthetic_da(name: str = "reflectivity") -> xr.DataArray:
    return xr.DataArray(
        np.arange(50, dtype=np.float32).reshape(5, 10),
        dims=("latitude", "longitude"),
        coords={
            "latitude": np.linspace(29.0, 30.0, 5),
            "longitude": np.linspace(-95.5, -94.0, 10),
        },
        name=name,
    )


def test_writes_dataarray_and_round_trips(tmp_path: Path) -> None:
    da = _synthetic_da()
    file_key = (
        "CONUS/MergedReflectivityComposite_00.50/20260501/"
        "MRMS_MergedReflectivityComposite_00.50_20260501-120000.grib2.gz"
    )

    locator = write_dataarray_to_zarr(da, target_root=tmp_path, file_key=file_key)

    assert isinstance(locator, MrmsGridLocator)
    assert locator.file_key == file_key
    assert locator.variable == "reflectivity"
    assert locator.dims == ("latitude", "longitude")
    assert locator.shape == (5, 10)
    assert locator.dtype == "float32"
    assert locator.nbytes == 5 * 10 * 4

    assert Path(locator.zarr_uri).exists()

    reopened = xr.open_zarr(locator.zarr_uri)
    np.testing.assert_array_equal(reopened["reflectivity"].values, da.values)


def test_overwrites_existing_zarr_store(tmp_path: Path) -> None:
    file_key = "x.grib2.gz"
    write_dataarray_to_zarr(_synthetic_da(), target_root=tmp_path, file_key=file_key)
    second = _synthetic_da().rename("reflectivity") * 2.0
    second.name = "reflectivity"  # double-check the name survives the multiply
    locator = write_dataarray_to_zarr(second, target_root=tmp_path, file_key=file_key)

    reopened = xr.open_zarr(locator.zarr_uri)
    np.testing.assert_array_equal(reopened["reflectivity"].values, second.values)


def test_creates_intermediate_directories(tmp_path: Path) -> None:
    file_key = "deep/nested/path/MRMS_X_00.00_20260501-120000.grib2.gz"
    locator = write_dataarray_to_zarr(_synthetic_da(), target_root=tmp_path, file_key=file_key)
    assert (tmp_path / "deep" / "nested" / "path").is_dir()
    assert Path(locator.zarr_uri).is_dir()


def test_unnamed_dataarray_falls_back_to_value(tmp_path: Path) -> None:
    da = xr.DataArray(np.zeros((3, 3), dtype=np.float32), dims=("y", "x"))
    locator = write_dataarray_to_zarr(da, target_root=tmp_path, file_key="anon.grib2.gz")
    assert locator.variable == "value"
    reopened = xr.open_zarr(locator.zarr_uri)
    assert "value" in reopened.data_vars


# --------------------------------------------------------------------------- #
# Locator → row dict                                                           #
# --------------------------------------------------------------------------- #


def test_locator_to_row_dict_serialises_dims_and_shape_as_json() -> None:
    locator = MrmsGridLocator(
        file_key="x",
        zarr_uri="/tmp/x.zarr",
        variable="r",
        dims=("lat", "lon"),
        shape=(100, 200),
        dtype="float32",
        nbytes=80_000,
    )
    row = locator_to_row_dict(locator)
    assert row["file_key"] == "x"
    assert row["zarr_uri"] == "/tmp/x.zarr"
    assert row["variable"] == "r"
    assert json.loads(row["dims_json"]) == ["lat", "lon"]
    assert json.loads(row["shape_json"]) == [100, 200]
    assert row["dtype"] == "float32"
    assert row["nbytes"] == 80_000


def test_locator_is_frozen_and_hashable() -> None:
    locator = MrmsGridLocator(
        file_key="x",
        zarr_uri="/tmp/x.zarr",
        variable="r",
        dims=("lat", "lon"),
        shape=(1, 1),
        dtype="float32",
        nbytes=4,
    )
    with pytest.raises(AttributeError):
        locator.zarr_uri = "/tmp/y.zarr"  # type: ignore[misc]
    assert {locator, locator} == {locator}
