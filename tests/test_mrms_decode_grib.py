"""Real cfgrib end-to-end tests.

These tests exercise the live decode path against an actual MRMS GRIB2
payload, which requires:

- the ``[grib]`` Python extra (``cfgrib``) installed via ``uv sync --extra grib``
- the ``eccodes`` system library (``brew install eccodes`` / ``apt install
  libeccodes-dev``)
- network access to ``s3://noaa-mrms-pds`` (anonymous read)

The :func:`grib_payload` fixture in ``conftest.py`` skips the test when
any of those is unavailable, so this suite is silently inert in
environments without eccodes (most contributor laptops). CI's
integration job installs eccodes and runs them under the ``grib`` marker.
"""

from __future__ import annotations

import pytest
import xarray as xr

from aeroza.ingest.mrms_decode import decode_grib2_to_dataarray
from aeroza.ingest.mrms_zarr import write_dataarray_to_zarr

pytestmark = pytest.mark.grib


def test_decodes_real_mrms_grib2_to_a_2d_dataarray(grib_payload: bytes) -> None:
    """A live MRMS PrecipRate file should decode into a 2D lat/lon grid."""
    da = decode_grib2_to_dataarray(grib_payload)

    assert isinstance(da, xr.DataArray)
    # MRMS CONUS grids are always 2D and named after the product. We don't
    # pin the variable name (cfgrib normalises GRIB2 short names), but we
    # do expect a roughly continental shape.
    assert len(da.dims) == 2, f"expected 2 dims, got {da.dims}"
    rows, cols = da.shape
    assert rows > 100 and cols > 100, f"grid too small to be CONUS-scale: {da.shape}"
    # Float-ish values (precipitation rate or reflectivity) — anything else
    # would suggest cfgrib normalised away the data variable.
    assert da.dtype.kind in {"f", "i"}, f"unexpected dtype kind: {da.dtype.kind}"


def test_real_decode_round_trips_through_zarr(grib_payload: bytes, tmp_path: str) -> None:
    """End-to-end: decode → write_zarr → re-open via xarray. The full
    materialisation path that ``materialise_mrms_file`` exercises in
    production, minus the S3 download (which is what the fixture already did)
    and minus the Postgres upsert (which is well-covered by
    ``test_mrms_grids.py``)."""
    da = decode_grib2_to_dataarray(grib_payload)
    locator = write_dataarray_to_zarr(
        da,
        target_root=tmp_path,
        file_key="grib_payload_fixture.grib2.gz",
    )

    reopened = xr.open_zarr(locator.zarr_uri)
    assert locator.variable in reopened.data_vars
    assert tuple(reopened[locator.variable].shape) == locator.shape
    assert str(reopened[locator.variable].dtype) == locator.dtype
