"""Region-slicing in ``_render_rgba`` must be byte-identical to a full-grid load.

``_render_rgba`` now loads only the tile's lat/lng window (+ margin) rather than
the whole grid. These tests assert that yields RGBA byte-for-byte identical to
the old full-load path across the zoom thresholds (z>=4 bilinear, z>=5 blur),
for both descending (MRMS-native) and ascending latitude axes — and that the
sliced path (not just the full-grid fallback) is actually exercised.
"""

from __future__ import annotations

import math
from pathlib import Path

import numpy as np
import xarray as xr

from aeroza.tiles.raster import (
    _render_rgba,
    _sample_into_tile,
    _tile_index_window,
    load_grid,
    render_tile_from_loaded_grid,
    render_tile_image,
)
from aeroza.tiles.web_mercator import tile_bounds

_VAR = "reflectivity"


def _write_grid(path: Path, *, descending_lat: bool) -> str:
    # CONUS-ish and fine enough that high-zoom tiles slice a strict sub-region.
    lat = np.linspace(50.0, 24.0, 260) if descending_lat else np.linspace(24.0, 50.0, 260)
    lng = np.linspace(-126.0, -66.0, 600)
    yy, xx = np.meshgrid(lat, lng, indexing="ij")
    # Spatially-varying field with NaN gaps so the blur + alpha edges matter.
    values = (45.0 * np.sin(np.radians(xx * 3.0)) * np.cos(np.radians(yy * 3.0))).astype(np.float32)
    values[values < 8.0] = np.nan
    da = xr.DataArray(
        values,
        coords={"latitude": lat, "longitude": lng},
        dims=("latitude", "longitude"),
        name=_VAR,
    )
    da.to_zarr(str(path), mode="w")
    return str(path)


def _tile_for(lat: float, lng: float, z: int) -> tuple[int, int, int]:
    n = 1 << z
    x = int((lng + 180.0) / 360.0 * n)
    y = int((1.0 - math.asinh(math.tan(math.radians(lat))) / math.pi) / 2.0 * n)
    return z, x, y


def _reference(uri: str, z: int, x: int, y: int) -> np.ndarray:
    ds = xr.open_zarr(uri)
    try:
        full = ds[_VAR].load()
        return _sample_into_tile(full, bounds=tile_bounds(z, x, y), tile_size=256, zoom=z)
    finally:
        ds.close()


# Tiles centred on the CONUS grid across the zoom thresholds (z=2 falls back to
# the full grid; z>=5 slices a strict sub-region + applies the blur).
_TILES = [_tile_for(38.0, -96.0, z) for z in (2, 4, 5, 6, 7, 8)]


def _check(uri: str) -> tuple[bool, bool]:
    sliced = False
    opaque = False
    for z, x, y in _TILES:
        bounds = tile_bounds(z, x, y)
        ds = xr.open_zarr(uri)
        try:
            if _tile_index_window(ds[_VAR], bounds=bounds) is not None:
                sliced = True
        finally:
            ds.close()
        actual = _render_rgba(zarr_uri=uri, variable=_VAR, bounds=bounds, tile_size=256, zoom=z)
        reference = _reference(uri, z, x, y)
        assert np.array_equal(actual, reference), f"RGBA mismatch at z{z}/{x}/{y}"
        if bool((reference[..., 3] > 0).any()):
            opaque = True
    return sliced, opaque


def test_region_sampling_matches_full_load_descending_lat(tmp_path: Path) -> None:
    sliced, opaque = _check(_write_grid(tmp_path / "g.zarr", descending_lat=True))
    assert sliced, "no tile exercised the sliced path"
    assert opaque, "no tile produced opaque pixels — the comparison was trivial"


def test_region_sampling_matches_full_load_ascending_lat(tmp_path: Path) -> None:
    sliced, _ = _check(_write_grid(tmp_path / "g.zarr", descending_lat=False))
    assert sliced, "no tile exercised the sliced path"


def test_load_once_render_matches_per_tile_render(tmp_path: Path) -> None:
    """The prewarm load-once path (``render_tile_from_loaded_grid`` over a
    grid loaded once by ``load_grid``) must be byte-for-byte identical to the
    on-demand per-tile path (``render_tile_image``, which re-opens the store
    each call) — same encoded bytes, transparent or not, across zooms and
    formats. This is what lets prewarm trade ~1500 store-opens for one load
    without changing a single output pixel."""
    uri = _write_grid(tmp_path / "g.zarr", descending_lat=True)
    grid = load_grid(uri, _VAR)

    distinct: set[bytes] = set()
    for z, x, y in (_tile_for(38.0, -96.0, z) for z in (2, 4, 5, 6, 7, 8)):
        for fmt in ("webp", "png"):
            once = render_tile_from_loaded_grid(grid, z=z, x=x, y=y, format=fmt)
            per_tile = render_tile_image(zarr_uri=uri, variable=_VAR, z=z, x=x, y=y, format=fmt)
            assert once == per_tile, f"load-once != per-tile at z{z}/{x}/{y}.{fmt}"
            distinct.add(once)

    # >1 distinct payload ⇒ we compared real (non-empty) tiles, not just
    # identical all-transparent frames that would pass trivially.
    assert len(distinct) > 1
