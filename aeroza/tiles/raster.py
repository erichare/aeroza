"""Render a single XYZ raster tile from a Zarr-backed MRMS grid.

The pipeline:

1. Open the Zarr store (eagerly — the grids are <50MB and we read them
   end-to-end anyway for the tile sample).
2. Compute the tile's pixel-center (lat, lng) grid.
3. Translate every pixel's longitude into the grid's native convention
   (MRMS uses ``[0, 360)``, slippy tiles use ``[-180, 180]``).
4. Nearest-neighbor lookup against the grid's regular axes.
5. Apply the dBZ colormap and PNG-encode.

Cells outside the grid extent become alpha=0, so a tile over Mexico
returns transparent without the caller having to know the CONUS bounds.
"""

from __future__ import annotations

import io
from typing import TYPE_CHECKING, Literal

import numpy as np

from aeroza.tiles.colormap import reflectivity_to_rgba
from aeroza.tiles.web_mercator import (
    TILE_SIZE,
    TileBounds,
    latlng_to_bilinear_indices,
    latlng_to_pixel_indices,
    pixel_lonlat_grid,
    tile_bounds,
)

# Output formats the renderer supports. WebP is content-negotiated
# via the ``Accept`` header in the route layer; PNG is the default
# fallback for older browsers and curl-style clients. WebP lossless
# is ~30-40% smaller than the equivalent PNG for the dBZ ramp's
# discrete colours, and Pillow encodes it ~2x faster than the default
# PNG codec — both wins compound on top of the per-tile LRU.
TileFormat = Literal["png", "webp"]
DEFAULT_TILE_FORMAT: TileFormat = "png"

# MIME types per format. The route layer maps both directions
# (Accept → format, format → Content-Type), so keep the table here.
TILE_FORMAT_CONTENT_TYPE: dict[TileFormat, str] = {
    "png": "image/png",
    "webp": "image/webp",
}

# Switch to bilinear sampling at this zoom and above. Below it, each tile
# pixel covers many native MRMS cells and nearest-neighbor produces an
# identical-looking but cheaper result. Above it, tile pixels span the
# space between cells, and bilinear gives the soft edges users expect
# from a modern radar viewer.
BILINEAR_MIN_ZOOM: int = 4

if TYPE_CHECKING:  # pragma: no cover - typing only
    import xarray as xr


def render_tile_png(
    *,
    zarr_uri: str,
    variable: str,
    z: int,
    x: int,
    y: int,
    tile_size: int = TILE_SIZE,
) -> bytes:
    """Render tile ``(z, x, y)`` of the grid stored at ``zarr_uri`` as PNG.

    Backwards-compat alias for :func:`render_tile_image` with
    ``format="png"``. Existing tests + the cache LRU's first
    iteration call this; new callers should prefer
    :func:`render_tile_image` so they can opt into WebP.
    """
    return render_tile_image(
        zarr_uri=zarr_uri,
        variable=variable,
        z=z,
        x=x,
        y=y,
        tile_size=tile_size,
        format="png",
    )


def render_tile_image(
    *,
    zarr_uri: str,
    variable: str,
    z: int,
    x: int,
    y: int,
    tile_size: int = TILE_SIZE,
    format: TileFormat = DEFAULT_TILE_FORMAT,
) -> bytes:
    """Render tile ``(z, x, y)`` of the grid stored at ``zarr_uri``.

    Returns the encoded image bytes in the requested ``format``.
    Empty / out-of-domain tiles are returned as a fully transparent
    image so the client doesn't have to handle 404s on every miss.
    """
    bounds = tile_bounds(z, x, y)
    rgba = _render_rgba(
        zarr_uri=zarr_uri,
        variable=variable,
        bounds=bounds,
        tile_size=tile_size,
        zoom=z,
    )
    return _encode(rgba, format=format)


def _render_rgba(
    *,
    zarr_uri: str,
    variable: str,
    bounds: TileBounds,
    tile_size: int,
    zoom: int,
) -> np.ndarray:
    """Sample the grid into a tile-shaped RGBA array."""
    import xarray as xr

    ds = xr.open_zarr(zarr_uri)
    try:
        if variable not in ds.variables:
            raise KeyError(f"variable {variable!r} not in store {zarr_uri}")
        da = ds[variable].load()
        return _sample_into_tile(da, bounds=bounds, tile_size=tile_size, zoom=zoom)
    finally:
        ds.close()


def _sample_into_tile(
    da: xr.DataArray,
    *,
    bounds: TileBounds,
    tile_size: int,
    zoom: int,
) -> np.ndarray:
    """Project the grid into the tile's pixel grid.

    Uses nearest-neighbor at coarse zoom (where each pixel already covers
    many native cells) and bilinear at finer zoom (where pixels fall
    between cells and bilinear gives smooth, modern-looking edges).
    """
    grid_lats = np.asarray(da["latitude"].values, dtype=np.float64)
    grid_lngs = np.asarray(da["longitude"].values, dtype=np.float64)
    values = np.asarray(da.values, dtype=np.float32)

    if values.ndim != 2:
        raise ValueError(f"expected 2D grid, got dims={da.dims} shape={values.shape}")

    # MRMS axes typically descend (north→south for latitude). The
    # nearest-neighbor index math expects ascending; if descending,
    # flip the array + axis once and treat as ascending. Cheap copy on
    # an already-loaded array.
    if grid_lats.size >= 2 and grid_lats[1] < grid_lats[0]:
        grid_lats = grid_lats[::-1]
        values = values[::-1, :]
    if grid_lngs.size >= 2 and grid_lngs[1] < grid_lngs[0]:
        grid_lngs = grid_lngs[::-1]
        values = values[:, ::-1]

    # Tile pixel centers in WGS84.
    lng_grid, lat_grid = pixel_lonlat_grid(bounds, tile_size=tile_size)

    # Translate request longitudes into the grid's convention.
    lng_grid = _to_grid_longitude(lng_grid, grid_lngs)

    if zoom >= BILINEAR_MIN_ZOOM:
        sampled = _bilinear_sample(
            values=values,
            grid_lats=grid_lats,
            grid_lngs=grid_lngs,
            lats=lat_grid,
            lngs=lng_grid,
        )
    else:
        rows, cols, in_bounds = latlng_to_pixel_indices(
            lats=lat_grid,
            lngs=lng_grid,
            grid_lats=grid_lats,
            grid_lngs=grid_lngs,
        )
        sampled = values[rows, cols].astype(np.float32)
        sampled = np.where(in_bounds, sampled, np.nan)
    return reflectivity_to_rgba(sampled)


def _bilinear_sample(
    *,
    values: np.ndarray,
    grid_lats: np.ndarray,
    grid_lngs: np.ndarray,
    lats: np.ndarray,
    lngs: np.ndarray,
) -> np.ndarray:
    """Bilinear sample of ``values`` at each (lat, lng) tile pixel.

    NaN propagates: any of the four corners being NaN makes the
    interpolated cell NaN, which the colormap renders transparent. That
    is the right behaviour for a pixel sitting on the edge of a missing
    region — we'd rather show a hole than a confidently wrong colour.
    """
    row_lo, row_hi, col_lo, col_hi, w_row, w_col, in_bounds = latlng_to_bilinear_indices(
        lats=lats,
        lngs=lngs,
        grid_lats=grid_lats,
        grid_lngs=grid_lngs,
    )

    v00 = values[row_lo, col_lo].astype(np.float32)
    v01 = values[row_lo, col_hi].astype(np.float32)
    v10 = values[row_hi, col_lo].astype(np.float32)
    v11 = values[row_hi, col_hi].astype(np.float32)

    one_minus_w_row = (1.0 - w_row).astype(np.float32)
    one_minus_w_col = (1.0 - w_col).astype(np.float32)
    w_row_f32 = w_row.astype(np.float32)
    w_col_f32 = w_col.astype(np.float32)

    # NaN-safe weighted blend. Any NaN in v** poisons the corresponding
    # weighted product; the np.where below masks the result back to NaN
    # rather than letting the float arithmetic silently zero it out.
    sampled = (
        v00 * one_minus_w_row * one_minus_w_col
        + v01 * one_minus_w_row * w_col_f32
        + v10 * w_row_f32 * one_minus_w_col
        + v11 * w_row_f32 * w_col_f32
    )
    has_nan = np.isnan(v00) | np.isnan(v01) | np.isnan(v10) | np.isnan(v11)
    sampled = np.where(has_nan, np.float32(np.nan), sampled)
    sampled = np.where(in_bounds, sampled, np.float32(np.nan))
    return sampled


def _to_grid_longitude(lngs: np.ndarray, grid_lngs: np.ndarray) -> np.ndarray:
    """If the grid uses ``[0, 360)`` longitudes (MRMS native), shift
    negative request longitudes into that range. Otherwise pass through.

    A grid is considered native-360 if its max longitude exceeds 180.
    """
    if grid_lngs.size == 0:
        return lngs
    if float(grid_lngs.max()) <= 180.0:
        return lngs
    out = lngs.copy()
    out[out < 0.0] += 360.0
    return out


def _encode(rgba: np.ndarray, *, format: TileFormat) -> bytes:
    """RGBA uint8 → encoded image bytes via Pillow.

    PNG is encoded with ``optimize=False`` (default zlib level) — the
    LRU + immutable cache headers absorb the size cost and the
    encoder is the cold-render bottleneck. WebP uses ``lossless=True``
    so the dBZ ramp's discrete bands stay sharp; Pillow encodes
    lossless WebP ~2x faster than PNG and the bytes are ~30-40%
    smaller, so both first-render and bytes-on-the-wire benefit.
    """
    from PIL import Image

    if rgba.dtype != np.uint8:
        rgba = rgba.astype(np.uint8)
    img = Image.fromarray(rgba, mode="RGBA")
    buf = io.BytesIO()
    if format == "webp":
        # ``method=4`` is the WebP encoder's middle setting — modest
        # compression effort, predictable latency. Lossless preserves
        # the discrete colour bands; lossy would smear the dBZ ramp.
        img.save(buf, format="WEBP", lossless=True, method=4)
    else:
        img.save(buf, format="PNG", optimize=False)
    return buf.getvalue()


def transparent_tile_png(*, tile_size: int = TILE_SIZE) -> bytes:
    """Return a fully-transparent PNG of the requested size.

    Backwards-compat alias for :func:`transparent_tile_bytes` with
    ``format="png"``.
    """
    return transparent_tile_bytes(tile_size=tile_size, format="png")


def transparent_tile_bytes(
    *,
    tile_size: int = TILE_SIZE,
    format: TileFormat = DEFAULT_TILE_FORMAT,
) -> bytes:
    """Return a fully-transparent tile in the requested format.

    Used as the fallback when a grid is unavailable — keeps the
    MapLibre raster source from spamming 404s. Both formats encode
    identical pixel data; only the byte stream differs.
    """
    rgba = np.zeros((tile_size, tile_size, 4), dtype=np.uint8)
    return _encode(rgba, format=format)


__all__ = [
    "DEFAULT_TILE_FORMAT",
    "TILE_FORMAT_CONTENT_TYPE",
    "TileFormat",
    "render_tile_image",
    "render_tile_png",
    "transparent_tile_bytes",
    "transparent_tile_png",
]
