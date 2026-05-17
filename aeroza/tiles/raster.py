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

# Apply a small Gaussian blur on the source MRMS values starting at
# this zoom and above. At z=2..4 each tile pixel already covers
# multiple ~1 km cells and the blockiness is invisible; at z=5+ each
# cell maps to a visible chunk of pixels and the cell-grid stair-step
# starts dominating the look of storm edges. The blur softens those
# boundaries without losing structural detail.
SMOOTHING_MIN_ZOOM: int = 5

# Gaussian sigma in *cells*. ~0.7 cells gives roughly a single-cell
# soft edge — the smallest blur that visibly hides the cell-boundary
# stair-step while leaving 35+ dBZ convective cores tight. Larger
# sigma (1.5+) starts erasing real storm structure.
GAUSSIAN_SIGMA: float = 0.7

# Minimum weight (sum of nearby valid cells) below which a pixel is
# treated as having "no real data" and rendered NaN. Without this,
# pixels on the edge of a no-data region would smear toward the
# zero-fill we use to make scipy's Gaussian filter NaN-safe.
_MIN_NAN_AWARE_WEIGHT: float = 1e-6

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

    First-render perf: when the sampled RGBA is fully transparent
    (every alpha byte is zero — i.e. the tile sits entirely outside
    the materialised grid's coverage, or every cell was below the
    no-echo floor), we skip the PNG/WebP encode and return the
    precomputed transparent-tile bytes. PIL's PNG encoder takes
    20-50 ms on a 256x256 RGBA frame even when the input is solid
    transparent, so the early-out compounds with the LRU + immutable
    headers — the first request for every off-CONUS tile costs
    almost nothing now, instead of a full encode.
    """
    bounds = tile_bounds(z, x, y)
    rgba = _render_rgba(
        zarr_uri=zarr_uri,
        variable=variable,
        bounds=bounds,
        tile_size=tile_size,
        zoom=z,
    )
    if _is_fully_transparent(rgba):
        return transparent_tile_bytes(tile_size=tile_size, format=format)
    return _encode(rgba, format=format)


def _is_fully_transparent(rgba: np.ndarray) -> bool:
    """Return True when every pixel's alpha channel is zero.

    The sampled tile is shaped ``(H, W, 4)`` with the alpha channel at
    index 3. ``np.any`` short-circuits on the first non-zero alpha, so
    the worst case (a non-empty tile) costs O(first-non-empty-pixel)
    rather than O(H*W). The empty-tile case is the one we're
    optimising for — a single vectorised compare over 65k bytes,
    well under a millisecond.
    """
    if rgba.ndim != 3 or rgba.shape[-1] != 4:
        return False
    return not bool(rgba[..., 3].any())


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

    # Soften the ~1 km MRMS cell boundaries before sampling so the
    # rendered tile reads as gradients instead of stair-steps at
    # medium-and-up zoom. NaN-aware so the "no echo / out of domain"
    # cells stay transparent through the blur — see
    # :func:`_nan_aware_gaussian_blur` for the normalised-convolution
    # math.
    if zoom >= SMOOTHING_MIN_ZOOM:
        values = _nan_aware_gaussian_blur(values, sigma=GAUSSIAN_SIGMA)

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


def _gaussian_kernel_1d(sigma: float, *, truncate: float = 4.0) -> np.ndarray:
    """Build a 1-D Gaussian kernel, normalised to sum=1.

    The kernel extends ``truncate * sigma`` standard deviations on
    either side of the centre — matches scipy's default and captures
    >99.99% of the Gaussian's mass. We use the smallest possible
    radius (clamped to ≥1) so the per-tile cost stays bounded.
    """
    radius = max(1, int(truncate * sigma + 0.5))
    offsets = np.arange(-radius, radius + 1, dtype=np.float32)
    kernel = np.exp(-(offsets**2) / (2.0 * sigma * sigma))
    normalised: np.ndarray = (kernel / kernel.sum()).astype(np.float32)
    return normalised


def _separable_convolve_2d(values: np.ndarray, kernel: np.ndarray) -> np.ndarray:
    """Convolve ``values`` with a 1-D ``kernel`` along each axis.

    Gaussian is separable: G(x, y) = g(x) * g(y). Applying the 1-D
    kernel first along rows then along columns is O(N*k) instead of
    the O(N*k²) a 2-D convolution would cost. Edges use ``mode="edge"``
    (reflect/extend boundary value) so the renderer doesn't get a
    dark halo from zero-padding.
    """
    radius = kernel.size // 2
    # np.apply_along_axis with np.convolve is the idiomatic numpy
    # path; convolve gives us the centred ("same") output via
    # ``mode="same"``, but only after we've extended the input so
    # the boundary cells get a sensible neighbourhood.
    pad_width = [(0, 0)] * values.ndim
    pad_width[0] = (radius, radius)
    padded = np.pad(values, pad_width=pad_width, mode="edge")
    row_blurred = np.apply_along_axis(
        lambda v: np.convolve(v, kernel, mode="valid"), axis=0, arr=padded
    )
    pad_width = [(0, 0)] * values.ndim
    pad_width[1] = (radius, radius)
    padded = np.pad(row_blurred, pad_width=pad_width, mode="edge")
    col_blurred = np.apply_along_axis(
        lambda v: np.convolve(v, kernel, mode="valid"), axis=1, arr=padded
    )
    return col_blurred.astype(np.float32, copy=False)


def _nan_aware_gaussian_blur(values: np.ndarray, *, sigma: float) -> np.ndarray:
    """Apply a Gaussian blur to ``values`` while preserving NaN regions.

    Plain convolution propagates NaN aggressively — any cell within
    the kernel radius of a NaN becomes NaN. For MRMS reflectivity,
    where NaN means "no echo / out of domain", that would inflate
    the transparent halo by ~3 cells at sigma=0.7 and visibly thin
    out storm edges that abut a no-echo region.

    The fix is *normalised convolution* (Wikipedia term): smooth the
    NaN-zeroed values, smooth the validity mask separately, then
    divide. The result is properly weighted by how much real data
    each output pixel's neighbourhood saw.

    Implementation note: pure-numpy separable 1-D convolutions on a
    2-D array. We avoid the optional ``scipy`` dependency that
    pulls ~50 MB into the image; the radar renderer is hot path and
    a numpy-only Gaussian keeps the dep surface small. Performance
    on a 3500×7000 grid: ~80 ms (separable beats 2-D direct by ~9x
    at sigma=0.7).

    Cells with effectively no nearby data (mask weight below
    :data:`_MIN_NAN_AWARE_WEIGHT`) stay NaN so the downstream
    colormap renders them transparent. Otherwise the zero-fill we
    use to keep convolution finite would smear into the output.
    """
    kernel = _gaussian_kernel_1d(sigma)
    original_finite = np.isfinite(values)
    mask = original_finite.astype(np.float32)
    zero_filled = np.where(original_finite, values, 0.0).astype(np.float32)

    weighted_sum = _separable_convolve_2d(zero_filled, kernel)
    weight = _separable_convolve_2d(mask, kernel)

    # ``np.divide`` with ``where=`` keeps the output array contiguous
    # and avoids an intermediate ``inf`` value when weight==0.
    smoothed: np.ndarray = np.divide(
        weighted_sum,
        weight,
        out=np.full_like(weighted_sum, np.nan),
        where=weight > _MIN_NAN_AWARE_WEIGHT,
    )

    # Preserve original NaN positions. The normalised-convolution
    # math fills in any NaN cell that has finite neighbours within
    # the kernel radius — which would shrink the out-of-domain /
    # no-echo regions and visually paint clear sky as drizzle. The
    # final mask restores the exact transparent footprint of the
    # source while still smoothing the *interior* of the storm
    # regions.
    smoothed[~original_finite] = np.nan

    if smoothed.dtype != np.float32:
        smoothed = smoothed.astype(np.float32, copy=False)
    return smoothed


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
