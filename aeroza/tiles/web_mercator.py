"""Web Mercator (EPSG:3857) ↔ WGS84 (EPSG:4326) tile math.

Pure functions over plain floats / numpy arrays — kept separate from the
rendering pipeline so they're cheap to unit-test without xarray or PIL.

The convention follows OSM / TMS / XYZ:
    n = 2 ** z       (number of tiles at zoom z, per axis)
    x ∈ [0, n)       (column, west → east)
    y ∈ [0, n)       (row, north → south)

Latitude is bounded by ``±MAX_LATITUDE`` (the standard Web Mercator clip)
so the tile grid stays square.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Final

import numpy as np

# Web Mercator stops short of the geographic poles; this is the standard
# clipping latitude (~85.05113°), the value of `atan(sinh(π))`.
MAX_LATITUDE: Final[float] = 85.0511287798066
TILE_SIZE: Final[int] = 256


@dataclass(frozen=True, slots=True)
class TileBounds:
    """Geographic bounds of a single XYZ tile.

    ``lng_min`` / ``lng_max`` are in ``[-180, 180]``. ``lat_min`` / ``lat_max``
    are in ``[-MAX_LATITUDE, MAX_LATITUDE]``. ``lat_max`` is the *northern*
    edge (top of the tile) and ``lat_min`` the southern edge.
    """

    z: int
    x: int
    y: int
    lng_min: float
    lng_max: float
    lat_min: float
    lat_max: float


def tile_bounds(z: int, x: int, y: int) -> TileBounds:
    """Return the geographic bounds of tile ``(z, x, y)``.

    Raises ``ValueError`` if the tile coordinates are outside the
    zoom-level grid.
    """
    n = 1 << z
    if not (0 <= x < n and 0 <= y < n):
        raise ValueError(f"tile out of range for z={z}: x={x}, y={y} (n={n})")

    lng_min = x / n * 360.0 - 180.0
    lng_max = (x + 1) / n * 360.0 - 180.0
    lat_max = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * y / n))))
    lat_min = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / n))))
    return TileBounds(
        z=z,
        x=x,
        y=y,
        lng_min=lng_min,
        lng_max=lng_max,
        lat_min=lat_min,
        lat_max=lat_max,
    )


def pixel_lonlat_grid(
    bounds: TileBounds, *, tile_size: int = TILE_SIZE
) -> tuple[np.ndarray, np.ndarray]:
    """Return per-pixel (longitude, latitude) arrays for a tile.

    Coordinates are computed at the *center* of each pixel — the standard
    XYZ-tile convention. Output shape is ``(tile_size, tile_size)`` for
    each array.

    The longitude grid is constant per row (linear in pixel-x). Latitude
    requires the inverse mercator transform, so we compute it once per
    row and broadcast.
    """
    if tile_size <= 0:
        raise ValueError(f"tile_size must be positive, got {tile_size}")

    n = 1 << bounds.z
    # Pixel centers in the tile (half-pixel offset).
    px = np.arange(tile_size) + 0.5
    py = np.arange(tile_size) + 0.5

    # Mercator pixel coordinates inside the world raster.
    world_x = bounds.x * tile_size + px
    world_y = bounds.y * tile_size + py

    lng = world_x / (n * tile_size) * 360.0 - 180.0
    # Standard slippy-tile inverse: y = atan(sinh(π · (1 - 2·world_y/(n·tile_size))))
    yfrac = world_y / (n * tile_size)
    lat = np.degrees(np.arctan(np.sinh(math.pi * (1 - 2 * yfrac))))

    # Broadcast to (H, W): row i shares latitude `lat[i]`; column j shares
    # longitude `lng[j]`. Resulting arrays are (tile_size, tile_size).
    lng_grid = np.broadcast_to(lng[None, :], (tile_size, tile_size)).copy()
    lat_grid = np.broadcast_to(lat[:, None], (tile_size, tile_size)).copy()
    return lng_grid, lat_grid


def latlng_to_pixel_indices(
    *,
    lats: np.ndarray,
    lngs: np.ndarray,
    grid_lats: np.ndarray,
    grid_lngs: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Map an array of (lat, lng) pixel coordinates to grid indices via
    nearest-neighbor lookup along a regular axis.

    The MRMS grid is regular in lat/lng, so the index for each pixel is
    just ``round((value - axis[0]) / step)``. This avoids a 2D KD-tree
    and is ~constant-time per pixel.

    Returns ``(row_idx, col_idx, in_bounds_mask)``. Pixels outside the
    grid extent get a mask=False; callers should NaN those before
    indexing.
    """
    lat_step = float(grid_lats[1] - grid_lats[0])
    lng_step = float(grid_lngs[1] - grid_lngs[0])
    lat0 = float(grid_lats[0])
    lng0 = float(grid_lngs[0])
    h = grid_lats.shape[0]
    w = grid_lngs.shape[0]

    # Snap pixel center to the nearest grid index.
    row = np.round((lats - lat0) / lat_step).astype(np.int64)
    col = np.round((lngs - lng0) / lng_step).astype(np.int64)

    in_bounds = (row >= 0) & (row < h) & (col >= 0) & (col < w)
    # Clip out-of-bounds indices so the gather is safe; we mask their
    # contribution to alpha=0 below.
    row = np.clip(row, 0, max(h - 1, 0))
    col = np.clip(col, 0, max(w - 1, 0))
    return row, col, in_bounds


def latlng_to_bilinear_indices(
    *,
    lats: np.ndarray,
    lngs: np.ndarray,
    grid_lats: np.ndarray,
    grid_lngs: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Bilinear lookup: each pixel resolves to the four enclosing grid
    corners plus row/col interpolation weights.

    Returns ``(row_lo, row_hi, col_lo, col_hi, w_row, w_col, in_bounds)``.
    Use as::

        v00 = values[row_lo, col_lo]
        v01 = values[row_lo, col_hi]
        v10 = values[row_hi, col_lo]
        v11 = values[row_hi, col_hi]
        out = (
            v00 * (1 - w_row) * (1 - w_col)
            + v01 * (1 - w_row) * w_col
            + v10 * w_row * (1 - w_col)
            + v11 * w_row * w_col
        )

    NaN propagation is the caller's job: any of the four corners being
    NaN poisons the result. That's correct — a pixel sitting on the edge
    of a missing region shouldn't paint a confident colour.

    The ``in_bounds`` mask is True only for pixels whose nearest enclosing
    cell is fully inside the grid. Pixels outside or on the wrong side of
    the boundary get clipped indices but should be treated as transparent.
    """
    lat_step = float(grid_lats[1] - grid_lats[0])
    lng_step = float(grid_lngs[1] - grid_lngs[0])
    lat0 = float(grid_lats[0])
    lng0 = float(grid_lngs[0])
    h = grid_lats.shape[0]
    w = grid_lngs.shape[0]

    fr = (lats - lat0) / lat_step
    fc = (lngs - lng0) / lng_step

    row_lo = np.floor(fr).astype(np.int64)
    col_lo = np.floor(fc).astype(np.int64)
    w_row = (fr - row_lo).astype(np.float64)
    w_col = (fc - col_lo).astype(np.float64)
    row_hi = row_lo + 1
    col_hi = col_lo + 1

    # In-bounds requires the FULL 2x2 stencil to live inside the grid.
    in_bounds = (row_lo >= 0) & (row_hi < h) & (col_lo >= 0) & (col_hi < w)

    # Clamp so the gather never hits a negative/oversized index. The
    # mask gates the contribution.
    row_lo = np.clip(row_lo, 0, max(h - 1, 0))
    row_hi = np.clip(row_hi, 0, max(h - 1, 0))
    col_lo = np.clip(col_lo, 0, max(w - 1, 0))
    col_hi = np.clip(col_hi, 0, max(w - 1, 0))
    return row_lo, row_hi, col_lo, col_hi, w_row, w_col, in_bounds


__all__ = [
    "MAX_LATITUDE",
    "TILE_SIZE",
    "TileBounds",
    "latlng_to_bilinear_indices",
    "latlng_to_pixel_indices",
    "pixel_lonlat_grid",
    "tile_bounds",
]
