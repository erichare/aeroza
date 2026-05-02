"""Reflectivity (dBZ) → RGBA colormap.

The ramp is a faithful copy of the NWS standard reflectivity scale used
by every US weather radar viewer most users have ever seen — light blues
for drizzle, greens for moderate rain, yellow/orange for heavy rain, red
for severe, magenta for hail/extreme. Cells below the threshold (or NaN)
return fully transparent so the basemap shows through.

Pure numpy — no PIL, no xarray. The renderer module composes this with
the tile-pixel grid.
"""

from __future__ import annotations

from typing import Final

import numpy as np

# (dBZ stop, R, G, B) — sorted ascending. The last stop is held flat for
# anything stronger.
DBZ_STOPS: Final[tuple[tuple[float, int, int, int], ...]] = (
    (5.0, 4, 233, 231),  # < 5 → transparent (handled separately); 5 dBZ first visible
    (10.0, 1, 159, 244),
    (15.0, 3, 0, 244),
    (20.0, 2, 253, 2),
    (25.0, 1, 197, 1),
    (30.0, 0, 142, 0),
    (35.0, 253, 248, 2),
    (40.0, 229, 188, 0),
    (45.0, 253, 149, 0),
    (50.0, 253, 0, 0),
    (55.0, 212, 0, 0),
    (60.0, 188, 0, 0),
    (65.0, 248, 0, 253),
    (70.0, 152, 84, 198),
)

# Below this dBZ value, cells are rendered fully transparent. The NWS
# convention treats <5 dBZ as "no echo" / clear air. NaN values are also
# transparent regardless.
TRANSPARENT_BELOW_DBZ: Final[float] = 5.0

# Faint reflectivity ramps its opacity linearly between ``TRANSPARENT_BELOW_DBZ``
# (alpha=ALPHA_MIN) and ``ALPHA_FULL_DBZ`` (alpha=ALPHA_MAX). Above
# ``ALPHA_FULL_DBZ`` the opacity is constant. This gives the storm a
# soft outer edge rather than the hard cliff a constant alpha leaves at
# the threshold — and lets the basemap (now with state borders!) read
# through light precip.
ALPHA_FULL_DBZ: Final[float] = 25.0
ALPHA_MIN: Final[int] = 90
ALPHA_MAX: Final[int] = 215


def reflectivity_to_rgba(values: np.ndarray) -> np.ndarray:
    """Map a 2D dBZ array to an ``(H, W, 4)`` uint8 RGBA tile.

    NaN cells and cells below :data:`TRANSPARENT_BELOW_DBZ` are written
    as alpha=0 (fully transparent). Cells in-range get a linearly
    interpolated colour between the bracketing stops; cells above the
    last stop get the strongest colour.
    """
    if values.ndim != 2:
        raise ValueError(f"expected 2D array, got shape {values.shape}")

    h, w = values.shape
    rgba = np.zeros((h, w, 4), dtype=np.uint8)

    # Mask out NaN + below-threshold cells.
    finite = np.isfinite(values)
    visible = finite & (values >= TRANSPARENT_BELOW_DBZ)

    if not visible.any():
        return rgba

    stops = np.asarray([s[0] for s in DBZ_STOPS], dtype=np.float64)
    rgb = np.asarray([s[1:] for s in DBZ_STOPS], dtype=np.float64)

    # Restrict work to visible pixels for speed; flatten then unflatten.
    v = values[visible].astype(np.float64)

    # For each value, find the bracketing stop index.
    idx = np.searchsorted(stops, v, side="right") - 1
    idx = np.clip(idx, 0, len(stops) - 2)

    lower_v = stops[idx]
    upper_v = stops[idx + 1]
    span = upper_v - lower_v
    # Avoid div-by-zero when consecutive stops happen to coincide.
    span = np.where(span == 0, 1.0, span)
    t = np.clip((v - lower_v) / span, 0.0, 1.0)

    lower_rgb = rgb[idx]
    upper_rgb = rgb[idx + 1]
    interp = lower_rgb + (upper_rgb - lower_rgb) * t[:, None]
    interp_u8 = np.clip(interp, 0, 255).astype(np.uint8)

    # Above the last stop → flat top colour.
    above = v >= stops[-1]
    interp_u8[above] = rgb[-1].astype(np.uint8)

    flat = rgba.reshape(-1, 4)
    visible_flat = visible.reshape(-1)
    flat[visible_flat, 0:3] = interp_u8
    # Faint cells (just above the transparent threshold) ramp their alpha
    # in linearly so the storm has a soft outer edge instead of a hard
    # cliff at 5 dBZ. Above ALPHA_FULL_DBZ the opacity is constant.
    fade_lo = TRANSPARENT_BELOW_DBZ
    fade_hi = ALPHA_FULL_DBZ
    fade_t = np.clip((v - fade_lo) / max(fade_hi - fade_lo, 1e-9), 0.0, 1.0)
    alpha = (ALPHA_MIN + (ALPHA_MAX - ALPHA_MIN) * fade_t).astype(np.uint8)
    flat[visible_flat, 3] = alpha
    return rgba


__all__ = [
    "DBZ_STOPS",
    "TRANSPARENT_BELOW_DBZ",
    "reflectivity_to_rgba",
]
