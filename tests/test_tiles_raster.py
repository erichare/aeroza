"""Unit tests for the raster renderer's smoothing helper.

We avoid the full ``render_tile_image`` pipeline here (it wants a Zarr
store on disk; integration coverage lives in :mod:`test_v1_mrms_tiles`).
The tests below pin the contract of :func:`_nan_aware_gaussian_blur`
since the visual quality of the deployed radar depends on it:

* The blur softens cell boundaries (output values differ from input
  in regions of strong gradient).
* NaN regions (no-echo / out-of-domain MRMS cells) do *not* grow —
  the transparent halo at storm edges stays exactly where it was.
* Determinism: same input bytes → same output bytes, so the per-tile
  LRU + immutable R2 cache headers still hold.
"""

from __future__ import annotations

import numpy as np
import pytest

from aeroza.tiles.raster import (
    GAUSSIAN_SIGMA,
    SMOOTHING_MIN_ZOOM,
    _nan_aware_gaussian_blur,
)

pytestmark = pytest.mark.unit


def test_blur_softens_a_sharp_step() -> None:
    """A 0→50 dBZ step edge should come out softer (intermediate
    values appear near the boundary). This is the visible win on
    storm edges at z=5+ where the cell-grid stair-step was dominant."""
    values = np.zeros((32, 32), dtype=np.float32)
    values[:, 16:] = 50.0  # vertical step in the middle

    smoothed = _nan_aware_gaussian_blur(values, sigma=GAUSSIAN_SIGMA)

    # On the immediate edge, plain zero and plain 50 should now blend
    # into intermediate values.
    left_of_edge = smoothed[16, 15]
    right_of_edge = smoothed[16, 16]
    # Both pixels straddling the step move toward the midpoint —
    # exact values depend on sigma but they must land strictly
    # inside (0, 50).
    assert 0.0 < float(left_of_edge) < 50.0
    assert 0.0 < float(right_of_edge) < 50.0
    # Far from the edge the data should be essentially untouched.
    assert smoothed[16, 0] == pytest.approx(0.0, abs=1e-3)
    assert smoothed[16, 31] == pytest.approx(50.0, abs=1e-3)


def test_blur_preserves_nan_regions() -> None:
    """No-echo / out-of-domain cells (``NaN``) stay ``NaN`` after the
    blur. Plain ``scipy.ndimage.gaussian_filter`` would inflate the
    NaN region by ~3 cells at sigma=0.7; the normalised-convolution
    in our helper keeps the transparent halo exactly where it was."""
    values = np.full((32, 32), 30.0, dtype=np.float32)
    # Carve out a 4x4 NaN region in the middle.
    values[12:16, 12:16] = np.nan

    smoothed = _nan_aware_gaussian_blur(values, sigma=GAUSSIAN_SIGMA)

    # The original NaN cells stay NaN — the no-echo signal is
    # preserved in the colormap output.
    assert np.all(np.isnan(smoothed[12:16, 12:16]))
    # Cells adjacent to the NaN region carry data (they have valid
    # neighbours within the kernel radius). They are *not* the
    # original 30.0 (the average over a partial neighbourhood pulls
    # them slightly toward 0), but they must be finite.
    border = smoothed[11, 12:16]
    assert np.all(np.isfinite(border))
    assert float(border.min()) > 0.0


def test_blur_is_deterministic() -> None:
    """Same input array → byte-identical output. The R2 + browser
    cache layer relies on tiles being content-addressed by
    ``(fileKey, z, x, y, format)``; non-determinism here would break
    the ``Cache-Control: immutable`` promise on the wire."""
    rng = np.random.default_rng(seed=42)
    values = rng.uniform(0.0, 60.0, size=(64, 64)).astype(np.float32)

    a = _nan_aware_gaussian_blur(values, sigma=GAUSSIAN_SIGMA)
    b = _nan_aware_gaussian_blur(values, sigma=GAUSSIAN_SIGMA)

    np.testing.assert_array_equal(a, b)


def test_blur_returns_float32() -> None:
    """Downstream colormap operates on float32. If we returned
    float64 the renderer would silently up-cast and waste memory
    (2x for the rendered tile) without any benefit."""
    values = np.full((16, 16), 25.0, dtype=np.float32)
    smoothed = _nan_aware_gaussian_blur(values, sigma=GAUSSIAN_SIGMA)
    assert smoothed.dtype == np.float32


def test_blur_handles_all_nan_input() -> None:
    """A tile entirely off-grid is all-NaN. The output must also be
    all-NaN — without the ``_MIN_NAN_AWARE_WEIGHT`` guard, the
    normalised-convolution divisor (weighted mask) would be zero
    everywhere and produce garbage."""
    values = np.full((16, 16), np.nan, dtype=np.float32)
    smoothed = _nan_aware_gaussian_blur(values, sigma=GAUSSIAN_SIGMA)
    assert np.all(np.isnan(smoothed))


def test_smoothing_threshold_constants_are_sane() -> None:
    """Cheap consistency check on the module-level knobs. If a future
    refactor flips these by accident — e.g. SMOOTHING_MIN_ZOOM=0 on
    a typo — every cold render gets a Gaussian pass even where
    there's nothing to soften."""
    # Smoothing kicks in once a tile pixel covers a fraction of a
    # cell rather than many cells. ``z >= 5`` is the empirically-
    # picked break-even; lower means waste, much higher means we
    # miss the medium-zoom win.
    assert SMOOTHING_MIN_ZOOM >= 4
    # Sigma is in source-cell units. Larger than ~1.5 starts erasing
    # storm structure rather than just hiding cell boundaries.
    assert 0.0 < GAUSSIAN_SIGMA < 1.5
