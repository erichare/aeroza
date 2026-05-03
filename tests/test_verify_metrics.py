"""Unit tests for the deterministic scoring metrics.

The functions are pure numpy — no DB, no Zarr. Tests pin every
property the calibration aggregator depends on (NaN handling, sample
count, shape mismatch raises).
"""

from __future__ import annotations

import numpy as np
import pytest

from aeroza.verify.metrics import (
    DEFAULT_THRESHOLD_DBZ,
    DeterministicMetrics,
    ProbabilisticMetrics,
    brier_score,
    crps_ensemble,
    csi,
    far,
    pod,
    score_deterministic_grids,
    score_probabilistic_grids,
)

pytestmark = pytest.mark.unit


def test_perfect_forecast_zero_error() -> None:
    arr = np.array([[1.0, 2.0], [3.0, 4.0]], dtype=np.float32)
    metrics = score_deterministic_grids(arr, arr)
    assert metrics.mae == 0.0
    assert metrics.bias == 0.0
    assert metrics.rmse == 0.0
    assert metrics.sample_count == 4


def test_uniform_offset_pure_bias() -> None:
    obs = np.zeros((2, 2), dtype=np.float32)
    forecast = obs + 5.0
    metrics = score_deterministic_grids(forecast, obs)
    assert metrics.bias == 5.0
    assert metrics.mae == 5.0
    assert metrics.rmse == 5.0
    assert metrics.sample_count == 4


def test_signed_errors_average_to_zero_bias() -> None:
    """MAE > 0 but bias = 0 when over- and under-shoots cancel."""
    obs = np.array([0.0, 0.0], dtype=np.float32)
    forecast = np.array([5.0, -5.0], dtype=np.float32)
    metrics = score_deterministic_grids(forecast, obs)
    assert metrics.mae == 5.0
    assert metrics.bias == 0.0
    assert metrics.rmse == 5.0


def test_rmse_penalises_large_errors_more_than_mae() -> None:
    obs = np.zeros(4, dtype=np.float32)
    forecast = np.array([1.0, 1.0, 1.0, 5.0], dtype=np.float32)
    metrics = score_deterministic_grids(forecast, obs)
    # MAE = 8/4 = 2.0; RMSE = sqrt(28/4) = sqrt(7) ≈ 2.646
    assert metrics.mae == pytest.approx(2.0)
    assert metrics.rmse == pytest.approx(2.6457513)


def test_nan_cells_excluded_from_sample_count_and_metrics() -> None:
    obs = np.array([1.0, 2.0, np.nan, 4.0], dtype=np.float32)
    forecast = np.array([1.0, 3.0, 100.0, 4.0], dtype=np.float32)
    metrics = score_deterministic_grids(forecast, obs)
    assert metrics.sample_count == 3  # NaN cell skipped
    assert metrics.mae == pytest.approx(1.0 / 3)  # only the (2 vs 3) miss


def test_all_nan_returns_zero_with_zero_sample_count() -> None:
    obs = np.full(3, np.nan, dtype=np.float32)
    forecast = np.array([1.0, 2.0, 3.0], dtype=np.float32)
    metrics = score_deterministic_grids(forecast, obs)
    assert metrics.sample_count == 0
    assert metrics.mae == 0.0
    assert metrics.bias == 0.0
    assert metrics.rmse == 0.0


def test_shape_mismatch_raises_valueerror() -> None:
    a = np.zeros((2, 2), dtype=np.float32)
    b = np.zeros((3, 3), dtype=np.float32)
    with pytest.raises(ValueError, match="shape mismatch"):
        score_deterministic_grids(a, b)


def test_metrics_dataclass_is_frozen() -> None:
    metrics = DeterministicMetrics(mae=1.0, bias=0.0, rmse=1.0, sample_count=1)
    with pytest.raises(AttributeError):
        metrics.mae = 2.0  # type: ignore[misc]


# --------------------------------------------------------------------------- #
# Categorical (POD / FAR / CSI) — default threshold 35 dBZ                    #
# --------------------------------------------------------------------------- #


def test_threshold_skips_when_set_to_none() -> None:
    obs = np.zeros((2, 2), dtype=np.float32)
    metrics = score_deterministic_grids(obs, obs, threshold_dbz=None)
    assert metrics.threshold_dbz is None
    assert metrics.hits == metrics.misses == metrics.false_alarms == 0
    assert metrics.correct_negatives == 0


def test_perfect_threshold_forecast_is_all_hits_and_correct_negatives() -> None:
    # Two cells crossing the 35 dBZ threshold, two cells clearly below.
    obs = np.array([40.0, 50.0, 10.0, 0.0], dtype=np.float32)
    forecast = obs.copy()
    metrics = score_deterministic_grids(forecast, obs)
    assert metrics.threshold_dbz == DEFAULT_THRESHOLD_DBZ
    assert metrics.hits == 2
    assert metrics.correct_negatives == 2
    assert metrics.misses == metrics.false_alarms == 0
    assert pod(metrics.hits, metrics.misses) == 1.0
    assert far(metrics.hits, metrics.false_alarms) == 0.0
    assert csi(metrics.hits, metrics.misses, metrics.false_alarms) == 1.0


def test_pure_miss_pure_false_alarm() -> None:
    # Forecast says no event everywhere; observation has one event cell.
    obs = np.array([40.0, 0.0, 0.0], dtype=np.float32)
    forecast_all_low = np.zeros_like(obs)
    metrics_miss = score_deterministic_grids(forecast_all_low, obs)
    assert metrics_miss.misses == 1
    assert metrics_miss.hits == 0
    assert pod(metrics_miss.hits, metrics_miss.misses) == 0.0

    forecast_all_high = np.array([0.0, 50.0, 0.0], dtype=np.float32)
    metrics_fa = score_deterministic_grids(forecast_all_high, obs)
    assert metrics_fa.false_alarms == 1
    assert metrics_fa.hits == 0
    assert far(metrics_fa.hits, metrics_fa.false_alarms) == 1.0


def test_pod_far_csi_return_none_when_denominators_are_zero() -> None:
    # No observed events and no forecast events → POD, FAR, CSI all None.
    assert pod(0, 0) is None
    assert far(0, 0) is None
    assert csi(0, 0, 0) is None


def test_threshold_uses_default_when_unspecified() -> None:
    obs = np.array([34.9, 35.0, 35.1], dtype=np.float32)
    forecast = obs.copy()
    metrics = score_deterministic_grids(forecast, obs)
    # 35.0 and 35.1 are at-or-above the threshold; 34.9 is below.
    assert metrics.hits == 2
    assert metrics.correct_negatives == 1


def test_custom_threshold_overrides_default() -> None:
    obs = np.array([10.0, 20.0, 30.0, 40.0], dtype=np.float32)
    forecast = obs.copy()
    metrics = score_deterministic_grids(forecast, obs, threshold_dbz=20.0)
    assert metrics.threshold_dbz == 20.0
    # ≥20: indices 1, 2, 3 → 3 hits.
    assert metrics.hits == 3
    assert metrics.correct_negatives == 1


def test_categorical_skips_nan_cells() -> None:
    obs = np.array([40.0, np.nan, 0.0], dtype=np.float32)
    forecast = np.array([40.0, 100.0, 0.0], dtype=np.float32)
    metrics = score_deterministic_grids(forecast, obs)
    # NaN cell excluded; remaining 2 cells: one hit, one correct-negative.
    assert metrics.sample_count == 2
    assert metrics.hits == 1
    assert metrics.correct_negatives == 1
    assert metrics.misses == metrics.false_alarms == 0


# --------------------------------------------------------------------------- #
# Probabilistic — Brier + CRPS over an ensemble                               #
# --------------------------------------------------------------------------- #


def test_brier_perfect_certainty_scores_zero() -> None:
    probs = np.array([1.0, 0.0, 1.0, 0.0], dtype=np.float32)
    events = np.array([1.0, 0.0, 1.0, 0.0], dtype=np.float32)
    assert brier_score(probs, events) == 0.0


def test_brier_constant_uncertainty_against_balanced_truth() -> None:
    # 50% probability against half-events: per-cell error 0.25, mean 0.25.
    probs = np.full(4, 0.5, dtype=np.float32)
    events = np.array([1.0, 0.0, 1.0, 0.0], dtype=np.float32)
    assert brier_score(probs, events) == pytest.approx(0.25)


def test_brier_skips_nan_cells() -> None:
    probs = np.array([0.5, np.nan, 1.0], dtype=np.float32)
    events = np.array([1.0, 0.0, 1.0], dtype=np.float32)
    # Only cells 0 and 2 contribute: ((0.5-1)^2 + (1-1)^2) / 2 = 0.125.
    assert brier_score(probs, events) == pytest.approx(0.125)


def test_brier_shape_mismatch_raises() -> None:
    with pytest.raises(ValueError, match="shape mismatch"):
        brier_score(np.zeros(3), np.zeros(4))


def test_crps_single_member_collapses_to_mae() -> None:
    # M=1 zeros the spread term; CRPS reduces to MAE.
    members = np.array([[1.0, 4.0, 9.0]], dtype=np.float32)  # shape (1, 3)
    obs = np.array([0.0, 0.0, 0.0], dtype=np.float32)
    crps_value, count = crps_ensemble(members, obs)
    assert count == 3
    # MAE = mean(|1|, |4|, |9|) = 14/3.
    assert crps_value == pytest.approx(14.0 / 3.0)


def test_crps_perfect_ensemble_at_truth_scores_zero() -> None:
    members = np.full((5, 3), 7.0, dtype=np.float32)
    obs = np.full(3, 7.0, dtype=np.float32)
    crps_value, count = crps_ensemble(members, obs)
    assert count == 3
    assert crps_value == pytest.approx(0.0)


def test_crps_known_two_member_value() -> None:
    # Members {0, 2} for a single cell, observation 1.
    # Fair CRPS = (1/M) Σ |x_i - y| - 1/(2 M (M-1)) Σ_i Σ_j |x_i - x_j|
    # = (1/2)(1 + 1) - 1/(2*2*1) * (0 + 2 + 2 + 0)
    # = 1 - 1 = 0.0
    members = np.array([[0.0], [2.0]], dtype=np.float32)
    obs = np.array([1.0], dtype=np.float32)
    crps_value, count = crps_ensemble(members, obs)
    assert count == 1
    assert crps_value == pytest.approx(0.0)


def test_crps_skips_cells_with_partial_member_nan() -> None:
    members = np.array(
        [
            [1.0, np.nan, 3.0],
            [2.0, 4.0, 4.0],
        ],
        dtype=np.float32,
    )
    obs = np.array([0.0, 0.0, 0.0], dtype=np.float32)
    _, count = crps_ensemble(members, obs)
    # Cell 1 has a NaN member → dropped. Two cells contribute.
    assert count == 2


def test_score_probabilistic_grids_records_threshold_and_sizes() -> None:
    # 4 members, all above the threshold at one cell, all below at another.
    # P(event) = 1.0 at cell 0, 0.0 at cell 1.
    # Observation: cell 0 above threshold, cell 1 below.
    members = np.array(
        [
            [40.0, 10.0],
            [40.0, 10.0],
            [40.0, 10.0],
            [40.0, 10.0],
        ],
        dtype=np.float32,
    )
    obs = np.array([40.0, 10.0], dtype=np.float32)
    metrics = score_probabilistic_grids(members, obs, threshold_dbz=35.0)
    assert isinstance(metrics, ProbabilisticMetrics)
    assert metrics.ensemble_size == 4
    assert metrics.sample_count == 2
    assert metrics.threshold_dbz == 35.0
    assert metrics.brier_score == pytest.approx(0.0)
    # CRPS for a perfect-certain ensemble at the truth is 0.
    assert metrics.crps == pytest.approx(0.0)


def test_score_probabilistic_grids_imperfect_brier() -> None:
    # 2-member ensemble: one above, one below the threshold → P(event) = 0.5.
    # One cell where event happened, one where it didn't.
    members = np.array(
        [
            [40.0, 40.0],
            [10.0, 10.0],
        ],
        dtype=np.float32,
    )
    obs = np.array([40.0, 10.0], dtype=np.float32)
    metrics = score_probabilistic_grids(members, obs, threshold_dbz=35.0)
    # Cell 0: (0.5 - 1.0)^2 = 0.25. Cell 1: (0.5 - 0.0)^2 = 0.25. Mean: 0.25.
    assert metrics.brier_score == pytest.approx(0.25)
    assert metrics.sample_count == 2


def test_score_probabilistic_grids_all_nan_returns_zero_with_zero_samples() -> None:
    members = np.full((3, 4), np.nan, dtype=np.float32)
    obs = np.full(4, np.nan, dtype=np.float32)
    metrics = score_probabilistic_grids(members, obs)
    assert metrics.sample_count == 0
    assert metrics.brier_score == 0.0
    assert metrics.crps == 0.0
    assert metrics.ensemble_size == 3


def test_probabilistic_metrics_dataclass_is_frozen() -> None:
    metrics = ProbabilisticMetrics(
        brier_score=0.0,
        crps=0.0,
        ensemble_size=1,
        sample_count=1,
        threshold_dbz=35.0,
    )
    with pytest.raises(AttributeError):
        metrics.brier_score = 1.0  # type: ignore[misc]


# --------------------------------------------------------------------------- #
# Reliability bins                                                            #
# --------------------------------------------------------------------------- #


def test_reliability_bins_count_and_layout() -> None:
    """Empty input: every bin shows up, ``count == 0``, lower edges
    evenly spaced from 0.0 to 0.9."""
    from aeroza.verify.metrics import (
        RELIABILITY_BIN_COUNT,
        reliability_bins_from_arrays,
    )

    bins = reliability_bins_from_arrays(np.array([]), np.array([]))
    assert len(bins) == RELIABILITY_BIN_COUNT
    for i, bin_ in enumerate(bins):
        assert bin_.lower == pytest.approx(i / RELIABILITY_BIN_COUNT)
        assert bin_.count == 0
        assert bin_.observed == 0


def test_reliability_bins_assign_to_correct_bucket() -> None:
    """Forecast 0.55 lands in the bin starting at 0.5; 0.05 in the
    first bin; 1.0 in the last bin (right-edge inclusive)."""
    from aeroza.verify.metrics import reliability_bins_from_arrays

    probs = np.array([0.05, 0.55, 1.0])
    events = np.array([1.0, 0.0, 1.0])
    bins = reliability_bins_from_arrays(probs, events, n_bins=10)
    assert bins[0].count == 1
    assert bins[5].count == 1
    assert bins[9].count == 1
    # Other bins are empty.
    for i in [1, 2, 3, 4, 6, 7, 8]:
        assert bins[i].count == 0


def test_reliability_bins_observed_count_and_mean_prob() -> None:
    """Multiple cells per bin: observed sums; mean_prob averages the
    forecast probs that landed in the bin."""
    from aeroza.verify.metrics import reliability_bins_from_arrays

    probs = np.array([0.62, 0.65, 0.68])  # all in bin 6 ([0.6, 0.7))
    events = np.array([1.0, 1.0, 0.0])
    bins = reliability_bins_from_arrays(probs, events, n_bins=10)
    assert bins[6].count == 3
    assert bins[6].observed == 2
    assert bins[6].mean_prob == pytest.approx((0.62 + 0.65 + 0.68) / 3)


def test_reliability_bins_skip_nan_cells() -> None:
    from aeroza.verify.metrics import reliability_bins_from_arrays

    probs = np.array([0.5, np.nan, 0.5])
    events = np.array([1.0, 0.0, np.nan])
    bins = reliability_bins_from_arrays(probs, events, n_bins=10)
    # Only the first cell (prob=0.5, event=1) contributes.
    assert bins[5].count == 1
    assert bins[5].observed == 1


def test_score_probabilistic_grids_attaches_reliability_bins() -> None:
    """End-to-end: a 4-member ensemble with two-thirds members above
    the threshold against a single observation produces one bin
    populated at 0.5–0.6 (P = 0.5)."""
    members = np.array(
        [
            [40.0, 40.0],
            [40.0, 10.0],
            [10.0, 10.0],
            [10.0, 10.0],
        ],
        dtype=np.float32,
    )
    obs = np.array([40.0, 10.0], dtype=np.float32)
    metrics = score_probabilistic_grids(members, obs, threshold_dbz=35.0)
    # Cell 0: P=0.5 (2/4 members above), observed=1
    # Cell 1: P=0.25, observed=0
    bins_by_lower = {round(b.lower, 1): b for b in metrics.reliability_bins}
    assert bins_by_lower[0.5].count == 1
    assert bins_by_lower[0.5].observed == 1
    assert bins_by_lower[0.2].count == 1
    assert bins_by_lower[0.2].observed == 0
