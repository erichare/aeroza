"""Pure scoring functions for verification.

I/O-free, side-effect-free. Given equally-shaped numpy arrays the
verifier passes in (forecast vs observation, or an ensemble vs an
observation), compute the metrics the ``nowcast_verifications`` table
records:

- **MAE / bias / RMSE** — continuous-value error metrics for a
  deterministic forecast.
- **Contingency-table counts** for a dBZ threshold — hits, misses,
  false alarms, correct negatives. From these the calibration
  aggregator computes the standard categorical-skill scores
  Probability of Detection (POD), False Alarm Ratio (FAR), and
  Critical Success Index (CSI).
- **Brier score** — the mean squared error of an event-probability
  forecast against a {0,1} truth. The probabilistic complement of
  POD/FAR/CSI: tells you "when the ensemble said 30%, did the event
  happen 30% of the time?"
- **CRPS** — the Continuous Ranked Probability Score. The
  generalisation of MAE to a full ensemble distribution; rewards a
  forecast whose CDF is close to the observation everywhere on the
  real line. Lower is better; equals MAE for a deterministic forecast.

Why both deterministic and probabilistic: continuous metrics tell
you "how far off, on average?", and categorical metrics tell you
"did we get the threshold crossing right?". Probabilistic metrics
tell you "is the *uncertainty* honest?" — the third leg of skill
scoring, and the one that's only meaningful once a forecaster emits
ensembles.

NaN handling: any cell where the observation is NaN is excluded from
the sample count for every metric. For ensemble metrics, any cell
where any member is NaN is also excluded — Brier / CRPS aren't
defined when the forecast distribution itself is partially missing.
MRMS occasionally publishes grids with masked-out cells; those should
not pull metrics toward zero or pollute the contingency table.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

import numpy as np

# Default dBZ threshold for the contingency table. 35 dBZ is the
# conventional "moderate rain / convective cell" cutoff in operational
# meteorology — POD/FAR/CSI at 35 are what dispatchers and forecasters
# look at first.
DEFAULT_THRESHOLD_DBZ: Final[float] = 35.0


@dataclass(frozen=True, slots=True)
class DeterministicMetrics:
    """One verification's worth of scoring numbers.

    All values are in the same units as the underlying variable (dBZ
    for reflectivity). ``sample_count`` is the number of cells that
    contributed to the metrics — both forecast and observation must
    be finite at that cell.

    The categorical fields are 0 when no threshold was applied (or no
    cells contributed); the route-side aggregator detects this from
    ``threshold_dbz is None`` and skips the POD/FAR/CSI columns.
    """

    mae: float
    bias: float
    rmse: float
    sample_count: int
    # Optional categorical (threshold-exceedance) metrics. None when
    # the verifier didn't run the threshold pass.
    threshold_dbz: float | None = None
    hits: int = 0
    misses: int = 0
    false_alarms: int = 0
    correct_negatives: int = 0


def score_deterministic_grids(
    forecast: np.ndarray,
    observation: np.ndarray,
    *,
    threshold_dbz: float | None = DEFAULT_THRESHOLD_DBZ,
) -> DeterministicMetrics:
    """Compute MAE / bias / RMSE + an optional contingency table.

    Cells where either array is NaN (masked) are excluded. When zero
    cells contribute (both arrays all-NaN, or shape mismatch handled
    upstream), the returned metrics are all 0.0 with ``sample_count=0``
    so the calibration aggregator can detect "no data".

    Pass ``threshold_dbz=None`` to skip the contingency-table pass —
    useful for unit tests that only want the continuous metrics, or
    for products where threshold-exceedance has no meaning.
    """
    if forecast.shape != observation.shape:
        raise ValueError(
            f"shape mismatch: forecast {forecast.shape} vs observation {observation.shape}"
        )

    valid = np.isfinite(forecast) & np.isfinite(observation)
    sample_count = int(valid.sum())
    if sample_count == 0:
        return DeterministicMetrics(
            mae=0.0,
            bias=0.0,
            rmse=0.0,
            sample_count=0,
            threshold_dbz=threshold_dbz,
        )

    f = forecast[valid].astype(np.float64)
    o = observation[valid].astype(np.float64)
    diff = f - o

    metrics = DeterministicMetrics(
        mae=float(np.mean(np.abs(diff))),
        bias=float(np.mean(diff)),
        rmse=float(np.sqrt(np.mean(diff**2))),
        sample_count=sample_count,
        threshold_dbz=threshold_dbz,
    )

    if threshold_dbz is None:
        return metrics

    f_event = f >= threshold_dbz
    o_event = o >= threshold_dbz
    hits = int(np.sum(f_event & o_event))
    misses = int(np.sum(~f_event & o_event))
    false_alarms = int(np.sum(f_event & ~o_event))
    correct_negatives = int(np.sum(~f_event & ~o_event))

    return DeterministicMetrics(
        mae=metrics.mae,
        bias=metrics.bias,
        rmse=metrics.rmse,
        sample_count=sample_count,
        threshold_dbz=threshold_dbz,
        hits=hits,
        misses=misses,
        false_alarms=false_alarms,
        correct_negatives=correct_negatives,
    )


@dataclass(frozen=True, slots=True)
class ProbabilisticMetrics:
    """One verification's worth of ensemble-forecast scoring.

    Computed against an ensemble of M members and one observation;
    only meaningful when M > 1 (M=1 collapses to deterministic and
    Brier=0/CRPS=MAE, which the calibration aggregator already
    captures via the deterministic columns).

    ``brier_score`` is the mean over valid cells of
    ``(p - event)^2`` where ``p`` is the empirical event probability
    over the ensemble for the chosen ``threshold_dbz`` and ``event``
    is the {0,1} indicator from the observation. Range [0, 1]; 0 is
    perfect.

    ``crps`` is the per-cell mean of the fair (unbiased) ensemble
    CRPS estimator (see :func:`crps_ensemble`). Same units as the
    forecast variable (dBZ for reflectivity); 0 is perfect.

    ``ensemble_size`` is the M passed in — recorded so the calibration
    aggregator can flag mixed-M buckets without re-reading the Zarr.
    """

    brier_score: float
    crps: float
    ensemble_size: int
    sample_count: int
    threshold_dbz: float


def brier_score(probabilities: np.ndarray, observed_events: np.ndarray) -> float:
    """Mean squared error of probabilistic forecast vs {0,1} truth.

    ``probabilities`` and ``observed_events`` must be the same shape;
    cells where either is NaN are excluded. ``observed_events`` is
    expected to hold {0, 1} (or boolean). Returns 0.0 when no cells
    contribute — caller should detect "no data" via the sample count
    rather than the score itself.
    """
    if probabilities.shape != observed_events.shape:
        raise ValueError(
            f"shape mismatch: probabilities {probabilities.shape} "
            f"vs observed_events {observed_events.shape}"
        )
    valid = np.isfinite(probabilities) & np.isfinite(observed_events)
    if not bool(valid.any()):
        return 0.0
    p = probabilities[valid].astype(np.float64)
    o = observed_events[valid].astype(np.float64)
    return float(np.mean((p - o) ** 2))


def crps_ensemble(members: np.ndarray, observations: np.ndarray) -> tuple[float, int]:
    """Fair (unbiased) ensemble Continuous Ranked Probability Score.

    Implements the standard estimator
    ``CRPS = (1/M) Σ |x_i - y| - (1/(2 M (M-1))) Σ_i Σ_j |x_i - x_j|``
    per cell, then averages over valid cells. The factor ``1/(M-1)``
    (instead of ``1/M``) makes the estimator unbiased for finite M —
    Ferro (2014), Zamo & Naveau (2018). Falls back to MAE when M=1
    (the second term is 0) so the function is safe for the
    deterministic edge case.

    ``members`` shape: ``(M, *spatial)``. ``observations`` shape:
    ``spatial``. A cell counts as valid only when the observation and
    every member at that cell are finite — Brier / CRPS are not
    defined when part of the forecast distribution is missing.

    Returns ``(crps_mean, sample_count)`` so the caller can treat
    "no data" as ``sample_count == 0`` without inspecting the score.
    """
    if members.ndim < 1:
        raise ValueError("members must have at least one dimension")
    n_members = members.shape[0]
    if members.shape[1:] != observations.shape:
        raise ValueError(
            f"shape mismatch: members spatial {members.shape[1:]} "
            f"vs observations {observations.shape}"
        )

    obs_finite = np.isfinite(observations)
    members_finite = np.all(np.isfinite(members), axis=0)
    valid = obs_finite & members_finite
    sample_count = int(valid.sum())
    if sample_count == 0:
        return 0.0, 0

    # Flatten the spatial dims; vectorise over the M members.
    flat_members = members.reshape(n_members, -1)[:, valid.ravel()].astype(np.float64)
    flat_obs = observations.ravel()[valid.ravel()].astype(np.float64)

    # First term: mean over members of |x_i - y|, per cell.
    abs_err = np.mean(np.abs(flat_members - flat_obs[np.newaxis, :]), axis=0)

    # Second term: spread term, computed per cell from the sorted
    # ensemble. For sorted x_(1) ≤ … ≤ x_(M):
    #   Σ_i Σ_j |x_i - x_j| = 2 Σ_k (2k - M - 1) x_(k)   (k starts at 1).
    # That's O(M log M) per cell instead of O(M^2).
    spread: float | np.ndarray
    if n_members < 2:
        spread = 0.0
    else:
        sorted_members = np.sort(flat_members, axis=0)
        k = np.arange(1, n_members + 1, dtype=np.float64)[:, np.newaxis]
        weighted = (2.0 * k - n_members - 1.0) * sorted_members
        spread = np.sum(weighted, axis=0) / (n_members * (n_members - 1))

    crps_per_cell = abs_err - spread
    return float(np.mean(crps_per_cell)), sample_count


def score_probabilistic_grids(
    members: np.ndarray,
    observation: np.ndarray,
    *,
    threshold_dbz: float = DEFAULT_THRESHOLD_DBZ,
) -> ProbabilisticMetrics:
    """Compute Brier score + CRPS for an ensemble forecast.

    ``members`` shape: ``(M, y, x)``. ``observation`` shape: ``(y, x)``.
    Brier is computed against the binary event ``observation >=
    threshold_dbz``; the forecast probability is the fraction of
    members at-or-above the threshold. CRPS is the fair ensemble
    estimator; same threshold is recorded but doesn't enter CRPS
    itself (CRPS scores the whole distribution, not just the
    threshold crossing).

    ``sample_count`` is the cell count where the observation and
    every member were finite — both Brier and CRPS use the same valid
    mask so a single number suffices.
    """
    if members.ndim < 1:
        raise ValueError("members must have at least one dimension")
    n_members = int(members.shape[0])
    if members.shape[1:] != observation.shape:
        raise ValueError(
            f"shape mismatch: members spatial {members.shape[1:]} "
            f"vs observation {observation.shape}"
        )

    obs_finite = np.isfinite(observation)
    members_finite = np.all(np.isfinite(members), axis=0)
    valid = obs_finite & members_finite
    sample_count = int(valid.sum())
    if sample_count == 0:
        return ProbabilisticMetrics(
            brier_score=0.0,
            crps=0.0,
            ensemble_size=n_members,
            sample_count=0,
            threshold_dbz=threshold_dbz,
        )

    member_events = (members >= threshold_dbz).astype(np.float64)
    probabilities = np.mean(member_events, axis=0)
    observed_events = (observation >= threshold_dbz).astype(np.float64)
    # NaN-mask valid cells in via the observation finiteness so the
    # downstream ``brier_score`` helper agrees with ``sample_count``.
    probabilities = np.where(valid, probabilities, np.nan)
    observed_events = np.where(valid, observed_events, np.nan)

    crps_mean, _ = crps_ensemble(members, observation)
    return ProbabilisticMetrics(
        brier_score=brier_score(probabilities, observed_events),
        crps=crps_mean,
        ensemble_size=n_members,
        sample_count=sample_count,
        threshold_dbz=threshold_dbz,
    )


def pod(hits: int, misses: int) -> float | None:
    """Probability of Detection: fraction of observed events caught.

    POD = hits / (hits + misses). Returns None when the denominator is
    zero (no observed events) — distinguishes "perfect" from "no data
    to score against" without a special sentinel.
    """
    denom = hits + misses
    return None if denom == 0 else hits / denom


def far(hits: int, false_alarms: int) -> float | None:
    """False Alarm Ratio: fraction of forecast events that didn't happen.

    FAR = false_alarms / (hits + false_alarms). Returns None when the
    denominator is zero (no forecast events).
    """
    denom = hits + false_alarms
    return None if denom == 0 else false_alarms / denom


def csi(hits: int, misses: int, false_alarms: int) -> float | None:
    """Critical Success Index: hits over (hits + misses + false alarms).

    Combined skill score; equivalent to the threat score in operational
    meteorology. Returns None when no events were observed or forecast
    (the denominator is the union of hit-or-miss-or-FA cells).
    """
    denom = hits + misses + false_alarms
    return None if denom == 0 else hits / denom


__all__ = [
    "DEFAULT_THRESHOLD_DBZ",
    "DeterministicMetrics",
    "ProbabilisticMetrics",
    "brier_score",
    "crps_ensemble",
    "csi",
    "far",
    "pod",
    "score_deterministic_grids",
    "score_probabilistic_grids",
]
