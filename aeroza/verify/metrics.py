"""Pure scoring functions for verification.

I/O-free, side-effect-free. Given two equally-shaped numpy arrays
(forecast vs observation), compute the deterministic-forecast metrics
the ``nowcast_verifications`` table records:

- **MAE / bias / RMSE** — continuous-value error metrics.
- **Contingency-table counts** for a dBZ threshold — hits, misses,
  false alarms, correct negatives. From these the calibration
  aggregator computes the standard categorical-skill scores
  Probability of Detection (POD), False Alarm Ratio (FAR), and
  Critical Success Index (CSI).

Why both: continuous metrics tell you "how far off, on average?", but
categorical metrics tell you "did we get the threshold crossing
right?" — which is what every operational user actually cares about
("did the storm cell over Houston exceed 35 dBZ at 30 minutes out?").

Brier / reliability / CRPS need probabilistic forecasts (ensemble
outputs). They land alongside ensemble pySTEPS / NowcastNet later;
the schema column for them is the next addition this module will
grow.

NaN handling: any cell that's NaN in either array is excluded from
the sample count (and from the metrics). MRMS occasionally publishes
grids with masked-out cells — those should not pull MAE toward zero
or pollute the contingency table.
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
    "csi",
    "far",
    "pod",
    "score_deterministic_grids",
]
