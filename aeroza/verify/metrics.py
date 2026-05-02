"""Pure scoring functions for verification.

I/O-free, side-effect-free. Given two equally-shaped numpy arrays
(forecast vs observation), compute the deterministic-forecast metrics
the ``nowcast_verifications`` table records: MAE, bias, RMSE.

Why these three: the plan §7 wants Brier and reliability diagrams, but
those need *probabilistic* forecasts (ensemble outputs). v1's
persistence forecaster is deterministic, so we pin those metrics here
and add probabilistic ones (Brier / reliability bins / CRPS) once the
forecaster Protocol grows ensemble support.

NaN handling: any cell that's NaN in either array is excluded from the
sample count (and from the metrics). MRMS occasionally publishes grids
with masked-out cells — those should not pull MAE toward zero.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True, slots=True)
class DeterministicMetrics:
    """One verification's worth of scoring numbers.

    All values are in the same units as the underlying variable (dBZ
    for reflectivity). ``sample_count`` is the number of cells that
    contributed to the metrics — both forecast and observation must
    be finite at that cell.
    """

    mae: float
    bias: float
    rmse: float
    sample_count: int


def score_deterministic_grids(
    forecast: np.ndarray,
    observation: np.ndarray,
) -> DeterministicMetrics:
    """Compute MAE / bias / RMSE between two equally-shaped grids.

    Cells where either array is NaN (masked) are excluded. When zero
    cells contribute (both arrays all-NaN, or shape mismatch handled
    upstream), the returned metrics are all 0.0 with ``sample_count=0``
    so the calibration aggregator can detect "no data".
    """
    if forecast.shape != observation.shape:
        raise ValueError(
            f"shape mismatch: forecast {forecast.shape} vs observation {observation.shape}"
        )

    valid = np.isfinite(forecast) & np.isfinite(observation)
    sample_count = int(valid.sum())
    if sample_count == 0:
        return DeterministicMetrics(mae=0.0, bias=0.0, rmse=0.0, sample_count=0)

    diff = forecast[valid].astype(np.float64) - observation[valid].astype(np.float64)
    return DeterministicMetrics(
        mae=float(np.mean(np.abs(diff))),
        bias=float(np.mean(diff)),
        rmse=float(np.sqrt(np.mean(diff**2))),
        sample_count=sample_count,
    )


__all__ = [
    "DeterministicMetrics",
    "score_deterministic_grids",
]
