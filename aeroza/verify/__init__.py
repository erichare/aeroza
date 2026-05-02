"""Verification — Phase 3.

Scores nowcast predictions against MRMS ground truth as observations
arrive. Persists per-(forecast, observation) MAE / bias / RMSE rows in
``nowcast_verifications`` so the public ``/v1/calibration`` aggregator
can answer "how well does our 30-min forecast actually do?" with a
real number.

Per the plan §3.3, calibration is the moat: the trust signal nobody
in the dev-API weather space publishes. Even with the v1 baseline
forecaster (persistence), the verification pipeline immediately yields
useful numbers — and once a real algorithm (pySTEPS / NowcastNet)
swaps in, the gap to persistence is the headline figure.
"""

from aeroza.verify.metrics import (
    DeterministicMetrics,
    score_deterministic_grids,
)

__all__ = [
    "DeterministicMetrics",
    "score_deterministic_grids",
]
