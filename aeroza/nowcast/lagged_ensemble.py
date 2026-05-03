"""Lagged-ensemble nowcaster.

Construction is dead simple: take the last K observation grids
(oldest → newest, *including* the current one), stack them along a
new ``member`` dim, and use that stack as a probabilistic forecast at
every requested horizon.

Why "lagged"? It's the operational technique of treating recent
forecasts (or, in our case, recent observations) as draws from the
forecast distribution. For nowcast horizons (≤60 min) at MRMS' ~2-min
cadence, the last K=8 observations span ~16 min of recent state and
encode the spatial uncertainty of "what's in the system right now"
without needing a dynamics model. Operationally cheap, and a
legitimate baseline against which true ensemble forecasters
(STEPS, NowcastNet) must demonstrate Brier-skill improvement.

Cold-start fallback: if fewer than ``ensemble_size`` observations
are available (newly-deployed system, or the catalog lost rows), the
forecaster degrades gracefully to a single-member ensemble equal to
the latest observation — same shape as :class:`PersistenceForecaster`
but tagged ``"lagged-ensemble"`` so calibration buckets stay
algorithm-clean.

The output ``data`` has shape ``(member, *spatial)``. The worker
preserves the ``member`` dim through the Zarr write, and the verifier
detects the ensemble by reading ``ensemble_size`` off the catalog row.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Final

import numpy as np
import structlog

if TYPE_CHECKING:  # pragma: no cover - typing only
    import xarray as xr

from aeroza.nowcast.engine import NowcastPrediction

log = structlog.get_logger(__name__)

LAGGED_ENSEMBLE_ALGORITHM: Final[str] = "lagged-ensemble"

# Default ensemble size. 8 trades resolution for honest spread:
# enough members that Brier reliability bins (typically 10) get
# something to chew on, few enough that the Zarr writes stay small
# (8 × ~12 MB per grid is ~100 MB per horizon — fine for the tick).
DEFAULT_ENSEMBLE_SIZE: Final[int] = 8


@dataclass(frozen=True, slots=True)
class LaggedEnsembleForecaster:
    """Members = last ``ensemble_size`` observations, persisted forward.

    Stateless: the worker hands us the current observation plus the
    catalog-resolved history. We construct one ensemble per horizon
    by stacking the available frames (no copies of the latest if we
    already have enough recent frames; fall back to repeating the
    latest only when history is thin).
    """

    ensemble_size: int = DEFAULT_ENSEMBLE_SIZE

    @property
    def algorithm(self) -> str:
        return LAGGED_ENSEMBLE_ALGORITHM

    @property
    def history_depth(self) -> int:
        # We need ``ensemble_size`` frames (current + ensemble_size-1
        # past) for a full ensemble. Add one frame of slack so a tick
        # that races the materialiser still gets a full ensemble most
        # of the time.
        return self.ensemble_size + 1

    async def forecast(
        self,
        observation: xr.DataArray,
        observation_valid_at: datetime,
        horizons_minutes: Sequence[int],
        history: Sequence[xr.DataArray] | None = None,
    ) -> Sequence[NowcastPrediction]:
        """Produce one ensemble per requested horizon.

        Each ensemble is the same stack — the lagged technique
        treats the recent past as the forecast distribution at every
        lead time, so members differ across cells but not across
        horizons. Real ensemble forecasters (STEPS) generate
        horizon-dependent spread; this baseline doesn't, and that's
        the gap their Brier improvement should close.
        """
        members = self._build_members(observation, history)
        n_members = members.shape[0]
        # Reuse the observation's coords / dims; just prepend a
        # ``member`` dim so the worker writes (member, *spatial) Zarr.
        import xarray as xr_runtime

        spatial_dims = tuple(str(d) for d in observation.dims)
        spatial_coords = {k: v for k, v in observation.coords.items() if k in spatial_dims}
        member_dim = "member"
        data = xr_runtime.DataArray(
            members.astype(observation.dtype, copy=False),
            dims=(member_dim, *spatial_dims),
            coords={member_dim: np.arange(n_members), **spatial_coords},
            name=observation.name if observation.name else "value",
            attrs=dict(observation.attrs),
        )

        predictions: list[NowcastPrediction] = []
        for horizon in horizons_minutes:
            valid_at = observation_valid_at + timedelta(minutes=horizon)
            predictions.append(
                NowcastPrediction(
                    horizon_minutes=horizon,
                    valid_at=valid_at,
                    data=data.copy(),
                    ensemble_size=n_members,
                )
            )

        log.info(
            "nowcast.lagged_ensemble.forecast",
            requested_size=self.ensemble_size,
            actual_size=n_members,
            horizons=list(horizons_minutes),
            spatial_shape=tuple(int(s) for s in observation.shape),
        )
        return predictions

    def _build_members(
        self,
        observation: xr.DataArray,
        history: Sequence[xr.DataArray] | None,
    ) -> np.ndarray:
        """Stack history + current observation into a (member, *spatial) array.

        Members are oldest → newest. When the available frames are
        fewer than ``ensemble_size``, we use what we have; we don't
        pad with copies of the latest (that would understate spread).
        """
        frames: list[xr.DataArray] = []
        if history is not None:
            # Take up to ``ensemble_size - 1`` past frames; the
            # current observation contributes the final member.
            past = list(history)[-(self.ensemble_size - 1) :]
            frames.extend(past)
        frames.append(observation)

        try:
            stack = np.stack([np.asarray(f.values) for f in frames], axis=0)
        except ValueError as exc:
            # Shape mismatch in the history → drop everything and
            # fall back to a single-member ensemble of the current
            # observation. Logged so a regression in the materialiser
            # doesn't silently degrade the ensemble.
            log.warning(
                "nowcast.lagged_ensemble.fallback_single_member",
                reason="shape_mismatch",
                error=str(exc),
            )
            return np.asarray(observation.values)[np.newaxis, ...]
        return stack


__all__ = [
    "DEFAULT_ENSEMBLE_SIZE",
    "LAGGED_ENSEMBLE_ALGORITHM",
    "LaggedEnsembleForecaster",
]
