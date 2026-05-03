"""pySTEPS Lagrangian-extrapolation nowcaster.

Implements the same :class:`aeroza.nowcast.engine.Forecaster` Protocol
as :class:`PersistenceForecaster`, so the worker swaps it in via the
``--algorithm pysteps`` CLI flag without any plumbing changes.

The math, briefly:

1. Lucas–Kanade dense optical flow over the last N observation grids
   (default N=3) → a (2, y, x) velocity field in pixels per timestep.
2. Semi-Lagrangian extrapolation: advect the most recent observation
   forward along that velocity field, one timestep per dt (where
   ``dt`` is the cadence of the observation series, ~2 min for MRMS).
3. For each requested horizon, pick the extrapolated frame at
   ``round(horizon_minutes / dt_minutes)`` steps and emit a
   :class:`NowcastPrediction`.

pySTEPS is a heavy native-extension dep (it pulls scipy + numpy +
optionally dask). It's lazy-imported here so importing this module
without ``pysteps`` installed only blows up when ``forecast()`` is
actually called — keeps unit tests for the rest of nowcast/ green
without forcing the install.

NaNs in the input — common where a cell is below the dBZ threshold or
outside MRMS' coverage mask — are filled with the no-echo floor
(``BELOW_THRESHOLD_DBZ``) before optical flow, then restored to NaN
in the output where the field falls back to the floor. pySTEPS' flow
solver can't tolerate NaNs; treating them as "no echo" is the same
convention the colormap uses.
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

PYSTEPS_ALGORITHM: Final[str] = "pysteps"

# Number of past observations the optical-flow solver wants. 3 is the
# pySTEPS docs' starting recommendation for "good enough" motion;
# higher N smooths over noisy motion but anchors against older state.
DEFAULT_LOOKBACK: Final[int] = 3

# MRMS publishes every ~2 minutes. Our extrapolation timestep matches
# the input cadence so each unit step lines up with the next observation.
DEFAULT_DT_MINUTES: Final[float] = 2.0

# Below this dBZ value, treat as no-echo for optical flow purposes.
# Matches the transparent-below threshold the colormap uses, so
# what's invisible on the map is also invisible to the motion solver.
BELOW_THRESHOLD_DBZ: Final[float] = 5.0


@dataclass(frozen=True, slots=True)
class PystepsForecaster:
    """Lucas–Kanade flow + semi-Lagrangian extrapolation.

    Stateless: ``forecast()`` takes the latest observation plus a
    caller-supplied window of past observations stacked as a 3D
    array. The dispatching worker is responsible for producing the
    history; the forecaster just runs the math.
    """

    lookback: int = DEFAULT_LOOKBACK
    dt_minutes: float = DEFAULT_DT_MINUTES

    @property
    def algorithm(self) -> str:
        return PYSTEPS_ALGORITHM

    @property
    def history_depth(self) -> int:
        # We need ``lookback`` consecutive frames (current + lookback-1
        # past) for Lucas–Kanade. Add one frame of slack so a tick that
        # races the materialiser still has enough.
        return self.lookback + 1

    async def forecast(
        self,
        observation: xr.DataArray,
        observation_valid_at: datetime,
        horizons_minutes: Sequence[int],
        history: Sequence[xr.DataArray] | None = None,
    ) -> Sequence[NowcastPrediction]:
        """Run optical flow + extrapolation.

        ``history`` is the sequence of past observations *not including*
        the current one, oldest→newest. When None (or shorter than
        ``self.lookback``), the forecaster falls back to persistence —
        cold-start protection so the worker doesn't crash on its first
        few ticks.
        """
        full_stack = self._stack_history(observation, history)
        if full_stack is None:
            log.warning(
                "nowcast.pysteps.fallback_persistence",
                reason="insufficient_history",
                have=len(history) if history is not None else 0,
                need=self.lookback,
            )
            return _persistence_predictions(observation, observation_valid_at, horizons_minutes)

        velocity = await _compute_motion(full_stack)
        extrapolated = await _extrapolate(
            full_stack[-1], velocity, n_leadtimes=self._max_leadtime(horizons_minutes)
        )
        return self._slice_predictions(
            extrapolated_stack=extrapolated,
            template=observation,
            observation_valid_at=observation_valid_at,
            horizons_minutes=horizons_minutes,
        )

    def _stack_history(
        self, observation: xr.DataArray, history: Sequence[xr.DataArray] | None
    ) -> np.ndarray | None:
        """Return a (T, y, x) numpy stack of values, NaNs filled, or None."""
        if history is None or len(history) < self.lookback - 1:
            # The current observation counts as one frame; we need
            # ``lookback - 1`` more from the history.
            return None
        # Take the most-recent ``lookback - 1`` from history and append the
        # current observation. Each frame must share the current's shape;
        # pySTEPS rejects mismatched stacks.
        recent = [*history[-(self.lookback - 1) :], observation]
        try:
            stack = np.stack([_to_filled_values(da) for da in recent], axis=0)
        except ValueError as exc:
            log.warning(
                "nowcast.pysteps.fallback_persistence",
                reason="shape_mismatch",
                error=str(exc),
            )
            return None
        return stack.astype(np.float32)

    def _max_leadtime(self, horizons_minutes: Sequence[int]) -> int:
        """Number of dt-minute steps needed to cover the longest horizon."""
        max_h = max(horizons_minutes) if horizons_minutes else 0
        return max(1, round(max_h / self.dt_minutes))

    def _slice_predictions(
        self,
        *,
        extrapolated_stack: np.ndarray,
        template: xr.DataArray,
        observation_valid_at: datetime,
        horizons_minutes: Sequence[int],
    ) -> Sequence[NowcastPrediction]:
        """Pick frames out of the (T, y, x) stack for each horizon."""
        import xarray as xr_runtime

        n_frames = extrapolated_stack.shape[0]
        predictions: list[NowcastPrediction] = []
        for horizon in horizons_minutes:
            step = max(1, round(horizon / self.dt_minutes))
            # The extrapolated stack indexes from 1 (the first lead time
            # is the next timestep after the current obs). Clamp so we
            # never index past the end.
            idx = min(step - 1, n_frames - 1)
            frame = _restore_below_floor_to_nan(extrapolated_stack[idx])
            predictions.append(
                NowcastPrediction(
                    horizon_minutes=horizon,
                    valid_at=observation_valid_at + timedelta(minutes=horizon),
                    data=xr_runtime.DataArray(
                        frame,
                        coords=template.coords,
                        dims=template.dims,
                        name=template.name,
                        attrs=dict(template.attrs),
                    ),
                )
            )
        log.info(
            "nowcast.pysteps.forecast",
            horizons=list(horizons_minutes),
            stack_shape=tuple(int(s) for s in extrapolated_stack.shape),
        )
        return predictions


# --------------------------------------------------------------------------- #
# Module-level helpers — lazy imports of pysteps so the rest of the codebase  #
# doesn't pay the install cost.                                               #
# --------------------------------------------------------------------------- #


def _to_filled_values(da: xr.DataArray) -> np.ndarray:
    """Extract values; replace NaN / below-threshold with the floor."""
    arr = np.asarray(da.values, dtype=np.float32)
    return np.where(np.isfinite(arr) & (arr >= BELOW_THRESHOLD_DBZ), arr, BELOW_THRESHOLD_DBZ)


def _restore_below_floor_to_nan(arr: np.ndarray) -> np.ndarray:
    """Anywhere the field is at the floor, the colormap will paint it
    transparent. Marking those cells NaN gives the verifier a clean
    'no data' signal that won't drag the calibration mean toward zero."""
    out = arr.copy()
    out[out <= BELOW_THRESHOLD_DBZ + 1e-6] = np.nan
    return out


async def _compute_motion(stack: np.ndarray) -> np.ndarray:
    """Lucas–Kanade dense optical flow over a (T, y, x) stack."""
    import asyncio

    return await asyncio.to_thread(_compute_motion_sync, stack)


def _compute_motion_sync(stack: np.ndarray) -> np.ndarray:
    """The synchronous pySTEPS flow call — runs on a worker thread."""
    from pysteps.motion.lucaskanade import dense_lucaskanade

    return np.asarray(dense_lucaskanade(stack), dtype=np.float32)


async def _extrapolate(
    last_frame: np.ndarray,
    velocity: np.ndarray,
    *,
    n_leadtimes: int,
) -> np.ndarray:
    """Semi-Lagrangian advection of the last frame through the field."""
    import asyncio

    return await asyncio.to_thread(_extrapolate_sync, last_frame, velocity, n_leadtimes)


def _extrapolate_sync(
    last_frame: np.ndarray,
    velocity: np.ndarray,
    n_leadtimes: int,
) -> np.ndarray:
    from pysteps.extrapolation.semilagrangian import extrapolate

    return np.asarray(extrapolate(last_frame, velocity, n_leadtimes), dtype=np.float32)


def _persistence_predictions(
    observation: xr.DataArray,
    observation_valid_at: datetime,
    horizons_minutes: Sequence[int],
) -> Sequence[NowcastPrediction]:
    """Cold-start fallback when pySTEPS can't run yet (no history)."""
    return [
        NowcastPrediction(
            horizon_minutes=horizon,
            valid_at=observation_valid_at + timedelta(minutes=horizon),
            data=observation.copy(),
        )
        for horizon in horizons_minutes
    ]


__all__ = [
    "BELOW_THRESHOLD_DBZ",
    "DEFAULT_DT_MINUTES",
    "DEFAULT_LOOKBACK",
    "PYSTEPS_ALGORITHM",
    "PystepsForecaster",
]
