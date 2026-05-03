"""Nowcast forecasters — pluggable, with persistence as the v1 default.

Two surfaces:

- :class:`Forecaster` Protocol — async ``forecast(observation,
  horizons_minutes) -> Sequence[NowcastPrediction]``. The worker
  knows nothing about the underlying algorithm; swapping pySTEPS in
  later is one new class implementing this Protocol.
- :class:`PersistenceForecaster` — returns the input grid unchanged
  for every requested horizon. The plan's documented baseline (§7);
  also the integration-test default so the test suite doesn't need
  pySTEPS / scipy.

A :class:`NowcastPrediction` carries the forecast as an
:class:`xarray.DataArray` plus its target ``valid_at``. The worker
serialises the array to Zarr and persists the locator; the algorithm
itself is responsible only for the math.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Final, Protocol

import structlog

if TYPE_CHECKING:  # pragma: no cover - typing only
    import xarray as xr

log = structlog.get_logger(__name__)

# Lead times the worker requests by default. Match the plan §7
# verification targets (10-min, 30-min, 60-min reliability diagrams).
DEFAULT_HORIZONS_MINUTES: Final[tuple[int, ...]] = (10, 30, 60)

# Algorithm tag persisted on every NowcastRow this forecaster produces.
# Mirrors the plan's algorithm naming so the calibration aggregator
# can group head-to-head: "persistence" / "pysteps" / "nowcastnet".
PERSISTENCE_ALGORITHM: Final[str] = "persistence"


@dataclass(frozen=True, slots=True)
class NowcastPrediction:
    """One forecast at one horizon.

    ``data`` is the predicted DataArray. For a deterministic
    forecaster (``ensemble_size == 1``) it has the same shape / coords
    as the observation. For an ensemble (``ensemble_size > 1``) it
    has a leading ``member`` dim of length ``ensemble_size`` —
    members are stacked oldest-or-by-index-first along that axis.

    ``valid_at`` is the wall-clock time this prediction is for
    (observation valid_at + horizon).

    The worker persists ``ensemble_size`` on the row so the verifier
    knows whether to score deterministic-only metrics (MAE/POD/etc.)
    or also probabilistic metrics (Brier/CRPS). Default 1 keeps the
    deterministic forecasters' construction call sites unchanged.
    """

    horizon_minutes: int
    valid_at: datetime
    data: xr.DataArray
    ensemble_size: int = 1


class Forecaster(Protocol):
    """The worker's view of "make some forecasts".

    Concrete implementations: :class:`PersistenceForecaster` (v1
    default), ``PystepsForecaster`` / ``NowcastNetForecaster`` (later).
    """

    @property
    def algorithm(self) -> str:  # pragma: no cover - interface
        """Tag persisted on every produced row (e.g. ``"persistence"``)."""
        ...

    @property
    def history_depth(self) -> int:  # pragma: no cover - interface
        """Number of past observations the worker should fetch alongside
        the current one. ``1`` means "no history needed" (persistence);
        optical-flow forecasters want their lookback; lagged ensembles
        want one frame per member. Defaults are conservative — over-
        fetching is one extra Zarr open per skipped frame, no big deal.
        """
        ...

    async def forecast(
        self,
        observation: xr.DataArray,
        observation_valid_at: datetime,
        horizons_minutes: Sequence[int],
        history: Sequence[xr.DataArray] | None = None,
    ) -> Sequence[NowcastPrediction]:  # pragma: no cover - interface
        """``history`` is an oldest→newest sequence of past observations.

        Persistence ignores it. Optical-flow forecasters (pySTEPS,
        NowcastNet) need a few past frames to compute motion. The
        worker passes whatever history the catalog has at tick time;
        an implementation is responsible for falling back gracefully
        if the history is shorter than required.
        """
        ...


class PersistenceForecaster:
    """Forecast = observation, copied to each requested lead time.

    The plan's §7 baseline ("Brier skill score must beat persistence at
    every horizon"). Computationally trivial; useful as a structural
    placeholder while the pySTEPS integration matures, and as a
    forever-baseline the verification page reports against.

    No-op for the data itself — we deep-clone via ``.copy()`` so the
    caller can mutate predictions without touching the input. Each
    prediction's ``valid_at`` is bumped by its horizon.
    """

    @property
    def algorithm(self) -> str:
        return PERSISTENCE_ALGORITHM

    @property
    def history_depth(self) -> int:
        # Persistence ignores past frames; ask for none.
        return 1

    async def forecast(
        self,
        observation: xr.DataArray,
        observation_valid_at: datetime,
        horizons_minutes: Sequence[int],
        history: Sequence[xr.DataArray] | None = None,
    ) -> Sequence[NowcastPrediction]:
        # `history` is part of the Protocol signature so optical-flow
        # forecasters can use it; persistence has no use for past frames.
        del history
        predictions: list[NowcastPrediction] = []
        for horizon in horizons_minutes:
            valid_at = observation_valid_at + timedelta(minutes=horizon)
            predictions.append(
                NowcastPrediction(
                    horizon_minutes=horizon,
                    valid_at=valid_at,
                    data=observation.copy(),
                )
            )
        log.debug(
            "nowcast.persistence.forecast",
            horizons=list(horizons_minutes),
            observation_shape=tuple(int(s) for s in observation.shape),
        )
        return predictions


__all__ = [
    "DEFAULT_HORIZONS_MINUTES",
    "PERSISTENCE_ALGORITHM",
    "Forecaster",
    "NowcastPrediction",
    "PersistenceForecaster",
]
