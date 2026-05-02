"""Predictive nowcasting — Phase 3.

The pipeline runs on every materialised MRMS grid event: read the
just-arrived observation, ask a :class:`Forecaster` for predictions
at 10/30/60-minute horizons, write each prediction as its own Zarr
store, and persist a locator row in ``mrms_nowcasts``.

The plan calls for **pySTEPS** as the v1 algorithm, with **NowcastNet**
in v2 and a custom fine-tune in v3. We ship a :class:`PersistenceForecaster`
as the structural placeholder so the pipeline + verification scaffolding
can land first; persistence is also the documented baseline (§7 of the
plan) that any real algorithm has to beat — so the verification numbers
the calibration page reports against persistence are immediately useful.

Swapping in pySTEPS later is a one-class change inside this module: the
worker, store, NATS event, CLI, and HTTP routes all stay put.
"""

from aeroza.nowcast.engine import (
    DEFAULT_HORIZONS_MINUTES,
    PERSISTENCE_ALGORITHM,
    Forecaster,
    PersistenceForecaster,
)

__all__ = [
    "DEFAULT_HORIZONS_MINUTES",
    "PERSISTENCE_ALGORITHM",
    "Forecaster",
    "PersistenceForecaster",
]
