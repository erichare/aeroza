"""Orchestration: observation grid → forecasts → Zarr → catalog row.

One tick per observation grid. The worker:

1. Reads the source DataArray from the observation's Zarr store.
2. Hands it to the configured :class:`Forecaster` along with the
   default horizons (10/30/60 minutes).
3. Writes each prediction to its own Zarr store under the nowcast
   target root.
4. Upserts a ``mrms_nowcasts`` row per prediction.
5. Publishes one ``aeroza.nowcast.grids.new`` event per persisted
   row so the dispatcher (Phase 4) and any other consumer can react.

The full pipeline is wrapped in a single function so the CLI's
event-triggered and one-shot modes share the same code path. Same
shape as :mod:`aeroza.ingest.mrms_materialise_poll`.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:  # pragma: no cover - typing only
    import xarray as xr

from aeroza.ingest.mrms_grids_models import MrmsGridRow
from aeroza.ingest.mrms_models import MrmsFileRow
from aeroza.ingest.mrms_zarr import MrmsGridLocator
from aeroza.nowcast.engine import (
    DEFAULT_HORIZONS_MINUTES,
    Forecaster,
    NowcastPrediction,
    PersistenceForecaster,
)
from aeroza.nowcast.models import NowcastRow
from aeroza.nowcast.store import upsert_nowcast
from aeroza.query.mrms_grids import find_recent_mrms_grids
from aeroza.shared.db import Database
from aeroza.stream.publisher import (
    NowcastGridPublisher,
    NullNowcastGridPublisher,
)

# Default cap on past observation grids loaded alongside the current
# one. Each forecaster declares its own ``history_depth`` via the
# Protocol; the worker uses the larger of that and this floor so a
# misconfigured forecaster doesn't accidentally starve itself.
DEFAULT_HISTORY_DEPTH: int = 4

log = structlog.get_logger(__name__)


@dataclass(frozen=True, slots=True)
class NowcastTickResult:
    """Per-tick outcome — useful for tests and the CLI's structured log."""

    persisted: tuple[NowcastRow, ...]
    skipped_reason: str | None = None


async def nowcast_observation_grid(
    *,
    db: Database,
    forecaster: Forecaster,
    target_root: str | Path,
    source_grid: MrmsGridRow,
    source_file: MrmsFileRow,
    horizons_minutes: Sequence[int] = DEFAULT_HORIZONS_MINUTES,
    publisher: NowcastGridPublisher | None = None,
) -> NowcastTickResult:
    """Generate forecasts from one observation grid and persist them.

    Caller passes the already-loaded ORM rows for the source grid and
    its catalog file (the worker reads ``zarr_uri`` / ``variable`` /
    ``valid_at`` off them). This keeps the worker test-friendly —
    you can build minimal fixture rows and exercise the pipeline
    without a live ingest path.

    Failures inside the forecaster are caught and surfaced as
    :class:`NowcastTickResult` so the per-event consumer can keep
    going on the next grid.
    """
    pub = publisher if publisher is not None else NullNowcastGridPublisher()

    try:
        observation = await asyncio.to_thread(
            _open_observation_dataarray,
            zarr_uri=source_grid.zarr_uri,
            variable=source_grid.variable,
        )
    except Exception as exc:
        log.exception(
            "nowcast.worker.read_failed",
            zarr_uri=source_grid.zarr_uri,
            error=str(exc),
        )
        return NowcastTickResult(persisted=(), skipped_reason=f"read_failed: {exc}")

    history = await _load_history(
        db=db,
        product=source_file.product,
        level=source_file.level,
        before=source_file.valid_at,
        skip_file_key=source_file.key,
        n=max(DEFAULT_HISTORY_DEPTH, forecaster.history_depth),
    )

    try:
        predictions = await forecaster.forecast(
            observation,
            observation_valid_at=source_file.valid_at,
            horizons_minutes=horizons_minutes,
            history=history,
        )
    except Exception as exc:
        log.exception(
            "nowcast.worker.forecast_failed",
            zarr_uri=source_grid.zarr_uri,
            error=str(exc),
        )
        return NowcastTickResult(persisted=(), skipped_reason=f"forecast_failed: {exc}")

    target_root_path = Path(target_root)
    target_root_path.mkdir(parents=True, exist_ok=True)

    persisted: list[NowcastRow] = []
    for prediction in predictions:
        try:
            row = await _materialise_prediction(
                db=db,
                target_root=target_root_path,
                source_file=source_file,
                source_grid=source_grid,
                algorithm=forecaster.algorithm,
                prediction=prediction,
            )
        except Exception as exc:
            log.exception(
                "nowcast.worker.persist_failed",
                source_file_key=source_file.key,
                horizon_minutes=prediction.horizon_minutes,
                error=str(exc),
            )
            continue
        persisted.append(row)
        try:
            await pub.publish_new_nowcast(row)
        except Exception as exc:
            log.exception(
                "nowcast.worker.publish_failed",
                row_id=str(row.id),
                error=str(exc),
            )

    log.info(
        "nowcast.worker.tick",
        source_file_key=source_file.key,
        algorithm=forecaster.algorithm,
        horizons=list(horizons_minutes),
        persisted=len(persisted),
    )
    return NowcastTickResult(persisted=tuple(persisted))


async def _materialise_prediction(
    *,
    db: Database,
    target_root: Path,
    source_file: MrmsFileRow,
    source_grid: MrmsGridRow,
    algorithm: str,
    prediction: NowcastPrediction,
) -> NowcastRow:
    """Write one prediction's DataArray to Zarr + upsert the catalog row."""
    locator = await asyncio.to_thread(
        _write_prediction_to_zarr,
        target_root=target_root,
        source_file_key=source_file.key,
        algorithm=algorithm,
        horizon_minutes=prediction.horizon_minutes,
        prediction=prediction,
    )
    async with db.sessionmaker() as session:
        row = await upsert_nowcast(
            session,
            source_file_key=source_file.key,
            product=source_file.product,
            level=source_file.level,
            algorithm=algorithm,
            horizon_minutes=prediction.horizon_minutes,
            valid_at=prediction.valid_at,
            zarr_uri=locator.zarr_uri,
            variable=locator.variable,
            dims=locator.dims,
            shape=locator.shape,
            dtype=locator.dtype,
            nbytes=locator.nbytes,
            ensemble_size=prediction.ensemble_size,
        )
        await session.commit()
    return row


async def _load_history(
    *,
    db: Database,
    product: str,
    level: str,
    before: datetime,
    skip_file_key: str,
    n: int,
) -> tuple[xr.DataArray, ...]:
    """Load up to ``n - 1`` past observation grids for the optical-flow
    forecasters' history input.

    Returns DataArrays sorted oldest → newest, excluding the current
    observation (``skip_file_key``). Failures are absorbed — a missing
    Zarr or a stale catalog row drops that frame from the stack but
    doesn't crash the tick.
    """
    if n <= 1:
        return ()
    async with db.sessionmaker() as session:
        views = await find_recent_mrms_grids(
            session,
            product=product,
            level=level,
            n=n,
            at_or_before=before,
        )
    history: list[xr.DataArray] = []
    for view in views:
        if view.file_key == skip_file_key:
            continue
        try:
            da = await asyncio.to_thread(
                _open_observation_dataarray,
                zarr_uri=view.zarr_uri,
                variable=view.variable,
            )
        except Exception as exc:
            log.warning(
                "nowcast.worker.history_skip",
                zarr_uri=view.zarr_uri,
                error=str(exc),
            )
            continue
        history.append(da)
    return tuple(history)


def _open_observation_dataarray(*, zarr_uri: str, variable: str) -> xr.DataArray:
    """Synchronous Zarr read. Loaded eagerly so the data lives outside
    the lazy-task scope of the file handle."""
    import xarray as xr_runtime

    ds = xr_runtime.open_zarr(zarr_uri)
    try:
        if variable not in ds.variables:
            raise KeyError(f"variable {variable!r} not in {zarr_uri}")
        loaded: xr_runtime.DataArray = ds[variable].load()
        return loaded
    finally:
        ds.close()


def _write_prediction_to_zarr(
    *,
    target_root: Path,
    source_file_key: str,
    algorithm: str,
    horizon_minutes: int,
    prediction: NowcastPrediction,
) -> MrmsGridLocator:
    """Write the prediction DataArray to a Zarr store under
    ``<target_root>/nowcasts/<algorithm>/<horizon>m/<rel-source-key>.zarr``.

    Path layout mirrors :func:`aeroza.ingest.mrms_zarr.zarr_path_for`
    so the source-key → store-path mapping is reversible the same way
    the materialised-grid layout is.
    """
    rel = Path(source_file_key)
    if rel.suffix == ".gz":
        rel = rel.with_suffix("")
    if rel.suffix == ".grib2":
        rel = rel.with_suffix("")
    target_path = (
        target_root
        / "nowcasts"
        / algorithm
        / f"{horizon_minutes}m"
        / rel.parent
        / (rel.name + ".zarr")
    )
    target_path.parent.mkdir(parents=True, exist_ok=True)

    da = prediction.data
    variable = str(da.name) if da.name else "value"
    da_named = da if da.name else da.rename(variable)
    da_named.to_zarr(str(target_path), mode="w")

    return MrmsGridLocator(
        file_key=source_file_key,
        zarr_uri=str(target_path),
        variable=variable,
        dims=tuple(str(d) for d in da_named.dims),
        shape=tuple(int(s) for s in da_named.shape),
        dtype=str(da_named.dtype),
        nbytes=int(da_named.nbytes),
    )


__all__ = [
    "NowcastTickResult",
    "PersistenceForecaster",  # re-export for CLI ergonomics
    "nowcast_observation_grid",
]
