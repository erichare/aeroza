"""Verification orchestrator: observation grid → scored forecasts.

Runs once per observation grid event:

1. Find every nowcast whose ``valid_at`` matches the observation's
   (within :data:`MATCH_WINDOW_SECONDS` of jitter) for the same
   product/level.
2. For each match, read both Zarr stores and compute MAE / bias /
   RMSE against the observation.
3. Upsert one ``nowcast_verifications`` row per pair.

Side effects: Zarr reads, DB writes. The session pool gets a fresh
session per upsert so a single bad forecast can't pin a connection.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:  # pragma: no cover - typing only
    import numpy as np

from aeroza.ingest.mrms_grids_models import MrmsGridRow
from aeroza.ingest.mrms_models import MrmsFileRow
from aeroza.nowcast.models import NowcastRow
from aeroza.nowcast.store import find_nowcasts_for_observation
from aeroza.shared.db import Database
from aeroza.verify.metrics import score_deterministic_grids
from aeroza.verify.models import VerificationRow
from aeroza.verify.store import upsert_verification

log = structlog.get_logger(__name__)

# When matching nowcasts to observations by ``valid_at``, allow ±N
# seconds of jitter to absorb the small timestamp drift in MRMS
# publishes. 60 seconds covers normal cases without grouping
# unrelated observations together.
MATCH_WINDOW_SECONDS: int = 60


@dataclass(frozen=True, slots=True)
class VerificationTickResult:
    """Per-tick outcome — useful for tests and the CLI's structured log."""

    scored: tuple[VerificationRow, ...]
    skipped_reason: str | None = None


async def verify_observation(
    *,
    db: Database,
    observation_grid: MrmsGridRow,
    observation_file: MrmsFileRow,
) -> VerificationTickResult:
    """Score every matching forecast against ``observation_grid``."""
    async with db.sessionmaker() as session:
        nowcasts: Sequence[NowcastRow] = await find_nowcasts_for_observation(
            session,
            valid_at=observation_file.valid_at,
            product=observation_file.product,
            level=observation_file.level,
            valid_at_window_seconds=MATCH_WINDOW_SECONDS,
        )

    if not nowcasts:
        log.debug(
            "verify.observation.no_matches",
            observation_file_key=observation_file.key,
            valid_at=observation_file.valid_at.isoformat(),
        )
        return VerificationTickResult(scored=(), skipped_reason="no_matching_nowcasts")

    try:
        observation_array = await asyncio.to_thread(
            _load_zarr_array,
            zarr_uri=observation_grid.zarr_uri,
            variable=observation_grid.variable,
        )
    except Exception as exc:
        log.exception(
            "verify.observation.load_failed",
            observation_file_key=observation_file.key,
            error=str(exc),
        )
        return VerificationTickResult(scored=(), skipped_reason=f"observation_load_failed: {exc}")

    scored: list[VerificationRow] = []
    for nowcast in nowcasts:
        try:
            forecast_array = await asyncio.to_thread(
                _load_zarr_array,
                zarr_uri=nowcast.zarr_uri,
                variable=nowcast.variable,
            )
        except Exception as exc:
            log.exception(
                "verify.nowcast.load_failed",
                nowcast_id=str(nowcast.id),
                error=str(exc),
            )
            continue

        if forecast_array.shape != observation_array.shape:
            log.warning(
                "verify.nowcast.shape_mismatch",
                nowcast_id=str(nowcast.id),
                forecast_shape=forecast_array.shape,
                observation_shape=observation_array.shape,
            )
            continue

        metrics = score_deterministic_grids(forecast_array, observation_array)

        async with db.sessionmaker() as session:
            row = await upsert_verification(
                session,
                nowcast_id=nowcast.id,
                observation_file_key=observation_file.key,
                product=observation_file.product,
                level=observation_file.level,
                algorithm=nowcast.algorithm,
                horizon_minutes=nowcast.forecast_horizon_minutes,
                valid_at=observation_file.valid_at,
                metrics=metrics,
            )
            await session.commit()
        scored.append(row)

    log.info(
        "verify.observation.tick",
        observation_file_key=observation_file.key,
        candidates=len(nowcasts),
        scored=len(scored),
    )
    return VerificationTickResult(scored=tuple(scored))


def _load_zarr_array(*, zarr_uri: str, variable: str) -> np.ndarray:
    """Synchronous Zarr load → numpy array (eagerly materialised)."""
    import numpy as np_runtime
    import xarray as xr

    ds = xr.open_zarr(zarr_uri)
    try:
        if variable not in ds.variables:
            raise KeyError(f"variable {variable!r} not in {zarr_uri}")
        values: np_runtime.ndarray = ds[variable].load().values
        return values
    finally:
        ds.close()


__all__ = [
    "MATCH_WINDOW_SECONDS",
    "VerificationTickResult",
    "verify_observation",
]
