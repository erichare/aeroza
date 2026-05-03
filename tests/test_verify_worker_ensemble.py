"""Integration tests for the ensemble path through the verifier.

Mirrors the deterministic flow in :mod:`tests.test_verify_worker` but
seeds an ensemble Zarr (leading ``member`` dim) plus a nowcast row
with ``ensemble_size > 1``. Asserts:

* The verifier writes ``brier_score`` / ``crps`` / ``ensemble_size``.
* The deterministic columns reflect the *member-mean* forecast.
* The calibration aggregator surfaces the new fields with proper
  null/zero semantics for buckets that contain no ensemble rows.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pytest
import xarray as xr
from sqlalchemy import select, text

from aeroza.ingest.mrms import MrmsFile
from aeroza.ingest.mrms_grids_store import upsert_mrms_grid
from aeroza.ingest.mrms_models import MrmsFileRow
from aeroza.ingest.mrms_store import upsert_mrms_files
from aeroza.ingest.mrms_zarr import MrmsGridLocator
from aeroza.nowcast.lagged_ensemble import LAGGED_ENSEMBLE_ALGORITHM
from aeroza.nowcast.store import upsert_nowcast
from aeroza.shared.db import Database
from aeroza.verify.store import aggregate_calibration, aggregate_calibration_series
from aeroza.verify.worker import verify_observation

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
async def _truncate_after(integration_db: Database) -> Any:
    yield
    async with integration_db.sessionmaker() as session:
        await session.execute(text("TRUNCATE TABLE mrms_files CASCADE"))
        await session.commit()


def _write_observation_grid(target: Path, *, value: float) -> str:
    da = xr.DataArray(
        np.full((3, 3), value, dtype=np.float32),
        coords={
            "latitude": [29.5, 29.76, 30.0],
            "longitude": [-96.0, -95.37, -95.0],
        },
        dims=("latitude", "longitude"),
        name="reflectivity",
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    da.to_zarr(str(target), mode="w")
    return str(target)


def _write_ensemble_grid(target: Path, *, member_values: list[float]) -> str:
    """Write a Zarr with shape (M, 3, 3) where each member is a constant grid."""
    n_members = len(member_values)
    arr = np.stack([np.full((3, 3), v, dtype=np.float32) for v in member_values], axis=0)
    da = xr.DataArray(
        arr,
        coords={
            "member": np.arange(n_members),
            "latitude": [29.5, 29.76, 30.0],
            "longitude": [-96.0, -95.37, -95.0],
        },
        dims=("member", "latitude", "longitude"),
        name="reflectivity",
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    da.to_zarr(str(target), mode="w")
    return str(target)


async def _seed_file(
    integration_db: Database,
    *,
    key: str,
    valid_at: datetime,
) -> None:
    async with integration_db.sessionmaker() as session:
        await upsert_mrms_files(
            session,
            (
                MrmsFile(
                    key=key,
                    product="MergedReflectivityComposite",
                    level="00.50",
                    valid_at=valid_at,
                    size_bytes=1_000,
                    etag="e",
                ),
            ),
        )
        await session.commit()


async def _seed_observation_grid(
    integration_db: Database,
    *,
    file_key: str,
    valid_at: datetime,
    zarr_uri: str,
) -> tuple[Any, Any]:
    await _seed_file(integration_db, key=file_key, valid_at=valid_at)
    locator = MrmsGridLocator(
        file_key=file_key,
        zarr_uri=zarr_uri,
        variable="reflectivity",
        dims=("latitude", "longitude"),
        shape=(3, 3),
        dtype="float32",
        nbytes=3 * 3 * 4,
    )
    async with integration_db.sessionmaker() as session:
        await upsert_mrms_grid(session, locator)
        await session.commit()

    async with integration_db.sessionmaker() as session:
        from aeroza.ingest.mrms_grids_models import MrmsGridRow

        result = await session.execute(
            select(MrmsGridRow, MrmsFileRow)
            .join(MrmsFileRow, MrmsFileRow.key == MrmsGridRow.file_key)
            .where(MrmsGridRow.file_key == file_key)
        )
        return result.one()


async def _seed_ensemble_nowcast(
    integration_db: Database,
    *,
    source_file_key: str,
    valid_at: datetime,
    horizon_minutes: int,
    zarr_uri: str,
    ensemble_size: int,
) -> None:
    async with integration_db.sessionmaker() as session:
        await upsert_nowcast(
            session,
            source_file_key=source_file_key,
            product="MergedReflectivityComposite",
            level="00.50",
            algorithm=LAGGED_ENSEMBLE_ALGORITHM,
            horizon_minutes=horizon_minutes,
            valid_at=valid_at,
            zarr_uri=zarr_uri,
            variable="reflectivity",
            dims=("member", "latitude", "longitude"),
            shape=(ensemble_size, 3, 3),
            dtype="float32",
            nbytes=ensemble_size * 3 * 3 * 4,
            ensemble_size=ensemble_size,
        )
        await session.commit()


async def test_ensemble_verification_records_brier_crps_and_ensemble_size(
    integration_db: Database, tmp_path: Path
) -> None:
    """4-member ensemble where 2 members are above and 2 below the threshold,
    observation above the threshold → P(event)=0.5, observed=1, Brier=0.25."""
    obs_at = datetime(2026, 5, 1, 12, 30, tzinfo=UTC)
    await _seed_file(integration_db, key="src", valid_at=obs_at - timedelta(minutes=30))

    forecast_uri = _write_ensemble_grid(
        tmp_path / "fc.zarr",
        member_values=[40.0, 40.0, 10.0, 10.0],
    )
    await _seed_ensemble_nowcast(
        integration_db,
        source_file_key="src",
        valid_at=obs_at,
        horizon_minutes=30,
        zarr_uri=forecast_uri,
        ensemble_size=4,
    )

    obs_uri = _write_observation_grid(tmp_path / "obs.zarr", value=40.0)
    grid_row, file_row = await _seed_observation_grid(
        integration_db, file_key="obs", valid_at=obs_at, zarr_uri=obs_uri
    )

    result = await verify_observation(
        db=integration_db, observation_grid=grid_row, observation_file=file_row
    )
    assert len(result.scored) == 1
    row = result.scored[0]
    assert row.algorithm == LAGGED_ENSEMBLE_ALGORITHM
    assert row.ensemble_size == 4
    # Brier: (0.5 - 1.0)^2 = 0.25 at every cell.
    assert row.brier_score == pytest.approx(0.25)
    # CRPS for two-member groups {40, 40} and {10, 10} against obs=40 averaged:
    # per cell mean across members: spread term = 0 within each pair, but across
    # all 4 members sorted [10, 10, 40, 40]:
    #   (1/4) Σ |x_i - 40| = (30 + 30 + 0 + 0) / 4 = 15.0
    #   spread = 1/(2*4*3) Σ_i Σ_j |x_i - x_j| = 1/24 * (0+0+30+30 + 0+0+30+30 + 30+30+0+0 + 30+30+0+0)
    #          = 1/24 * 240 = 10.0
    #   CRPS = 15 - 10 = 5.0
    assert row.crps == pytest.approx(5.0)
    # Member-mean forecast = (40+40+10+10)/4 = 25.0; bias = -15 (forecast < obs).
    assert row.bias == pytest.approx(-15.0)
    assert row.mae == pytest.approx(15.0)
    assert row.sample_count == 9


async def test_ensemble_verification_perfect_certainty(
    integration_db: Database, tmp_path: Path
) -> None:
    """All members equal to the observation → Brier=0, CRPS=0, MAE=0."""
    obs_at = datetime(2026, 5, 1, 12, 30, tzinfo=UTC)
    await _seed_file(integration_db, key="src", valid_at=obs_at - timedelta(minutes=30))

    forecast_uri = _write_ensemble_grid(
        tmp_path / "fc.zarr",
        member_values=[40.0, 40.0, 40.0],
    )
    await _seed_ensemble_nowcast(
        integration_db,
        source_file_key="src",
        valid_at=obs_at,
        horizon_minutes=30,
        zarr_uri=forecast_uri,
        ensemble_size=3,
    )

    obs_uri = _write_observation_grid(tmp_path / "obs.zarr", value=40.0)
    grid_row, file_row = await _seed_observation_grid(
        integration_db, file_key="obs", valid_at=obs_at, zarr_uri=obs_uri
    )

    result = await verify_observation(
        db=integration_db, observation_grid=grid_row, observation_file=file_row
    )
    row = result.scored[0]
    assert row.brier_score == pytest.approx(0.0)
    assert row.crps == pytest.approx(0.0)
    assert row.mae == pytest.approx(0.0)
    assert row.ensemble_size == 3


async def test_calibration_aggregate_surfaces_brier_and_crps(
    integration_db: Database, tmp_path: Path
) -> None:
    """A bucket containing one ensemble row exposes brierMean / crpsMean /
    ensembleSize; the deterministic counterpart in a sibling bucket stays null."""
    obs_at = datetime(2026, 5, 1, 12, 30, tzinfo=UTC)
    await _seed_file(integration_db, key="src", valid_at=obs_at - timedelta(minutes=30))

    forecast_uri = _write_ensemble_grid(
        tmp_path / "fc.zarr",
        member_values=[40.0, 40.0, 10.0, 10.0],
    )
    await _seed_ensemble_nowcast(
        integration_db,
        source_file_key="src",
        valid_at=obs_at,
        horizon_minutes=30,
        zarr_uri=forecast_uri,
        ensemble_size=4,
    )

    obs_uri = _write_observation_grid(tmp_path / "obs.zarr", value=40.0)
    grid_row, file_row = await _seed_observation_grid(
        integration_db, file_key="obs", valid_at=obs_at, zarr_uri=obs_uri
    )

    await verify_observation(
        db=integration_db, observation_grid=grid_row, observation_file=file_row
    )

    async with integration_db.sessionmaker() as session:
        buckets = await aggregate_calibration(
            session,
            since=datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
            window_hours=72,
        )
    ensemble_buckets = [b for b in buckets if b.algorithm == LAGGED_ENSEMBLE_ALGORITHM]
    assert len(ensemble_buckets) == 1
    bucket = ensemble_buckets[0]
    assert bucket.ensemble_size == 4
    assert bucket.brier_sample_count == 9
    assert bucket.brier_mean == pytest.approx(0.25)
    assert bucket.crps_mean == pytest.approx(5.0)


async def test_calibration_aggregate_brier_null_when_no_ensemble_rows(
    integration_db: Database, tmp_path: Path
) -> None:
    """Deterministic-only bucket → brierMean / crpsMean / ensembleSize null."""
    from aeroza.nowcast.engine import PERSISTENCE_ALGORITHM

    obs_at = datetime(2026, 5, 1, 12, 30, tzinfo=UTC)
    await _seed_file(integration_db, key="src", valid_at=obs_at - timedelta(minutes=30))

    forecast_uri = _write_observation_grid(tmp_path / "fc.zarr", value=40.0)
    async with integration_db.sessionmaker() as session:
        await upsert_nowcast(
            session,
            source_file_key="src",
            product="MergedReflectivityComposite",
            level="00.50",
            algorithm=PERSISTENCE_ALGORITHM,
            horizon_minutes=30,
            valid_at=obs_at,
            zarr_uri=forecast_uri,
            variable="reflectivity",
            dims=("latitude", "longitude"),
            shape=(3, 3),
            dtype="float32",
            nbytes=36,
        )
        await session.commit()

    obs_uri = _write_observation_grid(tmp_path / "obs.zarr", value=40.0)
    grid_row, file_row = await _seed_observation_grid(
        integration_db, file_key="obs", valid_at=obs_at, zarr_uri=obs_uri
    )

    await verify_observation(
        db=integration_db, observation_grid=grid_row, observation_file=file_row
    )

    async with integration_db.sessionmaker() as session:
        buckets = await aggregate_calibration(
            session,
            since=datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
            window_hours=72,
        )
    persistence = [b for b in buckets if b.algorithm == PERSISTENCE_ALGORITHM]
    assert len(persistence) == 1
    bucket = persistence[0]
    assert bucket.ensemble_size is None
    assert bucket.brier_mean is None
    assert bucket.crps_mean is None
    assert bucket.brier_sample_count == 0


async def test_calibration_series_carries_ensemble_metrics(
    integration_db: Database, tmp_path: Path
) -> None:
    obs_at = datetime(2026, 5, 1, 12, 30, tzinfo=UTC)
    await _seed_file(integration_db, key="src", valid_at=obs_at - timedelta(minutes=30))

    forecast_uri = _write_ensemble_grid(
        tmp_path / "fc.zarr",
        member_values=[40.0, 40.0, 10.0, 10.0],
    )
    await _seed_ensemble_nowcast(
        integration_db,
        source_file_key="src",
        valid_at=obs_at,
        horizon_minutes=30,
        zarr_uri=forecast_uri,
        ensemble_size=4,
    )

    obs_uri = _write_observation_grid(tmp_path / "obs.zarr", value=40.0)
    grid_row, file_row = await _seed_observation_grid(
        integration_db, file_key="obs", valid_at=obs_at, zarr_uri=obs_uri
    )

    await verify_observation(
        db=integration_db, observation_grid=grid_row, observation_file=file_row
    )

    async with integration_db.sessionmaker() as session:
        points = await aggregate_calibration_series(
            session,
            since=datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
            window_hours=72,
            bucket_seconds=3600,
        )
    ensemble_points = [p for p in points if p.algorithm == LAGGED_ENSEMBLE_ALGORITHM]
    assert len(ensemble_points) == 1
    point = ensemble_points[0]
    assert point.ensemble_size == 4
    assert point.brier_mean == pytest.approx(0.25)
    assert point.crps_mean == pytest.approx(5.0)


async def test_calibration_reliability_bins_summed_across_rows(
    integration_db: Database, tmp_path: Path
) -> None:
    """End-to-end: an ensemble verification writes a 10-bin histogram
    to JSONB; the reliability aggregator reads that back and groups
    by (algorithm, horizon). The 4-member ensemble produces P=0.5 on
    every cell against an obs that crosses the threshold, so the
    0.5–0.6 bin gets count=9, observed=9 and the rest are empty."""
    from aeroza.verify.store import aggregate_calibration_reliability

    obs_at = datetime(2026, 5, 1, 12, 30, tzinfo=UTC)
    await _seed_file(integration_db, key="src", valid_at=obs_at - timedelta(minutes=30))

    # Half the members above threshold, observation everywhere above
    # → P=0.5 every cell, observed=1 every cell.
    forecast_uri = _write_ensemble_grid(
        tmp_path / "fc.zarr",
        member_values=[40.0, 40.0, 10.0, 10.0],
    )
    await _seed_ensemble_nowcast(
        integration_db,
        source_file_key="src",
        valid_at=obs_at,
        horizon_minutes=30,
        zarr_uri=forecast_uri,
        ensemble_size=4,
    )

    obs_uri = _write_observation_grid(tmp_path / "obs.zarr", value=40.0)
    grid_row, file_row = await _seed_observation_grid(
        integration_db, file_key="obs", valid_at=obs_at, zarr_uri=obs_uri
    )

    await verify_observation(
        db=integration_db, observation_grid=grid_row, observation_file=file_row
    )

    async with integration_db.sessionmaker() as session:
        rows = await aggregate_calibration_reliability(
            session,
            since=datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
            window_hours=72,
        )
    ensemble_rows = [r for r in rows if r.algorithm == LAGGED_ENSEMBLE_ALGORITHM]
    assert len(ensemble_rows) == 1
    row = ensemble_rows[0]
    assert len(row.bins) == 10
    bin_05 = next(b for b in row.bins if abs(b.lower - 0.5) < 1e-6)
    assert bin_05.count == 9
    assert bin_05.observed == 9
    assert bin_05.mean_prob == pytest.approx(0.5)
    # Other bins empty.
    for bin_ in row.bins:
        if abs(bin_.lower - 0.5) < 1e-6:
            continue
        assert bin_.count == 0


async def test_calibration_reliability_skipped_when_no_ensemble_rows(
    integration_db: Database, tmp_path: Path
) -> None:
    """Deterministic-only window → reliability aggregator returns no rows."""
    from aeroza.nowcast.engine import PERSISTENCE_ALGORITHM
    from aeroza.verify.store import aggregate_calibration_reliability

    obs_at = datetime(2026, 5, 1, 12, 30, tzinfo=UTC)
    await _seed_file(integration_db, key="src", valid_at=obs_at - timedelta(minutes=30))

    forecast_uri = _write_observation_grid(tmp_path / "fc.zarr", value=40.0)
    async with integration_db.sessionmaker() as session:
        await upsert_nowcast(
            session,
            source_file_key="src",
            product="MergedReflectivityComposite",
            level="00.50",
            algorithm=PERSISTENCE_ALGORITHM,
            horizon_minutes=30,
            valid_at=obs_at,
            zarr_uri=forecast_uri,
            variable="reflectivity",
            dims=("latitude", "longitude"),
            shape=(3, 3),
            dtype="float32",
            nbytes=36,
        )
        await session.commit()

    obs_uri = _write_observation_grid(tmp_path / "obs.zarr", value=40.0)
    grid_row, file_row = await _seed_observation_grid(
        integration_db, file_key="obs", valid_at=obs_at, zarr_uri=obs_uri
    )
    await verify_observation(
        db=integration_db, observation_grid=grid_row, observation_file=file_row
    )

    async with integration_db.sessionmaker() as session:
        rows = await aggregate_calibration_reliability(
            session,
            since=datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
            window_hours=72,
        )
    assert rows == ()
