"""Integration tests for the verification worker + calibration aggregator.

Builds a tiny end-to-end pipeline: seed a forecast (Zarr + nowcast row),
then arrival of an observation (Zarr + mrms_grid row) produces one
verification row. The aggregate query then surfaces the right
sample-weighted means.
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
from aeroza.nowcast.engine import PERSISTENCE_ALGORITHM
from aeroza.nowcast.store import upsert_nowcast
from aeroza.shared.db import Database
from aeroza.verify.models import VerificationRow
from aeroza.verify.store import aggregate_calibration
from aeroza.verify.worker import verify_observation

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
async def _truncate_after(integration_db: Database) -> Any:
    yield
    async with integration_db.sessionmaker() as session:
        await session.execute(text("TRUNCATE TABLE mrms_files CASCADE"))
        await session.commit()


def _write_grid(target: Path, *, value: float) -> str:
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


async def _seed_file(
    integration_db: Database,
    *,
    key: str,
    valid_at: datetime,
    product: str = "MergedReflectivityComposite",
    level: str = "00.50",
) -> None:
    async with integration_db.sessionmaker() as session:
        await upsert_mrms_files(
            session,
            (
                MrmsFile(
                    key=key,
                    product=product,
                    level=level,
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
    """Create the observation file + mrms_grids row, return (grid_row, file_row)."""
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


async def _seed_nowcast_for(
    integration_db: Database,
    *,
    source_file_key: str,
    valid_at: datetime,
    horizon_minutes: int,
    zarr_uri: str,
    algorithm: str = PERSISTENCE_ALGORITHM,
) -> None:
    async with integration_db.sessionmaker() as session:
        await upsert_nowcast(
            session,
            source_file_key=source_file_key,
            product="MergedReflectivityComposite",
            level="00.50",
            algorithm=algorithm,
            horizon_minutes=horizon_minutes,
            valid_at=valid_at,
            zarr_uri=zarr_uri,
            variable="reflectivity",
            dims=("latitude", "longitude"),
            shape=(3, 3),
            dtype="float32",
            nbytes=36,
        )
        await session.commit()


# ---------------------------------------------------------------------------
# verify_observation


async def test_perfect_forecast_yields_zero_error_verification(
    integration_db: Database, tmp_path: Path
) -> None:
    """Forecast == observation → MAE/bias/RMSE = 0; sample_count = 9."""
    obs_at = datetime(2026, 5, 1, 12, 30, tzinfo=UTC)
    forecast_source_at = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)

    # Source observation that the forecast was made from.
    await _seed_file(integration_db, key="source", valid_at=forecast_source_at)
    forecast_uri = _write_grid(tmp_path / "fc.zarr", value=42.0)
    await _seed_nowcast_for(
        integration_db,
        source_file_key="source",
        valid_at=obs_at,
        horizon_minutes=30,
        zarr_uri=forecast_uri,
    )

    # The observation that arrives at obs_at.
    obs_uri = _write_grid(tmp_path / "obs.zarr", value=42.0)
    grid_row, file_row = await _seed_observation_grid(
        integration_db,
        file_key="obs",
        valid_at=obs_at,
        zarr_uri=obs_uri,
    )

    result = await verify_observation(
        db=integration_db,
        observation_grid=grid_row,
        observation_file=file_row,
    )
    assert len(result.scored) == 1
    row = result.scored[0]
    assert row.algorithm == PERSISTENCE_ALGORITHM
    assert row.forecast_horizon_minutes == 30
    assert row.mae == 0.0
    assert row.bias == 0.0
    assert row.rmse == 0.0
    assert row.sample_count == 9


async def test_offset_forecast_yields_pure_bias(integration_db: Database, tmp_path: Path) -> None:
    """Forecast 5dBZ above observation everywhere → bias = 5, MAE = 5, RMSE = 5."""
    obs_at = datetime(2026, 5, 1, 12, 30, tzinfo=UTC)
    await _seed_file(integration_db, key="source", valid_at=obs_at - timedelta(minutes=30))
    forecast_uri = _write_grid(tmp_path / "fc.zarr", value=45.0)
    await _seed_nowcast_for(
        integration_db,
        source_file_key="source",
        valid_at=obs_at,
        horizon_minutes=30,
        zarr_uri=forecast_uri,
    )

    obs_uri = _write_grid(tmp_path / "obs.zarr", value=40.0)
    grid_row, file_row = await _seed_observation_grid(
        integration_db, file_key="obs", valid_at=obs_at, zarr_uri=obs_uri
    )

    result = await verify_observation(
        db=integration_db,
        observation_grid=grid_row,
        observation_file=file_row,
    )
    assert len(result.scored) == 1
    row = result.scored[0]
    assert row.bias == pytest.approx(5.0)
    assert row.mae == pytest.approx(5.0)
    assert row.rmse == pytest.approx(5.0)


async def test_observation_without_matching_forecasts_is_skipped(
    integration_db: Database, tmp_path: Path
) -> None:
    obs_at = datetime(2026, 5, 1, 12, 30, tzinfo=UTC)
    obs_uri = _write_grid(tmp_path / "obs.zarr", value=10.0)
    grid_row, file_row = await _seed_observation_grid(
        integration_db, file_key="obs", valid_at=obs_at, zarr_uri=obs_uri
    )
    result = await verify_observation(
        db=integration_db,
        observation_grid=grid_row,
        observation_file=file_row,
    )
    assert result.scored == ()
    assert result.skipped_reason == "no_matching_nowcasts"


async def test_verify_idempotent_via_unique_constraint(
    integration_db: Database, tmp_path: Path
) -> None:
    """Re-running verify on the same (forecast, observation) updates in place."""
    obs_at = datetime(2026, 5, 1, 12, 30, tzinfo=UTC)
    await _seed_file(integration_db, key="source", valid_at=obs_at - timedelta(minutes=30))
    forecast_uri = _write_grid(tmp_path / "fc.zarr", value=42.0)
    await _seed_nowcast_for(
        integration_db,
        source_file_key="source",
        valid_at=obs_at,
        horizon_minutes=30,
        zarr_uri=forecast_uri,
    )
    obs_uri = _write_grid(tmp_path / "obs.zarr", value=42.0)
    grid_row, file_row = await _seed_observation_grid(
        integration_db, file_key="obs", valid_at=obs_at, zarr_uri=obs_uri
    )

    await verify_observation(
        db=integration_db, observation_grid=grid_row, observation_file=file_row
    )
    await verify_observation(
        db=integration_db, observation_grid=grid_row, observation_file=file_row
    )

    async with integration_db.sessionmaker() as session:
        rows = (await session.execute(select(VerificationRow))).scalars().all()
    assert len(rows) == 1


# ---------------------------------------------------------------------------
# aggregate_calibration


async def test_calibration_sample_weighted_means_across_buckets(
    integration_db: Database, tmp_path: Path
) -> None:
    """Two verifications under one (algorithm, horizon) → sample-weighted mean."""
    obs_at = datetime(2026, 5, 1, 12, 30, tzinfo=UTC)
    await _seed_file(integration_db, key="src1", valid_at=obs_at - timedelta(minutes=30))
    await _seed_file(integration_db, key="src2", valid_at=obs_at - timedelta(minutes=30))

    fc1 = _write_grid(tmp_path / "fc1.zarr", value=45.0)  # bias=+5 vs obs=40
    fc2 = _write_grid(tmp_path / "fc2.zarr", value=42.0)  # bias=+2 vs obs=40
    await _seed_nowcast_for(
        integration_db,
        source_file_key="src1",
        valid_at=obs_at,
        horizon_minutes=30,
        zarr_uri=fc1,
    )
    await _seed_nowcast_for(
        integration_db,
        source_file_key="src2",
        valid_at=obs_at,
        horizon_minutes=30,
        zarr_uri=fc2,
    )

    obs_uri = _write_grid(tmp_path / "obs.zarr", value=40.0)
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

    # One bucket: (persistence, 30min). Each verification has 9 samples;
    # both biases are positive (5 and 2). Weighted mean = (5*9 + 2*9) / 18 = 3.5.
    assert len(buckets) == 1
    bucket = buckets[0]
    assert bucket.algorithm == "persistence"
    assert bucket.forecast_horizon_minutes == 30
    assert bucket.verification_count == 2
    assert bucket.sample_count == 18
    assert bucket.bias_mean == pytest.approx(3.5)
    assert bucket.mae_mean == pytest.approx(3.5)


async def test_calibration_groups_separate_horizons(
    integration_db: Database, tmp_path: Path
) -> None:
    """Different horizons → different buckets."""
    obs_at_30 = datetime(2026, 5, 1, 12, 30, tzinfo=UTC)
    obs_at_60 = datetime(2026, 5, 1, 13, 0, tzinfo=UTC)
    await _seed_file(integration_db, key="src", valid_at=obs_at_30 - timedelta(minutes=30))
    fc = _write_grid(tmp_path / "fc.zarr", value=40.0)
    await _seed_nowcast_for(
        integration_db,
        source_file_key="src",
        valid_at=obs_at_30,
        horizon_minutes=30,
        zarr_uri=fc,
    )
    await _seed_nowcast_for(
        integration_db,
        source_file_key="src",
        valid_at=obs_at_60,
        horizon_minutes=60,
        zarr_uri=fc,
    )

    obs_uri = _write_grid(tmp_path / "obs.zarr", value=40.0)
    grid_30, file_30 = await _seed_observation_grid(
        integration_db, file_key="obs30", valid_at=obs_at_30, zarr_uri=obs_uri
    )
    grid_60, file_60 = await _seed_observation_grid(
        integration_db, file_key="obs60", valid_at=obs_at_60, zarr_uri=obs_uri
    )

    await verify_observation(db=integration_db, observation_grid=grid_30, observation_file=file_30)
    await verify_observation(db=integration_db, observation_grid=grid_60, observation_file=file_60)

    async with integration_db.sessionmaker() as session:
        buckets = await aggregate_calibration(
            session,
            since=datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
            window_hours=72,
        )

    assert {(b.algorithm, b.forecast_horizon_minutes) for b in buckets} == {
        ("persistence", 30),
        ("persistence", 60),
    }


async def test_calibration_empty_when_no_verifications(
    integration_db: Database,
) -> None:
    async with integration_db.sessionmaker() as session:
        buckets = await aggregate_calibration(session, window_hours=24)
    assert buckets == ()
