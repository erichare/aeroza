"""Integration tests for the nowcast worker.

Drives :func:`nowcast_observation_grid` end-to-end against real
Postgres + Zarr stores under ``tmp_path``. The persistence forecaster
is the v1 algorithm; tests verify that it produces a row per horizon,
the ``valid_at`` math is right, the locator round-trips, and the
publisher receives one event per persisted row.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pytest
import xarray as xr
from sqlalchemy import select, text

from aeroza.ingest.mrms import MrmsFile
from aeroza.ingest.mrms_grids_store import upsert_mrms_grid
from aeroza.ingest.mrms_store import upsert_mrms_files
from aeroza.ingest.mrms_zarr import MrmsGridLocator
from aeroza.nowcast.engine import PersistenceForecaster
from aeroza.nowcast.models import NowcastRow
from aeroza.nowcast.worker import nowcast_observation_grid
from aeroza.shared.db import Database
from aeroza.stream.publisher import InMemoryNowcastGridPublisher

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
async def _truncate_after(integration_db: Database) -> Any:
    yield
    async with integration_db.sessionmaker() as session:
        await session.execute(text("TRUNCATE TABLE mrms_files CASCADE"))
        await session.commit()


def _write_obs_grid(target: Path, *, value: float = 42.0) -> str:
    """Write a 3x3 observation Zarr grid."""
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


async def _seed_observation(
    integration_db: Database, *, file_key: str, zarr_uri: str, valid_at: datetime
) -> tuple[Any, Any]:
    """Insert mrms_files + mrms_grids rows for the observation."""
    file = MrmsFile(
        key=file_key,
        product="MergedReflectivityComposite",
        level="00.50",
        valid_at=valid_at,
        size_bytes=1_000,
        etag="e",
    )
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
        await upsert_mrms_files(session, (file,))
        await upsert_mrms_grid(session, locator)
        await session.commit()

    async with integration_db.sessionmaker() as session:
        from aeroza.ingest.mrms_grids_models import MrmsGridRow
        from aeroza.ingest.mrms_models import MrmsFileRow

        result = await session.execute(
            select(MrmsGridRow, MrmsFileRow)
            .join(MrmsFileRow, MrmsFileRow.key == MrmsGridRow.file_key)
            .where(MrmsGridRow.file_key == file_key)
        )
        grid_row, file_row = result.one()
    return grid_row, file_row


async def test_persistence_creates_row_per_horizon(
    integration_db: Database, tmp_path: Path
) -> None:
    obs_uri = _write_obs_grid(tmp_path / "obs.zarr", value=42.0)
    grid_row, file_row = await _seed_observation(
        integration_db,
        file_key="CONUS/X/MRMS_X_120000.grib2.gz",
        zarr_uri=obs_uri,
        valid_at=datetime(2026, 5, 1, 12, 0, tzinfo=UTC),
    )

    publisher = InMemoryNowcastGridPublisher()
    result = await nowcast_observation_grid(
        db=integration_db,
        forecaster=PersistenceForecaster(),
        target_root=tmp_path,
        source_grid=grid_row,
        source_file=file_row,
        horizons_minutes=(10, 30, 60),
        publisher=publisher,
    )

    assert len(result.persisted) == 3
    assert result.skipped_reason is None
    assert {r.forecast_horizon_minutes for r in result.persisted} == {10, 30, 60}
    assert {r.algorithm for r in result.persisted} == {"persistence"}

    # Each persisted row's valid_at = obs_valid_at + horizon.
    by_horizon = {r.forecast_horizon_minutes: r for r in result.persisted}
    assert by_horizon[10].valid_at == datetime(2026, 5, 1, 12, 10, tzinfo=UTC)
    assert by_horizon[30].valid_at == datetime(2026, 5, 1, 12, 30, tzinfo=UTC)
    assert by_horizon[60].valid_at == datetime(2026, 5, 1, 13, 0, tzinfo=UTC)

    # Publisher saw one event per row.
    assert len(publisher.published) == 3


async def test_persistence_predictions_match_observation(
    integration_db: Database, tmp_path: Path
) -> None:
    """The Zarr stores the worker writes should equal the observation."""
    obs_uri = _write_obs_grid(tmp_path / "obs.zarr", value=55.0)
    grid_row, file_row = await _seed_observation(
        integration_db,
        file_key="CONUS/X/MRMS_X_120000.grib2.gz",
        zarr_uri=obs_uri,
        valid_at=datetime(2026, 5, 1, 12, 0, tzinfo=UTC),
    )

    result = await nowcast_observation_grid(
        db=integration_db,
        forecaster=PersistenceForecaster(),
        target_root=tmp_path,
        source_grid=grid_row,
        source_file=file_row,
        horizons_minutes=(10,),
    )
    assert len(result.persisted) == 1
    nowcast_uri = result.persisted[0].zarr_uri

    forecast = xr.open_zarr(nowcast_uri).reflectivity.load().values
    np.testing.assert_array_equal(forecast, np.full((3, 3), 55.0, dtype=np.float32))


async def test_re_running_overwrites_via_unique_constraint(
    integration_db: Database, tmp_path: Path
) -> None:
    """Re-running the worker against the same source should idempotently
    update — same UNIQUE (source, algorithm, horizon)."""
    obs_uri = _write_obs_grid(tmp_path / "obs.zarr", value=10.0)
    grid_row, file_row = await _seed_observation(
        integration_db,
        file_key="CONUS/X/MRMS_X_120000.grib2.gz",
        zarr_uri=obs_uri,
        valid_at=datetime(2026, 5, 1, 12, 0, tzinfo=UTC),
    )

    await nowcast_observation_grid(
        db=integration_db,
        forecaster=PersistenceForecaster(),
        target_root=tmp_path,
        source_grid=grid_row,
        source_file=file_row,
        horizons_minutes=(10,),
    )
    second = await nowcast_observation_grid(
        db=integration_db,
        forecaster=PersistenceForecaster(),
        target_root=tmp_path,
        source_grid=grid_row,
        source_file=file_row,
        horizons_minutes=(10,),
    )
    # Still one row total — the unique constraint upserts.
    async with integration_db.sessionmaker() as session:
        rows = (await session.execute(select(NowcastRow))).scalars().all()
    assert len(rows) == 1
    assert second.persisted[0].id == rows[0].id


async def test_observation_with_unreadable_zarr_skips_with_reason(
    integration_db: Database, tmp_path: Path
) -> None:
    """A missing Zarr store surfaces as ``skipped_reason`` rather than
    raising — the per-event consumer expects this."""
    grid_row, file_row = await _seed_observation(
        integration_db,
        file_key="CONUS/X/MRMS_X_120000.grib2.gz",
        zarr_uri=str(tmp_path / "does-not-exist.zarr"),
        valid_at=datetime(2026, 5, 1, 12, 0, tzinfo=UTC),
    )

    result = await nowcast_observation_grid(
        db=integration_db,
        forecaster=PersistenceForecaster(),
        target_root=tmp_path,
        source_grid=grid_row,
        source_file=file_row,
        horizons_minutes=(10,),
    )
    assert result.persisted == ()
    assert result.skipped_reason is not None
    assert "read_failed" in result.skipped_reason


async def test_cascade_on_source_file_delete(integration_db: Database, tmp_path: Path) -> None:
    """FK CASCADE: deleting the source mrms_files row clears its nowcasts."""
    obs_uri = _write_obs_grid(tmp_path / "obs.zarr")
    grid_row, file_row = await _seed_observation(
        integration_db,
        file_key="CONUS/X/MRMS_X_120000.grib2.gz",
        zarr_uri=obs_uri,
        valid_at=datetime(2026, 5, 1, 12, 0, tzinfo=UTC),
    )
    await nowcast_observation_grid(
        db=integration_db,
        forecaster=PersistenceForecaster(),
        target_root=tmp_path,
        source_grid=grid_row,
        source_file=file_row,
        horizons_minutes=(10, 30),
    )

    async with integration_db.sessionmaker() as session:
        await session.execute(
            text("DELETE FROM mrms_files WHERE key = :k"),
            {"k": file_row.key},
        )
        await session.commit()
        rows = (await session.execute(select(NowcastRow))).scalars().all()
    assert rows == []
