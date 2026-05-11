"""Tests for the retention worker.

Filesystem and CLI-glue paths are pure unit tests. The end-to-end DB
cascade (mrms_files → mrms_grids / mrms_nowcasts / nowcast_verifications)
runs as an integration test against a real Postgres so we exercise the
``ondelete="CASCADE"`` machinery rather than mocking it.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from aeroza.ingest.mrms import MrmsFile
from aeroza.ingest.mrms_grids_models import MrmsGridRow
from aeroza.ingest.mrms_grids_store import upsert_mrms_grid
from aeroza.ingest.mrms_models import MrmsFileRow
from aeroza.ingest.mrms_store import upsert_mrms_files
from aeroza.ingest.mrms_zarr import MrmsGridLocator
from aeroza.ingest.nws_alerts import Alert, Certainty, Severity, Urgency
from aeroza.ingest.nws_alerts_models import NwsAlertRow
from aeroza.ingest.nws_alerts_store import upsert_alerts
from aeroza.nowcast.models import NowcastRow
from aeroza.nowcast.store import upsert_nowcast
from aeroza.retention.worker import (
    PruneResult,
    _remove_zarr_paths,
    prune_expired_alerts_once,
    prune_old_mrms_once,
)
from aeroza.shared.db import Database

# ---------------------------------------------------------------------------
# Pure unit tests — no DB required
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_prune_result_merge_sums_counters() -> None:
    a = PruneResult(deleted_files=1, deleted_zarrs=2, failed_zarrs=3, deleted_alerts=4)
    b = PruneResult(deleted_files=10, deleted_zarrs=20, failed_zarrs=30, deleted_alerts=40)

    merged = a.merged_with(b)

    assert merged == PruneResult(
        deleted_files=11,
        deleted_zarrs=22,
        failed_zarrs=33,
        deleted_alerts=44,
    )


@pytest.mark.unit
def test_remove_zarr_paths_deletes_real_directories(tmp_path: Path) -> None:
    a = tmp_path / "a.zarr"
    b = tmp_path / "nested" / "b.zarr"
    (a / "chunk").mkdir(parents=True)
    (a / "chunk" / "data").write_bytes(b"\x00" * 32)
    (b / "chunk").mkdir(parents=True)

    result = _remove_zarr_paths([str(a), str(b)])

    assert result.deleted == 2
    assert result.failed == 0
    assert not a.exists()
    assert not b.exists()


@pytest.mark.unit
def test_remove_zarr_paths_treats_missing_path_as_success(tmp_path: Path) -> None:
    """A redeploy can wipe the volume while DB rows persist — the row's
    zarr_uri then points at a non-existent path. That's fine: the row is
    being deleted anyway, so we count it as success and move on.
    """
    missing = tmp_path / "never_existed.zarr"
    result = _remove_zarr_paths([str(missing)])

    assert result.deleted == 1
    assert result.failed == 0


@pytest.mark.unit
def test_remove_zarr_paths_handles_empty_input() -> None:
    result = _remove_zarr_paths([])
    assert result.deleted == 0
    assert result.failed == 0


@pytest.mark.unit
async def test_prune_old_mrms_once_rejects_non_positive_retention() -> None:
    with pytest.raises(ValueError, match="retention_hours must be positive"):
        await prune_old_mrms_once(db=None, retention_hours=0)  # type: ignore[arg-type]


@pytest.mark.unit
async def test_prune_old_mrms_once_rejects_non_positive_batch() -> None:
    with pytest.raises(ValueError, match="batch_size must be positive"):
        await prune_old_mrms_once(db=None, retention_hours=1, batch_size=0)  # type: ignore[arg-type]


@pytest.mark.unit
async def test_prune_expired_alerts_once_rejects_non_positive_window() -> None:
    with pytest.raises(ValueError, match="retention_days must be positive"):
        await prune_expired_alerts_once(db=None, retention_days=0)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Integration tests — exercise the real FK cascade
# ---------------------------------------------------------------------------


FILE_KEY_OLD: str = (
    "CONUS/MergedReflectivityComposite_00.50/20260101/"
    "MRMS_MergedReflectivityComposite_00.50_20260101-000000.grib2.gz"
)
FILE_KEY_NEW: str = (
    "CONUS/MergedReflectivityComposite_00.50/20260501/"
    "MRMS_MergedReflectivityComposite_00.50_20260501-120000.grib2.gz"
)


def _mrms_file(key: str, *, valid_at: datetime) -> MrmsFile:
    return MrmsFile(
        key=key,
        product="MergedReflectivityComposite",
        level="00.50",
        valid_at=valid_at,
        size_bytes=10_000,
        etag="v1",
    )


def _grid_locator(*, file_key: str, zarr_uri: str) -> MrmsGridLocator:
    return MrmsGridLocator(
        file_key=file_key,
        zarr_uri=zarr_uri,
        variable="reflectivity",
        dims=("latitude", "longitude"),
        shape=(3500, 7000),
        dtype="float32",
        nbytes=3500 * 7000 * 4,
    )


def _make_zarr_dir(path: Path) -> None:
    """Create a real Zarr-shaped directory so rmtree has something to remove."""
    path.mkdir(parents=True, exist_ok=True)
    (path / ".zarray").write_text(json.dumps({"shape": [1]}))


async def _seed_observation_with_nowcast(
    *,
    session: AsyncSession,
    file: MrmsFile,
    obs_zarr: Path,
    nowcast_zarr: Path,
) -> uuid.UUID:
    await upsert_mrms_files(session, [file])
    await upsert_mrms_grid(
        session,
        _grid_locator(file_key=file.key, zarr_uri=str(obs_zarr)),
    )
    nowcast_row = await upsert_nowcast(
        session,
        source_file_key=file.key,
        product=file.product,
        level=file.level,
        algorithm="persistence",
        horizon_minutes=10,
        valid_at=file.valid_at + timedelta(minutes=10),
        zarr_uri=str(nowcast_zarr),
        variable="reflectivity",
        dims=("latitude", "longitude"),
        shape=(3500, 7000),
        dtype="float32",
        nbytes=3500 * 7000 * 4,
    )
    await session.commit()
    return nowcast_row.id


@pytest_asyncio.fixture
async def _truncate_after_each(integration_db: Database) -> AsyncIterator[None]:
    """Per-test scrub of the tables this module touches.

    ``mrms_files CASCADE`` walks the FK graph so grids, nowcasts, and
    verifications all clear with one TRUNCATE. ``nws_alerts`` lives in
    its own table without an FK relationship.
    """
    yield
    async with integration_db.sessionmaker() as session:
        await session.execute(text("TRUNCATE TABLE mrms_files CASCADE"))
        await session.execute(text("TRUNCATE TABLE nws_alerts"))
        await session.commit()


@pytest.mark.integration
async def test_prune_old_mrms_drops_zarrs_and_cascades_db(
    integration_db: Database,
    tmp_path: Path,
    _truncate_after_each: None,
) -> None:
    now = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    obs_zarr_old = tmp_path / "old_obs.zarr"
    nowcast_zarr_old = tmp_path / "old_nowcast.zarr"
    obs_zarr_new = tmp_path / "new_obs.zarr"
    for path in (obs_zarr_old, nowcast_zarr_old, obs_zarr_new):
        _make_zarr_dir(path)

    async with integration_db.sessionmaker() as session:
        await _seed_observation_with_nowcast(
            session=session,
            # 12h old — should be pruned at retention_hours=6
            file=_mrms_file(FILE_KEY_OLD, valid_at=now - timedelta(hours=12)),
            obs_zarr=obs_zarr_old,
            nowcast_zarr=nowcast_zarr_old,
        )
        # 1h old — should be kept
        await upsert_mrms_files(
            session,
            [_mrms_file(FILE_KEY_NEW, valid_at=now - timedelta(hours=1))],
        )
        await upsert_mrms_grid(
            session,
            _grid_locator(file_key=FILE_KEY_NEW, zarr_uri=str(obs_zarr_new)),
        )
        await session.commit()

    result = await prune_old_mrms_once(
        db=integration_db,
        retention_hours=6,
        batch_size=100,
        now=now,
    )

    assert result.deleted_files == 1
    # One observation zarr + one nowcast zarr
    assert result.deleted_zarrs == 2
    assert result.failed_zarrs == 0

    # Old zarrs gone, new zarr survives
    assert not obs_zarr_old.exists()
    assert not nowcast_zarr_old.exists()
    assert obs_zarr_new.exists()

    # Old DB rows cascaded; new row survives
    async with integration_db.sessionmaker() as session:
        remaining_files = (await session.execute(select(MrmsFileRow.key))).scalars().all()
        remaining_grids = (await session.execute(select(MrmsGridRow.file_key))).scalars().all()
        remaining_nowcasts = (
            (await session.execute(select(NowcastRow.source_file_key))).scalars().all()
        )

    assert list(remaining_files) == [FILE_KEY_NEW]
    assert list(remaining_grids) == [FILE_KEY_NEW]
    assert list(remaining_nowcasts) == []


@pytest.mark.integration
async def test_prune_old_mrms_idempotent_second_pass_is_noop(
    integration_db: Database,
    tmp_path: Path,
    _truncate_after_each: None,
) -> None:
    now = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    obs_zarr = tmp_path / "obs.zarr"
    nowcast_zarr = tmp_path / "nowcast.zarr"
    _make_zarr_dir(obs_zarr)
    _make_zarr_dir(nowcast_zarr)

    async with integration_db.sessionmaker() as session:
        await _seed_observation_with_nowcast(
            session=session,
            file=_mrms_file(FILE_KEY_OLD, valid_at=now - timedelta(hours=24)),
            obs_zarr=obs_zarr,
            nowcast_zarr=nowcast_zarr,
        )

    first = await prune_old_mrms_once(db=integration_db, retention_hours=6, batch_size=100, now=now)
    second = await prune_old_mrms_once(
        db=integration_db, retention_hours=6, batch_size=100, now=now
    )

    assert first.deleted_files == 1
    assert second.deleted_files == 0
    assert second.deleted_zarrs == 0


@pytest.mark.integration
async def test_prune_old_mrms_handles_missing_zarr_paths(
    integration_db: Database,
    tmp_path: Path,
    _truncate_after_each: None,
) -> None:
    """If the volume was wiped on a redeploy, zarr_uri points nowhere — the
    DB row still has to come out cleanly, and the counter records the path
    as deleted (not failed) since there's nothing to clean up.
    """
    now = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    missing_obs = tmp_path / "missing_obs.zarr"  # never created
    missing_nowcast = tmp_path / "missing_nowcast.zarr"

    async with integration_db.sessionmaker() as session:
        await _seed_observation_with_nowcast(
            session=session,
            file=_mrms_file(FILE_KEY_OLD, valid_at=now - timedelta(hours=24)),
            obs_zarr=missing_obs,
            nowcast_zarr=missing_nowcast,
        )

    result = await prune_old_mrms_once(
        db=integration_db, retention_hours=6, batch_size=100, now=now
    )

    assert result.deleted_files == 1
    assert result.deleted_zarrs == 2
    assert result.failed_zarrs == 0


@pytest.mark.integration
async def test_prune_old_mrms_batches_large_sets(
    integration_db: Database,
    tmp_path: Path,
    _truncate_after_each: None,
) -> None:
    """Force multiple batches with batch_size=2 over 5 expired files."""
    now = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    keys: list[str] = []
    async with integration_db.sessionmaker() as session:
        for i in range(5):
            key = f"CONUS/MergedReflectivityComposite_00.50/20260101/frame-{i:03d}.grib2.gz"
            keys.append(key)
            zarr = tmp_path / f"obs-{i}.zarr"
            _make_zarr_dir(zarr)
            await upsert_mrms_files(
                session,
                [_mrms_file(key, valid_at=now - timedelta(hours=24 + i))],
            )
            await upsert_mrms_grid(
                session,
                _grid_locator(file_key=key, zarr_uri=str(zarr)),
            )
        await session.commit()

    result = await prune_old_mrms_once(db=integration_db, retention_hours=6, batch_size=2, now=now)

    assert result.deleted_files == 5
    assert result.deleted_zarrs == 5


def _expired_alert(alert_id: str, *, expires: datetime) -> Alert:
    return Alert.model_validate(
        {
            "id": alert_id,
            "event": "Severe Thunderstorm Warning",
            "headline": "test",
            "description": "test",
            "instruction": None,
            "severity": Severity.MODERATE,
            "urgency": Urgency.EXPECTED,
            "certainty": Certainty.LIKELY,
            "sender_name": "NWS Test",
            "area_desc": "Test County",
            "effective": expires - timedelta(hours=1),
            "onset": expires - timedelta(hours=1),
            "expires": expires,
            "ends": expires,
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [[-95.7, 29.5], [-95.7, 30.0], [-95.0, 30.0], [-95.0, 29.5], [-95.7, 29.5]]
                ],
            },
        }
    )


@pytest.mark.integration
async def test_prune_expired_alerts_drops_old_keeps_recent(
    integration_db: Database,
    _truncate_after_each: None,
) -> None:
    now = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    long_expired = _expired_alert("urn:test:alert:old", expires=now - timedelta(days=60))
    recently_expired = _expired_alert("urn:test:alert:recent", expires=now - timedelta(days=5))

    async with integration_db.sessionmaker() as session:
        await upsert_alerts(session, [long_expired, recently_expired])
        await session.commit()

    result = await prune_expired_alerts_once(db=integration_db, retention_days=30, now=now)

    assert result.deleted_alerts == 1

    async with integration_db.sessionmaker() as session:
        remaining = (await session.execute(select(NwsAlertRow.id))).scalars().all()
    assert list(remaining) == ["urn:test:alert:recent"]


@pytest.mark.integration
async def test_prune_expired_alerts_keeps_null_expires(
    integration_db: Database,
    _truncate_after_each: None,
) -> None:
    """Alerts with ``expires IS NULL`` must never be dropped silently."""
    now = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    null_expiry = _expired_alert(
        "urn:test:alert:null", expires=now - timedelta(days=999)
    ).model_copy(update={"expires": None, "ends": None})

    async with integration_db.sessionmaker() as session:
        await upsert_alerts(session, [null_expiry])
        await session.commit()

    result = await prune_expired_alerts_once(db=integration_db, retention_days=30, now=now)

    assert result.deleted_alerts == 0

    async with integration_db.sessionmaker() as session:
        remaining = (await session.execute(select(NwsAlertRow.id))).scalars().all()
    assert "urn:test:alert:null" in list(remaining)
