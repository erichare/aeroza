"""Integration tests for the materialised-MRMS-grids persistence layer.

Each test seeds an ``mrms_files`` row first (the catalog is the FK
target), then exercises ``upsert_mrms_grid`` and asserts on the stored
row via SQLAlchemy.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from aeroza.ingest.mrms import MrmsFile
from aeroza.ingest.mrms_grids_models import MrmsGridRow
from aeroza.ingest.mrms_grids_store import parse_dims_shape, upsert_mrms_grid
from aeroza.ingest.mrms_store import upsert_mrms_files
from aeroza.ingest.mrms_zarr import MrmsGridLocator
from aeroza.shared.db import Database

pytestmark = pytest.mark.integration

FILE_KEY: str = (
    "CONUS/MergedReflectivityComposite_00.50/20260501/"
    "MRMS_MergedReflectivityComposite_00.50_20260501-120000.grib2.gz"
)


def _file(key: str = FILE_KEY) -> MrmsFile:
    return MrmsFile(
        key=key,
        product="MergedReflectivityComposite",
        level="00.50",
        valid_at=datetime(2026, 5, 1, 12, 0, tzinfo=UTC),
        size_bytes=10_000,
        etag="v1",
    )


def _locator(
    *,
    file_key: str = FILE_KEY,
    variable: str = "reflectivity",
    dims: tuple[str, ...] = ("latitude", "longitude"),
    shape: tuple[int, ...] = (3500, 7000),
    dtype: str = "float32",
    nbytes: int = 3500 * 7000 * 4,
    zarr_uri: str = "/var/data/mrms.zarr",
) -> MrmsGridLocator:
    return MrmsGridLocator(
        file_key=file_key,
        zarr_uri=zarr_uri,
        variable=variable,
        dims=dims,
        shape=shape,
        dtype=dtype,
        nbytes=nbytes,
    )


@pytest.fixture(autouse=True)
async def _truncate_after(integration_db: Database) -> object:
    yield
    async with integration_db.sessionmaker() as session:
        await session.execute(text("TRUNCATE TABLE mrms_grids, mrms_files"))
        await session.commit()


async def _seed_file(integration_db: Database, file: MrmsFile | None = None) -> None:
    async with integration_db.sessionmaker() as session:
        await upsert_mrms_files(session, [file or _file()])
        await session.commit()


async def test_inserts_new_grid(db_session: AsyncSession, integration_db: Database) -> None:
    await _seed_file(integration_db)

    inserted = await upsert_mrms_grid(db_session, _locator())
    await db_session.commit()
    assert inserted is True

    row = (await db_session.execute(select(MrmsGridRow))).scalar_one()
    assert row.file_key == FILE_KEY
    assert row.variable == "reflectivity"
    assert row.dtype == "float32"
    assert row.zarr_uri == "/var/data/mrms.zarr"
    dims, shape = parse_dims_shape(row)
    assert dims == ("latitude", "longitude")
    assert shape == (3500, 7000)


async def test_real_update_replaces_locator(
    db_session: AsyncSession, integration_db: Database
) -> None:
    await _seed_file(integration_db)

    await upsert_mrms_grid(db_session, _locator(zarr_uri="/var/data/v1.zarr"))
    await db_session.commit()
    inserted = await upsert_mrms_grid(db_session, _locator(zarr_uri="/var/data/v2.zarr"))
    await db_session.commit()
    assert inserted is False

    row = (await db_session.execute(select(MrmsGridRow))).scalar_one()
    assert row.zarr_uri == "/var/data/v2.zarr"


async def test_no_op_upsert_does_not_bump_materialised_at(
    db_session: AsyncSession, integration_db: Database
) -> None:
    await _seed_file(integration_db)

    locator = _locator()
    await upsert_mrms_grid(db_session, locator)
    await db_session.commit()
    pre = (await db_session.execute(select(MrmsGridRow))).scalar_one()
    initial_materialised_at = pre.materialised_at

    await upsert_mrms_grid(db_session, locator)
    await db_session.commit()
    refreshed = (
        await db_session.execute(select(MrmsGridRow).where(MrmsGridRow.file_key == FILE_KEY))
    ).scalar_one()
    assert refreshed.materialised_at == initial_materialised_at


async def test_foreign_key_to_mrms_files_is_enforced(
    db_session: AsyncSession,
) -> None:
    """No matching mrms_files row → upsert raises an integrity error."""
    from sqlalchemy.exc import IntegrityError

    locator = _locator(file_key="orphan-key.grib2.gz")
    with pytest.raises(IntegrityError):
        await upsert_mrms_grid(db_session, locator)
        await db_session.commit()
    await db_session.rollback()


async def test_dims_and_shape_round_trip_through_jsonb(
    db_session: AsyncSession, integration_db: Database
) -> None:
    await _seed_file(integration_db)
    locator = _locator(dims=("time", "y", "x"), shape=(1, 256, 256))
    await upsert_mrms_grid(db_session, locator)
    await db_session.commit()
    row = (await db_session.execute(select(MrmsGridRow))).scalar_one()
    dims, shape = parse_dims_shape(row)
    assert dims == ("time", "y", "x")
    assert shape == (1, 256, 256)


async def test_cascade_delete_when_parent_file_removed(
    db_session: AsyncSession, integration_db: Database
) -> None:
    """Deleting an mrms_files row also drops its mrms_grids row (ON DELETE CASCADE)."""
    await _seed_file(integration_db)
    await upsert_mrms_grid(db_session, _locator())
    await db_session.commit()

    await db_session.execute(text("DELETE FROM mrms_files WHERE key = :k"), {"k": FILE_KEY})
    await db_session.commit()
    remaining = (await db_session.execute(select(MrmsGridRow))).all()
    assert remaining == []
