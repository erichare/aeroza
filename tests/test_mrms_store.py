"""Integration tests for the MRMS file-catalog persistence layer."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from aeroza.ingest.mrms import MrmsFile
from aeroza.ingest.mrms_models import MrmsFileRow
from aeroza.ingest.mrms_store import upsert_mrms_files

pytestmark = pytest.mark.integration


def _file(
    key: str = (
        "CONUS/MergedReflectivityComposite_00.50/20260501/"
        "MRMS_MergedReflectivityComposite_00.50_20260501-120000.grib2.gz"
    ),
    *,
    product: str = "MergedReflectivityComposite",
    level: str = "00.50",
    valid_at: datetime | None = None,
    size_bytes: int = 10_000,
    etag: str | None = "deadbeef",
) -> MrmsFile:
    return MrmsFile(
        key=key,
        product=product,
        level=level,
        valid_at=valid_at or datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC),
        size_bytes=size_bytes,
        etag=etag,
    )


@pytest.fixture(autouse=True)
async def _truncate_after_each(integration_db: object) -> None:
    yield
    db = integration_db
    async with db.sessionmaker() as session:  # type: ignore[attr-defined]
        await session.execute(text("TRUNCATE TABLE mrms_files CASCADE"))
        await session.commit()


async def test_inserts_new_file(db_session: AsyncSession) -> None:
    result = await upsert_mrms_files(db_session, [_file()])
    await db_session.commit()

    assert result.inserted_keys == (
        "CONUS/MergedReflectivityComposite_00.50/20260501/"
        "MRMS_MergedReflectivityComposite_00.50_20260501-120000.grib2.gz",
    )
    assert result.updated_keys == ()

    row = (await db_session.execute(select(MrmsFileRow))).scalar_one()
    assert row.product == "MergedReflectivityComposite"
    assert row.level == "00.50"
    assert row.valid_at == datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC)
    assert row.size_bytes == 10_000
    assert row.etag == "deadbeef"


async def test_empty_input_is_noop(db_session: AsyncSession) -> None:
    result = await upsert_mrms_files(db_session, [])
    assert result.inserted_keys == ()
    assert result.updated_keys == ()
    assert result.total == 0


async def test_real_update_when_etag_changes(db_session: AsyncSession) -> None:
    initial = _file(etag="v1")
    await upsert_mrms_files(db_session, [initial])
    await db_session.commit()

    revised = _file(etag="v2")
    result = await upsert_mrms_files(db_session, [revised])
    await db_session.commit()

    assert result.inserted_keys == ()
    assert result.updated_keys == (initial.key,)
    row = (await db_session.execute(select(MrmsFileRow))).scalar_one()
    assert row.etag == "v2"


async def test_no_op_upsert_does_not_bump_updated_at(db_session: AsyncSession) -> None:
    same = _file()
    await upsert_mrms_files(db_session, [same])
    await db_session.commit()

    pre = (await db_session.execute(select(MrmsFileRow))).scalar_one()
    initial_updated_at = pre.updated_at

    second = await upsert_mrms_files(db_session, [same])
    await db_session.commit()

    assert second.inserted_keys == ()
    assert second.updated_keys == ()  # filtered out by WHERE IS DISTINCT FROM
    refreshed = (
        await db_session.execute(select(MrmsFileRow).where(MrmsFileRow.key == same.key))
    ).scalar_one()
    assert refreshed.updated_at == initial_updated_at


async def test_mixed_batch_separates_inserts_and_updates(db_session: AsyncSession) -> None:
    a_key = "CONUS/PrecipRate_00.00/20260501/MRMS_PrecipRate_00.00_20260501-120000.grib2.gz"
    b_key = "CONUS/PrecipRate_00.00/20260501/MRMS_PrecipRate_00.00_20260501-120200.grib2.gz"
    await upsert_mrms_files(
        db_session,
        [
            _file(
                key=a_key,
                product="PrecipRate",
                level="00.00",
                valid_at=datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC),
                etag="v1",
            )
        ],
    )
    await db_session.commit()

    result = await upsert_mrms_files(
        db_session,
        [
            _file(
                key=a_key,
                product="PrecipRate",
                level="00.00",
                valid_at=datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC),
                etag="v2",  # update
            ),
            _file(
                key=b_key,
                product="PrecipRate",
                level="00.00",
                valid_at=datetime(2026, 5, 1, 12, 2, 0, tzinfo=UTC),
                etag="new",  # insert
            ),
        ],
    )
    await db_session.commit()

    assert set(result.inserted_keys) == {b_key}
    assert set(result.updated_keys) == {a_key}
    assert result.total == 2


async def test_files_with_null_etag_are_supported(db_session: AsyncSession) -> None:
    no_etag = _file(etag=None)
    result = await upsert_mrms_files(db_session, [no_etag])
    await db_session.commit()
    assert result.inserted_keys == (no_etag.key,)
    row = (await db_session.execute(select(MrmsFileRow))).scalar_one()
    assert row.etag is None


async def test_valid_at_round_trip_keeps_timezone(db_session: AsyncSession) -> None:
    valid_at = datetime(2026, 5, 1, 12, 4, 0, tzinfo=UTC)
    await upsert_mrms_files(db_session, [_file(valid_at=valid_at)])
    await db_session.commit()
    row = (await db_session.execute(select(MrmsFileRow))).scalar_one()
    assert row.valid_at == valid_at
    # Ensure the stored timestamp survives the timedelta arithmetic that
    # downstream "what's the latest within 5 minutes?" queries will rely on.
    assert (datetime.now(UTC) - row.valid_at) > timedelta(seconds=0)
