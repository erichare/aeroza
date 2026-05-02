"""Integration tests for the materialise-mrms backfill orchestrator.

Exercises the un-materialised-files anti-join helper plus the
:func:`materialise_unmaterialised_once` tick. Heavy bits (download +
decode) are stubbed so the tests don't need cfgrib/eccodes; the real
Zarr writer + real Postgres are exercised end-to-end so we know the
locator round-trips.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch

import numpy as np
import pytest
import xarray as xr
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from aeroza.ingest.mrms import MrmsFile
from aeroza.ingest.mrms_grids_models import MrmsGridRow
from aeroza.ingest.mrms_grids_store import find_unmaterialised_files, upsert_mrms_grid
from aeroza.ingest.mrms_materialise_poll import materialise_unmaterialised_once
from aeroza.ingest.mrms_store import upsert_mrms_files
from aeroza.ingest.mrms_zarr import MrmsGridLocator
from aeroza.shared.db import Database
from aeroza.stream.publisher import InMemoryMrmsGridPublisher, MrmsGridPublisher

pytestmark = pytest.mark.integration

PRODUCT: str = "MergedReflectivityComposite"
LEVEL: str = "00.50"


def _file(
    *,
    suffix: str = "120000",
    valid_at: datetime | None = None,
    product: str = PRODUCT,
    level: str = LEVEL,
) -> MrmsFile:
    key = f"CONUS/{product}_{level}/20260501/MRMS_{product}_{level}_20260501-{suffix}.grib2.gz"
    return MrmsFile(
        key=key,
        product=product,
        level=level,
        valid_at=valid_at or datetime(2026, 5, 1, 12, 0, tzinfo=UTC),
        size_bytes=10_000,
        etag="v1",
    )


def _locator(file_key: str, *, zarr_uri: str = "/var/data/x.zarr") -> MrmsGridLocator:
    return MrmsGridLocator(
        file_key=file_key,
        zarr_uri=zarr_uri,
        variable="reflectivity",
        dims=("latitude", "longitude"),
        shape=(4, 5),
        dtype="float32",
        nbytes=4 * 5 * 4,
    )


def _synthetic_da() -> xr.DataArray:
    return xr.DataArray(
        np.arange(20, dtype=np.float32).reshape(4, 5),
        dims=("latitude", "longitude"),
        name="reflectivity",
    )


@pytest.fixture(autouse=True)
async def _truncate_after(integration_db: Database) -> Any:
    yield
    async with integration_db.sessionmaker() as session:
        await session.execute(text("TRUNCATE TABLE mrms_files CASCADE"))
        await session.commit()


async def _seed_files(integration_db: Database, *files: MrmsFile) -> None:
    async with integration_db.sessionmaker() as session:
        await upsert_mrms_files(session, files)
        await session.commit()


# ---------------------------------------------------------------------------
# find_unmaterialised_files


async def test_find_unmaterialised_returns_only_files_without_grid(
    db_session: AsyncSession, integration_db: Database
) -> None:
    a, b, c = _file(suffix="120000"), _file(suffix="120200"), _file(suffix="120400")
    await _seed_files(integration_db, a, b, c)

    # Mark `b` as already materialised.
    await upsert_mrms_grid(db_session, _locator(b.key))
    await db_session.commit()

    found = await find_unmaterialised_files(db_session, product=PRODUCT, level=LEVEL, limit=10)
    assert {f.key for f in found} == {a.key, c.key}


async def test_find_unmaterialised_orders_newest_first(
    db_session: AsyncSession, integration_db: Database
) -> None:
    older = _file(suffix="120000", valid_at=datetime(2026, 5, 1, 12, 0, tzinfo=UTC))
    newer = _file(suffix="120200", valid_at=datetime(2026, 5, 1, 12, 2, tzinfo=UTC))
    await _seed_files(integration_db, older, newer)

    found = await find_unmaterialised_files(db_session, product=PRODUCT, level=LEVEL, limit=10)
    assert tuple(f.key for f in found) == (newer.key, older.key)


async def test_find_unmaterialised_respects_limit(
    db_session: AsyncSession, integration_db: Database
) -> None:
    files = tuple(
        _file(
            suffix=f"12{i:02d}00",
            valid_at=datetime(2026, 5, 1, 12, i, tzinfo=UTC),
        )
        for i in range(5)
    )
    await _seed_files(integration_db, *files)

    found = await find_unmaterialised_files(db_session, product=PRODUCT, level=LEVEL, limit=2)
    assert len(found) == 2


async def test_find_unmaterialised_filters_by_product_and_level(
    db_session: AsyncSession, integration_db: Database
) -> None:
    target = _file(suffix="120000")
    other_product = _file(suffix="120200", product="PrecipRate", level="00.00")
    await _seed_files(integration_db, target, other_product)

    found = await find_unmaterialised_files(db_session, product=PRODUCT, level=LEVEL, limit=10)
    assert tuple(f.key for f in found) == (target.key,)


async def test_find_unmaterialised_returns_empty_when_all_done(
    db_session: AsyncSession, integration_db: Database
) -> None:
    f = _file()
    await _seed_files(integration_db, f)
    await upsert_mrms_grid(db_session, _locator(f.key))
    await db_session.commit()

    found = await find_unmaterialised_files(db_session, product=PRODUCT, level=LEVEL, limit=10)
    assert found == ()


# ---------------------------------------------------------------------------
# materialise_unmaterialised_once


def _patch_download_and_decode(*, da: xr.DataArray | None = None) -> Any:
    da = da if da is not None else _synthetic_da()
    download = patch(
        "aeroza.ingest.mrms_grids.download_grib2_payload",
        return_value=b"fake-grib2",
    )
    decode = patch(
        "aeroza.ingest.mrms_grids.decode_grib2_to_dataarray",
        return_value=da,
    )
    return download, decode


async def test_orchestrator_materialises_each_pending_file(
    integration_db: Database, tmp_path: Path
) -> None:
    a, b = _file(suffix="120000"), _file(suffix="120200")
    await _seed_files(integration_db, a, b)

    download, decode = _patch_download_and_decode()
    with download, decode:
        result = await materialise_unmaterialised_once(
            db=integration_db,
            s3_client=object(),
            target_root=tmp_path,
            product=PRODUCT,
            level=LEVEL,
            batch_size=10,
        )

    assert set(result.materialised_keys) == {a.key, b.key}
    assert result.failed_keys == ()

    async with integration_db.sessionmaker() as session:
        rows = (await session.execute(select(MrmsGridRow))).scalars().all()
    assert {r.file_key for r in rows} == {a.key, b.key}


async def test_orchestrator_idle_when_nothing_pending(
    integration_db: Database, tmp_path: Path
) -> None:
    f = _file()
    await _seed_files(integration_db, f)
    async with integration_db.sessionmaker() as session:
        await upsert_mrms_grid(session, _locator(f.key))
        await session.commit()

    result = await materialise_unmaterialised_once(
        db=integration_db,
        s3_client=object(),
        target_root=tmp_path,
        product=PRODUCT,
        level=LEVEL,
    )
    assert result.materialised_keys == ()
    assert result.failed_keys == ()


async def test_orchestrator_continues_on_per_file_failure(
    integration_db: Database, tmp_path: Path
) -> None:
    """One bad file must not block the rest of the batch."""
    from aeroza.ingest.mrms_decode import MrmsDecodeError

    a, b = _file(suffix="120000"), _file(suffix="120200")
    await _seed_files(integration_db, a, b)

    da = _synthetic_da()
    download = patch(
        "aeroza.ingest.mrms_grids.download_grib2_payload",
        return_value=b"fake",
    )

    call_count = {"n": 0}

    def flaky_decode(*_args: Any, **_kwargs: Any) -> xr.DataArray:
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise MrmsDecodeError("boom")
        return da

    decode = patch(
        "aeroza.ingest.mrms_grids.decode_grib2_to_dataarray",
        side_effect=flaky_decode,
    )

    with download, decode:
        result = await materialise_unmaterialised_once(
            db=integration_db,
            s3_client=object(),
            target_root=tmp_path,
            product=PRODUCT,
            level=LEVEL,
            batch_size=10,
        )

    assert result.materialised == 1
    assert result.failed == 1
    # The newer file is tried first; both `a` and `b` share valid_at — order is
    # not guaranteed, but exactly one of them should land in each bucket.
    assert set(result.materialised_keys) | set(result.failed_keys) == {a.key, b.key}


async def test_orchestrator_respects_batch_size(integration_db: Database, tmp_path: Path) -> None:
    """Large queue + small batch_size → only batch_size materialised this tick."""
    base = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    files = tuple(
        _file(suffix=f"12{i:02d}00", valid_at=base + timedelta(minutes=i)) for i in range(5)
    )
    await _seed_files(integration_db, *files)

    download, decode = _patch_download_and_decode()
    with download, decode:
        result = await materialise_unmaterialised_once(
            db=integration_db,
            s3_client=object(),
            target_root=tmp_path,
            product=PRODUCT,
            level=LEVEL,
            batch_size=2,
        )

    assert result.materialised == 2
    assert result.failed == 0


# ---------------------------------------------------------------------------
# Publisher integration


async def test_publishes_one_event_per_materialised_grid(
    integration_db: Database, tmp_path: Path
) -> None:
    a, b = _file(suffix="120000"), _file(suffix="120200")
    await _seed_files(integration_db, a, b)

    publisher = InMemoryMrmsGridPublisher()
    download, decode = _patch_download_and_decode()
    with download, decode:
        await materialise_unmaterialised_once(
            db=integration_db,
            s3_client=object(),
            target_root=tmp_path,
            product=PRODUCT,
            level=LEVEL,
            batch_size=10,
            publisher=publisher,
        )

    assert set(publisher.published_keys) == {a.key, b.key}


async def test_does_not_publish_when_decode_fails(integration_db: Database, tmp_path: Path) -> None:
    """A failed materialisation must NOT emit a grid event."""
    from aeroza.ingest.mrms_decode import MrmsDecodeError

    f = _file(suffix="120000")
    await _seed_files(integration_db, f)

    publisher = InMemoryMrmsGridPublisher()
    download = patch(
        "aeroza.ingest.mrms_grids.download_grib2_payload",
        return_value=b"fake",
    )
    decode = patch(
        "aeroza.ingest.mrms_grids.decode_grib2_to_dataarray",
        side_effect=MrmsDecodeError("boom"),
    )
    with download, decode:
        result = await materialise_unmaterialised_once(
            db=integration_db,
            s3_client=object(),
            target_root=tmp_path,
            product=PRODUCT,
            level=LEVEL,
            publisher=publisher,
        )

    assert result.failed_keys == (f.key,)
    assert publisher.published_keys == ()


async def test_publisher_failures_do_not_break_persistence(
    integration_db: Database, tmp_path: Path
) -> None:
    """A flaky publisher must not roll back the upsert. The catalog is
    durably persisted; a future replay catches up missed events."""

    class FlakyPublisher:
        async def publish_new_grid(self, locator: MrmsGridLocator) -> None:
            raise RuntimeError(f"transport down for {locator.file_key}")

    f = _file(suffix="120000")
    await _seed_files(integration_db, f)

    publisher: MrmsGridPublisher = FlakyPublisher()
    download, decode = _patch_download_and_decode()
    with download, decode:
        result = await materialise_unmaterialised_once(
            db=integration_db,
            s3_client=object(),
            target_root=tmp_path,
            product=PRODUCT,
            level=LEVEL,
            publisher=publisher,
        )

    # Materialisation succeeded even though publish raised.
    assert result.materialised_keys == (f.key,)
    assert result.failed_keys == ()
    async with integration_db.sessionmaker() as session:
        rows = (await session.execute(select(MrmsGridRow))).scalars().all()
    assert {r.file_key for r in rows} == {f.key}
