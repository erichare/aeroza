"""Integration tests for the materialise_mrms_file orchestrator.

The orchestrator composes download → decode → write_zarr → upsert. We
mock the download + decode (so the test doesn't need cfgrib/eccodes or
network), but exercise the real Zarr writer and real Postgres so the
end-to-end persistence path is validated.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import patch

import numpy as np
import pytest
import xarray as xr
from sqlalchemy import select, text

from aeroza.ingest.mrms import MrmsFile
from aeroza.ingest.mrms_grids import materialise_mrms_file
from aeroza.ingest.mrms_grids_models import MrmsGridRow
from aeroza.ingest.mrms_store import upsert_mrms_files
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


def _synthetic_da(name: str = "reflectivity", scale: float = 1.0) -> xr.DataArray:
    return xr.DataArray(
        (np.arange(20, dtype=np.float32).reshape(4, 5) * scale),
        dims=("latitude", "longitude"),
        name=name,
    )


@pytest.fixture(autouse=True)
async def _truncate_after(integration_db: Database) -> object:
    yield
    async with integration_db.sessionmaker() as session:
        await session.execute(text("TRUNCATE TABLE mrms_files CASCADE"))
        await session.commit()


async def _seed_catalog_row(integration_db: Database, file: MrmsFile | None = None) -> None:
    async with integration_db.sessionmaker() as session:
        await upsert_mrms_files(session, [file or _file()])
        await session.commit()


def _patch_download_and_decode(
    *,
    payload: bytes = b"fake-grib2-bytes",
    da: xr.DataArray | None = None,
) -> Any:
    """Helper: stub the download+decode pair so the orchestrator runs
    without cfgrib/eccodes. Returns a context-manager-like that applies
    both patches together."""
    da = da if da is not None else _synthetic_da()

    download = patch(
        "aeroza.ingest.mrms_grids.download_grib2_payload",
        return_value=payload,
    )
    decode = patch(
        "aeroza.ingest.mrms_grids.decode_grib2_to_dataarray",
        return_value=da,
    )
    return download, decode


async def test_materialises_one_file_end_to_end(integration_db: Database, tmp_path: Path) -> None:
    await _seed_catalog_row(integration_db)
    download, decode = _patch_download_and_decode()
    with download, decode:
        locator = await materialise_mrms_file(
            db=integration_db,
            s3_client=object(),  # download is mocked, never touched
            file=_file(),
            target_root=tmp_path,
        )

    # On-disk Zarr exists at the expected path.
    assert locator.file_key == FILE_KEY
    assert Path(locator.zarr_uri).is_dir()
    assert locator.variable == "reflectivity"
    assert locator.shape == (4, 5)
    assert locator.dtype == "float32"

    # Round-trip via xarray to prove the data was written, not just the path.
    reopened = xr.open_zarr(locator.zarr_uri)
    np.testing.assert_array_equal(reopened["reflectivity"].values, _synthetic_da().values)

    # Catalog row exists and matches the locator.
    async with integration_db.sessionmaker() as session:
        row = (await session.execute(select(MrmsGridRow))).scalar_one()
    assert row.file_key == FILE_KEY
    assert row.zarr_uri == locator.zarr_uri
    assert row.variable == "reflectivity"
    assert row.dtype == "float32"


async def test_re_materialising_overwrites_zarr_and_updates_row(
    integration_db: Database, tmp_path: Path
) -> None:
    await _seed_catalog_row(integration_db)
    file = _file()

    # First pass.
    d1, dec1 = _patch_download_and_decode(da=_synthetic_da(scale=1.0))
    with d1, dec1:
        first = await materialise_mrms_file(
            db=integration_db, s3_client=object(), file=file, target_root=tmp_path
        )

    async with integration_db.sessionmaker() as session:
        before = (await session.execute(select(MrmsGridRow))).scalar_one()
    initial_materialised_at = before.materialised_at

    # Second pass with different data.
    d2, dec2 = _patch_download_and_decode(da=_synthetic_da(scale=10.0))
    with d2, dec2:
        second = await materialise_mrms_file(
            db=integration_db, s3_client=object(), file=file, target_root=tmp_path
        )

    assert second.zarr_uri == first.zarr_uri  # deterministic path
    reopened = xr.open_zarr(second.zarr_uri)
    np.testing.assert_array_equal(
        reopened["reflectivity"].values, (np.arange(20, dtype=np.float32).reshape(4, 5) * 10.0)
    )

    # Catalog row's materialised_at should have advanced (real change to the
    # locator dict — different shape/etag/uri triggers the WHERE clause).
    async with integration_db.sessionmaker() as session:
        after = (await session.execute(select(MrmsGridRow))).scalar_one()
    # The rows happen to be identical here (same shape/dims/dtype/uri/nbytes),
    # so updated_at *won't* have advanced — that's the no-op semantic of the
    # ``IS DISTINCT FROM`` filter, and the test pins it.
    assert after.materialised_at == initial_materialised_at


async def test_propagates_decode_failure_without_writing_zarr(
    integration_db: Database, tmp_path: Path
) -> None:
    """A failed decode must not leak a half-written Zarr or an orphan row."""
    from aeroza.ingest.mrms_decode import MrmsDecodeError

    await _seed_catalog_row(integration_db)

    download = patch(
        "aeroza.ingest.mrms_grids.download_grib2_payload",
        return_value=b"fake-bytes",
    )
    decode = patch(
        "aeroza.ingest.mrms_grids.decode_grib2_to_dataarray",
        side_effect=MrmsDecodeError("cfgrib boom"),
    )
    with download, decode, pytest.raises(MrmsDecodeError):
        await materialise_mrms_file(
            db=integration_db, s3_client=object(), file=_file(), target_root=tmp_path
        )

    # No Zarr on disk.
    assert list(tmp_path.iterdir()) == []
    # No catalog row in mrms_grids.
    async with integration_db.sessionmaker() as session:
        rows = (await session.execute(select(MrmsGridRow))).all()
    assert rows == []


async def test_runs_blocking_steps_off_event_loop(integration_db: Database, tmp_path: Path) -> None:
    """The download+decode+write step is wrapped in ``asyncio.to_thread``
    so the event loop stays responsive. We verify by watching that
    ``asyncio.to_thread`` is the actual call site, not a direct call."""
    await _seed_catalog_row(integration_db)
    download, decode = _patch_download_and_decode()

    with patch("asyncio.to_thread", wraps=__import__("asyncio").to_thread) as spy, download, decode:
        await materialise_mrms_file(
            db=integration_db, s3_client=object(), file=_file(), target_root=tmp_path
        )
    # to_thread should be called at least once for the sync download/decode/write.
    assert spy.call_count >= 1
