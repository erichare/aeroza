"""End-to-end materialisation: an MRMS catalog row → Zarr on disk.

Composes the four primitives that already exist in their own modules:

    download_grib2_payload   (S3 + gzip)
    decode_grib2_to_dataarray (cfgrib via xarray)
    write_dataarray_to_zarr   (xarray.to_zarr)
    upsert_mrms_grid          (Postgres catalog)

into a single async orchestrator that turns one :class:`MrmsFile` into a
durable :class:`MrmsGridLocator`. The synchronous, CPU-bound steps
(download + decode + Zarr write) all run on a worker thread via
``asyncio.to_thread`` so the event loop stays responsive — multiple
materialisations can overlap on the I/O wait of S3 or Postgres.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import structlog

from aeroza.ingest.mrms import MRMS_BUCKET, MrmsFile
from aeroza.ingest.mrms_decode import (
    decode_grib2_to_dataarray,
    download_grib2_payload,
)
from aeroza.ingest.mrms_grids_store import upsert_mrms_grid
from aeroza.ingest.mrms_zarr import (
    MrmsGridLocator,
    write_dataarray_to_zarr,
)
from aeroza.shared.db import Database

log = structlog.get_logger(__name__)


async def materialise_mrms_file(
    *,
    db: Database,
    s3_client: Any,
    file: MrmsFile,
    target_root: str | Path,
    bucket: str = MRMS_BUCKET,
    variable_filter: str | None = None,
) -> MrmsGridLocator:
    """Decode ``file`` from S3 to Zarr and record the locator in Postgres.

    Returns the freshly-written :class:`MrmsGridLocator`. The
    transaction is committed before returning. Re-running on the same
    ``MrmsFile`` overwrites the on-disk Zarr and updates the catalog
    row in place — idempotent, suitable for replay.
    """
    log.info(
        "mrms.materialise.start",
        key=file.key,
        product=file.product,
        target_root=str(target_root),
    )

    locator = await asyncio.to_thread(
        _download_decode_and_write,
        s3_client=s3_client,
        bucket=bucket,
        file=file,
        target_root=target_root,
        variable_filter=variable_filter,
    )

    async with db.sessionmaker() as session:
        inserted = await upsert_mrms_grid(session, locator)
        await session.commit()

    log.info(
        "mrms.materialise.done",
        key=file.key,
        zarr_uri=locator.zarr_uri,
        variable=locator.variable,
        inserted=inserted,
    )
    return locator


def _download_decode_and_write(
    *,
    s3_client: Any,
    bucket: str,
    file: MrmsFile,
    target_root: str | Path,
    variable_filter: str | None,
) -> MrmsGridLocator:
    """Synchronous core of the materialiser. Runs on a worker thread.

    Lifted out of :func:`materialise_mrms_file` so a single
    ``asyncio.to_thread`` call covers the whole CPU+I/O sequence; if any
    step raises, the orchestrator's ``async`` frame sees the exception
    directly and the DB upsert is skipped.
    """
    payload = download_grib2_payload(s3_client, bucket=bucket, key=file.key)
    da = decode_grib2_to_dataarray(payload, variable_filter=variable_filter)
    return write_dataarray_to_zarr(da, target_root=target_root, file_key=file.key)
