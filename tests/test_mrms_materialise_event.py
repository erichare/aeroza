"""Integration tests for the event-triggered materialise driver.

The interval-loop tick is already covered by
:mod:`tests.test_mrms_materialise_poll`; this suite confirms that
pushing a file event into a :class:`MrmsFileSubscriber` triggers a
materialise tick against the same DB state. The download + decode
path is stubbed so we don't need eccodes; the rest of the pipeline
(catalog scan → Zarr write → upsert → grid event) runs end-to-end.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import patch

import numpy as np
import pytest
import xarray as xr
from sqlalchemy import select, text

from aeroza.ingest.mrms import MrmsFile
from aeroza.ingest.mrms_grids_models import MrmsGridRow
from aeroza.ingest.mrms_materialise_event import run_event_triggered_materialisation
from aeroza.ingest.mrms_store import upsert_mrms_files
from aeroza.shared.db import Database
from aeroza.stream.publisher import InMemoryMrmsGridPublisher
from aeroza.stream.subscriber import InMemoryMrmsFileSubscriber

pytestmark = pytest.mark.integration

PRODUCT: str = "MergedReflectivityComposite"
LEVEL: str = "00.50"


def _file(suffix: str = "120000", *, valid_at: datetime | None = None) -> MrmsFile:
    key = f"CONUS/{PRODUCT}_{LEVEL}/20260501/MRMS_{PRODUCT}_{LEVEL}_20260501-{suffix}.grib2.gz"
    return MrmsFile(
        key=key,
        product=PRODUCT,
        level=LEVEL,
        valid_at=valid_at or datetime(2026, 5, 1, 12, 0, tzinfo=UTC),
        size_bytes=10_000,
        etag="v1",
    )


def _synthetic_da() -> xr.DataArray:
    return xr.DataArray(
        np.arange(20, dtype=np.float32).reshape(4, 5),
        dims=("latitude", "longitude"),
        name="reflectivity",
    )


def _patch_decode() -> Any:
    download = patch(
        "aeroza.ingest.mrms_grids.download_grib2_payload",
        return_value=b"fake-grib2",
    )
    decode = patch(
        "aeroza.ingest.mrms_grids.decode_grib2_to_dataarray",
        return_value=_synthetic_da(),
    )
    return download, decode


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


async def _persisted_keys(integration_db: Database) -> set[str]:
    async with integration_db.sessionmaker() as session:
        rows = (await session.execute(select(MrmsGridRow))).scalars().all()
    return {r.file_key for r in rows}


async def test_event_triggers_materialise_tick(integration_db: Database, tmp_path: Path) -> None:
    """Pushing a file event causes the unmaterialised file to land as Zarr."""
    a = _file(suffix="120000")
    await _seed_files(integration_db, a)

    subscriber = InMemoryMrmsFileSubscriber()
    publisher = InMemoryMrmsGridPublisher()

    download, decode = _patch_decode()
    with download, decode:
        consumer = asyncio.create_task(
            run_event_triggered_materialisation(
                subscriber=subscriber,
                db=integration_db,
                s3_client=object(),
                target_root=tmp_path,
                product=PRODUCT,
                level=LEVEL,
                publisher=publisher,
            )
        )
        await subscriber.wait_for_subscriber_count(1)
        await subscriber.push(a)
        # Let the consumer drain the event and the resulting tick.
        await asyncio.sleep(0.05)
        await subscriber.close()
        await asyncio.wait_for(consumer, timeout=2.0)

    assert await _persisted_keys(integration_db) == {a.key}
    assert publisher.published_keys == (a.key,)


async def test_event_decoupled_from_payload_uses_catalog_scan(
    integration_db: Database, tmp_path: Path
) -> None:
    """An event for one key triggers a tick that picks up *all* unmaterialised
    files for the product/level — robust to bursts and missed events."""
    a = _file(suffix="120000")
    b = _file(
        suffix="120200",
        valid_at=datetime(2026, 5, 1, 12, 2, tzinfo=UTC),
    )
    await _seed_files(integration_db, a, b)

    subscriber = InMemoryMrmsFileSubscriber()
    download, decode = _patch_decode()
    with download, decode:
        consumer = asyncio.create_task(
            run_event_triggered_materialisation(
                subscriber=subscriber,
                db=integration_db,
                s3_client=object(),
                target_root=tmp_path,
                product=PRODUCT,
                level=LEVEL,
                batch_size=10,
            )
        )
        await subscriber.wait_for_subscriber_count(1)
        # Push a single event; the tick should still drain both pending files.
        await subscriber.push(a)
        await asyncio.sleep(0.05)
        await subscriber.close()
        await asyncio.wait_for(consumer, timeout=2.0)

    assert await _persisted_keys(integration_db) == {a.key, b.key}


async def test_consumer_survives_per_tick_failures(
    integration_db: Database, tmp_path: Path
) -> None:
    """A tick exception is logged, then the next event still triggers work."""
    a = _file(suffix="120000")
    b = _file(
        suffix="120200",
        valid_at=datetime(2026, 5, 1, 12, 2, tzinfo=UTC),
    )
    await _seed_files(integration_db, a, b)

    subscriber = InMemoryMrmsFileSubscriber()
    call_count = {"n": 0}

    def flaky(*_args: Any, **_kwargs: Any) -> Any:
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise RuntimeError("first tick blew up")
        # Real tick on subsequent calls.
        return None

    real_tick = "aeroza.ingest.mrms_materialise_event.materialise_unmaterialised_once"
    with patch(real_tick, side_effect=flaky):
        consumer = asyncio.create_task(
            run_event_triggered_materialisation(
                subscriber=subscriber,
                db=integration_db,
                s3_client=object(),
                target_root=tmp_path,
                product=PRODUCT,
                level=LEVEL,
            )
        )
        await subscriber.wait_for_subscriber_count(1)
        await subscriber.push(a)  # tick raises
        await asyncio.sleep(0.01)
        await subscriber.push(b)  # tick succeeds (no-op stub)
        await asyncio.sleep(0.01)
        await subscriber.close()
        await asyncio.wait_for(consumer, timeout=2.0)

    assert call_count["n"] == 2  # both events fired a tick


async def test_consumer_exits_when_subscriber_closes(
    integration_db: Database, tmp_path: Path
) -> None:
    """Closing the subscriber lets the consumer task complete cleanly."""
    subscriber = InMemoryMrmsFileSubscriber()
    consumer = asyncio.create_task(
        run_event_triggered_materialisation(
            subscriber=subscriber,
            db=integration_db,
            s3_client=object(),
            target_root=tmp_path,
            product=PRODUCT,
            level=LEVEL,
        )
    )
    await subscriber.wait_for_subscriber_count(1)
    await subscriber.close()
    await asyncio.wait_for(consumer, timeout=2.0)
    assert consumer.done() and consumer.exception() is None


async def test_consumer_cancellation_is_propagated(
    integration_db: Database, tmp_path: Path
) -> None:
    """Cancelling the surrounding task tears the subscription down cleanly."""
    subscriber = InMemoryMrmsFileSubscriber()
    consumer = asyncio.create_task(
        run_event_triggered_materialisation(
            subscriber=subscriber,
            db=integration_db,
            s3_client=object(),
            target_root=tmp_path,
            product=PRODUCT,
            level=LEVEL,
        )
    )
    await subscriber.wait_for_subscriber_count(1)
    consumer.cancel()
    with pytest.raises(asyncio.CancelledError):
        await consumer
