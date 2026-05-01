"""Integration tests for the MRMS poll orchestrator.

Stub fetcher (no S3), real Postgres via the integration_db fixture,
:class:`InMemoryMrmsFilePublisher` to verify which files produced events.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest
from sqlalchemy import text

from aeroza.ingest.mrms import MrmsFile
from aeroza.ingest.mrms_poll import poll_mrms_files_once
from aeroza.shared.db import Database
from aeroza.stream.publisher import InMemoryMrmsFilePublisher, MrmsFilePublisher

pytestmark = pytest.mark.integration


def _file(
    key: str,
    *,
    product: str = "MergedReflectivityComposite",
    level: str = "00.50",
    valid_at: datetime | None = None,
    size_bytes: int = 10_000,
    etag: str | None = "v1",
) -> MrmsFile:
    return MrmsFile(
        key=key,
        product=product,
        level=level,
        valid_at=valid_at or datetime(2026, 5, 1, 12, 0, tzinfo=UTC),
        size_bytes=size_bytes,
        etag=etag,
    )


def _stub_fetcher(*files: MrmsFile) -> Any:
    async def _fetch() -> tuple[MrmsFile, ...]:
        return tuple(files)

    return _fetch


@pytest.fixture(autouse=True)
async def _truncate_after(integration_db: Database) -> Any:
    yield
    async with integration_db.sessionmaker() as session:
        await session.execute(text("TRUNCATE TABLE mrms_files CASCADE"))
        await session.commit()


async def test_publishes_new_files_on_first_tick(integration_db: Database) -> None:
    publisher = InMemoryMrmsFilePublisher()
    fetcher = _stub_fetcher(_file("a"), _file("b"))

    result = await poll_mrms_files_once(db=integration_db, publisher=publisher, fetcher=fetcher)

    assert set(result.inserted_keys) == {"a", "b"}
    assert result.updated_keys == ()
    assert set(publisher.published_keys) == {"a", "b"}


async def test_does_not_publish_for_no_op_upsert(integration_db: Database) -> None:
    publisher = InMemoryMrmsFilePublisher()
    same = _file("a", etag="static")

    first = await poll_mrms_files_once(
        db=integration_db, publisher=publisher, fetcher=_stub_fetcher(same)
    )
    assert first.inserted_keys == ("a",)
    assert publisher.published_keys == ("a",)

    publisher.clear()
    second = await poll_mrms_files_once(
        db=integration_db, publisher=publisher, fetcher=_stub_fetcher(same)
    )
    assert second.inserted_keys == ()
    assert second.updated_keys == ()
    assert publisher.published_keys == ()


async def test_does_not_publish_for_real_updates(integration_db: Database) -> None:
    publisher = InMemoryMrmsFilePublisher()

    await poll_mrms_files_once(
        db=integration_db,
        publisher=publisher,
        fetcher=_stub_fetcher(_file("a", etag="v1")),
    )
    publisher.clear()

    second = await poll_mrms_files_once(
        db=integration_db,
        publisher=publisher,
        fetcher=_stub_fetcher(_file("a", etag="v2")),
    )
    assert second.inserted_keys == ()
    assert second.updated_keys == ("a",)
    # Updates must NOT produce events — only first-time-seen keys do.
    assert publisher.published_keys == ()


async def test_mixed_batch_publishes_only_inserts(integration_db: Database) -> None:
    publisher = InMemoryMrmsFilePublisher()
    await poll_mrms_files_once(
        db=integration_db,
        publisher=publisher,
        fetcher=_stub_fetcher(_file("a", etag="v1")),
    )
    publisher.clear()

    result = await poll_mrms_files_once(
        db=integration_db,
        publisher=publisher,
        fetcher=_stub_fetcher(
            _file("a", etag="v2"),  # update
            _file("b"),  # insert
        ),
    )

    assert set(result.inserted_keys) == {"b"}
    assert set(result.updated_keys) == {"a"}
    assert publisher.published_keys == ("b",)


async def test_publisher_failures_do_not_break_persistence(
    integration_db: Database,
) -> None:
    """A flaky publisher must not roll back the upsert. The catalog is
    durably persisted; a future replay job catches up missed events."""

    class FlakyPublisher:
        async def publish_new_file(self, file: MrmsFile) -> None:
            raise RuntimeError(f"transport down for {file.key}")

    publisher: MrmsFilePublisher = FlakyPublisher()
    result = await poll_mrms_files_once(
        db=integration_db,
        publisher=publisher,
        fetcher=_stub_fetcher(_file("a"), _file("b")),
    )

    assert result.inserted == 2
    async with integration_db.sessionmaker() as session:
        count = (await session.execute(text("SELECT COUNT(*) FROM mrms_files"))).scalar_one()
        assert count == 2


async def test_empty_fetch_is_safe(integration_db: Database) -> None:
    publisher = InMemoryMrmsFilePublisher()
    result = await poll_mrms_files_once(
        db=integration_db, publisher=publisher, fetcher=_stub_fetcher()
    )
    assert result.inserted_keys == ()
    assert result.updated_keys == ()
    assert publisher.published_keys == ()
