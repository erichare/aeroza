"""Unit tests for :class:`InMemoryMrmsFileSubscriber`.

Mirrors :mod:`tests.test_subscriber` so the file-event consumer enjoys
the same fan-out / lifecycle guarantees the alert consumer relies on.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

from aeroza.ingest.mrms import MrmsFile
from aeroza.stream.subscriber import (
    InMemoryMrmsFileSubscriber,
    MrmsFileSubscriber,
)

pytestmark = pytest.mark.unit


def _file(suffix: str) -> MrmsFile:
    return MrmsFile(
        key=f"CONUS/X_00.50/20260501/MRMS_X_00.50_20260501-{suffix}.grib2.gz",
        product="X",
        level="00.50",
        valid_at=datetime(2026, 5, 1, 12, 0, tzinfo=UTC),
        size_bytes=10,
        etag="e",
    )


async def test_drains_initial_files_then_blocks() -> None:
    subscriber = InMemoryMrmsFileSubscriber(initial=[_file("120000"), _file("120200")])
    received: list[str] = []
    stream = subscriber.subscribe_new_files()
    received.append((await stream.__anext__()).key)
    received.append((await stream.__anext__()).key)
    next_task = asyncio.create_task(stream.__anext__())
    await asyncio.sleep(0)
    assert not next_task.done()
    await subscriber.close()
    with pytest.raises(StopAsyncIteration):
        await next_task
    assert received == [_file("120000").key, _file("120200").key]


async def test_pushed_files_arrive_after_subscription() -> None:
    subscriber = InMemoryMrmsFileSubscriber()
    received: list[str] = []

    async def consume() -> None:
        async for file in subscriber.subscribe_new_files():
            received.append(file.key)

    consumer = asyncio.create_task(consume())
    await asyncio.sleep(0)
    await subscriber.push(_file("120000"))
    await subscriber.push(_file("120200"))
    await subscriber.close()
    await consumer
    assert received == [_file("120000").key, _file("120200").key]


async def test_multiple_subscribers_each_get_full_feed() -> None:
    subscriber = InMemoryMrmsFileSubscriber()
    a_received: list[str] = []
    b_received: list[str] = []

    async def consume(target: list[str]) -> None:
        async for file in subscriber.subscribe_new_files():
            target.append(file.key)

    a_task = asyncio.create_task(consume(a_received))
    b_task = asyncio.create_task(consume(b_received))
    await asyncio.sleep(0)
    await subscriber.push(_file("120000"))
    await subscriber.push(_file("120200"))
    await subscriber.close()
    await a_task
    await b_task
    assert a_received == [_file("120000").key, _file("120200").key]
    assert b_received == [_file("120000").key, _file("120200").key]


async def test_close_terminates_active_subscriptions() -> None:
    subscriber = InMemoryMrmsFileSubscriber()

    async def consume() -> int:
        count = 0
        async for _ in subscriber.subscribe_new_files():
            count += 1
        return count

    task = asyncio.create_task(consume())
    await asyncio.sleep(0)
    await subscriber.close()
    assert (await asyncio.wait_for(task, timeout=1.0)) == 0


def test_satisfies_file_subscriber_protocol() -> None:
    subscriber: MrmsFileSubscriber = InMemoryMrmsFileSubscriber()
    assert hasattr(subscriber, "subscribe_new_files")
