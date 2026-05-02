"""Unit tests for :class:`NatsMrmsFileSubscriber`.

Mirrors :mod:`tests.test_nats_subscriber`. The subscriber's only job
is to decode JSON payloads from a NATS subscription's async iterator
into :class:`MrmsFile` records and unsubscribe cleanly on exit. Both
ends are exercised against an in-memory stub — no broker required.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import pytest

from aeroza.ingest.mrms import MrmsFile
from aeroza.stream.nats import (
    MRMS_NEW_FILE_SUBJECT,
    NatsMrmsFilePublisher,
    NatsMrmsFileSubscriber,
)

pytestmark = pytest.mark.unit


class StubMessage:
    def __init__(self, data: bytes) -> None:
        self.data = data


class StubSubscription:
    def __init__(self) -> None:
        self._queue: asyncio.Queue[bytes | None] = asyncio.Queue()
        self.unsubscribed = False

    async def push(self, payload: bytes) -> None:
        await self._queue.put(payload)

    async def end(self) -> None:
        await self._queue.put(None)

    @property
    def messages(self) -> AsyncIterator[StubMessage]:
        async def gen() -> AsyncIterator[StubMessage]:
            while True:
                item = await self._queue.get()
                if item is None:
                    return
                yield StubMessage(item)

        return gen()

    async def unsubscribe(self) -> None:
        self.unsubscribed = True


class StubNatsClient:
    def __init__(self) -> None:
        self.subscribed_subjects: list[str] = []
        self.last_subscription: StubSubscription | None = None
        # Captures publishes when the same client is used as a publisher,
        # so a single test can prove the encoder/decoder round-trip.
        self.published: list[tuple[str, bytes]] = []

    async def subscribe(self, subject: str) -> StubSubscription:
        self.subscribed_subjects.append(subject)
        sub = StubSubscription()
        self.last_subscription = sub
        return sub

    async def publish(self, subject: str, payload: bytes) -> None:
        self.published.append((subject, payload))


def _file(key: str, *, valid_at: datetime | None = None) -> MrmsFile:
    return MrmsFile(
        key=key,
        product="MergedReflectivityComposite",
        level="00.50",
        valid_at=valid_at or datetime(2026, 5, 1, 12, 0, tzinfo=UTC),
        size_bytes=12_345,
        etag="etag-1",
    )


async def test_subscribes_to_default_subject() -> None:
    client = StubNatsClient()
    subscriber = NatsMrmsFileSubscriber(client)
    stream = subscriber.subscribe_new_files()
    starter = asyncio.create_task(stream.__anext__())
    await asyncio.sleep(0)
    assert client.subscribed_subjects == [MRMS_NEW_FILE_SUBJECT]
    starter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await starter


async def test_overrides_subject() -> None:
    client = StubNatsClient()
    subscriber = NatsMrmsFileSubscriber(client, subject="custom.mrms.subject")
    stream = subscriber.subscribe_new_files()
    starter = asyncio.create_task(stream.__anext__())
    await asyncio.sleep(0)
    assert client.subscribed_subjects == ["custom.mrms.subject"]
    assert subscriber.subject == "custom.mrms.subject"
    starter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await starter


async def test_round_trips_published_files_in_order() -> None:
    """The publisher's encoder + the subscriber's decoder must agree."""
    client = StubNatsClient()
    subscriber = NatsMrmsFileSubscriber(client)
    publisher = NatsMrmsFilePublisher(client)
    received: list[MrmsFile] = []

    async def consume() -> None:
        async for file in subscriber.subscribe_new_files():
            received.append(file)

    task = asyncio.create_task(consume())
    await asyncio.sleep(0)
    sub = client.last_subscription
    assert sub is not None

    a = _file("CONUS/MergedReflectivityComposite_00.50/20260501/MRMS_a.grib2.gz")
    b = _file(
        "CONUS/MergedReflectivityComposite_00.50/20260501/MRMS_b.grib2.gz",
        valid_at=datetime(2026, 5, 1, 12, 2, tzinfo=UTC),
    )

    await publisher.publish_new_file(a)
    await publisher.publish_new_file(b)

    # Hand the published bytes back into the subscription as if NATS had
    # delivered them — the round-trip is what we're proving.
    for _, payload in client.published:
        await sub.push(payload)
    await sub.end()
    await task

    assert received == [a, b]
    assert sub.unsubscribed is True


async def test_skips_invalid_payloads_without_terminating() -> None:
    client = StubNatsClient()
    subscriber = NatsMrmsFileSubscriber(client)
    received: list[str] = []

    async def consume() -> None:
        async for file in subscriber.subscribe_new_files():
            received.append(file.key)

    task = asyncio.create_task(consume())
    await asyncio.sleep(0)
    sub = client.last_subscription
    assert sub is not None

    good = json.dumps(
        {
            "key": "CONUS/X_00.50/20260501/MRMS_X.grib2.gz",
            "product": "X",
            "level": "00.50",
            "validAt": "2026-05-01T12:00:00+00:00",
            "sizeBytes": 1,
            "etag": None,
        }
    ).encode("utf-8")

    await sub.push(b"not json at all")
    await sub.push(b'{"missing":"fields"}')  # KeyError on `key`
    await sub.push(good)
    await sub.end()
    await task

    assert received == ["CONUS/X_00.50/20260501/MRMS_X.grib2.gz"]


async def test_unsubscribes_on_normal_exit() -> None:
    client = StubNatsClient()
    subscriber = NatsMrmsFileSubscriber(client)
    stream = subscriber.subscribe_new_files()
    starter = asyncio.create_task(stream.__anext__())
    await asyncio.sleep(0)
    sub = client.last_subscription
    assert sub is not None
    await NatsMrmsFilePublisher(client).publish_new_file(
        _file("CONUS/MergedReflectivityComposite_00.50/20260501/MRMS_z.grib2.gz")
    )
    await sub.push(client.published[0][1])
    await starter
    await stream.aclose()
    assert sub.unsubscribed is True
