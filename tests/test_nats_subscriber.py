"""Unit tests for :class:`NatsAlertSubscriber`.

We don't run a real NATS broker here. The subscriber's only job is to
decode messages from a NATS subscription's async iterator into pydantic
``Alert`` instances and unsubscribe cleanly on exit — both of which we
can fully exercise with an in-memory stub.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import pytest

from aeroza.ingest.nws_alerts import Alert
from aeroza.stream.nats import NWS_NEW_ALERT_SUBJECT, NatsAlertSubscriber

pytestmark = pytest.mark.unit


class StubMessage:
    def __init__(self, data: bytes) -> None:
        self.data = data


class StubSubscription:
    """Mimics a nats-py Subscription's ``messages`` async iterator."""

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

    async def subscribe(self, subject: str) -> StubSubscription:
        self.subscribed_subjects.append(subject)
        sub = StubSubscription()
        self.last_subscription = sub
        return sub


def _alert_payload(alert_id: str) -> bytes:
    alert = Alert.model_validate({"id": alert_id, "event": "Severe Thunderstorm Warning"})
    return alert.model_dump_json(by_alias=True).encode("utf-8")


async def test_subscribes_to_default_subject() -> None:
    client = StubNatsClient()
    subscriber = NatsAlertSubscriber(client)
    stream = subscriber.subscribe_new_alerts()
    # __anext__ on an unstarted stub will block — only spin it up briefly.
    starter = asyncio.create_task(stream.__anext__())
    await asyncio.sleep(0)
    assert client.subscribed_subjects == [NWS_NEW_ALERT_SUBJECT]
    starter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await starter


async def test_overrides_subject() -> None:
    client = StubNatsClient()
    subscriber = NatsAlertSubscriber(client, subject="custom.subject")
    stream = subscriber.subscribe_new_alerts()
    starter = asyncio.create_task(stream.__anext__())
    await asyncio.sleep(0)
    assert client.subscribed_subjects == ["custom.subject"]
    assert subscriber.subject == "custom.subject"
    starter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await starter


async def test_decodes_payloads_to_alerts_in_order() -> None:
    client = StubNatsClient()
    subscriber = NatsAlertSubscriber(client)
    received: list[str] = []

    async def consume() -> None:
        async for alert in subscriber.subscribe_new_alerts():
            received.append(alert.id)

    task = asyncio.create_task(consume())
    await asyncio.sleep(0)
    sub = client.last_subscription
    assert sub is not None
    await sub.push(_alert_payload("a"))
    await sub.push(_alert_payload("b"))
    await sub.push(_alert_payload("c"))
    await sub.end()
    await task
    assert received == ["a", "b", "c"]
    assert sub.unsubscribed is True


async def test_skips_invalid_payloads_without_terminating() -> None:
    client = StubNatsClient()
    subscriber = NatsAlertSubscriber(client)
    received: list[str] = []

    async def consume() -> None:
        async for alert in subscriber.subscribe_new_alerts():
            received.append(alert.id)

    task = asyncio.create_task(consume())
    await asyncio.sleep(0)
    sub = client.last_subscription
    assert sub is not None
    await sub.push(b"this is not json at all")
    await sub.push(_alert_payload("after-bad"))
    await sub.end()
    await task
    assert received == ["after-bad"]


async def test_unsubscribes_on_normal_exit() -> None:
    client = StubNatsClient()
    subscriber = NatsAlertSubscriber(client)
    stream = subscriber.subscribe_new_alerts()
    starter = asyncio.create_task(stream.__anext__())
    await asyncio.sleep(0)
    sub = client.last_subscription
    assert sub is not None
    await sub.push(_alert_payload("only-one"))
    await starter  # consume one message
    await stream.aclose()  # explicit close — what callers like StreamingResponse do
    assert sub.unsubscribed is True


async def test_unsubscribes_when_iteration_terminates_via_end() -> None:
    client = StubNatsClient()
    subscriber = NatsAlertSubscriber(client)

    async def consume() -> None:
        async for _ in subscriber.subscribe_new_alerts():
            pass

    task = asyncio.create_task(consume())
    await asyncio.sleep(0)
    sub = client.last_subscription
    assert sub is not None
    await sub.end()  # underlying source closes — generator exits naturally
    await task
    assert sub.unsubscribed is True
