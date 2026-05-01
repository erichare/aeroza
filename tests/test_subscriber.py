"""Unit tests for :class:`InMemoryAlertSubscriber`."""

from __future__ import annotations

import asyncio

import pytest

from aeroza.ingest.nws_alerts import Alert
from aeroza.stream.subscriber import AlertSubscriber, InMemoryAlertSubscriber

pytestmark = pytest.mark.unit


def _alert(alert_id: str) -> Alert:
    return Alert.model_validate({"id": alert_id, "event": "Severe Thunderstorm Warning"})


async def test_drains_initial_alerts_then_blocks() -> None:
    subscriber = InMemoryAlertSubscriber(initial=[_alert("a"), _alert("b")])
    received: list[str] = []
    stream = subscriber.subscribe_new_alerts()
    received.append((await stream.__anext__()).id)
    received.append((await stream.__anext__()).id)
    # The third __anext__ should block until either push() or close().
    next_task = asyncio.create_task(stream.__anext__())
    await asyncio.sleep(0)
    assert not next_task.done()
    await subscriber.close()
    with pytest.raises(StopAsyncIteration):
        await next_task
    assert received == ["a", "b"]


async def test_pushed_alerts_arrive_after_subscription() -> None:
    subscriber = InMemoryAlertSubscriber()
    received: list[str] = []

    async def consume() -> None:
        async for alert in subscriber.subscribe_new_alerts():
            received.append(alert.id)

    consumer = asyncio.create_task(consume())
    # Yield control so the consumer registers its queue before we push.
    await asyncio.sleep(0)
    await subscriber.push(_alert("late-1"))
    await subscriber.push(_alert("late-2"))
    await subscriber.close()
    await consumer
    assert received == ["late-1", "late-2"]


async def test_multiple_subscribers_each_get_full_feed() -> None:
    """NATS-style fan-out: each consumer receives every published alert."""
    subscriber = InMemoryAlertSubscriber()
    a_received: list[str] = []
    b_received: list[str] = []

    async def consume(target: list[str]) -> None:
        async for alert in subscriber.subscribe_new_alerts():
            target.append(alert.id)

    a_task = asyncio.create_task(consume(a_received))
    b_task = asyncio.create_task(consume(b_received))
    await asyncio.sleep(0)
    await subscriber.push(_alert("x"))
    await subscriber.push(_alert("y"))
    await subscriber.close()
    await a_task
    await b_task
    assert a_received == ["x", "y"]
    assert b_received == ["x", "y"]


async def test_close_terminates_active_subscriptions() -> None:
    subscriber = InMemoryAlertSubscriber()

    async def consume() -> int:
        count = 0
        async for _ in subscriber.subscribe_new_alerts():
            count += 1
        return count

    task = asyncio.create_task(consume())
    await asyncio.sleep(0)
    await subscriber.close()
    assert (await asyncio.wait_for(task, timeout=1.0)) == 0


def test_satisfies_alert_subscriber_protocol() -> None:
    subscriber: AlertSubscriber = InMemoryAlertSubscriber()
    assert hasattr(subscriber, "subscribe_new_alerts")
