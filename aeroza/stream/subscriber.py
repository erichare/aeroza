"""Subscriber abstractions for alert events.

Mirror of :mod:`aeroza.stream.publisher`. The :class:`AlertSubscriber`
Protocol decouples HTTP routes (and any other consumer) from a specific
transport — production wires up NATS via
:class:`aeroza.stream.nats.NatsAlertSubscriber`; tests use
:class:`InMemoryAlertSubscriber` to drive a fixed sequence of alerts
through the API surface without a broker.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterable
from typing import Protocol

import structlog

from aeroza.ingest.nws_alerts import Alert

log = structlog.get_logger(__name__)


class AlertSubscriber(Protocol):
    """Yields newly-observed alerts as they arrive."""

    def subscribe_new_alerts(self) -> AsyncIterator[Alert]:  # pragma: no cover - interface
        ...


class InMemoryAlertSubscriber:
    """Yields alerts from an in-process queue. Test-only.

    Calling :meth:`push` makes the next iteration of an active subscriber
    return that alert; multiple concurrent subscribers each get their own
    queue so fan-out semantics match what NATS provides at the broker level.
    """

    def __init__(self, initial: Iterable[Alert] = ()) -> None:
        self._initial = tuple(initial)
        self._queues: list[asyncio.Queue[Alert | None]] = []

    @property
    def subscriber_count(self) -> int:
        return len(self._queues)

    async def push(self, alert: Alert) -> None:
        for queue in self._queues:
            await queue.put(alert)

    async def close(self) -> None:
        """Signal every active subscription to terminate cleanly."""
        for queue in self._queues:
            await queue.put(None)

    async def wait_for_subscriber_count(self, count: int = 1, *, timeout: float = 1.0) -> None:
        """Test helper: block until at least ``count`` subscriptions are registered.

        Async generators register their internal queue only when ``__anext__``
        is first called, which can race with :meth:`push` when the test issues
        them in quick succession. Callers use this to ensure the registration
        has happened before pushing.
        """
        deadline = asyncio.get_running_loop().time() + timeout
        while self.subscriber_count < count:
            if asyncio.get_running_loop().time() >= deadline:
                raise TimeoutError(
                    f"only {self.subscriber_count} subscriber(s) after {timeout}s, expected {count}"
                )
            await asyncio.sleep(0)

    async def subscribe_new_alerts(self) -> AsyncIterator[Alert]:
        queue: asyncio.Queue[Alert | None] = asyncio.Queue()
        for alert in self._initial:
            await queue.put(alert)
        self._queues.append(queue)
        try:
            while True:
                item = await queue.get()
                if item is None:
                    return
                yield item
        finally:
            if queue in self._queues:
                self._queues.remove(queue)
