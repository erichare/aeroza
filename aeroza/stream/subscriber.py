"""Subscriber abstractions for ingest events.

Mirror of :mod:`aeroza.stream.publisher`. Each Protocol decouples
consumers (HTTP routes, the materialise worker, …) from a specific
transport — production wires up NATS via the implementations in
:mod:`aeroza.stream.nats`; tests drive in-memory queues so consumers
can be exercised without a broker.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterable
from typing import Protocol

import structlog

from aeroza.ingest.mrms import MrmsFile
from aeroza.ingest.mrms_zarr import MrmsGridLocator
from aeroza.ingest.nws_alerts import Alert

log = structlog.get_logger(__name__)


class AlertSubscriber(Protocol):
    """Yields newly-observed alerts as they arrive."""

    def subscribe_new_alerts(self) -> AsyncIterator[Alert]:  # pragma: no cover - interface
        ...


class MrmsFileSubscriber(Protocol):
    """Yields newly-observed MRMS catalog rows as they arrive.

    Same fan-out semantics as :class:`AlertSubscriber`: each call to
    :meth:`subscribe_new_files` opens an independent feed.
    """

    def subscribe_new_files(self) -> AsyncIterator[MrmsFile]:  # pragma: no cover - interface
        ...


class MrmsGridSubscriber(Protocol):
    """Yields newly-materialised MRMS grid locators as they arrive.

    Same fan-out semantics as :class:`MrmsFileSubscriber`. Used by the
    webhook dispatcher to drive rule evaluation on each new grid.
    """

    def subscribe_new_grids(
        self,
    ) -> AsyncIterator[MrmsGridLocator]:  # pragma: no cover - interface
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


class InMemoryMrmsFileSubscriber:
    """In-process queue of MRMS file events. Test-only.

    Identical fan-out / lifecycle semantics to :class:`InMemoryAlertSubscriber`;
    the only difference is the payload type. See its docstring for details.
    """

    def __init__(self, initial: Iterable[MrmsFile] = ()) -> None:
        self._initial = tuple(initial)
        self._queues: list[asyncio.Queue[MrmsFile | None]] = []

    @property
    def subscriber_count(self) -> int:
        return len(self._queues)

    async def push(self, file: MrmsFile) -> None:
        for queue in self._queues:
            await queue.put(file)

    async def close(self) -> None:
        for queue in self._queues:
            await queue.put(None)

    async def wait_for_subscriber_count(self, count: int = 1, *, timeout: float = 1.0) -> None:
        deadline = asyncio.get_running_loop().time() + timeout
        while self.subscriber_count < count:
            if asyncio.get_running_loop().time() >= deadline:
                raise TimeoutError(
                    f"only {self.subscriber_count} subscriber(s) after {timeout}s, expected {count}"
                )
            await asyncio.sleep(0)

    async def subscribe_new_files(self) -> AsyncIterator[MrmsFile]:
        queue: asyncio.Queue[MrmsFile | None] = asyncio.Queue()
        for file in self._initial:
            await queue.put(file)
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


class InMemoryMrmsGridSubscriber:
    """In-process queue of MRMS grid locator events. Test-only.

    Same shape as :class:`InMemoryMrmsFileSubscriber`. The webhook
    dispatcher (slice 3) consumes ``MrmsGridLocator`` events to drive
    rule evaluation; this in-memory variant lets us exercise the
    dispatcher without spinning up NATS.
    """

    def __init__(self, initial: Iterable[MrmsGridLocator] = ()) -> None:
        self._initial = tuple(initial)
        self._queues: list[asyncio.Queue[MrmsGridLocator | None]] = []

    @property
    def subscriber_count(self) -> int:
        return len(self._queues)

    async def push(self, locator: MrmsGridLocator) -> None:
        for queue in self._queues:
            await queue.put(locator)

    async def close(self) -> None:
        for queue in self._queues:
            await queue.put(None)

    async def wait_for_subscriber_count(self, count: int = 1, *, timeout: float = 1.0) -> None:
        deadline = asyncio.get_running_loop().time() + timeout
        while self.subscriber_count < count:
            if asyncio.get_running_loop().time() >= deadline:
                raise TimeoutError(
                    f"only {self.subscriber_count} subscriber(s) after {timeout}s, expected {count}"
                )
            await asyncio.sleep(0)

    async def subscribe_new_grids(self) -> AsyncIterator[MrmsGridLocator]:
        queue: asyncio.Queue[MrmsGridLocator | None] = asyncio.Queue()
        for locator in self._initial:
            await queue.put(locator)
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
