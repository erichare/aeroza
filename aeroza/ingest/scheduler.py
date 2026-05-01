"""Tiny async interval scheduler.

We don't need APScheduler / arq for one or two recurring jobs — an
``asyncio.Task`` that ``await``s a callable and sleeps in a loop is enough,
and keeping the implementation in-tree means tests can drive its lifecycle
deterministically (cancel, drain, assert tick count) without adding a
dependency or shimming a third-party scheduler.

Per-tick exceptions are logged but do **not** stop the loop — a transient
network failure on one fetch must not silence ingestion for the next hour.
The runner caller decides when to stop the loop (typically on SIGTERM).
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import suppress

import structlog

log = structlog.get_logger(__name__)

Tick = Callable[[], Awaitable[None]]


class IntervalLoop:
    """Run ``tick`` every ``interval_s`` seconds until :meth:`stop` is called.

    The first tick fires immediately (after :meth:`start`); subsequent ticks
    fire ``interval_s`` after the previous tick *finishes* (not "every N
    seconds wall-clock" — overlapping ticks are not allowed).
    """

    def __init__(
        self,
        *,
        tick: Tick,
        interval_s: float,
        name: str,
    ) -> None:
        if interval_s <= 0:
            raise ValueError(f"interval_s must be positive, got {interval_s}")
        self._tick = tick
        self._interval_s = interval_s
        self._name = name
        self._task: asyncio.Task[None] | None = None
        self._stopping = asyncio.Event()
        self._tick_count = 0

    @property
    def name(self) -> str:
        return self._name

    @property
    def tick_count(self) -> int:
        return self._tick_count

    @property
    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def start(self) -> None:
        if self.is_running:
            raise RuntimeError(f"loop {self._name!r} already running")
        self._stopping.clear()
        self._task = asyncio.create_task(self._run(), name=f"interval-loop:{self._name}")
        log.info("interval_loop.start", name=self._name, interval_s=self._interval_s)

    async def stop(self) -> None:
        """Signal the loop to stop and await its completion."""
        if self._task is None:
            return
        self._stopping.set()
        self._task.cancel()
        with suppress(asyncio.CancelledError):
            await self._task
        log.info("interval_loop.stop", name=self._name, ticks=self._tick_count)
        self._task = None

    async def _run(self) -> None:
        while not self._stopping.is_set():
            try:
                await self._tick()
                self._tick_count += 1
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.exception("interval_loop.tick_failed", name=self._name, error=str(exc))
            try:
                await asyncio.wait_for(self._stopping.wait(), timeout=self._interval_s)
            except TimeoutError:
                continue
            else:
                break
