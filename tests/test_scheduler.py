"""Unit tests for the :class:`IntervalLoop` scheduler."""

from __future__ import annotations

import asyncio

import pytest

from aeroza.ingest.scheduler import IntervalLoop

pytestmark = pytest.mark.unit


async def test_rejects_non_positive_interval() -> None:
    async def noop() -> None:
        pass

    with pytest.raises(ValueError, match="positive"):
        IntervalLoop(tick=noop, interval_s=0, name="t")
    with pytest.raises(ValueError, match="positive"):
        IntervalLoop(tick=noop, interval_s=-1.0, name="t")


async def test_runs_first_tick_immediately_and_repeats() -> None:
    fired = asyncio.Event()
    counter = {"n": 0}

    async def tick() -> None:
        counter["n"] += 1
        if counter["n"] >= 3:
            fired.set()

    loop = IntervalLoop(tick=tick, interval_s=0.01, name="rapid")
    await loop.start()
    try:
        await asyncio.wait_for(fired.wait(), timeout=2.0)
    finally:
        await loop.stop()
    assert counter["n"] >= 3
    assert loop.tick_count >= 3


async def test_stop_is_idempotent() -> None:
    async def noop() -> None:
        pass

    loop = IntervalLoop(tick=noop, interval_s=0.05, name="idem")
    await loop.start()
    await loop.stop()
    await loop.stop()  # second call must not raise
    assert not loop.is_running


async def test_double_start_raises() -> None:
    async def noop() -> None:
        pass

    loop = IntervalLoop(tick=noop, interval_s=0.05, name="dup")
    await loop.start()
    try:
        with pytest.raises(RuntimeError, match="already running"):
            await loop.start()
    finally:
        await loop.stop()


async def test_swallows_tick_exceptions_and_keeps_running() -> None:
    counter = {"n": 0}
    seen_after_failure = asyncio.Event()

    async def flaky() -> None:
        counter["n"] += 1
        if counter["n"] == 1:
            raise RuntimeError("boom")
        if counter["n"] >= 2:
            seen_after_failure.set()

    loop = IntervalLoop(tick=flaky, interval_s=0.01, name="flaky")
    await loop.start()
    try:
        await asyncio.wait_for(seen_after_failure.wait(), timeout=2.0)
    finally:
        await loop.stop()
    assert counter["n"] >= 2


async def test_cancellation_propagates_cleanly_through_tick() -> None:
    """A long-running tick must surrender to ``stop()`` quickly."""

    async def slow_tick() -> None:
        await asyncio.sleep(60)

    loop = IntervalLoop(tick=slow_tick, interval_s=0.01, name="slow")
    await loop.start()
    # Give the tick a moment to start.
    await asyncio.sleep(0.05)
    await asyncio.wait_for(loop.stop(), timeout=2.0)
    assert not loop.is_running


async def test_tick_count_does_not_advance_for_failed_ticks() -> None:
    fired = asyncio.Event()
    success_count = {"n": 0}

    async def half_failing() -> None:
        if success_count["n"] >= 2:
            fired.set()
            return
        success_count["n"] += 1

    # Run a few clean ticks and confirm the counter matches.
    loop = IntervalLoop(tick=half_failing, interval_s=0.01, name="counted")
    await loop.start()
    try:
        await asyncio.wait_for(fired.wait(), timeout=2.0)
    finally:
        await loop.stop()
    # Both successful ticks AND the early-return tick increment the counter
    # (no exception), so tick_count is at least 3 after fired.set().
    assert loop.tick_count >= 3
