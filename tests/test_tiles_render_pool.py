"""Unit tests for the shared render-concurrency semaphore."""

from __future__ import annotations

import asyncio
import os

import pytest

from aeroza.tiles.render_pool import (
    DEFAULT_RENDER_CONCURRENCY,
    get_render_semaphore,
    set_render_semaphore,
)

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _reset_semaphore() -> object:
    """Wipe the module singleton + the env override after each test so
    cases stay independent."""
    yield
    set_render_semaphore(asyncio.Semaphore(DEFAULT_RENDER_CONCURRENCY))
    os.environ.pop("AEROZA_TILE_RENDER_CONCURRENCY", None)


async def test_returns_singleton_across_calls() -> None:
    set_render_semaphore(asyncio.Semaphore(DEFAULT_RENDER_CONCURRENCY))
    first = get_render_semaphore()
    second = get_render_semaphore()
    assert first is second


async def test_set_render_semaphore_replaces_singleton() -> None:
    set_render_semaphore(asyncio.Semaphore(2))
    sema = get_render_semaphore()
    assert isinstance(sema, asyncio.Semaphore)
    # Acquire twice — both should succeed without blocking — then a
    # third acquire must block. ``locked()`` is True once the value
    # has been driven to zero.
    await sema.acquire()
    await sema.acquire()
    assert sema.locked()


async def test_env_override_picked_up_on_first_use() -> None:
    os.environ["AEROZA_TILE_RENDER_CONCURRENCY"] = "1"
    # Force re-construction by zeroing the cached singleton.
    set_render_semaphore.__globals__["_render_semaphore"] = None  # type: ignore[index]
    sema = get_render_semaphore()
    await sema.acquire()
    assert sema.locked(), "concurrency=1 should make the semaphore locked after one acquire"


async def test_invalid_env_falls_back_to_default() -> None:
    os.environ["AEROZA_TILE_RENDER_CONCURRENCY"] = "not-a-number"
    set_render_semaphore.__globals__["_render_semaphore"] = None  # type: ignore[index]
    sema = get_render_semaphore()
    # Default is >= 2; we should be able to grab two slots without locking.
    await sema.acquire()
    assert not sema.locked()
    await sema.acquire()


async def test_back_pressure_serialises_excess_acquirers() -> None:
    """A third acquirer against a 2-slot semaphore must wait until one
    of the first two releases — the contract live tile rendering relies
    on for self-protection under burst load."""
    set_render_semaphore(asyncio.Semaphore(2))
    sema = get_render_semaphore()

    holder_a_done = asyncio.Event()
    holder_b_done = asyncio.Event()
    third_started = asyncio.Event()
    third_acquired = asyncio.Event()

    async def hold(release: asyncio.Event) -> None:
        async with sema:
            await release.wait()

    async def third() -> None:
        third_started.set()
        async with sema:
            third_acquired.set()

    a = asyncio.create_task(hold(holder_a_done))
    b = asyncio.create_task(hold(holder_b_done))
    await asyncio.sleep(0)  # let A and B grab their slots
    c = asyncio.create_task(third())
    await third_started.wait()
    await asyncio.sleep(0.01)  # confirm C is blocked
    assert not third_acquired.is_set()

    holder_a_done.set()
    await asyncio.wait_for(third_acquired.wait(), timeout=0.5)
    holder_b_done.set()

    await asyncio.gather(a, b, c)
