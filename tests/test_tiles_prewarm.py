"""Unit tests for the tile-prewarm consumer.

Builds a tiny synthetic Zarr grid with ``xarray``, then exercises the
prewarm function and the NATS-driven consumer. No HTTP, no DB, no real
broker — the in-memory subscriber + the actual ``render_tile_image``
function are enough.
"""

from __future__ import annotations

import asyncio
import threading
from contextlib import suppress
from pathlib import Path

import numpy as np
import pytest
import xarray as xr

import aeroza.tiles.prewarm as prewarm_module
from aeroza.ingest.mrms_zarr import MrmsGridLocator
from aeroza.stream.subscriber import InMemoryMrmsGridSubscriber
from aeroza.tiles.cache import CacheKey, TilePngCache
from aeroza.tiles.prewarm import (
    CONUS_BBOX,
    _LatestGridMailbox,
    conus_tile_coords,
    prewarm_tiles_for_grid,
    run_prewarm_consumer,
)
from aeroza.tiles.render_pool import set_render_semaphore

pytestmark = pytest.mark.unit


def _write_conus_grid(target: Path) -> str:
    """Write a small CONUS-aligned Zarr grid the prewarm can sample."""
    lats = np.linspace(50.0, 24.0, 32)  # north → south, MRMS-style
    lngs = np.linspace(-125.0, -66.0, 32)
    values = np.full((32, 32), 30.0, dtype=np.float32)
    da = xr.DataArray(
        values,
        coords={"latitude": lats, "longitude": lngs},
        dims=("latitude", "longitude"),
        name="reflectivity",
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    da.to_zarr(str(target), mode="w")
    return str(target)


def _locator(zarr_uri: str, file_key: str = "k1") -> MrmsGridLocator:
    return MrmsGridLocator(
        file_key=file_key,
        zarr_uri=zarr_uri,
        variable="reflectivity",
        dims=("latitude", "longitude"),
        shape=(32, 32),
        dtype="float32",
        nbytes=32 * 32 * 4,
    )


@pytest.fixture(autouse=True)
def _reset_semaphore() -> object:
    """Bind a fresh semaphore for every test so the singleton's state
    doesn't bleed between cases (and so the semaphore lives on the
    test's event loop)."""
    set_render_semaphore(asyncio.Semaphore(2))
    yield


def test_conus_tile_coords_covers_bbox_at_each_zoom() -> None:
    """At every supported zoom we should get a non-empty rectangle of
    tiles whose corners sit inside CONUS. Cheap sanity check that the
    slippy-tile math doesn't return an empty range under the math's
    rounding edge cases."""
    for zoom in (4, 5, 6):
        coords = conus_tile_coords(zoom)
        assert len(coords) >= 1
        n = 1 << zoom
        for x, y in coords:
            assert 0 <= x < n
            assert 0 <= y < n


def test_conus_tile_coords_grows_with_zoom() -> None:
    """Higher zoom = strictly more tiles covering the same bbox.
    Catches regressions where the y-axis math accidentally clamps to
    a single row."""
    for prev_zoom, next_zoom in [(4, 5), (5, 6)]:
        prev_count = len(conus_tile_coords(prev_zoom))
        next_count = len(conus_tile_coords(next_zoom))
        assert next_count > prev_count


def test_conus_tile_coords_rejects_negative_zoom() -> None:
    with pytest.raises(ValueError):
        conus_tile_coords(-1)


def test_conus_bbox_anchored_to_continental_extent() -> None:
    """Sanity check the constant — if someone widens it accidentally,
    prewarm cost balloons quadratically."""
    lng_w, lat_s, lng_e, lat_n = CONUS_BBOX
    assert lng_w < lng_e
    assert lat_s < lat_n
    # Continental US sits inside the standard MapLibre default bounds.
    assert lng_w >= -125.0
    assert lng_e <= -66.0


async def test_prewarm_populates_cache_for_every_zoom_format(tmp_path: Path) -> None:
    uri = _write_conus_grid(tmp_path / "g.zarr")
    cache = TilePngCache(max_bytes=8 * 1024 * 1024)

    stats = await prewarm_tiles_for_grid(
        _locator(uri),
        r2_client=None,
        lru_cache=cache,
        zooms=(4,),
        formats=("png", "webp"),
    )

    expected = len(conus_tile_coords(4)) * 2  # zooms × formats
    assert stats.rendered == expected
    assert stats.failed == 0
    assert stats.skipped_existing == 0

    # Every (z, x, y, fmt) pair we just rendered should be retrievable.
    for x, y in conus_tile_coords(4):
        for fmt in ("png", "webp"):
            cached = cache.get(CacheKey(file_key="k1", z=4, x=x, y=y, format=fmt))
            assert cached is not None
            assert isinstance(cached, bytes) and len(cached) > 0


async def test_prewarm_skips_already_cached_entries(tmp_path: Path) -> None:
    uri = _write_conus_grid(tmp_path / "g.zarr")
    cache = TilePngCache(max_bytes=8 * 1024 * 1024)

    first = await prewarm_tiles_for_grid(
        _locator(uri), r2_client=None, lru_cache=cache, zooms=(4,), formats=("png",)
    )
    assert first.rendered > 0
    assert first.skipped_existing == 0

    # Re-prewarming the same grid (e.g. NATS at-least-once redelivery)
    # must be a no-op — every entry should be skipped, not re-rendered.
    second = await prewarm_tiles_for_grid(
        _locator(uri), r2_client=None, lru_cache=cache, zooms=(4,), formats=("png",)
    )
    assert second.rendered == 0
    assert second.failed == 0
    assert second.skipped_existing == first.rendered


async def test_prewarm_uploads_to_r2_when_client_supplied(tmp_path: Path) -> None:
    """With an R2 client the LRU path is bypassed entirely — every
    rendered tile becomes a ``put_tile`` call. This is the production
    path: tile bytes live in Cloudflare R2 + fronted by the CDN, not
    inside any single API replica's memory.
    """
    uri = _write_conus_grid(tmp_path / "g.zarr")

    class _FakeR2:
        def __init__(self) -> None:
            self.uploads: list[tuple[str, int, int, int, str, int]] = []

        async def put_tile(
            self,
            *,
            file_key: str,
            z: int,
            x: int,
            y: int,
            fmt: str,
            body: bytes,
        ) -> None:
            self.uploads.append((file_key, z, x, y, fmt, len(body)))

        async def object_exists(self, *, file_key: str, z: int, x: int, y: int, fmt: str) -> bool:
            return False

    fake_r2 = _FakeR2()

    stats = await prewarm_tiles_for_grid(
        _locator(uri),
        r2_client=fake_r2,  # type: ignore[arg-type]
        lru_cache=None,
        zooms=(4,),
        formats=("webp",),
    )

    expected = len(conus_tile_coords(4))
    assert stats.rendered == expected
    assert stats.failed == 0
    assert stats.skipped_existing == 0
    # Every CONUS coord at z=4 was uploaded; key shape carries fileKey,
    # zoom, coords, and format so the route layer is unambiguous.
    assert len(fake_r2.uploads) == expected
    for fk, z, _x, _y, fmt, nbytes in fake_r2.uploads:
        assert fk == "k1"
        assert z == 4
        assert fmt == "webp"
        assert nbytes > 0


async def test_prewarm_skips_r2_objects_that_already_exist(tmp_path: Path) -> None:
    """When R2 reports an object already exists, the render is skipped
    entirely — saves both CPU (Pillow encode) and a Class A op
    (redundant ``put_object``). NATS at-least-once redelivery is the
    obvious trigger; a re-deploy mid-event is the other.
    """
    uri = _write_conus_grid(tmp_path / "g.zarr")

    class _AlwaysExistsR2:
        async def put_tile(self, **_kw: object) -> None:
            raise AssertionError("put_tile must not run when object_exists is True")

        async def object_exists(self, **_kw: object) -> bool:
            return True

    stats = await prewarm_tiles_for_grid(
        _locator(uri),
        r2_client=_AlwaysExistsR2(),  # type: ignore[arg-type]
        lru_cache=None,
        zooms=(4,),
        formats=("webp",),
    )
    assert stats.rendered == 0
    assert stats.failed == 0
    assert stats.skipped_existing == len(conus_tile_coords(4))


async def test_prewarm_continues_on_per_tile_render_error(tmp_path: Path) -> None:
    """A bad locator (zarr_uri that doesn't exist) makes every render
    fail; the function should report them as failures and finish
    cleanly rather than raising."""
    cache = TilePngCache(max_bytes=1 * 1024 * 1024)
    bogus = MrmsGridLocator(
        file_key="missing",
        zarr_uri=str(tmp_path / "does-not-exist.zarr"),
        variable="reflectivity",
        dims=("latitude", "longitude"),
        shape=(0, 0),
        dtype="float32",
        nbytes=0,
    )

    stats = await prewarm_tiles_for_grid(
        bogus, r2_client=None, lru_cache=cache, zooms=(4,), formats=("png",)
    )
    assert stats.rendered == 0
    assert stats.failed == len(conus_tile_coords(4))


async def test_consumer_processes_grids_until_cancelled(tmp_path: Path) -> None:
    uri = _write_conus_grid(tmp_path / "g.zarr")
    locator = _locator(uri, file_key="event-1")
    cache = TilePngCache(max_bytes=4 * 1024 * 1024)
    subscriber = InMemoryMrmsGridSubscriber()

    consumer = asyncio.create_task(
        run_prewarm_consumer(
            subscriber=subscriber,
            r2_client=None,
            lru_cache=cache,
            zooms=(4,),
            formats=("png",),
        )
    )
    try:
        await subscriber.wait_for_subscriber_count(1, timeout=1.0)
        await subscriber.push(locator)
        # Wait until the cache is populated — we don't have a direct
        # "consumer drained one event" signal, so poll the cache for
        # the expected entries.
        deadline = asyncio.get_running_loop().time() + 2.0
        target = len(conus_tile_coords(4))
        while cache.stats().current_entries < target:
            if asyncio.get_running_loop().time() >= deadline:
                pytest.fail("prewarm consumer never populated the cache")
            await asyncio.sleep(0.02)
    finally:
        consumer.cancel()
        with suppress(asyncio.CancelledError):
            await consumer

    assert cache.stats().current_entries == len(conus_tile_coords(4))


async def test_consumer_swallows_per_event_errors(tmp_path: Path) -> None:
    """A locator pointing at a missing Zarr makes every tile-render fail
    *but the consumer must keep draining* — the next valid event should
    still be processed."""
    good_uri = _write_conus_grid(tmp_path / "g.zarr")
    bad = MrmsGridLocator(
        file_key="bad",
        zarr_uri=str(tmp_path / "does-not-exist.zarr"),
        variable="reflectivity",
        dims=("latitude", "longitude"),
        shape=(0, 0),
        dtype="float32",
        nbytes=0,
    )
    good = _locator(good_uri, file_key="good")

    cache = TilePngCache(max_bytes=4 * 1024 * 1024)
    subscriber = InMemoryMrmsGridSubscriber()

    consumer = asyncio.create_task(
        run_prewarm_consumer(
            subscriber=subscriber,
            r2_client=None,
            lru_cache=cache,
            zooms=(4,),
            formats=("png",),
        )
    )
    try:
        await subscriber.wait_for_subscriber_count(1, timeout=1.0)
        await subscriber.push(bad)
        await subscriber.push(good)

        deadline = asyncio.get_running_loop().time() + 2.0
        target = len(conus_tile_coords(4))
        while cache.stats().current_entries < target:
            if asyncio.get_running_loop().time() >= deadline:
                pytest.fail("good event was not processed after the bad one")
            await asyncio.sleep(0.02)
    finally:
        consumer.cancel()
        with suppress(asyncio.CancelledError):
            await consumer


# ---------------------------------------------------------------------------
# Conflate-to-latest mailbox — the fix for prewarm marching a stale FIFO
# backlog it can never clear (which left tiles.aeroza.app 404ing the latest
# grid, the only one the map ever requests).
# ---------------------------------------------------------------------------


async def test_mailbox_delivers_a_single_grid() -> None:
    box = _LatestGridMailbox()
    box.put(_locator("u", file_key="A"))

    got = await box.get()

    assert got is not None
    locator, superseded = got
    assert locator.file_key == "A"
    assert superseded == 0


async def test_mailbox_conflates_to_newest_grid() -> None:
    """Three grids queued before a single get → only the newest is
    delivered, and the count of superseded grids is reported."""
    box = _LatestGridMailbox()
    box.put(_locator("u", file_key="A"))
    box.put(_locator("u", file_key="B"))
    box.put(_locator("u", file_key="C"))

    got = await box.get()

    assert got is not None
    locator, superseded = got
    assert locator.file_key == "C"  # newest wins
    assert superseded == 2  # A and B were dropped, not rendered

    # Nothing else is pending — a second get must block, not return a stale grid.
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(box.get(), timeout=0.05)


async def test_mailbox_close_delivers_pending_then_none() -> None:
    box = _LatestGridMailbox()
    box.put(_locator("u", file_key="A"))
    box.close()

    first = await box.get()
    assert first is not None
    assert first[0].file_key == "A"

    # Drained: every subsequent get returns None so the consumer can exit.
    assert await box.get() is None
    assert await box.get() is None


async def test_mailbox_close_with_empty_slot_returns_none() -> None:
    box = _LatestGridMailbox()
    box.close()
    assert await box.get() is None


async def test_consumer_coalesces_grids_arriving_during_render(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """While grid A's pyramid renders, grids B and C arrive. The consumer
    must coalesce them to the newest (C) and skip the stale middle grid
    (B) entirely — this is what stops prewarm grinding through a backlog
    it can never clear and lets the CDN track 'now'."""
    uri = _write_conus_grid(tmp_path / "g.zarr")
    cache = TilePngCache(max_bytes=8 * 1024 * 1024)
    subscriber = InMemoryMrmsGridSubscriber()

    first_render_entered = threading.Event()
    release_first_render = threading.Event()
    real_render = prewarm_module.render_tile_image
    calls = {"n": 0}

    def gated_render(**kwargs: object) -> bytes:
        # Hold the very first tile render in-flight so B and C pile up
        # behind grid A and the mailbox has to conflate them.
        calls["n"] += 1
        if calls["n"] == 1:
            first_render_entered.set()
            if not release_first_render.wait(timeout=5.0):
                raise AssertionError("render gate was never released")
        return real_render(**kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(prewarm_module, "render_tile_image", gated_render)

    loop = asyncio.get_running_loop()
    target = len(conus_tile_coords(4))

    def rendered(file_key: str) -> int:
        return sum(
            1
            for x, y in conus_tile_coords(4)
            if cache.get(CacheKey(file_key=file_key, z=4, x=x, y=y, format="png")) is not None
        )

    consumer = asyncio.create_task(
        run_prewarm_consumer(
            subscriber=subscriber,
            r2_client=None,
            lru_cache=cache,
            zooms=(4,),
            formats=("png",),
        )
    )
    try:
        await subscriber.wait_for_subscriber_count(1, timeout=1.0)
        await subscriber.push(_locator(uri, file_key="A"))

        # Wait until A's first tile is rendering (blocked on the gate).
        deadline = loop.time() + 2.0
        while not first_render_entered.is_set():
            if loop.time() >= deadline:
                pytest.fail("first render never started")
            await asyncio.sleep(0.01)

        # B and C arrive mid-render; the ingest task conflates them in the
        # mailbox. Wait until the subscription queue is drained so we know
        # both reached the mailbox (B superseded by C) before releasing A.
        await subscriber.push(_locator(uri, file_key="B"))
        await subscriber.push(_locator(uri, file_key="C"))
        ingest_queue = subscriber._queues[0]
        deadline = loop.time() + 2.0
        while not ingest_queue.empty():
            if loop.time() >= deadline:
                pytest.fail("ingest never drained B and C into the mailbox")
            await asyncio.sleep(0.01)

        release_first_render.set()

        # A (the in-flight grid) finishes, then the loop jumps straight to C.
        deadline = loop.time() + 3.0
        while not (rendered("A") == target and rendered("C") == target):
            if loop.time() >= deadline:
                pytest.fail("A and C were not both rendered after coalescing")
            await asyncio.sleep(0.02)

        assert rendered("A") == target  # the render that was in flight completed
        assert rendered("C") == target  # the newest grid was picked up next
        assert rendered("B") == 0  # the stale middle grid was coalesced away
    finally:
        release_first_render.set()  # never leave a worker thread blocked
        consumer.cancel()
        with suppress(asyncio.CancelledError):
            await consumer


# ---------------------------------------------------------------------------
# latest.json pointer — lets the frontend pin to the newest grid that's
# actually in R2, so it never requests tiles still mid-render.
# ---------------------------------------------------------------------------

_VALID_MRMS_KEY = (
    "CONUS/MergedReflectivityComposite_00.50/20260604/"
    "MRMS_MergedReflectivityComposite_00.50_20260604-172042.grib2.gz"
)


def _mrms_locator(zarr_uri: str) -> MrmsGridLocator:
    """A locator whose ``file_key`` is a real MRMS key (parseable into
    product/level/valid_at), pointed at a renderable test grid."""
    return MrmsGridLocator(
        file_key=_VALID_MRMS_KEY,
        zarr_uri=zarr_uri,
        variable="reflectivity",
        dims=("latitude", "longitude"),
        shape=(32, 32),
        dtype="float32",
        nbytes=32 * 32 * 4,
    )


class _PointerRecordingR2:
    """Fake R2 client that records tile uploads + pointer writes."""

    def __init__(self) -> None:
        self.put_tile_calls: list[tuple[str, int, int, int, str]] = []
        self.pointer_calls: list[dict[str, str]] = []

    async def put_tile(
        self, *, file_key: str, z: int, x: int, y: int, fmt: str, body: bytes
    ) -> None:
        self.put_tile_calls.append((file_key, z, x, y, fmt))

    async def object_exists(self, *, file_key: str, z: int, x: int, y: int, fmt: str) -> bool:
        return False

    async def put_latest_pointer(
        self, *, file_key: str, valid_at: str, product: str, level: str
    ) -> None:
        self.pointer_calls.append(
            {"file_key": file_key, "valid_at": valid_at, "product": product, "level": level}
        )


async def test_consumer_publishes_latest_pointer_after_grid(tmp_path: Path) -> None:
    """After a grid's pyramid uploads, the consumer points latest.json at it
    with the fields parsed from the MRMS key — this is what lets the frontend
    pin to a grid guaranteed to be in R2."""
    uri = _write_conus_grid(tmp_path / "g.zarr")
    fake = _PointerRecordingR2()
    subscriber = InMemoryMrmsGridSubscriber()
    loop = asyncio.get_running_loop()

    consumer = asyncio.create_task(
        run_prewarm_consumer(
            subscriber=subscriber,
            r2_client=fake,  # type: ignore[arg-type]
            lru_cache=None,
            zooms=(4,),
            formats=("webp",),
        )
    )
    try:
        await subscriber.wait_for_subscriber_count(1, timeout=1.0)
        await subscriber.push(_mrms_locator(uri))

        deadline = loop.time() + 3.0
        while not fake.pointer_calls:
            if loop.time() >= deadline:
                pytest.fail("consumer never published the latest.json pointer")
            await asyncio.sleep(0.02)

        assert len(fake.pointer_calls) == 1
        call = fake.pointer_calls[0]
        assert call["file_key"] == _VALID_MRMS_KEY
        assert call["product"] == "MergedReflectivityComposite"
        assert call["level"] == "00.50"
        assert call["valid_at"] == "2026-06-04T17:20:42+00:00"
        # The pointer is published only after the tiles are actually in R2.
        assert len(fake.put_tile_calls) == len(conus_tile_coords(4))
    finally:
        consumer.cancel()
        with suppress(asyncio.CancelledError):
            await consumer


async def test_consumer_skips_pointer_for_unparseable_key(tmp_path: Path) -> None:
    """A grid whose key isn't MRMS-shaped still gets its tiles uploaded, but
    the pointer is *not* advanced to it — the map would have no validAt to
    show and the key may not be a real grid. The next valid grid advances it."""
    uri = _write_conus_grid(tmp_path / "g.zarr")
    fake = _PointerRecordingR2()
    subscriber = InMemoryMrmsGridSubscriber()
    loop = asyncio.get_running_loop()
    target = len(conus_tile_coords(4))

    consumer = asyncio.create_task(
        run_prewarm_consumer(
            subscriber=subscriber,
            r2_client=fake,  # type: ignore[arg-type]
            lru_cache=None,
            zooms=(4,),
            formats=("webp",),
        )
    )
    try:
        await subscriber.wait_for_subscriber_count(1, timeout=1.0)

        # Render the bad grid fully first (so it isn't coalesced away), then
        # a good grid — the only pointer write must be for the good one.
        await subscriber.push(_locator(uri, file_key="not-an-mrms-key"))
        deadline = loop.time() + 3.0
        while len(fake.put_tile_calls) < target:
            if loop.time() >= deadline:
                pytest.fail("bad grid's tiles were never uploaded")
            await asyncio.sleep(0.02)
        assert fake.pointer_calls == []  # bad grid did not advance the pointer

        await subscriber.push(_mrms_locator(uri))
        deadline = loop.time() + 3.0
        while not fake.pointer_calls:
            if loop.time() >= deadline:
                pytest.fail("good grid never advanced the pointer")
            await asyncio.sleep(0.02)
        assert len(fake.pointer_calls) == 1
        assert fake.pointer_calls[0]["file_key"] == _VALID_MRMS_KEY
    finally:
        consumer.cancel()
        with suppress(asyncio.CancelledError):
            await consumer
