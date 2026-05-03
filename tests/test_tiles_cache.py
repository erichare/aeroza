"""Unit tests for the bounded LRU tile cache.

Pure thread-safe data structure — no I/O, no FastAPI. The cache only
ever speaks about pinned tiles (the type system enforces this; the
constructor of :class:`CacheKey` requires a non-empty ``file_key``).
"""

from __future__ import annotations

import pytest

from aeroza.tiles.cache import CacheKey, TilePngCache

pytestmark = pytest.mark.unit


def _key(file_key: str = "k", *, z: int = 5, x: int = 0, y: int = 0) -> CacheKey:
    return CacheKey(file_key=file_key, z=z, x=x, y=y)


def test_get_returns_none_on_miss() -> None:
    cache = TilePngCache(max_bytes=1024)
    assert cache.get(_key()) is None
    assert cache.stats().misses == 1
    assert cache.stats().hits == 0


def test_put_then_get_returns_bytes_and_counts_hit() -> None:
    cache = TilePngCache(max_bytes=1024)
    cache.put(_key("a"), b"\x89PNG-payload-a")
    assert cache.get(_key("a")) == b"\x89PNG-payload-a"
    stats = cache.stats()
    assert stats.hits == 1
    assert stats.misses == 0
    assert stats.current_entries == 1
    assert stats.current_bytes == len(b"\x89PNG-payload-a")


def test_distinct_keys_share_cache_independently() -> None:
    cache = TilePngCache(max_bytes=1024)
    cache.put(_key("a"), b"first")
    cache.put(_key("b"), b"second")
    assert cache.get(_key("a")) == b"first"
    assert cache.get(_key("b")) == b"second"
    assert cache.stats().current_entries == 2


def test_repeated_put_overwrites_in_place_without_size_inflation() -> None:
    cache = TilePngCache(max_bytes=1024)
    cache.put(_key("a"), b"1234567890")
    cache.put(_key("a"), b"abcde")
    # Same key, smaller payload: bytes accounting reflects the overwrite,
    # not the sum.
    assert cache.stats().current_bytes == len(b"abcde")
    assert cache.stats().current_entries == 1
    assert cache.get(_key("a")) == b"abcde"


def test_eviction_drops_least_recently_used() -> None:
    # 30-byte budget: two 16-byte entries don't fit, so inserting a
    # second forces eviction of the LRU entry.
    cache = TilePngCache(max_bytes=30)
    cache.put(_key("oldest"), b"x" * 16)
    cache.put(_key("newest"), b"y" * 16)
    assert cache.get(_key("oldest")) is None  # evicted
    assert cache.get(_key("newest")) == b"y" * 16
    assert cache.stats().evictions == 1


def test_get_promotes_recency_to_protect_against_eviction() -> None:
    cache = TilePngCache(max_bytes=40)
    cache.put(_key("a"), b"a" * 16)
    cache.put(_key("b"), b"b" * 16)
    # Touching ``a`` makes it MRU; the next insertion should evict ``b``.
    assert cache.get(_key("a")) == b"a" * 16
    cache.put(_key("c"), b"c" * 16)
    assert cache.get(_key("a")) == b"a" * 16
    assert cache.get(_key("b")) is None
    assert cache.get(_key("c")) == b"c" * 16


def test_oversize_entry_is_dropped_silently_not_clearing_existing() -> None:
    cache = TilePngCache(max_bytes=20)
    cache.put(_key("safe"), b"small")
    huge = b"x" * 100  # > max_bytes
    cache.put(_key("huge"), huge)
    # The huge entry is dropped without trashing the existing one.
    assert cache.get(_key("safe")) == b"small"
    assert cache.get(_key("huge")) is None


def test_clear_drops_every_entry_and_resets_byte_accounting() -> None:
    cache = TilePngCache(max_bytes=1024)
    cache.put(_key("a"), b"hello")
    cache.put(_key("b"), b"world")
    cache.clear()
    assert cache.get(_key("a")) is None
    assert cache.stats().current_entries == 0
    assert cache.stats().current_bytes == 0


def test_max_bytes_must_be_positive() -> None:
    with pytest.raises(ValueError, match="max_bytes must be positive"):
        TilePngCache(max_bytes=0)


def test_cachekey_is_frozen_and_hashable() -> None:
    k1 = _key("k", z=5, x=1, y=2)
    k2 = _key("k", z=5, x=1, y=2)
    assert k1 == k2
    assert hash(k1) == hash(k2)
    with pytest.raises(AttributeError):
        k1.z = 6  # type: ignore[misc]
