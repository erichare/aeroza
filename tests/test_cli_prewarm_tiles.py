"""Tests for the ``aeroza-prewarm-tiles`` CLI entry-point."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import pytest

from aeroza.cli import prewarm_tiles
from aeroza.ingest.mrms_zarr import MrmsGridLocator
from aeroza.stream.subscriber import InMemoryMrmsGridSubscriber
from aeroza.tiles.cache import TilePngCache
from aeroza.tiles.prewarm import DEFAULT_PREWARM_FORMATS, DEFAULT_PREWARM_ZOOMS

pytestmark = pytest.mark.unit


def test_parser_defaults() -> None:
    args = prewarm_tiles.build_parser().parse_args([])
    assert tuple(args.zooms) == DEFAULT_PREWARM_ZOOMS
    assert tuple(args.formats) == DEFAULT_PREWARM_FORMATS
    assert args.no_r2 is False


def test_parser_accepts_zoom_overrides() -> None:
    args = prewarm_tiles.build_parser().parse_args(
        ["--zooms", "3", "4", "--formats", "webp", "--no-r2"]
    )
    assert args.zooms == [3, 4]
    assert args.formats == ["webp"]
    assert args.no_r2 is True


def test_parser_rejects_unknown_format() -> None:
    parser = prewarm_tiles.build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["--formats", "gif"])


def test_parser_rejects_unknown_flag() -> None:
    parser = prewarm_tiles.build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["--unknown"])


def test_main_dispatcher_routes_to_prewarm_tiles(monkeypatch: pytest.MonkeyPatch) -> None:
    from aeroza.cli import __main__ as cli_main

    captured: list[list[str] | None] = []

    def fake_main(argv: list[str] | None) -> int:
        captured.append(argv)
        return 0

    monkeypatch.setitem(cli_main.SUBCOMMANDS, "prewarm-tiles", fake_main)
    rc = cli_main.main(["prewarm-tiles", "--no-r2"])
    assert rc == 0
    assert captured == [["--no-r2"]]


async def test_drive_runs_consumer_until_stopper_fires(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``_drive`` wires the consumer + stopper and cancels cleanly on signal."""
    captured: dict[str, Any] = {}

    async def fake_consumer(**kwargs: Any) -> None:
        captured["kwargs"] = kwargs
        # Block forever; the stopper-driven cancellation propagates here.
        await asyncio.Event().wait()

    background_tasks: set[asyncio.Task[None]] = set()

    def installer(stopper: asyncio.Event) -> None:
        async def trip() -> None:
            await asyncio.sleep(0.05)
            stopper.set()

        task = asyncio.create_task(trip())
        background_tasks.add(task)
        task.add_done_callback(background_tasks.discard)

    monkeypatch.setattr(prewarm_tiles, "run_prewarm_consumer", fake_consumer)
    monkeypatch.setattr(prewarm_tiles, "_install_signal_handlers", installer)

    subscriber = InMemoryMrmsGridSubscriber()
    lru = TilePngCache()
    await prewarm_tiles._drive(
        subscriber=subscriber,
        r2_client=None,
        lru_cache=lru,
        zooms=(2,),
        formats=("webp",),
    )

    assert captured["kwargs"]["subscriber"] is subscriber
    assert captured["kwargs"]["r2_client"] is None
    assert captured["kwargs"]["lru_cache"] is lru
    assert captured["kwargs"]["zooms"] == (2,)
    assert captured["kwargs"]["formats"] == ("webp",)


async def test_run_falls_back_to_lru_when_r2_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When R2 creds are absent, ``_run`` builds an LRU and proceeds — no
    crash, no production-fatal exit. The warning log is the operator's
    signal to set the creds."""
    captured: dict[str, Any] = {}

    def fake_build(_settings: Any) -> None:
        return None

    class _FakeNatsContext:
        async def __aenter__(self) -> object:
            return object()

        async def __aexit__(self, *exc: object) -> None:
            return None

    def fake_nats_connection(_url: str) -> _FakeNatsContext:
        return _FakeNatsContext()

    class _FakeSubscriber:
        def __init__(self, _client: object) -> None:
            pass

        async def subscribe_new_grids(self) -> AsyncIterator[MrmsGridLocator]:
            if False:  # pragma: no cover - never iterated
                yield  # type: ignore[unreachable]

    async def fake_drive(**kwargs: Any) -> None:
        captured["kwargs"] = kwargs

    monkeypatch.setattr(prewarm_tiles, "build_r2_client", fake_build)
    monkeypatch.setattr(prewarm_tiles, "nats_connection", fake_nats_connection)
    monkeypatch.setattr(prewarm_tiles, "NatsMrmsGridSubscriber", _FakeSubscriber)
    monkeypatch.setattr(prewarm_tiles, "_drive", fake_drive)

    args = prewarm_tiles.build_parser().parse_args([])
    settings = type(
        "S", (), {"env": "test", "nats_url": "nats://x", "r2_endpoint": None}
    )()
    rc = await prewarm_tiles._run(args=args, settings=settings)
    assert rc == 0
    assert captured["kwargs"]["r2_client"] is None
    assert isinstance(captured["kwargs"]["lru_cache"], TilePngCache)


async def test_run_uses_r2_when_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    """When ``build_r2_client`` returns a client, no LRU fallback is wired."""
    sentinel_r2 = object()
    captured: dict[str, Any] = {}

    def fake_build(_settings: Any) -> object:
        return sentinel_r2

    class _FakeNatsContext:
        async def __aenter__(self) -> object:
            return object()

        async def __aexit__(self, *exc: object) -> None:
            return None

    def fake_nats_connection(_url: str) -> _FakeNatsContext:
        return _FakeNatsContext()

    class _FakeSubscriber:
        def __init__(self, _client: object) -> None:
            pass

    async def fake_drive(**kwargs: Any) -> None:
        captured["kwargs"] = kwargs

    monkeypatch.setattr(prewarm_tiles, "build_r2_client", fake_build)
    monkeypatch.setattr(prewarm_tiles, "nats_connection", fake_nats_connection)
    monkeypatch.setattr(prewarm_tiles, "NatsMrmsGridSubscriber", _FakeSubscriber)
    monkeypatch.setattr(prewarm_tiles, "_drive", fake_drive)

    args = prewarm_tiles.build_parser().parse_args([])
    settings = type("S", (), {"env": "production", "nats_url": "nats://x"})()
    rc = await prewarm_tiles._run(args=args, settings=settings)
    assert rc == 0
    assert captured["kwargs"]["r2_client"] is sentinel_r2
    assert captured["kwargs"]["lru_cache"] is None


async def test_run_no_r2_flag_skips_r2_even_when_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``--no-r2`` must not call ``build_r2_client`` at all."""
    captured: dict[str, Any] = {"build_called": False}

    def fake_build(_settings: Any) -> object:
        captured["build_called"] = True
        return object()

    class _FakeNatsContext:
        async def __aenter__(self) -> object:
            return object()

        async def __aexit__(self, *exc: object) -> None:
            return None

    def fake_nats_connection(_url: str) -> _FakeNatsContext:
        return _FakeNatsContext()

    class _FakeSubscriber:
        def __init__(self, _client: object) -> None:
            pass

    async def fake_drive(**kwargs: Any) -> None:
        captured["kwargs"] = kwargs

    monkeypatch.setattr(prewarm_tiles, "build_r2_client", fake_build)
    monkeypatch.setattr(prewarm_tiles, "nats_connection", fake_nats_connection)
    monkeypatch.setattr(prewarm_tiles, "NatsMrmsGridSubscriber", _FakeSubscriber)
    monkeypatch.setattr(prewarm_tiles, "_drive", fake_drive)

    args = prewarm_tiles.build_parser().parse_args(["--no-r2"])
    settings = type("S", (), {"env": "test", "nats_url": "nats://x"})()
    rc = await prewarm_tiles._run(args=args, settings=settings)
    assert rc == 0
    assert captured["build_called"] is False
    assert captured["kwargs"]["r2_client"] is None
    assert isinstance(captured["kwargs"]["lru_cache"], TilePngCache)
