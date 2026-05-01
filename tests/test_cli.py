"""Tests for the ``aeroza-ingest-alerts`` CLI entry-point."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

import pytest

from aeroza.cli import ingest_alerts
from aeroza.ingest.nws_alerts_store import UpsertResult
from aeroza.shared.db import Database
from aeroza.stream.publisher import AlertPublisher

pytestmark = pytest.mark.unit


def test_parser_accepts_only_known_flags() -> None:
    parser = ingest_alerts.build_parser()
    args = parser.parse_args(["--interval", "5", "--once", "--no-publish"])
    assert args.interval == 5.0
    assert args.once is True
    assert args.no_publish is True


def test_parser_defaults() -> None:
    args = ingest_alerts.build_parser().parse_args([])
    assert args.interval == ingest_alerts.DEFAULT_INTERVAL_SECONDS
    assert args.once is False
    assert args.no_publish is False


def test_parser_rejects_unknown_flag() -> None:
    parser = ingest_alerts.build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["--unknown"])


async def test_drive_once_invokes_poll_exactly_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []

    async def fake_poll(*, db: Database, publisher: AlertPublisher) -> UpsertResult:
        calls.append({"db": db, "publisher": publisher})
        return UpsertResult(inserted_ids=(), updated_ids=())

    monkeypatch.setattr(ingest_alerts, "poll_nws_alerts_once", fake_poll)

    args = ingest_alerts.build_parser().parse_args(["--once"])
    db = object()  # _drive doesn't touch the db; the fake poll observes it
    publisher_obj = object()
    await ingest_alerts._drive(
        db=db,  # type: ignore[arg-type]
        publisher=publisher_obj,  # type: ignore[arg-type]
        args=args,
    )

    assert len(calls) == 1
    assert calls[0]["db"] is db
    assert calls[0]["publisher"] is publisher_obj


async def test_drive_loop_mode_stops_on_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Drive loop mode but pre-set the stopper so it returns immediately
    after one tick — verifies the IntervalLoop is wired up and that stop()
    actually unwinds cleanly.
    """
    import asyncio

    tick_count = {"n": 0}

    async def fake_poll(*, db: Database, publisher: AlertPublisher) -> UpsertResult:
        tick_count["n"] += 1
        return UpsertResult(inserted_ids=(), updated_ids=())

    background_tasks: set[asyncio.Task[None]] = set()

    def installer(stopper: asyncio.Event) -> None:
        # Schedule the stopper to fire after the first tick has had a chance to run.
        async def trip() -> None:
            await asyncio.sleep(0.05)
            stopper.set()

        task = asyncio.create_task(trip())
        background_tasks.add(task)
        task.add_done_callback(background_tasks.discard)

    monkeypatch.setattr(ingest_alerts, "poll_nws_alerts_once", fake_poll)
    monkeypatch.setattr(ingest_alerts, "_install_signal_handlers", installer)

    args = ingest_alerts.build_parser().parse_args(["--interval", "10"])
    await ingest_alerts._drive(
        db=object(),  # type: ignore[arg-type]
        publisher=object(),  # type: ignore[arg-type]
        args=args,
    )
    assert tick_count["n"] >= 1


async def test_run_uses_null_publisher_when_no_publish(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``--no-publish`` must skip the NATS connection entirely."""
    captured: dict[str, Any] = {}

    def fake_create(_dsn: str) -> Database:
        # Minimal stub Database that disposes cleanly.
        class _Stub:
            engine = None
            sessionmaker = None

            async def dispose(self) -> None:
                captured["disposed"] = True

        return _Stub()  # type: ignore[return-value]

    async def fake_drive(
        *,
        db: Database,
        publisher: AlertPublisher,
        args: Any,
    ) -> None:
        captured["publisher_class"] = type(publisher).__name__

    async def explode_nats(_servers: str) -> None:  # pragma: no cover — must NOT run
        raise AssertionError("nats_connection should not be entered with --no-publish")

    monkeypatch.setattr(ingest_alerts, "create_engine_and_session", fake_create)
    monkeypatch.setattr(ingest_alerts, "_drive", fake_drive)
    monkeypatch.setattr(ingest_alerts, "nats_connection", explode_nats)

    args = ingest_alerts.build_parser().parse_args(["--no-publish", "--once"])
    settings = type("S", (), {"database_url": "x", "nats_url": "y", "env": "test"})()
    rc = await ingest_alerts._run(args=args, settings=settings)
    assert rc == 0
    assert captured["publisher_class"] == "NullAlertPublisher"
    assert captured["disposed"] is True


def test_main_dispatcher_routes_to_ingest_alerts(monkeypatch: pytest.MonkeyPatch) -> None:
    from aeroza.cli import __main__ as cli_main

    captured: list[list[str] | None] = []

    def fake_main(argv: list[str] | None) -> int:
        captured.append(argv)
        return 0

    monkeypatch.setitem(cli_main.SUBCOMMANDS, "ingest-alerts", fake_main)
    rc = cli_main.main(["ingest-alerts", "--once"])
    assert rc == 0
    assert captured == [["--once"]]


def _ensure_callable(target: Callable[..., Awaitable[int]]) -> None:
    """Type-narrowing helper used by IDEs; runtime no-op."""
    assert callable(target)


def test_main_module_exposes_run_helper() -> None:
    _ensure_callable(ingest_alerts._run)
