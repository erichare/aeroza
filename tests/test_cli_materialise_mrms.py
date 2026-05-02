"""Tests for the ``aeroza-materialise-mrms`` CLI entry-point."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from aeroza.cli import materialise_mrms
from aeroza.ingest.mrms_materialise_poll import MaterialiseResult
from aeroza.shared.db import Database
from aeroza.stream.publisher import (
    InMemoryMrmsGridPublisher,
    MrmsGridPublisher,
    NullMrmsGridPublisher,
)

pytestmark = pytest.mark.unit


def test_parser_accepts_known_flags(tmp_path: Path) -> None:
    args = materialise_mrms.build_parser().parse_args(
        [
            "--interval",
            "30",
            "--product",
            "PrecipRate",
            "--level",
            "00.00",
            "--batch-size",
            "4",
            "--target-root",
            str(tmp_path),
            "--once",
            "--no-publish",
        ]
    )
    assert args.interval == 30.0
    assert args.product == "PrecipRate"
    assert args.level == "00.00"
    assert args.batch_size == 4
    assert args.target_root == str(tmp_path)
    assert args.once is True
    assert args.no_publish is True


def test_parser_defaults() -> None:
    args = materialise_mrms.build_parser().parse_args([])
    assert args.interval == materialise_mrms.DEFAULT_INTERVAL_SECONDS
    assert args.product == materialise_mrms.DEFAULT_PRODUCT
    assert args.level == materialise_mrms.DEFAULT_LEVEL
    assert args.batch_size == materialise_mrms.DEFAULT_BATCH_SIZE
    assert args.target_root == materialise_mrms.DEFAULT_TARGET_ROOT
    assert args.once is False
    assert args.no_publish is False


def test_parser_rejects_unknown_flag() -> None:
    parser = materialise_mrms.build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["--unknown"])


async def test_drive_once_invokes_poll_exactly_once(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    calls: list[dict[str, Any]] = []

    async def fake_poll(
        *,
        db: Database,
        s3_client: Any,
        target_root: Any,
        product: str,
        level: str,
        batch_size: int,
        publisher: MrmsGridPublisher,
    ) -> MaterialiseResult:
        calls.append(
            {
                "db": db,
                "s3_client": s3_client,
                "target_root": target_root,
                "product": product,
                "level": level,
                "batch_size": batch_size,
                "publisher": publisher,
            }
        )
        return MaterialiseResult(materialised_keys=(), failed_keys=())

    monkeypatch.setattr(materialise_mrms, "materialise_unmaterialised_once", fake_poll)
    sentinel = object()
    monkeypatch.setattr(materialise_mrms, "open_data_s3_client", lambda: sentinel)

    args = materialise_mrms.build_parser().parse_args(["--once", "--target-root", str(tmp_path)])
    db_obj = object()
    publisher = InMemoryMrmsGridPublisher()
    await materialise_mrms._drive(
        db=db_obj,  # type: ignore[arg-type]
        publisher=publisher,
        subscriber=None,
        args=args,
    )

    assert len(calls) == 1
    assert calls[0]["db"] is db_obj
    assert calls[0]["s3_client"] is sentinel
    assert calls[0]["target_root"] == tmp_path
    assert calls[0]["product"] == materialise_mrms.DEFAULT_PRODUCT
    assert calls[0]["level"] == materialise_mrms.DEFAULT_LEVEL
    assert calls[0]["batch_size"] == materialise_mrms.DEFAULT_BATCH_SIZE
    assert calls[0]["publisher"] is publisher


async def test_drive_loop_mode_stops_on_event(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Drive loop mode but pre-set the stopper so it returns after one tick."""
    import asyncio

    tick_count = {"n": 0}

    async def fake_poll(**_kwargs: Any) -> MaterialiseResult:
        tick_count["n"] += 1
        return MaterialiseResult(materialised_keys=(), failed_keys=())

    background_tasks: set[asyncio.Task[None]] = set()

    def installer(stopper: asyncio.Event) -> None:
        async def trip() -> None:
            await asyncio.sleep(0.05)
            stopper.set()

        task = asyncio.create_task(trip())
        background_tasks.add(task)
        task.add_done_callback(background_tasks.discard)

    monkeypatch.setattr(materialise_mrms, "materialise_unmaterialised_once", fake_poll)
    monkeypatch.setattr(materialise_mrms, "_install_signal_handlers", installer)
    monkeypatch.setattr(materialise_mrms, "open_data_s3_client", lambda: object())

    args = materialise_mrms.build_parser().parse_args(
        ["--interval", "10", "--target-root", str(tmp_path)]
    )
    await materialise_mrms._drive(
        db=object(),  # type: ignore[arg-type]
        publisher=NullMrmsGridPublisher(),
        subscriber=None,
        args=args,
    )
    assert tick_count["n"] >= 1


async def test_run_disposes_database_even_on_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    captured: dict[str, Any] = {}

    def fake_create(_dsn: str) -> Database:
        class _Stub:
            engine = None
            sessionmaker = None

            async def dispose(self) -> None:
                captured["disposed"] = True

        return _Stub()  # type: ignore[return-value]

    async def fake_drive(
        *,
        db: Database,
        publisher: MrmsGridPublisher,
        subscriber: Any,
        args: Any,
    ) -> None:
        captured["drove"] = True
        captured["publisher_class"] = type(publisher).__name__
        captured["subscriber"] = subscriber

    async def explode_nats(_servers: str) -> None:  # pragma: no cover — must NOT run
        raise AssertionError("nats_connection should not be entered with --no-publish")

    monkeypatch.setattr(materialise_mrms, "create_engine_and_session", fake_create)
    monkeypatch.setattr(materialise_mrms, "_drive", fake_drive)
    monkeypatch.setattr(materialise_mrms, "nats_connection", explode_nats)

    args = materialise_mrms.build_parser().parse_args(
        ["--once", "--no-publish", "--target-root", str(tmp_path)]
    )
    settings = type("S", (), {"database_url": "x", "nats_url": "y", "env": "test"})()
    rc = await materialise_mrms._run(args=args, settings=settings)
    assert rc == 0
    assert captured["drove"] is True
    assert captured["disposed"] is True
    assert captured["publisher_class"] == "NullMrmsGridPublisher"


async def test_drive_creates_target_root_if_missing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The CLI should mkdir -p the Zarr root so first-run is friction-free."""
    target = tmp_path / "mrms-data" / "store"
    assert not target.exists()

    async def fake_poll(**_kwargs: Any) -> MaterialiseResult:
        return MaterialiseResult(materialised_keys=(), failed_keys=())

    monkeypatch.setattr(materialise_mrms, "materialise_unmaterialised_once", fake_poll)
    monkeypatch.setattr(materialise_mrms, "open_data_s3_client", lambda: object())

    args = materialise_mrms.build_parser().parse_args(["--once", "--target-root", str(target)])
    await materialise_mrms._drive(
        db=object(),  # type: ignore[arg-type]
        publisher=NullMrmsGridPublisher(),
        subscriber=None,
        args=args,
    )
    assert target.is_dir()


def test_main_dispatcher_routes_to_materialise_mrms(monkeypatch: pytest.MonkeyPatch) -> None:
    from aeroza.cli import __main__ as cli_main

    captured: list[list[str] | None] = []

    def fake_main(argv: list[str] | None) -> int:
        captured.append(argv)
        return 0

    monkeypatch.setitem(cli_main.SUBCOMMANDS, "materialise-mrms", fake_main)
    rc = cli_main.main(["materialise-mrms", "--once"])
    assert rc == 0
    assert captured == [["--once"]]
