"""Tests for the ``aeroza-prune-mrms`` CLI entry-point.

CLI wiring is pure unit-testable — the DB calls are monkeypatched out
and the underlying prune functions have their own integration tests
in :mod:`tests.test_retention_worker`. These tests verify the parser
defaults pick up the ``Settings`` values, ``--once`` invokes the prune
functions exactly once, and a tick that raises in one step still runs
the other.
"""

from __future__ import annotations

from typing import Any

import pytest

from aeroza.cli import prune_mrms
from aeroza.config import Settings
from aeroza.retention.worker import PruneResult
from aeroza.shared.db import Database

pytestmark = pytest.mark.unit


def _settings(**overrides: Any) -> Settings:
    base: dict[str, Any] = {
        "mrms_retention_hours": 6.0,
        "alert_retention_days": 30,
        "retention_interval_seconds": 600.0,
        "retention_batch_size": 500,
    }
    base.update(overrides)
    return Settings(**base)


def test_parser_defaults_come_from_settings() -> None:
    cfg = _settings(
        mrms_retention_hours=12.0,
        alert_retention_days=45,
        retention_interval_seconds=120.0,
        retention_batch_size=128,
    )
    args = prune_mrms.build_parser(defaults=cfg).parse_args([])

    assert args.interval == 120.0
    assert args.retention_hours == 12.0
    assert args.alert_retention_days == 45
    assert args.batch_size == 128
    assert args.once is False


def test_parser_accepts_overrides() -> None:
    args = prune_mrms.build_parser(defaults=_settings()).parse_args(
        [
            "--interval",
            "60",
            "--retention-hours",
            "3",
            "--alert-retention-days",
            "7",
            "--batch-size",
            "16",
            "--once",
        ]
    )
    assert args.interval == 60.0
    assert args.retention_hours == 3.0
    assert args.alert_retention_days == 7
    assert args.batch_size == 16
    assert args.once is True


def test_parser_rejects_unknown_flag() -> None:
    with pytest.raises(SystemExit):
        prune_mrms.build_parser(defaults=_settings()).parse_args(["--unknown"])


async def test_drive_once_invokes_each_prune_exactly_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mrms_calls: list[dict[str, Any]] = []
    alert_calls: list[dict[str, Any]] = []

    async def fake_mrms(
        *,
        db: Database,
        retention_hours: float,
        batch_size: int,
    ) -> PruneResult:
        mrms_calls.append({"db": db, "retention_hours": retention_hours, "batch_size": batch_size})
        return PruneResult(deleted_files=2, deleted_zarrs=4)

    async def fake_alerts(*, db: Database, retention_days: int) -> PruneResult:
        alert_calls.append({"db": db, "retention_days": retention_days})
        return PruneResult(deleted_alerts=7)

    monkeypatch.setattr(prune_mrms, "prune_old_mrms_once", fake_mrms)
    monkeypatch.setattr(prune_mrms, "prune_expired_alerts_once", fake_alerts)

    args = prune_mrms.build_parser(defaults=_settings()).parse_args(
        ["--once", "--retention-hours", "4", "--alert-retention-days", "10"]
    )
    db_obj: Database = object()  # type: ignore[assignment]
    await prune_mrms._drive(db=db_obj, args=args)

    assert len(mrms_calls) == 1
    assert mrms_calls[0]["db"] is db_obj
    assert mrms_calls[0]["retention_hours"] == 4.0
    assert mrms_calls[0]["batch_size"] == 500
    assert len(alert_calls) == 1
    assert alert_calls[0]["retention_days"] == 10


async def test_tick_swallows_mrms_failure_and_still_runs_alerts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The loop must not die on a transient DB hiccup — the alert prune
    should still run even when the MRMS prune raises, so partial progress
    is preserved across ticks.
    """
    alert_calls: list[dict[str, Any]] = []

    async def boom(**_: Any) -> PruneResult:
        raise RuntimeError("simulated db failure")

    async def fake_alerts(*, db: Database, retention_days: int) -> PruneResult:
        alert_calls.append({"db": db, "retention_days": retention_days})
        return PruneResult(deleted_alerts=3)

    monkeypatch.setattr(prune_mrms, "prune_old_mrms_once", boom)
    monkeypatch.setattr(prune_mrms, "prune_expired_alerts_once", fake_alerts)

    args = prune_mrms.build_parser(defaults=_settings()).parse_args(["--once"])
    result = await prune_mrms._tick(db=object(), args=args)  # type: ignore[arg-type]

    assert len(alert_calls) == 1
    assert result.deleted_files == 0  # MRMS step failed → zero
    assert result.deleted_alerts == 3


async def test_drive_loop_mode_runs_at_least_one_tick(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Drive loop mode briefly to verify the IntervalLoop wires up cleanly.

    Mirrors the pattern used in :mod:`tests.test_cli`: pre-set the stopper
    so we don't wait through a full interval, but we still observe a tick.
    """
    import asyncio
    from contextlib import suppress

    tick_count = {"n": 0}

    async def fake_mrms(**_: Any) -> PruneResult:
        tick_count["n"] += 1
        return PruneResult()

    async def fake_alerts(**_: Any) -> PruneResult:
        return PruneResult()

    monkeypatch.setattr(prune_mrms, "prune_old_mrms_once", fake_mrms)
    monkeypatch.setattr(prune_mrms, "prune_expired_alerts_once", fake_alerts)

    args = prune_mrms.build_parser(defaults=_settings()).parse_args(["--interval", "0.05"])

    background_tasks: set[asyncio.Task[None]] = set()

    async def run_drive() -> None:
        await prune_mrms._drive(db=object(), args=args)  # type: ignore[arg-type]

    drive_task = asyncio.create_task(run_drive())
    background_tasks.add(drive_task)
    try:
        # Wait long enough for at least one tick to fire, then cancel.
        await asyncio.sleep(0.15)
        drive_task.cancel()
        with suppress(asyncio.CancelledError):
            await drive_task
    finally:
        background_tasks.discard(drive_task)

    assert tick_count["n"] >= 1
