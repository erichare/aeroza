"""Long-lived ``aeroza-prune-mrms`` worker.

Ticks every ``--interval`` seconds; each tick:

  1. Drops MRMS observation Zarrs (+ derived nowcast Zarrs + their
     catalog rows + their verification rows, via FK cascade) whose
     source file's ``valid_at`` is older than ``--retention-hours``.
  2. Drops ``nws_alerts`` rows whose ``expires`` is older than
     ``--alert-retention-days``.

Process-level shape mirrors :mod:`aeroza.cli.ingest_alerts`: argparse
parser, ``_drive`` helper that's directly testable, SIGTERM/SIGINT
graceful shutdown via :class:`IntervalLoop`. The MRMS prune is the
disk-pressure relief; alert prune is bookkeeping for the row count.

``--once`` runs a single tick and exits — cron-friendly, and how the
unit tests exercise the path end-to-end without a long-lived loop.
"""

from __future__ import annotations

import argparse
import asyncio
import signal
from contextlib import suppress
from typing import Final

import structlog

from aeroza.config import Settings, get_settings
from aeroza.ingest.scheduler import IntervalLoop
from aeroza.retention.worker import (
    DEFAULT_BATCH_SIZE,
    PruneResult,
    prune_expired_alerts_once,
    prune_old_mrms_once,
)
from aeroza.shared.db import Database, create_engine_and_session

log = structlog.get_logger(__name__)

LOOP_NAME: Final[str] = "retention.prune"


def build_parser(defaults: Settings | None = None) -> argparse.ArgumentParser:
    """Build the argparse parser. ``defaults`` lets tests pin the values
    explicitly; production reads them from the environment via ``Settings``.
    """
    cfg = defaults or get_settings()
    parser = argparse.ArgumentParser(
        prog="aeroza-prune-mrms",
        description=(
            "Prune old MRMS observation + nowcast Zarr stores from disk "
            "and drop their catalog rows. Also prunes expired NWS alerts."
        ),
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=cfg.retention_interval_seconds,
        help=(
            "Seconds between ticks "
            f"(default: {cfg.retention_interval_seconds}, env "
            "AEROZA_RETENTION_INTERVAL_SECONDS)."
        ),
    )
    parser.add_argument(
        "--retention-hours",
        type=float,
        default=cfg.mrms_retention_hours,
        help=(
            "MRMS observations older than this (by file ``valid_at``) are "
            f"dropped (default: {cfg.mrms_retention_hours}, env "
            "AEROZA_MRMS_RETENTION_HOURS). Must exceed the longest "
            "forecast horizon the verifier needs to score (default 60min)."
        ),
    )
    parser.add_argument(
        "--alert-retention-days",
        type=int,
        default=cfg.alert_retention_days,
        help=(
            "NWS alerts with ``expires`` older than this are deleted "
            f"(default: {cfg.alert_retention_days}, env "
            "AEROZA_ALERT_RETENTION_DAYS). Alerts with no expiry are kept."
        ),
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=cfg.retention_batch_size,
        help=(
            "Maximum mrms_files keys to prune per DB transaction "
            f"(default: {cfg.retention_batch_size}, env "
            "AEROZA_RETENTION_BATCH_SIZE). Larger batches reduce overhead "
            "but lengthen the write lock window."
        ),
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single tick and exit (cron-friendly).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    settings = get_settings()
    args = build_parser(defaults=settings).parse_args(argv)
    log.info(
        "prune_mrms.start",
        interval_s=args.interval,
        retention_hours=args.retention_hours,
        alert_retention_days=args.alert_retention_days,
        batch_size=args.batch_size,
        once=args.once,
        env=settings.env,
    )
    return asyncio.run(_run(args=args, settings=settings))


async def _run(*, args: argparse.Namespace, settings: Settings) -> int:
    db = create_engine_and_session(settings.database_url)
    try:
        await _drive(db=db, args=args)
        return 0
    finally:
        await db.dispose()


async def _drive(*, db: Database, args: argparse.Namespace) -> None:
    if args.once:
        await _tick(db=db, args=args)
        return

    async def tick() -> None:
        await _tick(db=db, args=args)

    loop = IntervalLoop(tick=tick, interval_s=args.interval, name=LOOP_NAME)
    stopper = asyncio.Event()
    _install_signal_handlers(stopper)
    await loop.start()
    try:
        await stopper.wait()
    finally:
        await loop.stop()


async def _tick(*, db: Database, args: argparse.Namespace) -> PruneResult:
    """One prune cycle. Errors in either step are logged and swallowed —
    the loop must not die on a transient DB hiccup, and the worker is
    re-runnable so dropped work is picked up next tick.
    """
    batch_size = args.batch_size if args.batch_size > 0 else DEFAULT_BATCH_SIZE

    try:
        mrms_result = await prune_old_mrms_once(
            db=db,
            retention_hours=args.retention_hours,
            batch_size=batch_size,
        )
    except Exception as exc:
        log.exception("prune_mrms.tick.mrms_failed", error=str(exc))
        mrms_result = PruneResult()

    try:
        alert_result = await prune_expired_alerts_once(
            db=db,
            retention_days=args.alert_retention_days,
        )
    except Exception as exc:
        log.exception("prune_mrms.tick.alerts_failed", error=str(exc))
        alert_result = PruneResult()

    combined = mrms_result.merged_with(alert_result)
    log.info(
        "prune_mrms.tick",
        deleted_files=combined.deleted_files,
        deleted_zarrs=combined.deleted_zarrs,
        failed_zarrs=combined.failed_zarrs,
        deleted_alerts=combined.deleted_alerts,
    )
    return combined


def _install_signal_handlers(stopper: asyncio.Event) -> None:
    """Wire SIGTERM/SIGINT to set ``stopper``. Same pattern as ingest-alerts."""
    asyncio_loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        with suppress(NotImplementedError):
            asyncio_loop.add_signal_handler(sig, stopper.set)


if __name__ == "__main__":  # pragma: no cover - module-as-script entry
    raise SystemExit(main())
