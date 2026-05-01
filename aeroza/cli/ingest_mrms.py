"""Long-lived ``ingest-mrms`` worker.

Wires together :class:`aeroza.shared.db.Database`,
:func:`aeroza.stream.nats.nats_connection`,
:class:`aeroza.stream.nats.NatsMrmsFilePublisher`,
:class:`aeroza.ingest.scheduler.IntervalLoop`, and
:func:`aeroza.ingest.mrms_poll.poll_mrms_files_once` into a single
process. SIGTERM/SIGINT trigger graceful shutdown.

The poller asks S3 for files in a sliding ``[now - lookback, now)``
window each tick — wider than the tick interval so a paused/late tick
can still catch up without missing keys, and idempotent because the
underlying upsert is.
"""

from __future__ import annotations

import argparse
import asyncio
import signal
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from typing import Final

import structlog

from aeroza.config import Settings, get_settings
from aeroza.ingest._aws import open_data_s3_client
from aeroza.ingest.mrms import MrmsFile, list_mrms_files
from aeroza.ingest.mrms_poll import poll_mrms_files_once
from aeroza.ingest.scheduler import IntervalLoop
from aeroza.shared.db import Database, create_engine_and_session
from aeroza.stream.nats import NatsMrmsFilePublisher, nats_connection
from aeroza.stream.publisher import MrmsFilePublisher, NullMrmsFilePublisher

log = structlog.get_logger(__name__)

DEFAULT_INTERVAL_SECONDS: Final[float] = 60.0
DEFAULT_LOOKBACK_MINUTES: Final[float] = 5.0
DEFAULT_PRODUCT: Final[str] = "MergedReflectivityComposite"
DEFAULT_LEVEL: Final[str] = "00.50"
LOOP_NAME: Final[str] = "ingest.mrms"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="aeroza-ingest-mrms",
        description=(
            "Continuously list MRMS files on AWS Open Data, upsert the "
            "catalog, and publish a NATS event per newly-discovered key."
        ),
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=DEFAULT_INTERVAL_SECONDS,
        help=f"Seconds between ticks (default: {DEFAULT_INTERVAL_SECONDS}).",
    )
    parser.add_argument(
        "--lookback-minutes",
        type=float,
        default=DEFAULT_LOOKBACK_MINUTES,
        help=(
            f"How far back each tick scans (default: {DEFAULT_LOOKBACK_MINUTES} min). "
            "Set wider than --interval so a paused/late tick can catch up."
        ),
    )
    parser.add_argument(
        "--product",
        default=DEFAULT_PRODUCT,
        help=f"MRMS product (default: {DEFAULT_PRODUCT!r}).",
    )
    parser.add_argument(
        "--level",
        default=DEFAULT_LEVEL,
        help=f"MRMS product level (default: {DEFAULT_LEVEL!r}).",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single tick and exit (cron-friendly).",
    )
    parser.add_argument(
        "--no-publish",
        action="store_true",
        help=(
            "Persist but do not publish — backfills, schema migrations, "
            "or environments without a NATS broker."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings = get_settings()
    log.info(
        "ingest_mrms.start",
        interval_s=args.interval,
        lookback_min=args.lookback_minutes,
        product=args.product,
        level=args.level,
        once=args.once,
        publish=not args.no_publish,
        env=settings.env,
    )
    return asyncio.run(_run(args=args, settings=settings))


async def _run(*, args: argparse.Namespace, settings: Settings) -> int:
    db = create_engine_and_session(settings.database_url)
    try:
        if args.no_publish:
            await _drive(db=db, publisher=NullMrmsFilePublisher(), args=args)
            return 0

        async with nats_connection(settings.nats_url) as nats_client:
            publisher = NatsMrmsFilePublisher(nats_client)
            await _drive(db=db, publisher=publisher, args=args)
        return 0
    finally:
        await db.dispose()


async def _drive(
    *,
    db: Database,
    publisher: MrmsFilePublisher,
    args: argparse.Namespace,
) -> None:
    fetcher = _build_fetcher(args)

    if args.once:
        await poll_mrms_files_once(db=db, publisher=publisher, fetcher=fetcher)
        return

    async def tick() -> None:
        await poll_mrms_files_once(db=db, publisher=publisher, fetcher=fetcher)

    loop = IntervalLoop(tick=tick, interval_s=args.interval, name=LOOP_NAME)
    stopper = asyncio.Event()
    _install_signal_handlers(stopper)
    await loop.start()
    try:
        await stopper.wait()
    finally:
        await loop.stop()


def _build_fetcher(args: argparse.Namespace):  # type: ignore[no-untyped-def]
    """Return an :class:`MrmsFetcher` that lists the recent window each call.

    Encloses ``args`` and a single shared S3 client so each tick reuses
    the connection pool. Computing ``since`` from ``now`` at *call* time
    (rather than at fetcher-construction time) makes the window slide
    forward across ticks.

    The bucket layout uses one prefix per UTC day, so we *must* scope each
    listing to specific day-prefixes — passing ``day=None`` would walk
    every historical day of the bucket (millions of keys). When the
    lookback window straddles midnight UTC we list both days and merge.
    """
    s3_client = open_data_s3_client()

    async def fetcher() -> tuple[MrmsFile, ...]:
        now = datetime.now(UTC)
        since = now - timedelta(minutes=args.lookback_minutes)
        files: list[MrmsFile] = []
        for day in _unique_utc_days(since, now):
            chunk = await list_mrms_files(
                product=args.product,
                level=args.level,
                day=day,
                since=since,
                until=now,
                s3_client=s3_client,
            )
            files.extend(chunk)
        files.sort(key=lambda f: f.valid_at)
        return tuple(files)

    return fetcher


def _unique_utc_days(since: datetime, until: datetime) -> tuple[datetime, ...]:
    """Distinct UTC-day boundaries the half-open ``[since, until)`` interval touches.

    Returns at least one day even when ``since == until``. Caps at two days
    to keep the listing call count bounded; lookback windows wider than 24h
    are intentionally unsupported here.
    """
    start_day = datetime(since.year, since.month, since.day, tzinfo=UTC)
    end_day = datetime(until.year, until.month, until.day, tzinfo=UTC)
    if start_day == end_day:
        return (start_day,)
    return (start_day, end_day)


def _install_signal_handlers(stopper: asyncio.Event) -> None:
    """Wire SIGTERM/SIGINT to set ``stopper``. Same pattern as ingest-alerts."""
    asyncio_loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        with suppress(NotImplementedError):
            asyncio_loop.add_signal_handler(sig, stopper.set)


if __name__ == "__main__":  # pragma: no cover - module-as-script entry
    raise SystemExit(main())
