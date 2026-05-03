"""Long-lived ``materialise-mrms`` worker.

Reads ``mrms_files`` rows that lack a matching ``mrms_grids`` row,
downloads each from S3, decodes the GRIB2 to xarray, writes a Zarr
store, and records the locator. The discovery worker
(:mod:`aeroza.cli.ingest_mrms`) feeds it; this one is the heavy step
that depends on cfgrib + eccodes.

Decoupled from discovery on purpose: cfgrib + Zarr writes are
slower and more failure-prone than S3 listing, and we don't want a
decode error or missing system library to silence "what files exist
right now" telemetry.

When NATS is wired up (the default) the worker also subscribes to
``aeroza.mrms.files.new`` and runs a tick per event, so a freshly-
discovered file lands as a queryable Zarr grid within seconds. The
interval loop continues to run as a backstop for missed events / cold
start. Pass ``--no-publish`` to disable both directions of NATS (no
publish, no subscribe — interval-only).
"""

from __future__ import annotations

import argparse
import asyncio
import signal
import sys
from contextlib import suppress
from pathlib import Path
from typing import Final

import structlog

from aeroza.config import Settings, get_settings
from aeroza.ingest._aws import open_data_s3_client
from aeroza.ingest.mrms_decode import CfgribUnavailableError, ensure_cfgrib_available
from aeroza.ingest.mrms_materialise_event import run_event_triggered_materialisation
from aeroza.ingest.mrms_materialise_poll import materialise_unmaterialised_once
from aeroza.ingest.scheduler import IntervalLoop
from aeroza.shared.db import Database, create_engine_and_session
from aeroza.stream.nats import (
    NatsMrmsFileSubscriber,
    NatsMrmsGridPublisher,
    nats_connection,
)
from aeroza.stream.publisher import MrmsGridPublisher, NullMrmsGridPublisher
from aeroza.stream.subscriber import MrmsFileSubscriber

log = structlog.get_logger(__name__)

DEFAULT_INTERVAL_SECONDS: Final[float] = 60.0
DEFAULT_PRODUCT: Final[str] = "MergedReflectivityComposite"
DEFAULT_LEVEL: Final[str] = "00.50"
DEFAULT_BATCH_SIZE: Final[int] = 8
DEFAULT_TARGET_ROOT: Final[str] = "./data/mrms"
LOOP_NAME: Final[str] = "ingest.mrms.materialise"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="aeroza-materialise-mrms",
        description=(
            "Continuously materialise un-decoded MRMS files: download, "
            "decode (cfgrib), write Zarr, record the locator."
        ),
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=DEFAULT_INTERVAL_SECONDS,
        help=f"Seconds between ticks (default: {DEFAULT_INTERVAL_SECONDS}).",
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
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=(
            f"Maximum files to materialise per tick (default: {DEFAULT_BATCH_SIZE}). "
            "Each file is decoded sequentially within a tick."
        ),
    )
    parser.add_argument(
        "--target-root",
        default=DEFAULT_TARGET_ROOT,
        help=(
            f"Root directory for Zarr stores (default: {DEFAULT_TARGET_ROOT!r}). "
            "Each materialised file becomes one .zarr directory beneath it."
        ),
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
            "Materialise but do not publish — backfills, schema migrations, "
            "or environments without a NATS broker."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings = get_settings()
    # Probe cfgrib at startup. Without this, every queued grid would
    # fail decode with the same xarray "unrecognized engine" error,
    # flooding the log without an actionable next step. Failing fast
    # here surfaces the install hint exactly once.
    try:
        ensure_cfgrib_available()
    except CfgribUnavailableError as exc:
        log.error("materialise_mrms.cfgrib_unavailable", hint=str(exc))
        # Print to stderr too so the operator sees it even when the
        # structlog renderer is configured terse / piped to a file.
        print(f"\n[materialise-mrms] {exc}\n", file=sys.stderr)
        return 2
    log.info(
        "materialise_mrms.start",
        interval_s=args.interval,
        product=args.product,
        level=args.level,
        batch_size=args.batch_size,
        target_root=args.target_root,
        once=args.once,
        publish=not args.no_publish,
        env=settings.env,
    )
    return asyncio.run(_run(args=args, settings=settings))


async def _run(*, args: argparse.Namespace, settings: Settings) -> int:
    db = create_engine_and_session(settings.database_url)
    try:
        if args.no_publish:
            await _drive(db=db, publisher=NullMrmsGridPublisher(), subscriber=None, args=args)
            return 0

        async with nats_connection(settings.nats_url) as nats_client:
            publisher = NatsMrmsGridPublisher(nats_client)
            subscriber = NatsMrmsFileSubscriber(nats_client)
            await _drive(db=db, publisher=publisher, subscriber=subscriber, args=args)
        return 0
    finally:
        await db.dispose()


async def _drive(
    *,
    db: Database,
    publisher: MrmsGridPublisher,
    subscriber: MrmsFileSubscriber | None,
    args: argparse.Namespace,
) -> None:
    target_root = Path(args.target_root)
    target_root.mkdir(parents=True, exist_ok=True)
    s3_client = open_data_s3_client()

    async def tick() -> None:
        await materialise_unmaterialised_once(
            db=db,
            s3_client=s3_client,
            target_root=target_root,
            product=args.product,
            level=args.level,
            batch_size=args.batch_size,
            publisher=publisher,
        )

    if args.once:
        # `--once` is a cron-friendly single tick; subscriptions don't make
        # sense here (they'd never be drained before the process exits).
        await tick()
        return

    loop = IntervalLoop(tick=tick, interval_s=args.interval, name=LOOP_NAME)
    stopper = asyncio.Event()
    _install_signal_handlers(stopper)
    await loop.start()

    # Run the event-driven consumer concurrently with the interval loop.
    # When the subscriber is None (e.g. `--no-publish`) the worker is
    # interval-only — same behaviour as before this feature landed.
    consumer_task: asyncio.Task[None] | None = None
    if subscriber is not None:
        consumer_task = asyncio.create_task(
            run_event_triggered_materialisation(
                subscriber=subscriber,
                db=db,
                s3_client=s3_client,
                target_root=target_root,
                product=args.product,
                level=args.level,
                batch_size=args.batch_size,
                publisher=publisher,
            ),
            name="mrms.materialise.event_consumer",
        )

    try:
        await stopper.wait()
    finally:
        if consumer_task is not None:
            consumer_task.cancel()
            with suppress(asyncio.CancelledError):
                await consumer_task
        await loop.stop()


def _install_signal_handlers(stopper: asyncio.Event) -> None:
    """Wire SIGTERM/SIGINT to set ``stopper``. Same pattern as ingest-mrms."""
    asyncio_loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        with suppress(NotImplementedError):
            asyncio_loop.add_signal_handler(sig, stopper.set)


if __name__ == "__main__":  # pragma: no cover - module-as-script entry
    raise SystemExit(main())
