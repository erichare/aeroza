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
"""

from __future__ import annotations

import argparse
import asyncio
import signal
from contextlib import suppress
from pathlib import Path
from typing import Final

import structlog

from aeroza.config import Settings, get_settings
from aeroza.ingest._aws import open_data_s3_client
from aeroza.ingest.mrms_materialise_poll import materialise_unmaterialised_once
from aeroza.ingest.scheduler import IntervalLoop
from aeroza.shared.db import Database, create_engine_and_session
from aeroza.stream.nats import NatsMrmsGridPublisher, nats_connection
from aeroza.stream.publisher import MrmsGridPublisher, NullMrmsGridPublisher

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
            await _drive(db=db, publisher=NullMrmsGridPublisher(), args=args)
            return 0

        async with nats_connection(settings.nats_url) as nats_client:
            publisher = NatsMrmsGridPublisher(nats_client)
            await _drive(db=db, publisher=publisher, args=args)
        return 0
    finally:
        await db.dispose()


async def _drive(
    *,
    db: Database,
    publisher: MrmsGridPublisher,
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
        await tick()
        return

    loop = IntervalLoop(tick=tick, interval_s=args.interval, name=LOOP_NAME)
    stopper = asyncio.Event()
    _install_signal_handlers(stopper)
    await loop.start()
    try:
        await stopper.wait()
    finally:
        await loop.stop()


def _install_signal_handlers(stopper: asyncio.Event) -> None:
    """Wire SIGTERM/SIGINT to set ``stopper``. Same pattern as ingest-mrms."""
    asyncio_loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        with suppress(NotImplementedError):
            asyncio_loop.add_signal_handler(sig, stopper.set)


if __name__ == "__main__":  # pragma: no cover - module-as-script entry
    raise SystemExit(main())
