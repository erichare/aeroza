"""Long-lived ``ingest-alerts`` worker.

Wires together :class:`aeroza.shared.db.Database`,
:func:`aeroza.stream.nats.nats_connection`, :class:`NatsAlertPublisher`,
:class:`IntervalLoop`, and :func:`poll_nws_alerts_once` into a single
process. SIGTERM/SIGINT trigger graceful shutdown: the in-flight tick
finishes, the loop stops, the NATS connection closes, and the database
engine disposes.

The polling cadence defaults to 30 seconds — NWS guidance discourages
more aggressive polling on a single client and 30s comfortably beats
the 60s alert publication cadence.
"""

from __future__ import annotations

import argparse
import asyncio
import signal
from contextlib import suppress
from typing import Final

import structlog

from aeroza.config import Settings, get_settings
from aeroza.ingest.poll import poll_nws_alerts_once
from aeroza.ingest.scheduler import IntervalLoop
from aeroza.shared.db import Database, create_engine_and_session
from aeroza.stream.nats import NatsAlertPublisher, nats_connection
from aeroza.stream.publisher import AlertPublisher, NullAlertPublisher

log = structlog.get_logger(__name__)

DEFAULT_INTERVAL_SECONDS: Final[float] = 30.0
LOOP_NAME: Final[str] = "ingest.nws_alerts"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="aeroza-ingest-alerts",
        description="Continuously fetch NWS active alerts, persist them, "
        "and publish a NATS event per newly-observed alert.",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=DEFAULT_INTERVAL_SECONDS,
        help=(
            f"Seconds between ticks (default: {DEFAULT_INTERVAL_SECONDS}). "
            "NWS guidance discourages anything more aggressive than ~30s."
        ),
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single tick and exit (handy for cron-driven deployments).",
    )
    parser.add_argument(
        "--no-publish",
        action="store_true",
        help=(
            "Persist but do not publish — useful for backfills, schema "
            "migrations, or environments without a NATS broker."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings = get_settings()
    log.info(
        "ingest_alerts.start",
        interval_s=args.interval,
        once=args.once,
        publish=not args.no_publish,
        env=settings.env,
    )
    return asyncio.run(_run(args=args, settings=settings))


async def _run(*, args: argparse.Namespace, settings: Settings) -> int:
    db = create_engine_and_session(settings.database_url)
    try:
        if args.no_publish:
            await _drive(db=db, publisher=NullAlertPublisher(), args=args)
            return 0

        async with nats_connection(settings.nats_url) as nats_client:
            publisher = NatsAlertPublisher(nats_client)
            await _drive(db=db, publisher=publisher, args=args)
        return 0
    finally:
        await db.dispose()


async def _drive(*, db: Database, publisher: AlertPublisher, args: argparse.Namespace) -> None:
    if args.once:
        await poll_nws_alerts_once(db=db, publisher=publisher)
        return

    async def tick() -> None:
        await poll_nws_alerts_once(db=db, publisher=publisher)

    loop = IntervalLoop(tick=tick, interval_s=args.interval, name=LOOP_NAME)
    stopper = asyncio.Event()
    _install_signal_handlers(stopper)
    await loop.start()
    try:
        await stopper.wait()
    finally:
        await loop.stop()


def _install_signal_handlers(stopper: asyncio.Event) -> None:
    """Wire SIGTERM/SIGINT to set ``stopper``.

    On platforms where ``add_signal_handler`` is unsupported (notably
    Windows), fall back silently — pytest, the CLI smoke test, and
    Linux/macOS production all support the asyncio path.
    """
    asyncio_loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        with suppress(NotImplementedError):
            asyncio_loop.add_signal_handler(sig, stopper.set)


if __name__ == "__main__":  # pragma: no cover - module-as-script entry
    raise SystemExit(main())
