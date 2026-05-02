"""``aeroza-verify-nowcasts`` worker.

Long-lived consumer that subscribes to ``aeroza.mrms.grids.new`` and
scores every previously-issued forecast against the just-arrived
observation. Persists per-(forecast, observation) MAE / bias / RMSE
to ``nowcast_verifications``.

The verification worker doesn't publish its own NATS event — the
operator UI / calibration aggregator reads scoring rows directly. We
can add ``aeroza.verify.scored`` later if a downstream consumer wants
push notifications.
"""

from __future__ import annotations

import argparse
import asyncio
import signal
from contextlib import suppress

import structlog

from aeroza.config import Settings, get_settings
from aeroza.shared.db import Database, create_engine_and_session
from aeroza.stream.nats import NatsMrmsGridSubscriber, nats_connection
from aeroza.verify.event_worker import run_event_triggered_verify

log = structlog.get_logger(__name__)


def build_parser() -> argparse.ArgumentParser:
    return argparse.ArgumentParser(
        prog="aeroza-verify-nowcasts",
        description=(
            "Subscribe to materialised-grid events and score matching "
            "nowcasts against the just-arrived observation. Populates "
            "``nowcast_verifications`` for the public calibration aggregator."
        ),
    )


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings = get_settings()
    log.info("verify_nowcasts.start", env=settings.env)
    return asyncio.run(_run(args=args, settings=settings))


async def _run(*, args: argparse.Namespace, settings: Settings) -> int:
    db = create_engine_and_session(settings.database_url)
    try:
        async with nats_connection(settings.nats_url) as nats_client:
            grid_sub = NatsMrmsGridSubscriber(nats_client)
            await _drive(db=db, grid_sub=grid_sub)
        return 0
    finally:
        await db.dispose()


async def _drive(
    *,
    db: Database,
    grid_sub: NatsMrmsGridSubscriber,
) -> None:
    stopper = asyncio.Event()
    _install_signal_handlers(stopper)
    consumer_task = asyncio.create_task(
        run_event_triggered_verify(subscriber=grid_sub, db=db),
        name="verify.event_consumer",
    )
    try:
        await stopper.wait()
    finally:
        consumer_task.cancel()
        with suppress(asyncio.CancelledError):
            await consumer_task


def _install_signal_handlers(stopper: asyncio.Event) -> None:
    asyncio_loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        with suppress(NotImplementedError):
            asyncio_loop.add_signal_handler(sig, stopper.set)


if __name__ == "__main__":  # pragma: no cover - module-as-script entry
    raise SystemExit(main())
