"""``aeroza-dispatch-webhooks`` worker.

Long-running consumer over the three NATS event subjects. For each
event: fan it out to every active subscription whose ``events``
includes the subject (raw fan-out), and — for grid events — also
evaluate every active alert rule and POST to bound subscriptions on a
predicate transition.

This worker is the third and final slice of Phase 4. The subscription
+ HMAC primitive (#31) and the alert-rule DSL (#32) land first; this
process ties them together and actually delivers HTTP.
"""

from __future__ import annotations

import argparse
import asyncio
import signal
from contextlib import suppress
from typing import Final

import httpx
import structlog

from aeroza.config import Settings, get_settings
from aeroza.shared.db import create_engine_and_session
from aeroza.stream.nats import (
    NatsAlertSubscriber,
    NatsMrmsFileSubscriber,
    NatsMrmsGridSubscriber,
    nats_connection,
)
from aeroza.webhooks.orchestrator import (
    AUTO_DISABLE_CONSECUTIVE_FAILURES,
    run_dispatcher,
)

log = structlog.get_logger(__name__)

DEFAULT_HTTP_TIMEOUT_S: Final[float] = 10.0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="aeroza-dispatch-webhooks",
        description=(
            "Subscribe to NATS event subjects and deliver matching events "
            "to active webhook subscriptions. Runs alongside the API and "
            "the materialise worker as a third long-running process."
        ),
    )
    parser.add_argument(
        "--auto-disable-threshold",
        type=int,
        default=AUTO_DISABLE_CONSECUTIVE_FAILURES,
        help=(
            "Consecutive terminal-failure deliveries to one subscription "
            "before it auto-disables. Set to 0 to disable the circuit "
            f"breaker (default: {AUTO_DISABLE_CONSECUTIVE_FAILURES})."
        ),
    )
    parser.add_argument(
        "--http-timeout",
        type=float,
        default=DEFAULT_HTTP_TIMEOUT_S,
        help=(
            "Per-request HTTP timeout in seconds. Each delivery may make "
            f"multiple attempts (default: {DEFAULT_HTTP_TIMEOUT_S}s)."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings = get_settings()
    log.info(
        "dispatch_webhooks.start",
        auto_disable_threshold=args.auto_disable_threshold,
        http_timeout_s=args.http_timeout,
        env=settings.env,
    )
    return asyncio.run(_run(args=args, settings=settings))


async def _run(*, args: argparse.Namespace, settings: Settings) -> int:
    db = create_engine_and_session(settings.database_url)
    try:
        async with (
            nats_connection(settings.nats_url) as nats_client,
            httpx.AsyncClient(timeout=args.http_timeout) as http_client,
        ):
            alert_subscriber = NatsAlertSubscriber(nats_client)
            file_subscriber = NatsMrmsFileSubscriber(nats_client)
            grid_subscriber = NatsMrmsGridSubscriber(nats_client)

            stopper = asyncio.Event()
            _install_signal_handlers(stopper)
            dispatcher_task = asyncio.create_task(
                run_dispatcher(
                    db=db,
                    http_client=http_client,
                    alert_subscriber=alert_subscriber,
                    file_subscriber=file_subscriber,
                    grid_subscriber=grid_subscriber,
                    auto_disable_threshold=args.auto_disable_threshold,
                ),
                name="webhooks.dispatcher",
            )
            try:
                await stopper.wait()
            finally:
                dispatcher_task.cancel()
                with suppress(asyncio.CancelledError):
                    await dispatcher_task
        return 0
    finally:
        await db.dispose()


def _install_signal_handlers(stopper: asyncio.Event) -> None:
    """Wire SIGTERM/SIGINT to set ``stopper``. Same pattern as the
    other long-running workers."""
    asyncio_loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        with suppress(NotImplementedError):
            asyncio_loop.add_signal_handler(sig, stopper.set)


if __name__ == "__main__":  # pragma: no cover - module-as-script entry
    raise SystemExit(main())
