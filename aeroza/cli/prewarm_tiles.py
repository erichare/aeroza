"""``aeroza-prewarm-tiles`` worker.

Long-lived consumer that subscribes to ``aeroza.mrms.grids.new`` and
renders the full CONUS tile pyramid (z=2..8) for each freshly-
materialised grid, uploading every tile to Cloudflare R2 so the static
``tiles.aeroza.app`` origin serves them sub-100ms at 100% hit rate.

Without this process running, the only thing populating R2 is the
write-through path on the on-demand FastAPI tile route (PR #94) — and
that only fills tiles a user has already requested, which is too slow
for the radar loop's hammering pattern. The result is the production
404 storm this CLI exists to close out.

Same shape as ``aeroza-nowcast-mrms`` — long-lived NATS consumer
process with SIGTERM/SIGINT handling. Falls back gracefully when R2
isn't configured (warns and exits 0 in prod, or populates the
in-process LRU in dev) so the worker never crash-loops a deploy that
hasn't been given R2 creds yet.
"""

from __future__ import annotations

import argparse
import asyncio
import signal
import sys
from collections.abc import Iterable
from contextlib import suppress
from typing import Final

import structlog

from aeroza.config import Settings, get_settings
from aeroza.stream.nats import NatsMrmsGridSubscriber, nats_connection
from aeroza.stream.subscriber import MrmsGridSubscriber
from aeroza.tiles.cache import TilePngCache
from aeroza.tiles.prewarm import (
    DEFAULT_PREWARM_FORMATS,
    DEFAULT_PREWARM_ZOOMS,
    run_prewarm_consumer,
)
from aeroza.tiles.r2 import R2Client, build_r2_client
from aeroza.tiles.raster import TileFormat

log = structlog.get_logger(__name__)

_VALID_FORMATS: Final[frozenset[str]] = frozenset({"webp", "png"})


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="aeroza-prewarm-tiles",
        description=(
            "Subscribe to materialised-grid events and pre-render the "
            "CONUS tile pyramid (z=2..8) into Cloudflare R2. Steady-state "
            "this is what makes 'tiles.aeroza.app/{fileKey}/{z}/{x}/{y}.webp' "
            "a 100% R2 hit instead of falling back to the on-demand "
            "FastAPI tile route."
        ),
    )
    parser.add_argument(
        "--zooms",
        type=int,
        nargs="+",
        default=list(DEFAULT_PREWARM_ZOOMS),
        help=(
            "Zoom levels to prewarm "
            f"(default: {' '.join(map(str, DEFAULT_PREWARM_ZOOMS))}). "
            "z>=9 quadruples tile count per step and is left to the "
            "on-demand write-through path."
        ),
    )
    parser.add_argument(
        "--formats",
        nargs="+",
        choices=sorted(_VALID_FORMATS),
        default=list(DEFAULT_PREWARM_FORMATS),
        help=(
            "Tile formats to upload "
            f"(default: {' '.join(DEFAULT_PREWARM_FORMATS)}). "
            "PNG is rarely requested in production; staying webp-only "
            "halves R2 storage + write-op cost."
        ),
    )
    parser.add_argument(
        "--no-r2",
        action="store_true",
        help=(
            "Disable the R2 upload path even when creds are configured. "
            "Falls back to populating an in-process LRU — useful for "
            "local smoke tests where R2 must not be touched."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings = get_settings()
    log.info(
        "prewarm_tiles.start",
        zooms=args.zooms,
        formats=args.formats,
        env=settings.env,
        r2_enabled=not args.no_r2,
    )
    return asyncio.run(_run(args=args, settings=settings))


async def _run(*, args: argparse.Namespace, settings: Settings) -> int:
    r2_client: R2Client | None = None if args.no_r2 else build_r2_client(settings)
    lru_cache: TilePngCache | None = None

    if r2_client is None:
        # Fall back to the in-process LRU so local dev / smoke tests
        # still benefit. In production this branch is a misconfiguration —
        # warn loudly so the operator notices, but don't crash-loop the
        # process (Railway would burn deploy budget restart-loop-pulling
        # the image while the operator is still typing the R2 creds).
        if settings.env == "production":
            log.warning(
                "prewarm_tiles.r2_disabled_in_production",
                hint=(
                    "Set AEROZA_R2_ENDPOINT, AEROZA_R2_BUCKET, "
                    "AEROZA_R2_ACCESS_KEY_ID, AEROZA_R2_SECRET_ACCESS_KEY "
                    "so prewarm can populate the CDN-backed tile origin. "
                    "Without these, tiles.aeroza.app will 404 until the "
                    "on-demand write-through path catches up grid-by-grid."
                ),
            )
            print(
                "\n[prewarm-tiles] R2 not configured in production — "
                "tiles.aeroza.app will keep returning 404 until creds are "
                "set. See docs/DEPLOY-RAILWAY.md for the env var list.\n",
                file=sys.stderr,
            )
        lru_cache = TilePngCache()

    async with nats_connection(settings.nats_url) as nats_client:
        subscriber = NatsMrmsGridSubscriber(nats_client)
        await _drive(
            subscriber=subscriber,
            r2_client=r2_client,
            lru_cache=lru_cache,
            zooms=tuple(args.zooms),
            formats=tuple(args.formats),
        )
    return 0


async def _drive(
    *,
    subscriber: MrmsGridSubscriber,
    r2_client: R2Client | None,
    lru_cache: TilePngCache | None,
    zooms: Iterable[int],
    formats: Iterable[TileFormat],
) -> None:
    stopper = asyncio.Event()
    _install_signal_handlers(stopper)
    consumer_task = asyncio.create_task(
        run_prewarm_consumer(
            subscriber=subscriber,
            r2_client=r2_client,
            lru_cache=lru_cache,
            zooms=zooms,
            formats=formats,
        ),
        name="tiles.prewarm.event_consumer",
    )
    try:
        await stopper.wait()
    finally:
        consumer_task.cancel()
        with suppress(asyncio.CancelledError):
            await consumer_task


def _install_signal_handlers(stopper: asyncio.Event) -> None:
    """SIGTERM / SIGINT → set the stopper event."""
    asyncio_loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        with suppress(NotImplementedError):
            asyncio_loop.add_signal_handler(sig, stopper.set)


if __name__ == "__main__":  # pragma: no cover - module-as-script entry
    raise SystemExit(main())
