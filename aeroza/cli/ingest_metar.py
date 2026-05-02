"""Long-lived ``ingest-metar`` worker.

Polls the AWC METAR JSON endpoint for a configurable list of stations
and upserts each observation into ``metar_observations``. Mirrors the
shape of :mod:`aeroza.cli.ingest_alerts`: SIGTERM/SIGINT trigger
graceful shutdown, ``--once`` runs a single tick, ``--interval``
overrides the default cadence.

Why polling and not webhook / SSE: AWC doesn't publish a streaming
feed for METAR. METAR cycles are hourly with SPECI between them, so
a 5-minute poll catches every update with negligible duplicate work
(the upsert is a no-op when nothing changed).

Default station list is a small CONUS sample so the worker boots out
of the box; pass ``--stations KIAH,KHOU,...`` to override. Reading
the full FAA / ICAO list from a CSV file is a follow-up — for now
operators specify the airports they care about.
"""

from __future__ import annotations

import argparse
import asyncio
import signal
from collections.abc import Sequence
from contextlib import suppress
from typing import Final

import structlog

from aeroza.config import Settings, get_settings
from aeroza.ingest.metar import fetch_metar_observations
from aeroza.ingest.metar_store import upsert_metar_observations
from aeroza.ingest.scheduler import IntervalLoop
from aeroza.shared.db import Database, create_engine_and_session

log = structlog.get_logger(__name__)

DEFAULT_INTERVAL_SECONDS: Final[float] = 300.0  # 5 minutes
LOOP_NAME: Final[str] = "ingest.metar"

# A small but meaningful CONUS sample so the worker has something to
# do on a fresh checkout. Top-25-ish airports by enplanements; not
# canonical, just useful. Operators override with --stations.
DEFAULT_STATIONS: Final[tuple[str, ...]] = (
    "KATL",
    "KLAX",
    "KORD",
    "KDFW",
    "KDEN",
    "KJFK",
    "KSFO",
    "KSEA",
    "KLAS",
    "KMCO",
    "KCLT",
    "KEWR",
    "KMIA",
    "KPHX",
    "KIAH",
    "KBOS",
    "KMSP",
    "KFLL",
    "KDTW",
    "KPHL",
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="aeroza-ingest-metar",
        description=(
            "Continuously fetch METAR observations from the Aviation Weather "
            "Center and upsert them into metar_observations."
        ),
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=DEFAULT_INTERVAL_SECONDS,
        help=(
            f"Seconds between ticks (default: {DEFAULT_INTERVAL_SECONDS}). "
            "AWC's METAR cycle is hourly + SPECI; 5 min is a safe default."
        ),
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single tick and exit (cron-driven deployments).",
    )
    parser.add_argument(
        "--stations",
        type=str,
        default=",".join(DEFAULT_STATIONS),
        help=(
            "Comma-separated ICAO ids to fetch (default: a CONUS top-20 sample). "
            "Case-insensitive; we uppercase before sending."
        ),
    )
    return parser


def _parse_stations(raw: str) -> tuple[str, ...]:
    """Normalise the ``--stations`` arg.

    Strips whitespace, drops empty entries, uppercases, dedupes while
    preserving order. AWC tolerates lowercase ids but pinning the wire
    shape makes log lines easier to grep.
    """
    seen: set[str] = set()
    out: list[str] = []
    for item in raw.split(","):
        canon = item.strip().upper()
        if not canon or canon in seen:
            continue
        seen.add(canon)
        out.append(canon)
    return tuple(out)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    stations = _parse_stations(args.stations)
    if not stations:
        log.error("ingest_metar.no_stations")
        return 2
    settings = get_settings()
    log.info(
        "ingest_metar.start",
        interval_s=args.interval,
        once=args.once,
        station_count=len(stations),
        env=settings.env,
    )
    return asyncio.run(_run(args=args, settings=settings, stations=stations))


async def _run(
    *,
    args: argparse.Namespace,
    settings: Settings,
    stations: Sequence[str],
) -> int:
    db = create_engine_and_session(settings.database_url)
    try:
        await _drive(db=db, args=args, stations=stations)
        return 0
    finally:
        await db.dispose()


async def _tick(*, db: Database, stations: Sequence[str]) -> None:
    """One fetch → upsert cycle. Logs the per-station counts."""
    observations = await fetch_metar_observations(station_ids=stations)
    if not observations:
        log.info("ingest_metar.tick.empty", requested=len(stations))
        return
    async with db.sessionmaker() as session:
        result = await upsert_metar_observations(session, observations)
        await session.commit()
    log.info(
        "ingest_metar.tick.done",
        requested=len(stations),
        observed=len(observations),
        inserted=result.inserted,
        updated=result.updated,
    )


async def _drive(
    *,
    db: Database,
    args: argparse.Namespace,
    stations: Sequence[str],
) -> None:
    if args.once:
        await _tick(db=db, stations=stations)
        return

    async def tick() -> None:
        await _tick(db=db, stations=stations)

    loop = IntervalLoop(tick=tick, interval_s=args.interval, name=LOOP_NAME)
    stopper = asyncio.Event()
    _install_signal_handlers(stopper)
    await loop.start()
    try:
        await stopper.wait()
    finally:
        await loop.stop()


def _install_signal_handlers(stopper: asyncio.Event) -> None:
    asyncio_loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        with suppress(NotImplementedError):
            asyncio_loop.add_signal_handler(sig, stopper.set)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
