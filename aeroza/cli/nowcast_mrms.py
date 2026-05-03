"""``aeroza-nowcast-mrms`` worker.

Long-lived consumer that subscribes to ``aeroza.mrms.grids.new`` and
runs the nowcast pipeline (currently :class:`PersistenceForecaster`)
on each freshly-materialised observation grid. Predictions land in
``mrms_nowcasts`` and a corresponding ``aeroza.nowcast.grids.new``
event is published so the dispatcher (Phase 4) and the verification
worker pick them up.

Same shape as ``aeroza-materialise-mrms`` — long-lived NATS consumer
process with SIGTERM/SIGINT handling.
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
from aeroza.nowcast.engine import (
    DEFAULT_HORIZONS_MINUTES,
    Forecaster,
    PersistenceForecaster,
)
from aeroza.nowcast.event_worker import run_event_triggered_nowcast
from aeroza.shared.db import Database, create_engine_and_session
from aeroza.stream.nats import (
    NatsMrmsGridSubscriber,
    NatsNowcastGridPublisher,
    nats_connection,
)
from aeroza.stream.publisher import NullNowcastGridPublisher

log = structlog.get_logger(__name__)

DEFAULT_TARGET_ROOT: Final[str] = "./data/mrms"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="aeroza-nowcast-mrms",
        description=(
            "Subscribe to materialised-grid events and produce nowcasts at "
            "the configured horizons. v1 algorithm: persistence (forecast == "
            "observation) — the documented baseline that real algorithms "
            "must beat."
        ),
    )
    parser.add_argument(
        "--target-root",
        default=DEFAULT_TARGET_ROOT,
        help=(
            f"Root directory where Zarr stores live (default: {DEFAULT_TARGET_ROOT!r}). "
            "Nowcast Zarr stores live under "
            "<target_root>/nowcasts/<algorithm>/<horizon>m/."
        ),
    )
    parser.add_argument(
        "--horizons",
        type=int,
        nargs="+",
        default=list(DEFAULT_HORIZONS_MINUTES),
        help=(
            "Forecast horizons in minutes "
            f"(default: {' '.join(map(str, DEFAULT_HORIZONS_MINUTES))})."
        ),
    )
    parser.add_argument(
        "--no-publish",
        action="store_true",
        help=(
            "Compute + persist nowcasts but do not publish to NATS — "
            "backfills, schema migrations, environments without a broker."
        ),
    )
    parser.add_argument(
        "--algorithm",
        choices=["persistence", "pysteps", "lagged-ensemble"],
        default="persistence",
        help=(
            "Forecaster to run. 'persistence' (default) is the §7 baseline "
            "and pulls in zero extra deps. 'pysteps' runs Lucas–Kanade "
            "optical flow + semi-Lagrangian extrapolation; needs the "
            "[nowcast] extra (`uv sync --extra nowcast`). "
            "'lagged-ensemble' is the simplest probabilistic forecaster: "
            "members are the last K observations, persisted forward. "
            "Pulls in zero extra deps and unlocks Brier/CRPS scoring on "
            "/v1/calibration."
        ),
    )
    parser.add_argument(
        "--ensemble-size",
        type=int,
        default=None,
        help=(
            "Number of ensemble members for probabilistic forecasters "
            "(currently only 'lagged-ensemble'). Defaults to the "
            "forecaster's own default (8 for lagged-ensemble). Ignored "
            "for deterministic algorithms."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings = get_settings()
    log.info(
        "nowcast_mrms.start",
        target_root=args.target_root,
        horizons=args.horizons,
        publish=not args.no_publish,
        env=settings.env,
    )
    return asyncio.run(_run(args=args, settings=settings))


async def _run(*, args: argparse.Namespace, settings: Settings) -> int:
    db = create_engine_and_session(settings.database_url)
    forecaster = _build_forecaster(args.algorithm, ensemble_size=args.ensemble_size)
    try:
        if args.no_publish:
            # NATS-free path: still need the input subscriber, so keep
            # the NATS connection open but use NullNowcastGridPublisher.
            async with nats_connection(settings.nats_url) as nats_client:
                grid_sub = NatsMrmsGridSubscriber(nats_client)
                await _drive(
                    db=db,
                    target_root=args.target_root,
                    horizons=tuple(args.horizons),
                    grid_sub=grid_sub,
                    publisher=NullNowcastGridPublisher(),
                    forecaster=forecaster,
                )
            return 0

        async with nats_connection(settings.nats_url) as nats_client:
            grid_sub = NatsMrmsGridSubscriber(nats_client)
            publisher = NatsNowcastGridPublisher(nats_client)
            await _drive(
                db=db,
                target_root=args.target_root,
                horizons=tuple(args.horizons),
                grid_sub=grid_sub,
                publisher=publisher,
                forecaster=forecaster,
            )
        return 0
    finally:
        await db.dispose()


def _build_forecaster(algorithm: str, *, ensemble_size: int | None = None) -> Forecaster:
    """Resolve the CLI ``--algorithm`` choice to a concrete forecaster."""
    if algorithm == "persistence":
        return PersistenceForecaster()
    if algorithm == "pysteps":
        # Lazy import — pysteps is in the [nowcast] extra and pulls
        # scipy + skimage as transitive deps.
        from aeroza.nowcast.pysteps_forecaster import PystepsForecaster

        return PystepsForecaster()
    if algorithm == "lagged-ensemble":
        from aeroza.nowcast.lagged_ensemble import (
            DEFAULT_ENSEMBLE_SIZE,
            LaggedEnsembleForecaster,
        )

        size = ensemble_size if ensemble_size is not None else DEFAULT_ENSEMBLE_SIZE
        if size < 1:
            raise ValueError(f"ensemble_size must be >= 1, got {size}")
        return LaggedEnsembleForecaster(ensemble_size=size)
    raise ValueError(f"unknown algorithm: {algorithm!r}")


async def _drive(
    *,
    db: Database,
    target_root: str,
    horizons: tuple[int, ...],
    grid_sub: NatsMrmsGridSubscriber,
    publisher: NatsNowcastGridPublisher | NullNowcastGridPublisher,
    forecaster: Forecaster,
) -> None:
    Path(target_root).mkdir(parents=True, exist_ok=True)

    stopper = asyncio.Event()
    _install_signal_handlers(stopper)
    consumer_task = asyncio.create_task(
        run_event_triggered_nowcast(
            subscriber=grid_sub,
            db=db,
            forecaster=forecaster,
            target_root=target_root,
            horizons_minutes=horizons,
            publisher=publisher,
        ),
        name="nowcast.event_consumer",
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
