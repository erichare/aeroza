"""Event-triggered driver for the nowcast worker.

Subscribes to ``aeroza.mrms.grids.new`` and runs one nowcast tick per
event. Pattern parallels :mod:`aeroza.ingest.mrms_materialise_event`:
the subscriber is the I/O surface, the tick function (here:
:func:`nowcast_observation_grid`) is reusable on its own.

Robust to bursts and missed events because each tick only handles
the specific grid event it received — no shared queue, no lost work
beyond the at-most-once delivery semantic of NATS core. A future
backstop (interval scan over recently-materialised grids without a
matching nowcast) can layer on top.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import structlog
from sqlalchemy import select

from aeroza.ingest.mrms_grids_models import MrmsGridRow
from aeroza.ingest.mrms_models import MrmsFileRow
from aeroza.nowcast.engine import DEFAULT_HORIZONS_MINUTES, Forecaster
from aeroza.nowcast.worker import nowcast_observation_grid
from aeroza.shared.db import Database
from aeroza.stream.publisher import NowcastGridPublisher, NullNowcastGridPublisher
from aeroza.stream.subscriber import MrmsGridSubscriber

log = structlog.get_logger(__name__)


async def run_event_triggered_nowcast(
    *,
    subscriber: MrmsGridSubscriber,
    db: Database,
    forecaster: Forecaster,
    target_root: str | Path,
    horizons_minutes: tuple[int, ...] = DEFAULT_HORIZONS_MINUTES,
    publisher: NowcastGridPublisher | None = None,
) -> None:
    """Long-running consumer: one nowcast tick per grid event.

    Returns when the subscriber's stream ends. Per-event failures are
    logged but do not stop the loop — the worker is a long-lived
    process; a single bad grid must not silence forecasting for the
    next two minutes.
    """
    pub = publisher if publisher is not None else NullNowcastGridPublisher()
    log.info(
        "nowcast.event_triggered.start",
        algorithm=forecaster.algorithm,
        horizons=list(horizons_minutes),
    )
    try:
        async for locator in subscriber.subscribe_new_grids():
            try:
                await _process_event(
                    db=db,
                    forecaster=forecaster,
                    target_root=target_root,
                    horizons_minutes=horizons_minutes,
                    publisher=pub,
                    file_key=locator.file_key,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.exception(
                    "nowcast.event_triggered.tick_failed",
                    triggered_by=locator.file_key,
                    error=str(exc),
                )
    finally:
        log.info("nowcast.event_triggered.stop")


async def _process_event(
    *,
    db: Database,
    forecaster: Forecaster,
    target_root: str | Path,
    horizons_minutes: tuple[int, ...],
    publisher: NowcastGridPublisher,
    file_key: str,
) -> None:
    """Look up the source grid + file rows by key, then run one tick."""
    async with db.sessionmaker() as session:
        result = await session.execute(
            select(MrmsGridRow, MrmsFileRow)
            .join(MrmsFileRow, MrmsFileRow.key == MrmsGridRow.file_key)
            .where(MrmsGridRow.file_key == file_key)
        )
        rows = result.first()
    if rows is None:
        log.warning("nowcast.event_triggered.unknown_file_key", file_key=file_key)
        return

    grid_row, file_row = rows
    await nowcast_observation_grid(
        db=db,
        forecaster=forecaster,
        target_root=target_root,
        source_grid=grid_row,
        source_file=file_row,
        horizons_minutes=horizons_minutes,
        publisher=publisher,
    )


__all__ = ["run_event_triggered_nowcast"]
