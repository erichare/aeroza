"""Event-triggered driver for the verification worker.

Subscribes to ``aeroza.mrms.grids.new`` and runs one verification
tick per observation event. Same shape as
:mod:`aeroza.nowcast.event_worker` — the verifier and the nowcaster
both react to observations, just on different sides of the
forecast → measure → score loop.
"""

from __future__ import annotations

import asyncio

import structlog
from sqlalchemy import select

from aeroza.ingest.mrms_grids_models import MrmsGridRow
from aeroza.ingest.mrms_models import MrmsFileRow
from aeroza.shared.db import Database
from aeroza.stream.subscriber import MrmsGridSubscriber
from aeroza.verify.worker import verify_observation

log = structlog.get_logger(__name__)


async def run_event_triggered_verify(
    *,
    subscriber: MrmsGridSubscriber,
    db: Database,
) -> None:
    """Long-running consumer: one verification tick per grid event."""
    log.info("verify.event_triggered.start")
    try:
        async for locator in subscriber.subscribe_new_grids():
            try:
                await _process_event(db=db, file_key=locator.file_key)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.exception(
                    "verify.event_triggered.tick_failed",
                    triggered_by=locator.file_key,
                    error=str(exc),
                )
    finally:
        log.info("verify.event_triggered.stop")


async def _process_event(*, db: Database, file_key: str) -> None:
    """Look up the source grid + file rows by key, then verify against
    every matching nowcast."""
    async with db.sessionmaker() as session:
        result = await session.execute(
            select(MrmsGridRow, MrmsFileRow)
            .join(MrmsFileRow, MrmsFileRow.key == MrmsGridRow.file_key)
            .where(MrmsGridRow.file_key == file_key)
        )
        rows = result.first()
    if rows is None:
        log.warning("verify.event_triggered.unknown_file_key", file_key=file_key)
        return

    grid_row, file_row = rows
    await verify_observation(
        db=db,
        observation_grid=grid_row,
        observation_file=file_row,
    )


__all__ = ["run_event_triggered_verify"]
