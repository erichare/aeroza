"""Event-triggered driver for the materialise worker.

Companion to :mod:`aeroza.ingest.mrms_materialise_poll`. The interval
loop in the poll module is a backstop — it sweeps the catalog every N
seconds and catches anything that hasn't been materialised. This module
adds the *push* path: subscribe to ``aeroza.mrms.files.new`` and run
one materialise tick per event, so a freshly-discovered file becomes a
queryable Zarr grid within seconds rather than within the next interval.

Key design choice: each event triggers a generic
:func:`materialise_unmaterialised_once` tick rather than handing the
specific file straight to the decoder. That makes the consumer robust
to:

- **Cold start** — events that landed before the worker was running
  show up in the first tick anyway via the catalog scan.
- **Missed events** — NATS core delivery is at-most-once for ephemeral
  subscriptions; whatever slipped through is picked up by the next
  event or the interval backstop.
- **Bursts** — N events arriving in quick succession collapse into
  fewer ticks once the in-flight tick finishes; ``batch_size`` caps the
  per-tick work either way.

This mirrors the SSE-stream pattern in :mod:`aeroza.query.v1`: the
subscriber is the I/O surface, the work is delegated to a function that
already knows how to stand alone.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import structlog

from aeroza.ingest.mrms_materialise_poll import materialise_unmaterialised_once
from aeroza.shared.db import Database
from aeroza.stream.publisher import MrmsGridPublisher, NullMrmsGridPublisher
from aeroza.stream.subscriber import MrmsFileSubscriber

log = structlog.get_logger(__name__)


async def run_event_triggered_materialisation(
    *,
    subscriber: MrmsFileSubscriber,
    db: Database,
    s3_client: Any,
    target_root: str | Path,
    product: str,
    level: str,
    batch_size: int = 8,
    publisher: MrmsGridPublisher | None = None,
) -> None:
    """Long-running consumer: one tick per ``aeroza.mrms.files.new`` event.

    Returns when the subscriber's stream ends (broker disconnect, the
    in-memory subscriber's ``close()``, or the surrounding task being
    cancelled). Per-event tick failures are logged but do **not** stop
    the loop — the same robustness rule as the interval scheduler.

    The triggering ``MrmsFile`` is logged for traceability but never
    fed into the tick directly: a ticked materialisation always queries
    the catalog for unmaterialised work, and that's the only call site
    that decides what gets decoded next. See module docstring for why.
    """
    pub = publisher if publisher is not None else NullMrmsGridPublisher()
    log.info(
        "mrms.materialise.event_triggered.start",
        product=product,
        level=level,
        batch_size=batch_size,
    )
    try:
        async for file in subscriber.subscribe_new_files():
            log.debug(
                "mrms.materialise.event_triggered.fired",
                triggered_by=file.key,
                product=file.product,
                level=file.level,
            )
            try:
                await materialise_unmaterialised_once(
                    db=db,
                    s3_client=s3_client,
                    target_root=target_root,
                    product=product,
                    level=level,
                    batch_size=batch_size,
                    publisher=pub,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.exception(
                    "mrms.materialise.event_triggered.tick_failed",
                    triggered_by=file.key,
                    error=str(exc),
                )
    finally:
        log.info("mrms.materialise.event_triggered.stop")


__all__ = ["run_event_triggered_materialisation"]
