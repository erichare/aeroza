"""Backfill orchestrator: materialise the next batch of un-decoded MRMS files.

The discovery worker (:mod:`aeroza.cli.ingest_mrms`) populates
``mrms_files`` with object metadata. This orchestrator picks up where
that one leaves off — it scans for catalog rows that lack a matching
``mrms_grids`` row, downloads + decodes + writes Zarr for each, and
records the locator. Decoupling discovery from materialisation lets the
heavy step (cfgrib + Zarr) fail or fall behind without losing the
"what's available" feed.

The two workers are independent processes in production. Tests exercise
both the per-tick function (:func:`materialise_unmaterialised_once`) and
the CLI driver (:mod:`aeroza.cli.materialise_mrms`).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import structlog

from aeroza.ingest.mrms_grids import materialise_mrms_file
from aeroza.ingest.mrms_grids_store import find_unmaterialised_files
from aeroza.shared.db import Database
from aeroza.stream.publisher import MrmsGridPublisher, NullMrmsGridPublisher

log = structlog.get_logger(__name__)


@dataclass(frozen=True, slots=True)
class MaterialiseResult:
    """Per-tick outcome — useful for tests, structured logs, and metrics."""

    materialised_keys: tuple[str, ...]
    failed_keys: tuple[str, ...]

    @property
    def materialised(self) -> int:
        return len(self.materialised_keys)

    @property
    def failed(self) -> int:
        return len(self.failed_keys)


_EMPTY: MaterialiseResult = MaterialiseResult(materialised_keys=(), failed_keys=())


async def materialise_unmaterialised_once(
    *,
    db: Database,
    s3_client: Any,
    target_root: str | Path,
    product: str,
    level: str,
    batch_size: int = 8,
    publisher: MrmsGridPublisher | None = None,
) -> MaterialiseResult:
    """One materialisation tick.

    1. Pick up to ``batch_size`` catalog rows for ``product``/``level``
       that lack a matching ``mrms_grids`` row, newest first.
    2. For each, run :func:`materialise_mrms_file` (download + decode +
       Zarr write + upsert), sequentially. We deliberately *don't*
       fan out across files — Zarr writes touch the filesystem and
       cfgrib's eccodes binding is not thread-safe; one-at-a-time keeps
       the worker boring and predictable.
    3. After a successful materialisation, publish ``aeroza.mrms.grids.new``
       via ``publisher``. Defaults to :class:`NullMrmsGridPublisher` when
       streaming is disabled. Publisher errors are logged but do not roll
       back the materialisation — the catalog is durable, a future replay
       can catch up missed events. Same envelope as the file-catalog
       publisher.
    4. Per-file failures are caught and logged; the rest of the batch
       still runs. The returned :class:`MaterialiseResult` reports both
       success and failure keys so the CLI can surface them.

    Single-worker assumption: ``find_unmaterialised_files`` only returns
    files without a grid, so every successful tick is a fresh
    materialisation. With concurrent workers the upsert's WHERE-clause
    no-op semantic protects DB integrity, but the publisher would still
    fire — see the file-catalog publisher for the same trade-off.
    """
    pub = publisher if publisher is not None else NullMrmsGridPublisher()

    async with db.sessionmaker() as session:
        files = await find_unmaterialised_files(
            session, product=product, level=level, limit=batch_size
        )

    if not files:
        log.debug("mrms.materialise.tick.idle", product=product, level=level)
        return _EMPTY

    materialised: list[str] = []
    failed: list[str] = []
    for file in files:
        try:
            locator = await materialise_mrms_file(
                db=db,
                s3_client=s3_client,
                file=file,
                target_root=target_root,
            )
        except Exception as exc:
            log.exception(
                "mrms.materialise.tick.failed",
                key=file.key,
                product=product,
                level=level,
                error=str(exc),
            )
            failed.append(file.key)
            continue

        materialised.append(file.key)
        try:
            await pub.publish_new_grid(locator)
        except Exception as exc:
            log.exception(
                "mrms.materialise.tick.publish_failed",
                key=file.key,
                error=str(exc),
            )

    log.info(
        "mrms.materialise.tick",
        product=product,
        level=level,
        candidates=len(files),
        materialised=len(materialised),
        failed=len(failed),
    )
    return MaterialiseResult(
        materialised_keys=tuple(materialised),
        failed_keys=tuple(failed),
    )


__all__ = [
    "MaterialiseResult",
    "materialise_unmaterialised_once",
]
