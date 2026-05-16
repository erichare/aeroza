"""Prune old MRMS Zarr stores + catalog rows + expired alerts.

The two public entry-points are :func:`prune_old_mrms_once` and
:func:`prune_expired_alerts_once`. Both are idempotent per tick — they
operate on whatever's currently older than the cutoff, so re-running
after a crash or partial completion converges to the same end state.

Single-pass MRMS prune
----------------------
The chain of FK cascades from ``mrms_files`` (the natural root) makes
the DB cleanup a one-liner — deleting one file row drops the matching
grid, every nowcast derived from it, and every verification scored
against either side. The filesystem side isn't in the cascade, so the
worker collects every ``zarr_uri`` referenced by the doomed rows
first, commits the DB delete, **then** ``rm -rf``s each path outside
the transaction.

The DB-before-disk order is deliberate: while a live tile request
might race the sweeper, once the row is gone, the route's
``find_mrms_grid_by_key`` returns None and the transparent-tile
fallback kicks in — no 500. The opposite ordering (disk first) opens
a race window where a concurrent request can read the row's
``zarr_uri`` and call ``xr.open_zarr`` on a directory we've already
``rm -rf``d, raising ``FileNotFoundError`` from inside the renderer.
Orphan Zarr directories from a partial disk-delete failure are
recoverable (a future sweep can detect zarr paths with no catalog
row); user-visible 500s are not.

Batching
--------
File keys are processed in chunks of ``batch_size`` so a long-running
prune (e.g. after a deploy gap) doesn't hold a write lock for minutes.
Each batch is its own transaction; failure of one batch logs and the
next tick picks up where this one left off.
"""

from __future__ import annotations

import shutil
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Final

import structlog
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from aeroza.ingest.mrms_grids_models import MrmsGridRow
from aeroza.ingest.mrms_models import MrmsFileRow
from aeroza.ingest.nws_alerts_models import NwsAlertRow
from aeroza.nowcast.models import NowcastRow
from aeroza.shared.db import Database
from aeroza.tiles.r2 import R2Client

log = structlog.get_logger(__name__)

DEFAULT_BATCH_SIZE: Final[int] = 500


@dataclass(frozen=True, slots=True)
class PruneResult:
    """Per-tick prune outcome — surfaced by structured logs and tests."""

    deleted_files: int = 0
    deleted_zarrs: int = 0
    failed_zarrs: int = 0
    deleted_alerts: int = 0
    deleted_r2_objects: int = 0
    failed_r2_grids: int = 0

    def merged_with(self, other: PruneResult) -> PruneResult:
        return PruneResult(
            deleted_files=self.deleted_files + other.deleted_files,
            deleted_zarrs=self.deleted_zarrs + other.deleted_zarrs,
            failed_zarrs=self.failed_zarrs + other.failed_zarrs,
            deleted_alerts=self.deleted_alerts + other.deleted_alerts,
            deleted_r2_objects=self.deleted_r2_objects + other.deleted_r2_objects,
            failed_r2_grids=self.failed_r2_grids + other.failed_r2_grids,
        )


_EMPTY: PruneResult = PruneResult()


async def prune_old_mrms_once(
    *,
    db: Database,
    retention_hours: float,
    batch_size: int = DEFAULT_BATCH_SIZE,
    now: datetime | None = None,
    r2_client: R2Client | None = None,
) -> PruneResult:
    """Prune MRMS files (and their cascades) older than ``retention_hours``.

    Selects ``mrms_files`` rows whose ``valid_at`` is older than the
    cutoff, removes every Zarr store referenced by their grid / nowcast
    catalog rows, then deletes the file rows in batches. FK cascades
    drop ``mrms_grids``, ``mrms_nowcasts``, and ``nowcast_verifications``.

    When ``r2_client`` is supplied, also deletes the pre-rendered tile
    pyramid under each pruned ``file_key/`` prefix — same DB-before-
    disk ordering applies, and orphan R2 objects are caught by the
    bucket's lifecycle rule as a backstop.

    ``now`` is injectable so tests can pin time without monkeypatching
    ``datetime.now``. Defaults to ``datetime.now(UTC)``.
    """
    if retention_hours <= 0:
        raise ValueError(f"retention_hours must be positive, got {retention_hours}")
    if batch_size <= 0:
        raise ValueError(f"batch_size must be positive, got {batch_size}")

    cutoff = (now or datetime.now(UTC)) - timedelta(hours=retention_hours)
    total = _EMPTY

    while True:
        async with db.sessionmaker() as session:
            keys = await _select_expired_file_keys(session, cutoff=cutoff, limit=batch_size)
            if not keys:
                break

            # Collect zarr paths *before* the delete — the FK cascade
            # would clear the rows that hold the URIs, leaving nothing
            # for the disk pass to act on.
            zarr_paths = await _collect_zarr_paths_for_keys(session, keys=keys)

            # DB row delete commits first. Once this lands, any live
            # request resolving to one of these keys gets a None from
            # ``find_mrms_grid_by_key`` and hits the transparent-tile
            # fallback — no opportunity to call ``xr.open_zarr`` on a
            # path we're about to remove.
            deleted_files = await _delete_file_rows(session, keys=keys)
            await session.commit()

        # Outside the session: the rows are committed-gone, so it is
        # now safe to remove the on-disk artefacts. A failure here
        # produces orphan Zarr directories (recoverable via a future
        # orphan sweep) rather than a 500 surfaced to the client mid-
        # render.
        fs_result = _remove_zarr_paths(zarr_paths)

        # R2 tile cleanup — same DB-before-disk ordering as the
        # Zarr pass. By the time we get here the catalog row is gone,
        # so a future live request will route through the
        # transparent-tile fallback whether or not the R2 delete
        # succeeds. Failures count as ``failed_r2_grids`` and are
        # backstopped by the bucket's lifecycle rule.
        r2_result = await _delete_r2_grids(r2_client, keys=keys)

        batch_result = PruneResult(
            deleted_files=deleted_files,
            deleted_zarrs=fs_result.deleted,
            failed_zarrs=fs_result.failed,
            deleted_r2_objects=r2_result.deleted_objects,
            failed_r2_grids=r2_result.failed_grids,
        )
        log.info(
            "retention.mrms.batch",
            cutoff=cutoff.isoformat(),
            deleted_files=batch_result.deleted_files,
            deleted_zarrs=batch_result.deleted_zarrs,
            failed_zarrs=batch_result.failed_zarrs,
            deleted_r2_objects=batch_result.deleted_r2_objects,
            failed_r2_grids=batch_result.failed_r2_grids,
        )
        total = total.merged_with(batch_result)

        # If the batch came back short, there's nothing left to prune
        # at this cutoff — break out instead of issuing one more empty
        # SELECT to confirm. Saves a round trip every tick.
        if len(keys) < batch_size:
            break

    log.info(
        "retention.mrms.tick",
        retention_hours=retention_hours,
        deleted_files=total.deleted_files,
        deleted_zarrs=total.deleted_zarrs,
        failed_zarrs=total.failed_zarrs,
        deleted_r2_objects=total.deleted_r2_objects,
        failed_r2_grids=total.failed_r2_grids,
    )
    return total


async def prune_expired_alerts_once(
    *,
    db: Database,
    retention_days: int,
    now: datetime | None = None,
) -> PruneResult:
    """Delete ``nws_alerts`` rows whose ``expires`` is older than the cutoff.

    Alerts with ``expires IS NULL`` are kept indefinitely — those are
    "no explicit expiry" alerts where the issuing office hasn't set one,
    and we don't want to drop them silently.
    """
    if retention_days <= 0:
        raise ValueError(f"retention_days must be positive, got {retention_days}")

    cutoff = (now or datetime.now(UTC)) - timedelta(days=retention_days)
    async with db.sessionmaker() as session:
        stmt = (
            delete(NwsAlertRow)
            .where(NwsAlertRow.expires.is_not(None))
            .where(NwsAlertRow.expires < cutoff)
        )
        result = await session.execute(stmt)
        await session.commit()
        deleted = int(result.rowcount or 0)  # type: ignore[attr-defined]

    log.info(
        "retention.alerts.tick",
        retention_days=retention_days,
        cutoff=cutoff.isoformat(),
        deleted=deleted,
    )
    return PruneResult(deleted_alerts=deleted)


@dataclass(frozen=True, slots=True)
class _FsResult:
    deleted: int
    failed: int


@dataclass(frozen=True, slots=True)
class _R2Result:
    deleted_objects: int
    failed_grids: int


async def _delete_r2_grids(
    r2_client: R2Client | None,
    *,
    keys: Sequence[str],
) -> _R2Result:
    """Best-effort delete of every R2 tile under each pruned file_key.

    When ``r2_client is None`` (R2 disabled in local dev / tests),
    this is a no-op returning all zeros. Failures are logged and
    counted but never raised — orphan R2 objects are recoverable via
    the bucket's lifecycle rule, but a sweeper that aborts on a
    transient 503 from Cloudflare would leave a long tail of
    un-pruned grids.
    """
    if r2_client is None or not keys:
        return _R2Result(deleted_objects=0, failed_grids=0)

    deleted_objects = 0
    failed_grids = 0
    for file_key in keys:
        try:
            removed = await r2_client.delete_grid(file_key=file_key)
        except Exception as exc:
            failed_grids += 1
            log.warning(
                "retention.r2.delete_failed",
                file_key=file_key,
                error=str(exc),
            )
            continue
        deleted_objects += removed
    return _R2Result(deleted_objects=deleted_objects, failed_grids=failed_grids)


async def _select_expired_file_keys(
    session: AsyncSession,
    *,
    cutoff: datetime,
    limit: int,
) -> list[str]:
    stmt = (
        select(MrmsFileRow.key)
        .where(MrmsFileRow.valid_at < cutoff)
        .order_by(MrmsFileRow.valid_at.asc())
        .limit(limit)
    )
    result = await session.execute(stmt)
    return [row[0] for row in result.all()]


async def _collect_zarr_paths_for_keys(
    session: AsyncSession,
    *,
    keys: Sequence[str],
) -> list[str]:
    """Return every Zarr URI referenced by the given source file keys.

    Grids are 1:1 with files; nowcasts are 1:N. We pull both so a single
    rm-rf pass cleans up the whole derivative chain before the DB cascade
    drops the rows.
    """
    if not keys:
        return []

    grid_stmt = select(MrmsGridRow.zarr_uri).where(MrmsGridRow.file_key.in_(keys))
    nowcast_stmt = select(NowcastRow.zarr_uri).where(NowcastRow.source_file_key.in_(keys))

    grid_paths = [row[0] for row in (await session.execute(grid_stmt)).all()]
    nowcast_paths = [row[0] for row in (await session.execute(nowcast_stmt)).all()]
    # Preserve order (observations before nowcasts) and dedupe — the
    # filesystem doesn't care, but the log line reads better grouped.
    seen: set[str] = set()
    ordered: list[str] = []
    for path in (*grid_paths, *nowcast_paths):
        if path in seen:
            continue
        seen.add(path)
        ordered.append(path)
    return ordered


def _remove_zarr_paths(paths: Sequence[str]) -> _FsResult:
    """Best-effort ``rm -rf`` for every Zarr path. Never raises.

    A missing path counts as a successful delete (the row will still be
    cleared from the catalog) — happens when the volume was wiped on a
    redeploy but the DB persisted. Real failures (permission denied,
    I/O error) are logged and counted as ``failed`` so the prune tick
    can surface them in metrics without aborting.
    """
    deleted = 0
    failed = 0
    for raw_path in paths:
        path = Path(raw_path)
        if not path.exists():
            deleted += 1
            continue
        try:
            shutil.rmtree(path)
            deleted += 1
        except OSError as exc:
            failed += 1
            log.warning("retention.zarr.remove_failed", path=str(path), error=str(exc))
    return _FsResult(deleted=deleted, failed=failed)


async def _delete_file_rows(session: AsyncSession, *, keys: Sequence[str]) -> int:
    if not keys:
        return 0
    stmt = delete(MrmsFileRow).where(MrmsFileRow.key.in_(keys))
    result = await session.execute(stmt)
    return int(result.rowcount or 0)  # type: ignore[attr-defined]


__all__ = [
    "DEFAULT_BATCH_SIZE",
    "PruneResult",
    "prune_expired_alerts_once",
    "prune_old_mrms_once",
]
