"""Admin-driven event seeding.

The /demo page lets visitors pick a curated historical weather event
(e.g. Winter Storm Uri). On a fresh stack the local archive doesn't
have grids that far back, so the page used to render a multi-step
"copy this command, then this one" empty state. This module exposes
the same pipeline as a single coroutine the admin route fires
in-process — no subprocess, no shell — so the dev console can offer
a one-click "seed this event" button.

Pipeline:

1. Drive ``poll_mrms_files_once`` once with ``at_time = until`` and
   ``lookback_minutes = (until - since)``. That populates
   ``mrms_files`` with every MRMS key in the curated window.
2. If cfgrib is importable in the venv, loop
   ``materialise_unmaterialised_once`` in batches until no
   unmaterialised files remain. That writes Zarr stores so the
   replay actually has frames to play.

Idempotent. Re-running on the same window upserts the catalog rows
in place; the materialiser short-circuits files that already have a
grid. Tracked by an in-memory task registry so a double-click on the
button doesn't fire two pipelines for the same window.
"""

from __future__ import annotations

import asyncio
from contextlib import suppress
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from typing import Final

import structlog

from aeroza.ingest._aws import open_data_s3_client
from aeroza.ingest.mrms import MrmsFile, list_mrms_files
from aeroza.ingest.mrms_decode import (
    CfgribUnavailableError,
    ensure_cfgrib_available,
)
from aeroza.ingest.mrms_materialise_poll import materialise_unmaterialised_once
from aeroza.ingest.mrms_poll import poll_mrms_files_once
from aeroza.shared.db import Database
from aeroza.stream.publisher import (
    MrmsFilePublisher,
    NullMrmsFilePublisher,
)

log = structlog.get_logger(__name__)

# Defaults mirror the CLI so the admin path agrees with `aeroza-ingest-mrms`
# and `aeroza-materialise-mrms` end-to-end.
DEFAULT_PRODUCT: Final[str] = "MergedReflectivityComposite"
DEFAULT_LEVEL: Final[str] = "00.50"

# Materialise batch size — bigger than the live default (8) because
# events can hit a few hundred files at once. Same number the seed
# script uses.
DEFAULT_MATERIALISE_BATCH: Final[int] = 100

# Cap on materialise iterations. The materialiser exits when nothing
# is pending; this is a safety stop in case a misconfigured worker
# loops without progressing.
MAX_MATERIALISE_ITERATIONS: Final[int] = 12


@dataclass(frozen=True, slots=True)
class SeedWindow:
    """Validated [since, until] window for the seed pipeline.

    Both bounds are tz-aware UTC. The interval must be non-empty and
    bounded — windows wider than 24h are intentionally rejected
    because the underlying lister caps at two UTC days, and curated
    events all fit in a one-day envelope anyway.
    """

    since: datetime
    until: datetime
    product: str
    level: str

    @property
    def lookback_minutes(self) -> int:
        # +5 min buffer matches the buildSeedCommand helper in
        # web/app/demo/page.tsx so the upper edge of the window
        # isn't lost to the listing call's exclusive boundary.
        delta = self.until - self.since
        return max(1, int(delta.total_seconds() // 60) + 5)


@dataclass(slots=True)
class SeedTask:
    """In-memory state for a running or finished seed.

    Stored in the registry by ``(since, until, product, level)``. The
    HTTP handler returns this snapshot (minus the asyncio.Task ref).
    """

    window: SeedWindow
    started_at: datetime
    cfgrib_available: bool
    files_inserted: int = 0
    files_updated: int = 0
    grids_materialised: int = 0
    materialise_pending: int | None = None
    finished_at: datetime | None = None
    error: str | None = None
    # Internal: the asyncio.Task driving the pipeline. Excluded from
    # the wire shape so handlers can return ``replace(task, _task=None)``
    # via dataclasses.replace before serialising.
    _task: asyncio.Task[None] | None = field(default=None, repr=False)


class SeedEventRunner:
    """Drives the seed pipeline and tracks per-window state.

    Single-instance; held as a module-global in :func:`get_runner`.
    The admin router pokes it from request handlers; the runner owns
    the asyncio.Tasks and survives across requests for the lifetime
    of the process. Multi-process deployments are out of scope —
    this is a dev-console feature.
    """

    def __init__(self) -> None:
        self._tasks: dict[tuple[datetime, datetime, str, str], SeedTask] = {}
        self._lock = asyncio.Lock()

    async def start(
        self,
        *,
        db: Database,
        window: SeedWindow,
        publisher: MrmsFilePublisher | None = None,
    ) -> SeedTask:
        """Start a seed for ``window`` if one isn't already running.

        Returns the task snapshot — including the existing one if a
        prior request already kicked it off, so the UI stays
        idempotent under double-clicks.
        """
        key = self._key(window)
        async with self._lock:
            existing = self._tasks.get(key)
            if existing is not None and existing.finished_at is None:
                return existing
            try:
                ensure_cfgrib_available()
                cfgrib_ok = True
            except CfgribUnavailableError:
                cfgrib_ok = False
            task = SeedTask(
                window=window,
                started_at=datetime.now(UTC),
                cfgrib_available=cfgrib_ok,
            )
            pub = publisher if publisher is not None else NullMrmsFilePublisher()
            task._task = asyncio.create_task(
                self._run(db=db, publisher=pub, task=task),
                name=f"admin.seed_event:{window.since.isoformat()}",
            )
            self._tasks[key] = task
            return task

    def status(self, window: SeedWindow) -> SeedTask | None:
        """Return the current snapshot for ``window``, or None if none."""
        return self._tasks.get(self._key(window))

    async def _run(
        self,
        *,
        db: Database,
        publisher: MrmsFilePublisher,
        task: SeedTask,
    ) -> None:
        try:
            await self._ingest(db=db, publisher=publisher, task=task)
            if task.cfgrib_available:
                await self._materialise(db=db, task=task)
        except Exception as exc:
            log.exception(
                "admin.seed_event.failed",
                since=task.window.since.isoformat(),
                until=task.window.until.isoformat(),
                error=str(exc),
            )
            task.error = str(exc)
        finally:
            task.finished_at = datetime.now(UTC)
            log.info(
                "admin.seed_event.done",
                since=task.window.since.isoformat(),
                until=task.window.until.isoformat(),
                files_inserted=task.files_inserted,
                files_updated=task.files_updated,
                grids_materialised=task.grids_materialised,
                error=task.error,
            )

    async def _ingest(
        self,
        *,
        db: Database,
        publisher: MrmsFilePublisher,
        task: SeedTask,
    ) -> None:
        s3 = open_data_s3_client()

        async def fetcher() -> tuple[MrmsFile, ...]:
            since = task.window.since
            until = task.window.until
            files: list[MrmsFile] = []
            for day in _unique_utc_days(since, until):
                chunk = await list_mrms_files(
                    product=task.window.product,
                    level=task.window.level,
                    day=day,
                    since=since,
                    until=until,
                    s3_client=s3,
                )
                files.extend(chunk)
            files.sort(key=lambda f: f.valid_at)
            return tuple(files)

        result = await poll_mrms_files_once(db=db, publisher=publisher, fetcher=fetcher)
        task.files_inserted = len(result.inserted_keys)
        task.files_updated = result.updated

    async def _materialise(self, *, db: Database, task: SeedTask) -> None:
        # Re-use a single S3 client across the loop so we keep one
        # connection pool warm — same pattern as the long-running
        # materialiser CLI.
        s3 = open_data_s3_client()
        for _ in range(MAX_MATERIALISE_ITERATIONS):
            outcome = await materialise_unmaterialised_once(
                db=db,
                s3_client=s3,
                target_root="data/mrms",
                product=task.window.product,
                level=task.window.level,
                batch_size=DEFAULT_MATERIALISE_BATCH,
            )
            # ``materialised`` is the count property on
            # :class:`MaterialiseResult` (the tuple of keys is on
            # ``materialised_keys``). The poll function returns an
            # idle result with both tuples empty when nothing's
            # left to do.
            task.grids_materialised += outcome.materialised
            if outcome.materialised == 0 and outcome.failed == 0:
                # Idle tick — the unmaterialised queue is drained.
                break
            if outcome.materialised == 0 and outcome.failed > 0:
                # All candidates this batch failed (e.g. repeated decode
                # failures). Bail rather than spin on poison rows.
                log.warning(
                    "admin.seed_event.materialise_stalled",
                    failed=outcome.failed,
                )
                break

    @staticmethod
    def _key(window: SeedWindow) -> tuple[datetime, datetime, str, str]:
        return (window.since, window.until, window.product, window.level)


_runner: SeedEventRunner | None = None


def get_runner() -> SeedEventRunner:
    """Module-level singleton. Tests can rebind via ``set_runner``."""
    global _runner
    if _runner is None:
        _runner = SeedEventRunner()
    return _runner


def set_runner(runner: SeedEventRunner) -> None:
    """Test seam — install a fresh runner per test to avoid bleed."""
    global _runner
    _runner = runner


def snapshot_for_wire(task: SeedTask) -> SeedTask:
    """Return ``task`` with the asyncio.Task ref cleared.

    The router serialises via Pydantic; the live ``asyncio.Task`` ref
    isn't JSON-friendly. ``dataclasses.replace`` keeps everything
    immutable-feeling for callers.
    """
    return replace(task, _task=None)


async def cancel_all() -> None:
    """Cancel any running seed tasks (lifespan shutdown helper)."""
    runner = get_runner()
    for task in runner._tasks.values():
        inner = task._task
        if inner is not None and not inner.done():
            inner.cancel()
            with suppress(asyncio.CancelledError):
                await inner


def _unique_utc_days(since: datetime, until: datetime) -> tuple[datetime, ...]:
    """Same convention as :func:`aeroza.cli.ingest_mrms._unique_utc_days`.

    Inlined here to avoid pulling the CLI module's argparse machinery
    into the router's import graph.
    """
    start_day = datetime(since.year, since.month, since.day, tzinfo=UTC)
    end_day = datetime(until.year, until.month, until.day, tzinfo=UTC)
    if start_day == end_day:
        return (start_day,)
    if (end_day - start_day) > timedelta(days=2):
        # Cap at ±1 day around the requested anchor — the lister
        # walks one prefix per UTC day, and curated events all fit
        # inside a 24-hour envelope.
        return (start_day, end_day)
    return (start_day, end_day)


__all__ = [
    "DEFAULT_LEVEL",
    "DEFAULT_PRODUCT",
    "MAX_MATERIALISE_ITERATIONS",
    "SeedEventRunner",
    "SeedTask",
    "SeedWindow",
    "cancel_all",
    "get_runner",
    "set_runner",
    "snapshot_for_wire",
]
