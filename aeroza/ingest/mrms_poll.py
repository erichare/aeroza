"""End-to-end orchestration for the MRMS catalog: list → upsert → publish.

Mirrors :mod:`aeroza.ingest.poll`. The scheduler drives this function on
an interval; the loop logic stays out of here so the orchestrator is
trivially testable: pass an :class:`InMemoryMrmsFilePublisher` and a stub
fetcher, then assert which keys ended up in the publisher's queue.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

import structlog

from aeroza.ingest.mrms import MrmsFile
from aeroza.ingest.mrms_store import MrmsUpsertResult, upsert_mrms_files
from aeroza.shared.db import Database
from aeroza.stream.publisher import MrmsFilePublisher

log = structlog.get_logger(__name__)

MrmsFetcher = Callable[[], Awaitable[tuple[MrmsFile, ...]]]


async def poll_mrms_files_once(
    *,
    db: Database,
    publisher: MrmsFilePublisher,
    fetcher: MrmsFetcher,
) -> MrmsUpsertResult:
    """One iteration of the MRMS catalog pipeline.

    1. fetch the configured slice of files from S3 (callers bind product /
       level / time-window into ``fetcher`` ahead of time);
    2. upsert in a committed session;
    3. publish a "new file" event for each newly-inserted key. Updates do
       not produce events — the consumer-facing semantic is "first time
       we've seen this S3 key".

    Publisher errors are logged but do not roll back the upsert; the
    catalog is durably persisted, and a future replay job can catch up
    on missed events.
    """
    files = await fetcher()
    async with db.sessionmaker() as session:
        result = await upsert_mrms_files(session, files)
        await session.commit()

    if not result.inserted_keys:
        log.debug("poll.mrms.no_new", fetched=len(files), updated=result.updated)
        return result

    inserted = set(result.inserted_keys)
    by_key = {f.key: f for f in files}
    for key in result.inserted_keys:
        file = by_key.get(key)
        if file is None:
            # Defensive: upsert returned a key we didn't fetch — should not happen.
            log.warning("poll.mrms.missing_file_for_key", key=key)
            continue
        try:
            await publisher.publish_new_file(file)
        except Exception as exc:
            log.exception("poll.mrms.publish_failed", key=key, error=str(exc))
    log.info(
        "poll.mrms.tick",
        fetched=len(files),
        inserted=len(inserted),
        updated=result.updated,
    )
    return result
