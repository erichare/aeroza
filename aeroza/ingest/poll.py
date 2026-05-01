"""End-to-end orchestration: fetch → upsert → publish.

The scheduler (landed in a follow-up slice) drives this function on an
interval. By keeping the loop logic out of here, the orchestrator stays
trivially testable: pass an ``InMemoryAlertPublisher`` and a respx-mocked
fetcher, and assert which ids ended up in the publisher's queue.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

import structlog

from aeroza.ingest.nws_alerts import Alert, fetch_active_alerts
from aeroza.ingest.nws_alerts_store import UpsertResult, upsert_alerts
from aeroza.shared.db import Database
from aeroza.stream.publisher import AlertPublisher

log = structlog.get_logger(__name__)

AlertFetcher = Callable[[], Awaitable[tuple[Alert, ...]]]


async def poll_nws_alerts_once(
    *,
    db: Database,
    publisher: AlertPublisher,
    fetcher: AlertFetcher = fetch_active_alerts,
) -> UpsertResult:
    """One iteration of the alerts pipeline.

    1. fetch active alerts;
    2. upsert in a transaction (committed on success);
    3. publish a "new alert" event for each row that was newly inserted —
       updates do not produce events (the consumer-facing event semantic is
       "first time we've seen this alert id").

    Publisher errors are logged but do not roll back the upsert; the data is
    already durably persisted, and the next tick will retry the publish via
    the same id-diff (or a future replay job will).
    """
    alerts = await fetcher()
    async with db.sessionmaker() as session:
        result = await upsert_alerts(session, alerts)
        await session.commit()

    if not result.inserted_ids:
        log.debug("poll.nws_alerts.no_new", fetched=len(alerts), updated=result.updated)
        return result

    inserted_set = set(result.inserted_ids)
    by_id = {alert.id: alert for alert in alerts}
    for alert_id in result.inserted_ids:
        alert = by_id.get(alert_id)
        if alert is None:
            # Defensive: upsert returned an id we didn't fetch — should not happen.
            log.warning("poll.nws_alerts.missing_alert_for_id", id=alert_id)
            continue
        try:
            await publisher.publish_new_alert(alert)
        except Exception as exc:
            log.exception("poll.nws_alerts.publish_failed", id=alert_id, error=str(exc))
    log.info(
        "poll.nws_alerts.tick",
        fetched=len(alerts),
        inserted=len(inserted_set),
        updated=result.updated,
    )
    return result
