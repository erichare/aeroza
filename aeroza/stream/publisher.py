"""Publisher abstractions for alert events.

The :class:`AlertPublisher` Protocol decouples the ingest orchestrator from
any specific transport — production wires up a NATS-backed implementation
(landed in a follow-up slice), tests use :class:`InMemoryAlertPublisher`
to assert which alert ids were emitted, and :class:`NullAlertPublisher`
is the safe default for environments that haven't configured streaming
yet (e.g. local one-off backfills).
"""

from __future__ import annotations

from typing import Protocol

import structlog

from aeroza.ingest.nws_alerts import Alert

log = structlog.get_logger(__name__)


class AlertPublisher(Protocol):
    """Emits one event per newly-observed alert."""

    async def publish_new_alert(self, alert: Alert) -> None:  # pragma: no cover - interface
        ...


class NullAlertPublisher:
    """Drops every event. Useful as a default when streaming is disabled."""

    async def publish_new_alert(self, alert: Alert) -> None:
        log.debug("publisher.null.drop", alert_id=alert.id)


class InMemoryAlertPublisher:
    """Captures published alerts in a list — for tests only.

    The captured list is intentionally ordered: callers can assert ordering
    of inserts as they were observed.
    """

    def __init__(self) -> None:
        self._published: list[Alert] = []

    @property
    def published(self) -> tuple[Alert, ...]:
        return tuple(self._published)

    @property
    def published_ids(self) -> tuple[str, ...]:
        return tuple(alert.id for alert in self._published)

    async def publish_new_alert(self, alert: Alert) -> None:
        self._published.append(alert)

    def clear(self) -> None:
        self._published.clear()
