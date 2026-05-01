"""NATS-backed implementation of :class:`AlertPublisher`.

Subject taxonomy
----------------
NWS alerts are published to a single subject per provenance:

    aeroza.alerts.nws.new

Future work will add severity / region facets to the subject hierarchy
(e.g. ``aeroza.alerts.nws.new.us.tx.severe``) so subscribers can filter
without consuming the full firehose. We keep the surface narrow for v1.

Payload
-------
Each message body is the alert serialised by ``Alert.model_dump_json()``.
The pydantic model is the source of truth for the wire shape — when fields
are added there, subscribers see them automatically.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Final, Protocol

import structlog

from aeroza.ingest.nws_alerts import Alert

log = structlog.get_logger(__name__)

NWS_NEW_ALERT_SUBJECT: Final[str] = "aeroza.alerts.nws.new"


class NatsClient(Protocol):
    """Minimal NATS client surface area :class:`NatsAlertPublisher` needs.

    Defined as a Protocol (rather than depending on ``nats.aio.client.Client``
    directly) so unit tests can substitute an in-memory stub without pulling
    the live driver into the import graph for the test runner.
    """

    async def publish(self, subject: str, payload: bytes) -> None:  # pragma: no cover - interface
        ...


class NatsAlertPublisher:
    """Publishes one NATS message per newly-observed alert."""

    def __init__(
        self,
        client: NatsClient,
        *,
        subject: str = NWS_NEW_ALERT_SUBJECT,
    ) -> None:
        self._client = client
        self._subject = subject

    @property
    def subject(self) -> str:
        return self._subject

    async def publish_new_alert(self, alert: Alert) -> None:
        payload = alert.model_dump_json(by_alias=True).encode("utf-8")
        await self._client.publish(self._subject, payload)
        log.debug("nats.alerts.publish", subject=self._subject, id=alert.id, bytes=len(payload))


@asynccontextmanager
async def nats_connection(servers: str | list[str]) -> AsyncIterator[NatsClient]:
    """Open a NATS connection and close it on exit.

    Imports ``nats`` lazily so importing :mod:`aeroza.stream.nats` (e.g. for
    its ``NatsAlertPublisher`` class in tests) does not pull the driver onto
    the import path.
    """
    import nats

    nc = await nats.connect(servers=servers)
    try:
        yield nc
    finally:
        await nc.close()
