"""NATS-backed implementations of :mod:`aeroza.stream.publisher` and
:mod:`aeroza.stream.subscriber`.

Subject taxonomy
----------------
- ``aeroza.alerts.nws.new`` — one message per newly-observed NWS alert.
- ``aeroza.mrms.files.new`` — one message per newly-discovered MRMS object
  on AWS Open Data.

Future work will add severity / region / product facets so subscribers
can filter without consuming the full firehose; we keep the surface
narrow for v1.

Payload
-------
- Alert messages: ``Alert.model_dump_json(by_alias=True)`` bytes — the
  pydantic model is the source of truth for the wire shape.
- MRMS file messages: hand-rolled JSON because :class:`MrmsFile` is a
  frozen dataclass, not pydantic. Field names are camelCased on the wire
  to match the alert convention; ``valid_at`` is ISO-8601.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, Final, Protocol

import structlog
from pydantic import ValidationError

from aeroza.ingest.mrms import MrmsFile
from aeroza.ingest.nws_alerts import Alert

log = structlog.get_logger(__name__)

NWS_NEW_ALERT_SUBJECT: Final[str] = "aeroza.alerts.nws.new"
MRMS_NEW_FILE_SUBJECT: Final[str] = "aeroza.mrms.files.new"


class NatsPublisher(Protocol):
    """Narrow surface :class:`NatsAlertPublisher` needs from a NATS client."""

    async def publish(self, subject: str, payload: bytes) -> None:  # pragma: no cover - interface
        ...


class NatsSubscription(Protocol):
    """A handle to a single NATS subscription."""

    @property
    def messages(self) -> AsyncIterator[Any]:  # pragma: no cover - interface
        ...

    async def unsubscribe(self) -> None:  # pragma: no cover - interface
        ...


class NatsSubscriberClient(Protocol):
    """Narrow surface :class:`NatsAlertSubscriber` needs from a NATS client."""

    async def subscribe(self, subject: str) -> NatsSubscription:  # pragma: no cover - interface
        ...


class NatsClient(NatsPublisher, NatsSubscriberClient, Protocol):
    """Combined publish + subscribe surface — what :func:`nats_connection` yields."""


class NatsAlertPublisher:
    """Publishes one NATS message per newly-observed alert."""

    def __init__(
        self,
        client: NatsPublisher,
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


class NatsMrmsFilePublisher:
    """Publishes one NATS message per newly-observed MRMS file."""

    def __init__(
        self,
        client: NatsPublisher,
        *,
        subject: str = MRMS_NEW_FILE_SUBJECT,
    ) -> None:
        self._client = client
        self._subject = subject

    @property
    def subject(self) -> str:
        return self._subject

    async def publish_new_file(self, file: MrmsFile) -> None:
        payload = _encode_mrms_file(file)
        await self._client.publish(self._subject, payload)
        log.debug(
            "nats.mrms.publish",
            subject=self._subject,
            key=file.key,
            bytes=len(payload),
        )


def _encode_mrms_file(file: MrmsFile) -> bytes:
    return json.dumps(
        {
            "key": file.key,
            "product": file.product,
            "level": file.level,
            "validAt": file.valid_at.isoformat(),
            "sizeBytes": file.size_bytes,
            "etag": file.etag,
        }
    ).encode("utf-8")


class NatsAlertSubscriber:
    """Yields alerts arriving on a NATS subject as they are published.

    Each call to :meth:`subscribe_new_alerts` opens a fresh subscription;
    NATS itself handles fan-out across multiple consumers, so per-request
    subscriptions are the natural shape for an SSE endpoint where each
    HTTP client wants its own independent feed.

    Malformed payloads are logged and skipped — one bad publisher must
    not knock the whole feed offline.
    """

    def __init__(
        self,
        client: NatsSubscriberClient,
        *,
        subject: str = NWS_NEW_ALERT_SUBJECT,
    ) -> None:
        self._client = client
        self._subject = subject

    @property
    def subject(self) -> str:
        return self._subject

    async def subscribe_new_alerts(self) -> AsyncIterator[Alert]:
        sub = await self._client.subscribe(self._subject)
        log.debug("nats.alerts.subscribe", subject=self._subject)
        try:
            async for msg in sub.messages:
                data: bytes = getattr(msg, "data", b"")
                try:
                    yield Alert.model_validate_json(data)
                except ValidationError as exc:
                    log.warning(
                        "nats.alerts.subscribe.bad_payload",
                        subject=self._subject,
                        error=str(exc),
                    )
        finally:
            await sub.unsubscribe()
            log.debug("nats.alerts.unsubscribe", subject=self._subject)


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
