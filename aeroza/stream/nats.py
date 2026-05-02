"""NATS-backed implementations of :mod:`aeroza.stream.publisher` and
:mod:`aeroza.stream.subscriber`.

Subject taxonomy
----------------
- ``aeroza.alerts.nws.new`` — one message per newly-observed NWS alert.
- ``aeroza.mrms.files.new`` — one message per newly-discovered MRMS object
  on AWS Open Data.
- ``aeroza.mrms.grids.new`` — one message per newly-materialised Zarr grid.

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
- MRMS grid messages: hand-rolled JSON for the same reason. Camel-cased
  field names match the ``/v1/mrms/grids`` wire shape.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Final, Protocol

import structlog
from pydantic import ValidationError

from aeroza.ingest.mrms import MrmsFile
from aeroza.ingest.mrms_zarr import MrmsGridLocator
from aeroza.ingest.nws_alerts import Alert
from aeroza.nowcast.models import NowcastRow

log = structlog.get_logger(__name__)

NWS_NEW_ALERT_SUBJECT: Final[str] = "aeroza.alerts.nws.new"
MRMS_NEW_FILE_SUBJECT: Final[str] = "aeroza.mrms.files.new"
MRMS_NEW_GRID_SUBJECT: Final[str] = "aeroza.mrms.grids.new"
NOWCAST_NEW_GRID_SUBJECT: Final[str] = "aeroza.nowcast.grids.new"


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


def _decode_mrms_file(data: bytes) -> MrmsFile:
    """Reverse of :func:`_encode_mrms_file`. Raises ``ValueError`` on
    malformed input — callers turn that into a "bad payload, skipping"
    log entry rather than tearing the consumer down.
    """
    obj = json.loads(data)
    return MrmsFile(
        key=obj["key"],
        product=obj["product"],
        level=obj["level"],
        valid_at=datetime.fromisoformat(obj["validAt"]),
        size_bytes=int(obj.get("sizeBytes", 0)),
        etag=obj.get("etag"),
    )


class NatsMrmsGridPublisher:
    """Publishes one NATS message per newly-materialised MRMS grid."""

    def __init__(
        self,
        client: NatsPublisher,
        *,
        subject: str = MRMS_NEW_GRID_SUBJECT,
    ) -> None:
        self._client = client
        self._subject = subject

    @property
    def subject(self) -> str:
        return self._subject

    async def publish_new_grid(self, locator: MrmsGridLocator) -> None:
        payload = _encode_mrms_grid(locator)
        await self._client.publish(self._subject, payload)
        log.debug(
            "nats.mrms_grid.publish",
            subject=self._subject,
            file_key=locator.file_key,
            bytes=len(payload),
        )


def _encode_mrms_grid(locator: MrmsGridLocator) -> bytes:
    return json.dumps(
        {
            "fileKey": locator.file_key,
            "zarrUri": locator.zarr_uri,
            "variable": locator.variable,
            "dims": list(locator.dims),
            "shape": list(locator.shape),
            "dtype": locator.dtype,
            "nbytes": locator.nbytes,
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


class NatsMrmsFileSubscriber:
    """Yields MRMS file events arriving on a NATS subject as they are published.

    Mirror of :class:`NatsAlertSubscriber`. Each call opens an
    independent subscription; malformed payloads are logged and skipped
    so one bad publisher cannot tear the materialise worker down.
    """

    def __init__(
        self,
        client: NatsSubscriberClient,
        *,
        subject: str = MRMS_NEW_FILE_SUBJECT,
    ) -> None:
        self._client = client
        self._subject = subject

    @property
    def subject(self) -> str:
        return self._subject

    async def subscribe_new_files(self) -> AsyncIterator[MrmsFile]:
        sub = await self._client.subscribe(self._subject)
        log.debug("nats.mrms.subscribe", subject=self._subject)
        try:
            async for msg in sub.messages:
                data: bytes = getattr(msg, "data", b"")
                try:
                    yield _decode_mrms_file(data)
                except (ValueError, KeyError) as exc:
                    log.warning(
                        "nats.mrms.subscribe.bad_payload",
                        subject=self._subject,
                        error=str(exc),
                    )
        finally:
            await sub.unsubscribe()
            log.debug("nats.mrms.unsubscribe", subject=self._subject)


def _decode_mrms_grid(data: bytes) -> MrmsGridLocator:
    """Reverse of :func:`_encode_mrms_grid`. Raises ``ValueError`` /
    ``KeyError`` on malformed input; the subscriber turns either into
    a "bad payload, skipping" log entry."""
    obj = json.loads(data)
    return MrmsGridLocator(
        file_key=obj["fileKey"],
        zarr_uri=obj["zarrUri"],
        variable=obj["variable"],
        dims=tuple(str(d) for d in obj.get("dims", ())),
        shape=tuple(int(s) for s in obj.get("shape", ())),
        dtype=obj["dtype"],
        nbytes=int(obj.get("nbytes", 0)),
    )


class NatsMrmsGridSubscriber:
    """Yields MRMS grid locator events arriving on a NATS subject.

    Mirror of :class:`NatsMrmsFileSubscriber`; same per-call independent
    subscription + log-and-skip semantics on bad payloads.
    """

    def __init__(
        self,
        client: NatsSubscriberClient,
        *,
        subject: str = MRMS_NEW_GRID_SUBJECT,
    ) -> None:
        self._client = client
        self._subject = subject

    @property
    def subject(self) -> str:
        return self._subject

    async def subscribe_new_grids(self) -> AsyncIterator[MrmsGridLocator]:
        sub = await self._client.subscribe(self._subject)
        log.debug("nats.mrms_grid.subscribe", subject=self._subject)
        try:
            async for msg in sub.messages:
                data: bytes = getattr(msg, "data", b"")
                try:
                    yield _decode_mrms_grid(data)
                except (ValueError, KeyError) as exc:
                    log.warning(
                        "nats.mrms_grid.subscribe.bad_payload",
                        subject=self._subject,
                        error=str(exc),
                    )
        finally:
            await sub.unsubscribe()
            log.debug("nats.mrms_grid.unsubscribe", subject=self._subject)


def _encode_nowcast_row(row: NowcastRow) -> bytes:
    """Wire shape for ``aeroza.nowcast.grids.new`` events.

    Camel-cased keys mirror the ``/v1/nowcasts`` HTTP response so a
    consumer that already parses the catalog list can reuse its types.
    Lists for ``dims``/``shape`` are eagerly materialised — the JSONB
    column they came from is opaque.
    """
    import json

    dims = row.dims_json if isinstance(row.dims_json, list) else json.loads(row.dims_json)
    shape = row.shape_json if isinstance(row.shape_json, list) else json.loads(row.shape_json)
    return json.dumps(
        {
            "id": str(row.id),
            "sourceFileKey": row.source_file_key,
            "product": row.product,
            "level": row.level,
            "algorithm": row.algorithm,
            "forecastHorizonMinutes": row.forecast_horizon_minutes,
            "validAt": row.valid_at.isoformat(),
            "zarrUri": row.zarr_uri,
            "variable": row.variable,
            "dims": list(dims),
            "shape": list(shape),
            "dtype": row.dtype,
            "nbytes": row.nbytes,
            "generatedAt": row.generated_at.isoformat() if row.generated_at else None,
        }
    ).encode("utf-8")


def _decode_nowcast_row_envelope(data: bytes) -> dict[str, Any]:
    """Parse the JSON wire payload back into a plain dict. We don't
    rebuild a :class:`NowcastRow` because the SQLAlchemy ORM can't be
    instantiated from camelCase keys, and downstream consumers (the
    webhook dispatcher) don't need the ORM shape — just the data."""
    obj: dict[str, Any] = json.loads(data)
    return obj


class NatsNowcastGridPublisher:
    """Publishes one NATS message per newly-persisted nowcast row."""

    def __init__(
        self,
        client: NatsPublisher,
        *,
        subject: str = NOWCAST_NEW_GRID_SUBJECT,
    ) -> None:
        self._client = client
        self._subject = subject

    @property
    def subject(self) -> str:
        return self._subject

    async def publish_new_nowcast(self, row: NowcastRow) -> None:
        payload = _encode_nowcast_row(row)
        await self._client.publish(self._subject, payload)
        log.debug(
            "nats.nowcast.publish",
            subject=self._subject,
            id=str(row.id),
            algorithm=row.algorithm,
            horizon_minutes=row.forecast_horizon_minutes,
            bytes=len(payload),
        )


class NatsNowcastGridSubscriber:
    """Yields nowcast envelopes (decoded JSON dicts) from a NATS subject."""

    def __init__(
        self,
        client: NatsSubscriberClient,
        *,
        subject: str = NOWCAST_NEW_GRID_SUBJECT,
    ) -> None:
        self._client = client
        self._subject = subject

    @property
    def subject(self) -> str:
        return self._subject

    async def subscribe_new_nowcasts(self) -> AsyncIterator[dict[str, Any]]:
        sub = await self._client.subscribe(self._subject)
        log.debug("nats.nowcast.subscribe", subject=self._subject)
        try:
            async for msg in sub.messages:
                data: bytes = getattr(msg, "data", b"")
                try:
                    yield _decode_nowcast_row_envelope(data)
                except (ValueError, KeyError) as exc:
                    log.warning(
                        "nats.nowcast.subscribe.bad_payload",
                        subject=self._subject,
                        error=str(exc),
                    )
        finally:
            await sub.unsubscribe()
            log.debug("nats.nowcast.unsubscribe", subject=self._subject)


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
