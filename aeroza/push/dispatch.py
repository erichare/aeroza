"""Warning → push dispatch.

When the alerts poller ingests a *new* NWS warning it calls
``publisher.publish_new_alert(alert)`` (see :func:`aeroza.ingest.poll`).
:class:`PushDispatchPublisher` decorates that publisher: for each qualifying
new warning it finds the registered devices whose saved location falls inside
the warning polygon (PostGIS ``ST_Intersects``) and sends each an APNs alert,
then forwards to the wrapped publisher. Wrapping (rather than editing the poll
core) keeps the fan-out composable and the poll logic untouched.

The push carries the lean ``aps.alert`` envelope plus the ``userInfo`` the iOS
Notification Service Extension reads (``alert_id``, ``aeroza_base_url``, the
device's ``lat``/``lng``) to hydrate the notification with a fresh reflectivity
sample at the saved point.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import structlog
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from aeroza.config import Settings
from aeroza.ingest.nws_alerts import Alert, Severity, severity_rank
from aeroza.ingest.nws_alerts_models import NWS_ALERTS_SRID, NwsAlertRow
from aeroza.push.apns import ApnsClient, ApnsSettings, PushSender
from aeroza.push.models import DeviceTokenRow
from aeroza.push.store import prune_devices
from aeroza.shared.db import Database
from aeroza.stream.publisher import AlertPublisher

log = structlog.get_logger(__name__)

# Warnings at or above this severity push. Severe + Extreme covers tornado,
# severe thunderstorm, and flash-flood warnings; watches/advisories
# (Moderate / Minor) stay quiet to avoid notification fatigue.
DEFAULT_MIN_SEVERITY: Severity = Severity.SEVERE


def should_dispatch(alert: Alert, *, min_severity: Severity = DEFAULT_MIN_SEVERITY) -> bool:
    """Whether a warning is severe enough to push."""
    return severity_rank(alert.severity) >= severity_rank(min_severity)


def build_payload(alert: Alert, device: DeviceTokenRow, *, base_url: str) -> dict[str, Any]:
    """Build the APNs payload for one device.

    ``mutable-content`` lets the Notification Service Extension hydrate the
    push; ``lat``/``lng``/``aeroza_base_url``/``alert_id`` are the ``userInfo``
    keys that extension reads. The user's API key is intentionally absent — we
    never store the plaintext key, and the hosted API serves the hydration
    sample anonymously.
    """
    body = alert.headline or alert.area_desc or "Tap for details."
    aps: dict[str, Any] = {
        "alert": {"title": alert.event, "body": body},
        "sound": "default",
        "mutable-content": 1,
        "interruption-level": "time-sensitive",
    }
    payload: dict[str, Any] = {
        "aps": aps,
        "alert_id": alert.id,
        "aeroza_base_url": base_url,
    }
    if device.location_lat is not None and device.location_lng is not None:
        payload["lat"] = device.location_lat
        payload["lng"] = device.location_lng
    return payload


async def select_devices_for_alert(
    session: AsyncSession, alert_id: str
) -> Sequence[DeviceTokenRow]:
    """Devices whose saved point falls inside the persisted alert's geometry."""
    point = func.ST_SetSRID(
        func.ST_MakePoint(DeviceTokenRow.location_lng, DeviceTokenRow.location_lat),
        NWS_ALERTS_SRID,
    )
    stmt = (
        select(DeviceTokenRow)
        .join(NwsAlertRow, NwsAlertRow.id == alert_id)
        .where(DeviceTokenRow.location_lat.is_not(None))
        .where(DeviceTokenRow.location_lng.is_not(None))
        .where(NwsAlertRow.geometry.is_not(None))
        .where(func.ST_Intersects(NwsAlertRow.geometry, point))
    )
    result = await session.execute(stmt)
    return result.scalars().all()


@dataclass(frozen=True, slots=True)
class DispatchOutcome:
    sent: int
    unregistered_tokens: tuple[str, ...]


async def dispatch_to_devices(
    *,
    sender: PushSender,
    devices: Sequence[DeviceTokenRow],
    alert: Alert,
    base_url: str,
) -> DispatchOutcome:
    """Send the warning to each device; collect tokens APNs reports as dead."""
    sent = 0
    unregistered: list[str] = []
    for device in devices:
        payload = build_payload(alert, device, base_url=base_url)
        result = await sender.send(
            device_token=device.token, environment=device.environment, payload=payload
        )
        if result.unregistered:
            unregistered.append(device.token)
        elif result.ok:
            sent += 1
    return DispatchOutcome(sent=sent, unregistered_tokens=tuple(unregistered))


class PushDispatchPublisher:
    """An :class:`AlertPublisher` decorator that fans new warnings out as pushes."""

    def __init__(
        self,
        *,
        inner: AlertPublisher,
        db: Database,
        sender: PushSender,
        base_url: str,
        min_severity: Severity = DEFAULT_MIN_SEVERITY,
    ) -> None:
        self._inner = inner
        self._db = db
        self._sender = sender
        self._base_url = base_url
        self._min_severity = min_severity

    async def publish_new_alert(self, alert: Alert) -> None:
        try:
            await self._dispatch(alert)
        except Exception:  # never let a push failure break ingest
            log.exception("push.dispatch.failed", alert_id=alert.id)
        await self._inner.publish_new_alert(alert)

    async def _dispatch(self, alert: Alert) -> None:
        if not should_dispatch(alert, min_severity=self._min_severity):
            return
        async with self._db.sessionmaker() as session:
            devices = await select_devices_for_alert(session, alert.id)
            if not devices:
                return
            outcome = await dispatch_to_devices(
                sender=self._sender, devices=devices, alert=alert, base_url=self._base_url
            )
            await prune_devices(session, outcome.unregistered_tokens)
        log.info(
            "push.dispatch.complete",
            alert_id=alert.id,
            event=alert.event,
            matched=len(devices),
            sent=outcome.sent,
            pruned=len(outcome.unregistered_tokens),
        )

    async def aclose(self) -> None:
        if isinstance(self._sender, ApnsClient):
            await self._sender.aclose()


def build_apns_sender(settings: Settings) -> ApnsClient | None:
    """Build an APNs sender from settings, or ``None`` when APNs isn't configured."""
    if not settings.apns_configured:
        return None
    return ApnsClient(
        ApnsSettings(
            key_id=settings.apns_key_id,
            team_id=settings.apns_team_id,
            private_key_pem=settings.apns_private_key,
            topic=settings.apns_topic,
            use_sandbox=settings.apns_use_sandbox,
        )
    )


def build_push_publisher(
    inner: AlertPublisher, *, db: Database, settings: Settings
) -> AlertPublisher:
    """Wrap ``inner`` with push dispatch when APNs is configured, else return it."""
    sender = build_apns_sender(settings)
    if sender is None:
        return inner
    return PushDispatchPublisher(
        inner=inner, db=db, sender=sender, base_url=settings.public_api_base_url
    )


async def aclose_publisher(publisher: AlertPublisher) -> None:
    """Close push resources if ``publisher`` is a :class:`PushDispatchPublisher`."""
    if isinstance(publisher, PushDispatchPublisher):
        await publisher.aclose()
