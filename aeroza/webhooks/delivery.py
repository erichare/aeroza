"""HTTP delivery for a single webhook event.

Signs the payload with the subscription's HMAC key, POSTs to its URL,
retries with exponential backoff on transient failures, and records
one ``webhook_deliveries`` row per attempt. The orchestrator (slice 3)
calls :func:`deliver_payload` once per (subscription, event); this
module owns the delivery semantics and retry policy.

Retry policy
------------
- Up to :data:`MAX_ATTEMPTS` total attempts (one initial + retries).
- Exponential backoff between attempts:
  ``base * 2 ** (attempt - 1)``.
- 2xx responses → success (write one ``status='ok'`` row, return).
- 4xx responses → terminal failure: do not retry. The endpoint
  rejected the payload; retrying won't help. One ``status='failed'``
  row, return.
- 5xx / network errors → transient failure: write one
  ``status='retrying'`` row and try again until attempts exhausted.
  The final attempt (whether 5xx or network error) is recorded as
  ``status='failed'``.

Pure-ish: the only side effects are the HTTP call and the DB writes.
The orchestrator owns transactions; this function flushes
delivery rows immediately so a crash mid-retry-loop doesn't lose the
audit trail.
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from dataclasses import dataclass
from typing import Any, Final

import httpx
import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from aeroza.webhooks.delivery_models import WebhookDeliveryRow
from aeroza.webhooks.signing import sign_payload

log = structlog.get_logger(__name__)

MAX_ATTEMPTS: Final[int] = 4
INITIAL_BACKOFF_S: Final[float] = 1.0
DEFAULT_TIMEOUT_S: Final[float] = 5.0
RESPONSE_BODY_PREVIEW_BYTES: Final[int] = 1024


@dataclass(frozen=True, slots=True)
class DeliveryRequest:
    """Inputs for one logical delivery — orchestrator builds, dispatcher delivers."""

    subscription_id: uuid.UUID
    rule_id: uuid.UUID | None
    url: str
    secret: str
    event_type: str
    payload: dict[str, Any]


@dataclass(frozen=True, slots=True)
class DeliveryOutcome:
    """The result the orchestrator inspects to drive the circuit breaker.

    ``terminal_failure=True`` means the destination either rejected
    the payload (4xx) or exhausted retries (sustained 5xx/network).
    The orchestrator counts these toward the auto-disable threshold.
    """

    delivered: bool
    attempts: int
    last_status: int | None
    last_error: str | None
    terminal_failure: bool


async def deliver_payload(
    session: AsyncSession,
    *,
    request: DeliveryRequest,
    http_client: httpx.AsyncClient,
    max_attempts: int = MAX_ATTEMPTS,
    initial_backoff_s: float = INITIAL_BACKOFF_S,
    sleep: Any = None,
) -> DeliveryOutcome:
    """Deliver ``request.payload`` to ``request.url`` with retries.

    Each attempt writes a ``webhook_deliveries`` row and **commits**
    so the audit trail is durable even if the worker crashes
    mid-loop. The session is committed per row; orchestrator passes
    a fresh session per delivery so this commits don't tangle with
    other work.

    ``sleep`` is the async sleep callable used between retries — tests
    inject a no-op to make assertions fast. Defaults to
    :func:`asyncio.sleep`.
    """
    sleep_fn = sleep if sleep is not None else asyncio.sleep
    payload_bytes = json.dumps(request.payload, separators=(",", ":")).encode("utf-8")

    last_status: int | None = None
    last_error: str | None = None

    for attempt in range(1, max_attempts + 1):
        signed = sign_payload(payload=payload_bytes, secret=request.secret)
        started = time.perf_counter()
        try:
            response = await http_client.post(
                request.url,
                content=payload_bytes,
                headers={
                    "Content-Type": "application/json",
                    **signed.as_dict(),
                },
                timeout=DEFAULT_TIMEOUT_S,
            )
        except (httpx.HTTPError, OSError) as exc:
            duration_ms = int((time.perf_counter() - started) * 1000)
            last_error = f"{type(exc).__name__}: {exc}"
            last_status = None
            is_final = attempt == max_attempts
            await _record_attempt(
                session,
                request,
                attempt=attempt,
                status="failed" if is_final else "retrying",
                response_status=None,
                response_body_preview=None,
                error_reason=last_error,
                duration_ms=duration_ms,
            )
            if is_final:
                log.warning(
                    "webhooks.delivery.exhausted",
                    subscription_id=str(request.subscription_id),
                    attempts=attempt,
                    error=last_error,
                )
                return DeliveryOutcome(
                    delivered=False,
                    attempts=attempt,
                    last_status=None,
                    last_error=last_error,
                    terminal_failure=True,
                )
            await sleep_fn(initial_backoff_s * (2 ** (attempt - 1)))
            continue

        duration_ms = int((time.perf_counter() - started) * 1000)
        last_status = response.status_code
        body_preview = response.text[:RESPONSE_BODY_PREVIEW_BYTES]

        if 200 <= response.status_code < 300:
            await _record_attempt(
                session,
                request,
                attempt=attempt,
                status="ok",
                response_status=response.status_code,
                response_body_preview=body_preview,
                error_reason=None,
                duration_ms=duration_ms,
            )
            log.info(
                "webhooks.delivery.ok",
                subscription_id=str(request.subscription_id),
                attempts=attempt,
                response_status=response.status_code,
            )
            return DeliveryOutcome(
                delivered=True,
                attempts=attempt,
                last_status=response.status_code,
                last_error=None,
                terminal_failure=False,
            )

        # 4xx: don't retry — the destination has rejected the payload.
        if 400 <= response.status_code < 500:
            last_error = f"4xx response: {response.status_code} {response.reason_phrase}"
            await _record_attempt(
                session,
                request,
                attempt=attempt,
                status="failed",
                response_status=response.status_code,
                response_body_preview=body_preview,
                error_reason=last_error,
                duration_ms=duration_ms,
            )
            log.warning(
                "webhooks.delivery.client_error",
                subscription_id=str(request.subscription_id),
                response_status=response.status_code,
            )
            return DeliveryOutcome(
                delivered=False,
                attempts=attempt,
                last_status=response.status_code,
                last_error=last_error,
                terminal_failure=True,
            )

        # 5xx or anything else (including 1xx/3xx, which we treat as
        # server-side weirdness): retry.
        last_error = f"server error: {response.status_code} {response.reason_phrase}"
        is_final = attempt == max_attempts
        await _record_attempt(
            session,
            request,
            attempt=attempt,
            status="failed" if is_final else "retrying",
            response_status=response.status_code,
            response_body_preview=body_preview,
            error_reason=last_error,
            duration_ms=duration_ms,
        )
        if is_final:
            log.warning(
                "webhooks.delivery.exhausted",
                subscription_id=str(request.subscription_id),
                attempts=attempt,
                response_status=response.status_code,
            )
            return DeliveryOutcome(
                delivered=False,
                attempts=attempt,
                last_status=response.status_code,
                last_error=last_error,
                terminal_failure=True,
            )
        await sleep_fn(initial_backoff_s * (2 ** (attempt - 1)))

    # Unreachable — the loop returns inside both branches above.
    return DeliveryOutcome(  # pragma: no cover - defensive
        delivered=False,
        attempts=max_attempts,
        last_status=last_status,
        last_error=last_error,
        terminal_failure=True,
    )


async def _record_attempt(
    session: AsyncSession,
    request: DeliveryRequest,
    *,
    attempt: int,
    status: str,
    response_status: int | None,
    response_body_preview: str | None,
    error_reason: str | None,
    duration_ms: int | None,
) -> None:
    """Persist one ``webhook_deliveries`` row and commit immediately."""
    row = WebhookDeliveryRow(
        subscription_id=request.subscription_id,
        rule_id=request.rule_id,
        event_type=request.event_type,
        payload=request.payload,
        status=status,
        attempt=attempt,
        response_status=response_status,
        response_body_preview=response_body_preview,
        error_reason=error_reason,
        duration_ms=duration_ms,
    )
    session.add(row)
    await session.commit()


__all__ = [
    "DEFAULT_TIMEOUT_S",
    "INITIAL_BACKOFF_S",
    "MAX_ATTEMPTS",
    "RESPONSE_BODY_PREVIEW_BYTES",
    "DeliveryOutcome",
    "DeliveryRequest",
    "deliver_payload",
]
