"""Integration tests for the HTTP delivery primitive.

Exercises ``deliver_payload`` against an httpx ``MockTransport`` so we
can pin retry policy, the ok / 4xx / 5xx / network-error branches, and
the per-attempt webhook_deliveries log rows. The DB is real Postgres;
the HTTP destination is a programmable in-process stub.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from typing import Any

import httpx
import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from aeroza.shared.db import Database
from aeroza.webhooks.delivery import (
    DeliveryRequest,
    deliver_payload,
)
from aeroza.webhooks.delivery_models import WebhookDeliveryRow
from aeroza.webhooks.schemas import WebhookSubscriptionCreate
from aeroza.webhooks.signing import (
    SIGNATURE_HEADER,
    TIMESTAMP_HEADER,
    verify_signature,
)
from aeroza.webhooks.store import create_subscription

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
async def _truncate_after(integration_db: Database) -> Any:
    yield
    async with integration_db.sessionmaker() as session:
        await session.execute(text("TRUNCATE TABLE webhook_subscriptions CASCADE"))
        await session.commit()


async def _make_subscription(session: AsyncSession) -> tuple[uuid.UUID, str]:
    row = await create_subscription(
        session,
        WebhookSubscriptionCreate(
            url="https://hook.example.com/incoming",
            events=["aeroza.alerts.nws.new"],
            description=None,
        ),
        secret="deadbeef" * 8,  # deterministic — tests verify the sig
    )
    await session.commit()
    return row.id, row.secret


def _request(
    sub_id: uuid.UUID, secret: str, *, payload: dict[str, Any] | None = None
) -> DeliveryRequest:
    return DeliveryRequest(
        subscription_id=sub_id,
        rule_id=None,
        url="https://hook.example.com/incoming",
        secret=secret,
        event_type="aeroza.alerts.nws.new",
        payload=payload or {"event": "aeroza.alerts.nws.new", "deliveryId": "x", "data": {}},
    )


async def _row_count(integration_db: Database, sub_id: uuid.UUID) -> int:
    async with integration_db.sessionmaker() as session:
        result = await session.execute(
            select(WebhookDeliveryRow).where(WebhookDeliveryRow.subscription_id == sub_id)
        )
        return len(list(result.scalars().all()))


async def _no_sleep(_seconds: float) -> None:
    """Fast-forward through retry backoffs in tests."""


def _stub_transport(
    handler: Callable[[httpx.Request], httpx.Response],
) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


# ---------------------------------------------------------------------------
# Happy path


async def test_deliver_2xx_records_one_ok_row(
    db_session: AsyncSession, integration_db: Database
) -> None:
    sub_id, secret = await _make_subscription(db_session)
    received: list[httpx.Request] = []

    def handler(req: httpx.Request) -> httpx.Response:
        received.append(req)
        return httpx.Response(200, json={"received": True})

    async with _stub_transport(handler) as client, integration_db.sessionmaker() as fresh:
        outcome = await deliver_payload(
            fresh,
            request=_request(sub_id, secret),
            http_client=client,
            sleep=_no_sleep,
        )

    assert outcome.delivered is True
    assert outcome.attempts == 1
    assert outcome.terminal_failure is False
    assert outcome.last_status == 200
    assert await _row_count(integration_db, sub_id) == 1

    # Signature headers reach the destination and verify against the
    # subscription's secret.
    assert len(received) == 1
    sent = received[0]
    payload = sent.read()
    verify_signature(
        payload=payload,
        secret=secret,
        timestamp_header=sent.headers[TIMESTAMP_HEADER],
        signature_header=sent.headers[SIGNATURE_HEADER],
    )


async def test_deliver_logs_durable_row_per_attempt(
    db_session: AsyncSession, integration_db: Database
) -> None:
    """Each attempt commits its own row — a crash mid-loop preserves
    the audit trail up to that point."""
    sub_id, secret = await _make_subscription(db_session)

    call_count = {"n": 0}

    def handler(_req: httpx.Request) -> httpx.Response:
        call_count["n"] += 1
        if call_count["n"] < 3:
            return httpx.Response(503, text="overloaded")
        return httpx.Response(200, text="ok")

    async with _stub_transport(handler) as client, integration_db.sessionmaker() as fresh:
        outcome = await deliver_payload(
            fresh,
            request=_request(sub_id, secret),
            http_client=client,
            sleep=_no_sleep,
        )

    assert outcome.delivered is True
    assert outcome.attempts == 3
    # Three rows: two retrying (5xx) + one ok.
    async with integration_db.sessionmaker() as session:
        rows = (
            (
                await session.execute(
                    select(WebhookDeliveryRow)
                    .where(WebhookDeliveryRow.subscription_id == sub_id)
                    .order_by(WebhookDeliveryRow.attempt)
                )
            )
            .scalars()
            .all()
        )
    statuses = [row.status for row in rows]
    assert statuses == ["retrying", "retrying", "ok"]
    assert [row.attempt for row in rows] == [1, 2, 3]


# ---------------------------------------------------------------------------
# 4xx — no retry


async def test_deliver_4xx_does_not_retry(
    db_session: AsyncSession, integration_db: Database
) -> None:
    sub_id, secret = await _make_subscription(db_session)
    call_count = {"n": 0}

    def handler(_req: httpx.Request) -> httpx.Response:
        call_count["n"] += 1
        return httpx.Response(404, text="not found")

    async with _stub_transport(handler) as client, integration_db.sessionmaker() as fresh:
        outcome = await deliver_payload(
            fresh,
            request=_request(sub_id, secret),
            http_client=client,
            sleep=_no_sleep,
        )

    assert outcome.delivered is False
    assert outcome.terminal_failure is True
    assert outcome.attempts == 1
    assert outcome.last_status == 404
    assert call_count["n"] == 1
    assert await _row_count(integration_db, sub_id) == 1


# ---------------------------------------------------------------------------
# 5xx — exhaust retries


async def test_deliver_5xx_exhausts_attempts_then_fails(
    db_session: AsyncSession, integration_db: Database
) -> None:
    sub_id, secret = await _make_subscription(db_session)

    def handler(_req: httpx.Request) -> httpx.Response:
        return httpx.Response(502, text="bad gateway")

    async with _stub_transport(handler) as client, integration_db.sessionmaker() as fresh:
        outcome = await deliver_payload(
            fresh,
            request=_request(sub_id, secret),
            http_client=client,
            max_attempts=3,
            sleep=_no_sleep,
        )

    assert outcome.delivered is False
    assert outcome.terminal_failure is True
    assert outcome.attempts == 3
    assert outcome.last_status == 502
    # Two retrying rows + one terminal failed row.
    async with integration_db.sessionmaker() as session:
        rows = (
            (
                await session.execute(
                    select(WebhookDeliveryRow)
                    .where(WebhookDeliveryRow.subscription_id == sub_id)
                    .order_by(WebhookDeliveryRow.attempt)
                )
            )
            .scalars()
            .all()
        )
    assert [row.status for row in rows] == ["retrying", "retrying", "failed"]


# ---------------------------------------------------------------------------
# Network error


async def test_deliver_network_error_records_no_response_status(
    db_session: AsyncSession, integration_db: Database
) -> None:
    sub_id, secret = await _make_subscription(db_session)

    def handler(_req: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    async with _stub_transport(handler) as client, integration_db.sessionmaker() as fresh:
        outcome = await deliver_payload(
            fresh,
            request=_request(sub_id, secret),
            http_client=client,
            max_attempts=2,
            sleep=_no_sleep,
        )

    assert outcome.delivered is False
    assert outcome.terminal_failure is True
    assert outcome.attempts == 2
    assert outcome.last_status is None
    assert outcome.last_error is not None
    assert "ConnectError" in outcome.last_error
    async with integration_db.sessionmaker() as session:
        rows = (
            (
                await session.execute(
                    select(WebhookDeliveryRow)
                    .where(WebhookDeliveryRow.subscription_id == sub_id)
                    .order_by(WebhookDeliveryRow.attempt)
                )
            )
            .scalars()
            .all()
        )
    assert [row.status for row in rows] == ["retrying", "failed"]
    for row in rows:
        assert row.response_status is None
        assert row.error_reason is not None
