"""Integration tests for ``GET /v1/webhooks/{id}/deliveries``.

Inserts ``webhook_deliveries`` rows directly via the ORM (so we don't
need to drive the dispatcher end-to-end) and exercises the audit-trail
read path.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import text

from aeroza.shared.db import Database
from aeroza.webhooks.delivery_models import WebhookDeliveryRow

pytestmark = pytest.mark.integration


SUB_ROUTE: str = "/v1/webhooks"


@pytest.fixture(autouse=True)
async def _truncate_after(integration_db: Database) -> Any:
    yield
    async with integration_db.sessionmaker() as session:
        await session.execute(text("TRUNCATE TABLE webhook_subscriptions CASCADE"))
        await session.commit()


async def _create_subscription(api_client: AsyncClient) -> str:
    response = await api_client.post(
        SUB_ROUTE,
        json={
            "url": "https://example.com/hook",
            "events": ["aeroza.alerts.nws.new"],
            "description": None,
        },
    )
    assert response.status_code == 201
    return response.json()["id"]


async def _insert_delivery(
    integration_db: Database,
    *,
    subscription_id: uuid.UUID,
    status: str,
    attempt: int,
    response_status: int | None = None,
    response_body_preview: str | None = None,
    error_reason: str | None = None,
    duration_ms: int | None = None,
    created_at: datetime | None = None,
    rule_id: uuid.UUID | None = None,
) -> uuid.UUID:
    async with integration_db.sessionmaker() as session:
        row = WebhookDeliveryRow(
            subscription_id=subscription_id,
            rule_id=rule_id,
            event_type="aeroza.alerts.nws.new",
            payload={"event": "aeroza.alerts.nws.new", "data": {}},
            status=status,
            attempt=attempt,
            response_status=response_status,
            response_body_preview=response_body_preview,
            error_reason=error_reason,
            duration_ms=duration_ms,
        )
        if created_at is not None:
            row.created_at = created_at
        session.add(row)
        await session.commit()
        return row.id


# ---------------------------------------------------------------------------
# 404


async def test_unknown_subscription_returns_404(api_client: AsyncClient) -> None:
    fake = uuid.uuid4()
    response = await api_client.get(f"{SUB_ROUTE}/{fake}/deliveries")
    assert response.status_code == 404


# ---------------------------------------------------------------------------
# Empty subscription


async def test_no_deliveries_yet_returns_empty_list(
    api_client: AsyncClient,
) -> None:
    sub_id = await _create_subscription(api_client)
    response = await api_client.get(f"{SUB_ROUTE}/{sub_id}/deliveries")
    assert response.status_code == 200
    body = response.json()
    assert body["type"] == "WebhookDeliveryList"
    assert body["items"] == []


# ---------------------------------------------------------------------------
# Newest-first + payload omission


async def test_returns_rows_newest_first_without_payload(
    api_client: AsyncClient, integration_db: Database
) -> None:
    sub_id = await _create_subscription(api_client)
    sub_uuid = uuid.UUID(sub_id)

    # Insert two attempts ~10 minutes apart.
    older = datetime.now(UTC) - timedelta(minutes=10)
    newer = datetime.now(UTC)
    older_id = await _insert_delivery(
        integration_db,
        subscription_id=sub_uuid,
        status="ok",
        attempt=1,
        response_status=200,
        response_body_preview="ok",
        duration_ms=42,
        created_at=older,
    )
    newer_id = await _insert_delivery(
        integration_db,
        subscription_id=sub_uuid,
        status="failed",
        attempt=4,
        response_status=503,
        response_body_preview="Service Unavailable",
        error_reason="server error: 503 Service Unavailable",
        duration_ms=120,
        created_at=newer,
    )

    response = await api_client.get(f"{SUB_ROUTE}/{sub_id}/deliveries")
    assert response.status_code == 200
    items = response.json()["items"]
    assert [item["id"] for item in items] == [str(newer_id), str(older_id)]
    # Each row must mirror the wire shape exactly — and must NOT carry
    # the raw payload.
    head = items[0]
    assert head["type"] == "WebhookDelivery"
    assert head["subscriptionId"] == sub_id
    assert head["eventType"] == "aeroza.alerts.nws.new"
    assert head["status"] == "failed"
    assert head["attempt"] == 4
    assert head["responseStatus"] == 503
    assert head["responseBodyPreview"] == "Service Unavailable"
    assert head["errorReason"] == "server error: 503 Service Unavailable"
    assert head["durationMs"] == 120
    assert "createdAt" in head
    assert "payload" not in head


# ---------------------------------------------------------------------------
# Status filter


async def test_status_filter_narrows_results(
    api_client: AsyncClient, integration_db: Database
) -> None:
    sub_id = await _create_subscription(api_client)
    sub_uuid = uuid.UUID(sub_id)
    await _insert_delivery(
        integration_db, subscription_id=sub_uuid, status="ok", attempt=1, response_status=200
    )
    await _insert_delivery(
        integration_db,
        subscription_id=sub_uuid,
        status="failed",
        attempt=4,
        response_status=503,
    )
    await _insert_delivery(
        integration_db,
        subscription_id=sub_uuid,
        status="retrying",
        attempt=2,
        response_status=503,
    )

    response = await api_client.get(f"{SUB_ROUTE}/{sub_id}/deliveries", params={"status": "failed"})
    assert response.status_code == 200
    items = response.json()["items"]
    assert len(items) == 1
    assert items[0]["status"] == "failed"


# ---------------------------------------------------------------------------
# Limit


async def test_limit_caps_returned_rows(api_client: AsyncClient, integration_db: Database) -> None:
    sub_id = await _create_subscription(api_client)
    sub_uuid = uuid.UUID(sub_id)
    for attempt in range(1, 6):  # five attempts
        await _insert_delivery(
            integration_db,
            subscription_id=sub_uuid,
            status="ok",
            attempt=attempt,
            response_status=200,
        )

    response = await api_client.get(f"{SUB_ROUTE}/{sub_id}/deliveries", params={"limit": 2})
    assert response.status_code == 200
    assert len(response.json()["items"]) == 2


async def test_limit_above_max_is_rejected(api_client: AsyncClient) -> None:
    sub_id = await _create_subscription(api_client)
    response = await api_client.get(f"{SUB_ROUTE}/{sub_id}/deliveries", params={"limit": 10_000})
    assert response.status_code == 422


# ---------------------------------------------------------------------------
# Isolation


async def test_only_returns_rows_for_requested_subscription(
    api_client: AsyncClient, integration_db: Database
) -> None:
    sub_a = await _create_subscription(api_client)
    sub_b_response = await api_client.post(
        SUB_ROUTE,
        json={
            "url": "https://example.com/other",
            "events": ["aeroza.alerts.nws.new"],
            "description": None,
        },
    )
    sub_b = sub_b_response.json()["id"]
    await _insert_delivery(
        integration_db,
        subscription_id=uuid.UUID(sub_a),
        status="ok",
        attempt=1,
        response_status=200,
    )
    await _insert_delivery(
        integration_db,
        subscription_id=uuid.UUID(sub_b),
        status="ok",
        attempt=1,
        response_status=200,
    )

    response = await api_client.get(f"{SUB_ROUTE}/{sub_a}/deliveries")
    items = response.json()["items"]
    assert len(items) == 1
    assert items[0]["subscriptionId"] == sub_a
