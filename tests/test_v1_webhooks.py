"""End-to-end integration tests for ``/v1/webhooks/*``.

Exercises the full HTTP shape (status codes, request validation, secret
redaction across the create vs read responses) against the in-process
``api_client`` fixture and a real Postgres.
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import text

from aeroza.shared.db import Database

pytestmark = pytest.mark.integration

LIST_ROUTE: str = "/v1/webhooks"


@pytest.fixture(autouse=True)
async def _truncate_after(integration_db: Database) -> Any:
    yield
    async with integration_db.sessionmaker() as session:
        await session.execute(text("TRUNCATE TABLE webhook_subscriptions"))
        await session.commit()


def _create_payload(
    *,
    url: str = "https://example.com/webhook",
    events: list[str] | None = None,
    description: str | None = "test",
) -> dict[str, Any]:
    return {
        "url": url,
        "events": events or ["aeroza.alerts.nws.new"],
        "description": description,
    }


# ---------------------------------------------------------------------------
# POST


async def test_create_returns_201_with_secret(api_client: AsyncClient) -> None:
    response = await api_client.post(LIST_ROUTE, json=_create_payload())
    assert response.status_code == 201
    body = response.json()

    assert body["type"] == "WebhookSubscription"
    assert "id" in body
    uuid.UUID(body["id"])  # must parse
    assert body["url"] == "https://example.com/webhook"
    assert body["events"] == ["aeroza.alerts.nws.new"]
    assert body["description"] == "test"
    assert body["status"] == "active"
    # The secret is exposed exactly once, on the create response.
    assert isinstance(body["secret"], str)
    assert len(body["secret"]) == 64
    assert "createdAt" in body
    assert "updatedAt" in body


async def test_create_rejects_unknown_event_type(api_client: AsyncClient) -> None:
    response = await api_client.post(LIST_ROUTE, json=_create_payload(events=["aeroza.nope"]))
    assert response.status_code == 422


async def test_create_rejects_non_http_url(api_client: AsyncClient) -> None:
    response = await api_client.post(LIST_ROUTE, json=_create_payload(url="ftp://example.com"))
    assert response.status_code == 422


async def test_create_dedups_repeated_event_types(api_client: AsyncClient) -> None:
    response = await api_client.post(
        LIST_ROUTE,
        json=_create_payload(
            events=[
                "aeroza.alerts.nws.new",
                "aeroza.mrms.files.new",
                "aeroza.alerts.nws.new",  # duplicate
            ]
        ),
    )
    assert response.status_code == 201
    assert response.json()["events"] == [
        "aeroza.alerts.nws.new",
        "aeroza.mrms.files.new",
    ]


# ---------------------------------------------------------------------------
# GET (list)


async def test_list_returns_redacted_items_newest_first(
    api_client: AsyncClient,
) -> None:
    a = await api_client.post(LIST_ROUTE, json=_create_payload(url="https://a.example.com"))
    b = await api_client.post(LIST_ROUTE, json=_create_payload(url="https://b.example.com"))
    assert a.status_code == b.status_code == 201

    response = await api_client.get(LIST_ROUTE)
    assert response.status_code == 200
    body = response.json()
    assert body["type"] == "WebhookSubscriptionList"
    assert len(body["items"]) == 2
    # Newest first.
    assert body["items"][0]["id"] == b.json()["id"]
    assert body["items"][1]["id"] == a.json()["id"]
    # Every item must be the redacted shape — no secret on list endpoints.
    for item in body["items"]:
        assert item["type"] == "WebhookSubscriptionRedacted"
        assert "secret" not in item


async def test_list_filters_by_status(api_client: AsyncClient) -> None:
    a = await api_client.post(LIST_ROUTE, json=_create_payload(url="https://a.example.com"))
    b = await api_client.post(LIST_ROUTE, json=_create_payload(url="https://b.example.com"))
    paused_id = b.json()["id"]
    await api_client.patch(f"{LIST_ROUTE}/{paused_id}", json={"status": "paused"})

    active_only = await api_client.get(LIST_ROUTE, params={"status": "active"})
    assert active_only.status_code == 200
    items = active_only.json()["items"]
    assert {item["id"] for item in items} == {a.json()["id"]}


# ---------------------------------------------------------------------------
# GET (detail)


async def test_get_returns_redacted_subscription(api_client: AsyncClient) -> None:
    created = await api_client.post(LIST_ROUTE, json=_create_payload())
    sub_id = created.json()["id"]

    response = await api_client.get(f"{LIST_ROUTE}/{sub_id}")
    assert response.status_code == 200
    body = response.json()
    assert body["type"] == "WebhookSubscriptionRedacted"
    assert body["id"] == sub_id
    assert "secret" not in body


async def test_get_returns_404_for_unknown_id(api_client: AsyncClient) -> None:
    response = await api_client.get(f"{LIST_ROUTE}/{uuid.uuid4()}")
    assert response.status_code == 404
    assert "not found" in response.json()["detail"]


# ---------------------------------------------------------------------------
# PATCH


async def test_patch_updates_partial_fields(api_client: AsyncClient) -> None:
    created = await api_client.post(LIST_ROUTE, json=_create_payload(description="old"))
    sub_id = created.json()["id"]

    response = await api_client.patch(
        f"{LIST_ROUTE}/{sub_id}",
        json={"description": "new", "status": "paused"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["description"] == "new"
    assert body["status"] == "paused"
    assert body["url"] == "https://example.com/webhook"  # untouched


async def test_patch_returns_404_for_unknown_id(api_client: AsyncClient) -> None:
    response = await api_client.patch(f"{LIST_ROUTE}/{uuid.uuid4()}", json={"description": "x"})
    assert response.status_code == 404


async def test_patch_rejects_unknown_status(api_client: AsyncClient) -> None:
    created = await api_client.post(LIST_ROUTE, json=_create_payload())
    sub_id = created.json()["id"]
    response = await api_client.patch(f"{LIST_ROUTE}/{sub_id}", json={"status": "explosive"})
    assert response.status_code == 422


# ---------------------------------------------------------------------------
# DELETE


async def test_delete_returns_204_then_404(api_client: AsyncClient) -> None:
    created = await api_client.post(LIST_ROUTE, json=_create_payload())
    sub_id = created.json()["id"]

    first = await api_client.delete(f"{LIST_ROUTE}/{sub_id}")
    assert first.status_code == 204

    second = await api_client.delete(f"{LIST_ROUTE}/{sub_id}")
    assert second.status_code == 404


async def test_delete_returns_404_for_unknown_id(api_client: AsyncClient) -> None:
    response = await api_client.delete(f"{LIST_ROUTE}/{uuid.uuid4()}")
    assert response.status_code == 404
