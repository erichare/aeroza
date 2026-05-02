"""End-to-end integration tests for ``/v1/alert-rules/*``.

Exercises the HTTP shape (status codes, validation, the discriminated
config union, FK enforcement vs missing subscription) against the
in-process ``api_client`` and a real Postgres.
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import text

from aeroza.shared.db import Database

pytestmark = pytest.mark.integration

WEBHOOKS_ROUTE: str = "/v1/webhooks"
RULES_ROUTE: str = "/v1/alert-rules"


@pytest.fixture(autouse=True)
async def _truncate_after(integration_db: Database) -> Any:
    yield
    async with integration_db.sessionmaker() as session:
        await session.execute(text("TRUNCATE TABLE webhook_subscriptions CASCADE"))
        await session.commit()


async def _make_subscription(api_client: AsyncClient) -> str:
    response = await api_client.post(
        WEBHOOKS_ROUTE,
        json={
            "url": "https://example.com/hook",
            "events": ["aeroza.mrms.grids.new"],
        },
    )
    assert response.status_code == 201
    return response.json()["id"]


def _point_payload(subscription_id: str, *, name: str = "Houston ≥ 40") -> dict[str, Any]:
    return {
        "subscriptionId": subscription_id,
        "name": name,
        "config": {
            "type": "point",
            "lat": 29.76,
            "lng": -95.37,
            "predicate": {"op": ">=", "threshold": 40.0},
        },
    }


def _polygon_payload(subscription_id: str, *, name: str = "Region max ≥ 40") -> dict[str, Any]:
    return {
        "subscriptionId": subscription_id,
        "name": name,
        "config": {
            "type": "polygon",
            "polygon": "-95.7,29.5,-95.0,29.5,-95.0,30.0,-95.7,30.0",
            "reducer": "max",
            "predicate": {"op": ">=", "threshold": 40.0},
        },
    }


# ---------------------------------------------------------------------------
# POST


async def test_create_point_rule_returns_201(api_client: AsyncClient) -> None:
    sub_id = await _make_subscription(api_client)
    response = await api_client.post(RULES_ROUTE, json=_point_payload(sub_id))
    assert response.status_code == 201
    body = response.json()

    assert body["type"] == "AlertRule"
    assert body["subscriptionId"] == sub_id
    assert body["name"] == "Houston ≥ 40"
    assert body["status"] == "active"
    assert body["currentlyFiring"] is False
    assert body["lastValue"] is None
    assert body["config"]["type"] == "point"
    assert body["config"]["lat"] == 29.76
    assert body["config"]["predicate"]["op"] == ">="
    # No snake_case leaks on the wire.
    assert "subscription_id" not in body
    assert "currently_firing" not in body


async def test_create_polygon_rule(api_client: AsyncClient) -> None:
    sub_id = await _make_subscription(api_client)
    response = await api_client.post(RULES_ROUTE, json=_polygon_payload(sub_id))
    assert response.status_code == 201
    body = response.json()
    assert body["config"]["type"] == "polygon"
    assert body["config"]["reducer"] == "max"


async def test_create_404_when_subscription_missing(api_client: AsyncClient) -> None:
    response = await api_client.post(
        RULES_ROUTE,
        json=_point_payload(str(uuid.uuid4())),
    )
    assert response.status_code == 404
    assert "webhook subscription" in response.json()["detail"]


async def test_create_rejects_unknown_predicate_op(api_client: AsyncClient) -> None:
    sub_id = await _make_subscription(api_client)
    payload = _point_payload(sub_id)
    payload["config"]["predicate"]["op"] = "approximately"
    response = await api_client.post(RULES_ROUTE, json=payload)
    assert response.status_code == 422


async def test_create_polygon_count_ge_requires_threshold(api_client: AsyncClient) -> None:
    sub_id = await _make_subscription(api_client)
    payload = _polygon_payload(sub_id)
    payload["config"]["reducer"] = "count_ge"
    response = await api_client.post(RULES_ROUTE, json=payload)
    assert response.status_code == 422
    assert "countThreshold" in response.text


async def test_create_polygon_count_ge_with_threshold_succeeds(
    api_client: AsyncClient,
) -> None:
    sub_id = await _make_subscription(api_client)
    payload = _polygon_payload(sub_id)
    payload["config"]["reducer"] = "count_ge"
    payload["config"]["countThreshold"] = 40.0
    response = await api_client.post(RULES_ROUTE, json=payload)
    assert response.status_code == 201
    body = response.json()
    assert body["config"]["countThreshold"] == 40.0


# ---------------------------------------------------------------------------
# GET


async def test_list_returns_rules_newest_first_with_filters(
    api_client: AsyncClient,
) -> None:
    sub_a = await _make_subscription(api_client)
    sub_b = await _make_subscription(api_client)
    a = await api_client.post(RULES_ROUTE, json=_point_payload(sub_a, name="a"))
    b = await api_client.post(RULES_ROUTE, json=_point_payload(sub_b, name="b"))
    assert a.status_code == b.status_code == 201

    response = await api_client.get(RULES_ROUTE)
    assert response.status_code == 200
    body = response.json()
    assert body["type"] == "AlertRuleList"
    assert len(body["items"]) == 2

    only_a = await api_client.get(RULES_ROUTE, params={"subscriptionId": sub_a})
    assert {item["id"] for item in only_a.json()["items"]} == {a.json()["id"]}


async def test_get_by_id(api_client: AsyncClient) -> None:
    sub_id = await _make_subscription(api_client)
    created = await api_client.post(RULES_ROUTE, json=_point_payload(sub_id))
    rule_id = created.json()["id"]

    response = await api_client.get(f"{RULES_ROUTE}/{rule_id}")
    assert response.status_code == 200
    assert response.json()["id"] == rule_id


async def test_get_404_for_unknown_id(api_client: AsyncClient) -> None:
    response = await api_client.get(f"{RULES_ROUTE}/{uuid.uuid4()}")
    assert response.status_code == 404


# ---------------------------------------------------------------------------
# PATCH


async def test_patch_updates_status_and_name(api_client: AsyncClient) -> None:
    sub_id = await _make_subscription(api_client)
    created = await api_client.post(RULES_ROUTE, json=_point_payload(sub_id))
    rule_id = created.json()["id"]

    response = await api_client.patch(
        f"{RULES_ROUTE}/{rule_id}",
        json={"name": "renamed", "status": "paused"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["name"] == "renamed"
    assert body["status"] == "paused"


async def test_patch_replaces_config_to_polygon(api_client: AsyncClient) -> None:
    sub_id = await _make_subscription(api_client)
    created = await api_client.post(RULES_ROUTE, json=_point_payload(sub_id))
    rule_id = created.json()["id"]

    response = await api_client.patch(
        f"{RULES_ROUTE}/{rule_id}",
        json={
            "config": {
                "type": "polygon",
                "polygon": "-95.7,29.5,-95.0,29.5,-95.0,30.0,-95.7,30.0",
                "reducer": "mean",
                "predicate": {"op": "<", "threshold": 20.0},
            }
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["config"]["type"] == "polygon"
    assert body["config"]["reducer"] == "mean"


async def test_patch_404_for_unknown_id(api_client: AsyncClient) -> None:
    response = await api_client.patch(f"{RULES_ROUTE}/{uuid.uuid4()}", json={"name": "x"})
    assert response.status_code == 404


# ---------------------------------------------------------------------------
# DELETE


async def test_delete_then_404(api_client: AsyncClient) -> None:
    sub_id = await _make_subscription(api_client)
    created = await api_client.post(RULES_ROUTE, json=_point_payload(sub_id))
    rule_id = created.json()["id"]

    first = await api_client.delete(f"{RULES_ROUTE}/{rule_id}")
    assert first.status_code == 204
    second = await api_client.delete(f"{RULES_ROUTE}/{rule_id}")
    assert second.status_code == 404


async def test_delete_cascades_when_subscription_deleted(
    api_client: AsyncClient,
) -> None:
    sub_id = await _make_subscription(api_client)
    created = await api_client.post(RULES_ROUTE, json=_point_payload(sub_id))
    rule_id = created.json()["id"]

    sub_del = await api_client.delete(f"{WEBHOOKS_ROUTE}/{sub_id}")
    assert sub_del.status_code == 204

    response = await api_client.get(f"{RULES_ROUTE}/{rule_id}")
    assert response.status_code == 404
