"""Integration tests for the /v1/push device endpoints.

Marked ``integration`` — needs ``AEROZA_TEST_DATABASE_URL`` + the migrated
schema. ``make test`` skips these automatically.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from httpx import AsyncClient
from sqlalchemy import text

from aeroza.shared.db import Database

pytestmark = pytest.mark.integration

_TOKEN = "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4"


@pytest_asyncio.fixture(autouse=True)
async def _clean_device_tokens(integration_db: Database) -> AsyncIterator[None]:
    yield
    async with integration_db.sessionmaker() as session:
        await session.execute(text("TRUNCATE TABLE device_tokens"))
        await session.commit()


def _payload(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "token": _TOKEN,
        "platform": "ios",
        "environment": "production",
        "latitude": 35.4676,
        "longitude": -97.5164,
    }
    base.update(overrides)
    return base


async def test_register_device_returns_201(api_client: AsyncClient) -> None:
    resp = await api_client.post("/v1/push/devices", json=_payload())
    assert resp.status_code == 201
    body = resp.json()
    assert body["token"] == _TOKEN
    assert body["platform"] == "ios"
    assert "registeredAt" in body


async def test_register_is_idempotent_upsert(api_client: AsyncClient) -> None:
    await api_client.post("/v1/push/devices", json=_payload(latitude=35.0))
    await api_client.post("/v1/push/devices", json=_payload(latitude=40.0))
    resp = await api_client.get(f"/v1/push/devices/{_TOKEN}")
    assert resp.status_code == 200
    assert resp.json()["latitude"] == 40.0


async def test_get_unknown_device_404(api_client: AsyncClient) -> None:
    resp = await api_client.get("/v1/push/devices/deadbeefdeadbeef")
    assert resp.status_code == 404


async def test_delete_device(api_client: AsyncClient) -> None:
    await api_client.post("/v1/push/devices", json=_payload())
    delete_resp = await api_client.delete(f"/v1/push/devices/{_TOKEN}")
    assert delete_resp.status_code == 204
    get_resp = await api_client.get(f"/v1/push/devices/{_TOKEN}")
    assert get_resp.status_code == 404


async def test_delete_unknown_device_404(api_client: AsyncClient) -> None:
    resp = await api_client.delete("/v1/push/devices/deadbeefdeadbeef")
    assert resp.status_code == 404


async def test_register_rejects_bad_latitude(api_client: AsyncClient) -> None:
    resp = await api_client.post("/v1/push/devices", json=_payload(latitude=120.0))
    assert resp.status_code == 422
