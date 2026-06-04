"""Integration tests for the admin test-push endpoint."""

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


async def _register(client: AsyncClient) -> None:
    await client.post(
        "/v1/push/devices",
        json={"token": _TOKEN, "platform": "ios", "latitude": 35.0, "longitude": -97.0},
    )


async def test_test_push_404_when_device_unknown(api_client: AsyncClient) -> None:
    resp = await api_client.post("/v1/admin/push/test", json={"token": "deadbeefdeadbeef"})
    assert resp.status_code == 404


async def test_test_push_503_when_apns_unconfigured(api_client: AsyncClient) -> None:
    # No AEROZA_APNS_* in the test env → apns_configured is False.
    await _register(api_client)
    resp = await api_client.post("/v1/admin/push/test", json={"token": _TOKEN})
    assert resp.status_code == 503


async def test_test_push_404_when_admin_disabled(
    api_client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("AEROZA_DEV_ADMIN_ENABLED", "false")
    resp = await api_client.post("/v1/admin/push/test", json={"token": _TOKEN})
    assert resp.status_code == 404
