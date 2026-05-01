"""Smoke tests for the health and root endpoints."""

from __future__ import annotations

import pytest
from httpx import AsyncClient

from aeroza import __version__

pytestmark = pytest.mark.unit


async def test_health_returns_ok(client: AsyncClient) -> None:
    response = await client.get("/health")
    assert response.status_code == 200
    body = response.json()
    assert body == {"status": "ok", "version": __version__}


async def test_root_advertises_docs(client: AsyncClient) -> None:
    response = await client.get("/")
    assert response.status_code == 200
    body = response.json()
    assert body["name"] == "Aeroza"
    assert body["version"] == __version__
    assert body["docs"] == "/docs"


async def test_openapi_schema_is_served(client: AsyncClient) -> None:
    response = await client.get("/openapi.json")
    assert response.status_code == 200
    schema = response.json()
    assert schema["info"]["title"] == "Aeroza"
    assert schema["info"]["version"] == __version__
