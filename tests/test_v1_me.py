"""End-to-end tests for ``GET /v1/me``.

The route is the single thing a token holder can do with their key
in v1. Tests cover the happy path (mint via the store + auth via the
header), the unauth paths (missing / wrong scheme / unknown prefix /
hash mismatch / revoked), and the wire format (camelCase keys,
``last_used_at`` populated by the auth dependency's touch).
"""

from __future__ import annotations

import pytest
from httpx import AsyncClient

from aeroza.auth.hashing import API_KEY_TOKEN_PREFIX, mint_api_key_token
from aeroza.auth.store import create_api_key, revoke_api_key
from aeroza.config import get_settings
from aeroza.shared.db import Database

pytestmark = pytest.mark.integration

ROUTE = "/v1/me"


async def _seed_key(
    db: Database,
    *,
    name: str = "test-key",
    owner: str = "tester@example.com",
    scopes: tuple[str, ...] = ("read:alerts",),
) -> tuple[str, str]:
    """Mint + persist a key. Returns (token, key_id)."""
    minted = mint_api_key_token(salt=get_settings().api_key_salt)
    async with db.sessionmaker() as session:
        row = await create_api_key(
            session,
            name=name,
            prefix=minted.prefix,
            key_hash=minted.key_hash,
            owner=owner,
            scopes=scopes,
        )
        await session.commit()
        return minted.token, str(row.id)


async def test_me_requires_authorization(api_client: AsyncClient) -> None:
    response = await api_client.get(ROUTE)
    assert response.status_code == 401
    assert response.headers.get("www-authenticate") == "Bearer"


async def test_me_rejects_wrong_scheme(api_client: AsyncClient) -> None:
    response = await api_client.get(ROUTE, headers={"Authorization": "Basic abc"})
    assert response.status_code == 401


async def test_me_rejects_unknown_prefix(api_client: AsyncClient) -> None:
    response = await api_client.get(
        ROUTE,
        headers={"Authorization": f"Bearer {API_KEY_TOKEN_PREFIX}deadbeefnope"},
    )
    assert response.status_code == 401


async def test_me_returns_camelcase_payload(
    api_client: AsyncClient, integration_db: Database
) -> None:
    token, _ = await _seed_key(integration_db, name="primary", owner="ops@example.com")
    response = await api_client.get(ROUTE, headers={"Authorization": f"Bearer {token}"})
    assert response.status_code == 200
    body = response.json()
    assert body["type"] == "Me"
    assert body["name"] == "primary"
    assert body["owner"] == "ops@example.com"
    assert body["scopes"] == ["read:alerts"]
    assert body["rateLimitClass"] == "default"
    # Snake-case keys must not bleed onto the wire.
    assert "rate_limit_class" not in body
    assert "last_used_at" not in body
    # Prefix should be visible (8 chars by spec).
    assert len(body["prefix"]) == 8


async def test_me_revoked_key_is_unauthorized(
    api_client: AsyncClient, integration_db: Database
) -> None:
    token, key_id = await _seed_key(integration_db)
    import uuid as uuid_mod

    async with integration_db.sessionmaker() as session:
        await revoke_api_key(session, key_id=uuid_mod.UUID(key_id))
        await session.commit()

    response = await api_client.get(ROUTE, headers={"Authorization": f"Bearer {token}"})
    assert response.status_code == 401


async def test_me_hash_mismatch_is_unauthorized(
    api_client: AsyncClient, integration_db: Database
) -> None:
    """Same prefix, wrong secret. Hard to trigger naturally — we mint two
    keys, then forge a token by splicing the first key's prefix onto the
    second key's random part. The middleware should reject it."""
    token_a, _ = await _seed_key(integration_db, name="a")
    token_b, _ = await _seed_key(integration_db, name="b")

    # Splice: prefix from A (first 8 chars after the brand), tail from B.
    a_random = token_a[len(API_KEY_TOKEN_PREFIX) :]
    b_random = token_b[len(API_KEY_TOKEN_PREFIX) :]
    forged = f"{API_KEY_TOKEN_PREFIX}{a_random[:8]}{b_random[8:]}"

    response = await api_client.get(ROUTE, headers={"Authorization": f"Bearer {forged}"})
    assert response.status_code == 401


async def test_me_records_last_used_after_call(
    api_client: AsyncClient, integration_db: Database
) -> None:
    """A first call should populate ``lastUsedAt`` (the auth dependency
    touches it before the route reads from the same session). The
    second call should see it set."""
    token, _ = await _seed_key(integration_db)

    first = await api_client.get(ROUTE, headers={"Authorization": f"Bearer {token}"})
    assert first.status_code == 200
    # /v1/me reads after the touch in the same session, so the first
    # response already carries a populated lastUsedAt.
    assert first.json()["lastUsedAt"] is not None

    second = await api_client.get(ROUTE, headers={"Authorization": f"Bearer {token}"})
    assert second.status_code == 200
    assert second.json()["lastUsedAt"] is not None


@pytest.fixture(autouse=True)
async def _truncate_api_keys(integration_db: Database) -> None:
    """Clean ``api_keys`` between tests so seeded rows don't leak."""
    yield
    from sqlalchemy import text

    async with integration_db.sessionmaker() as session:
        await session.execute(text("TRUNCATE TABLE api_keys"))
        await session.commit()
