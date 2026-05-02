"""Integration tests for the auth store.

The CLI (``aeroza-api-keys``) and the FastAPI dependency are both
composed from these primitives, so the contract here is the
load-bearing one: persist + read-back, prefix uniqueness, idempotent
revoke, list ordering, and ``last_used_at`` touch.
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from aeroza.auth.hashing import mint_api_key_token
from aeroza.auth.store import (
    create_api_key,
    find_active_api_key,
    list_api_keys,
    revoke_api_key,
    touch_api_key_last_used,
)
from aeroza.shared.db import Database

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
async def _truncate(integration_db: Database) -> None:
    yield
    async with integration_db.sessionmaker() as session:
        await session.execute(text("TRUNCATE TABLE api_keys"))
        await session.commit()


async def test_create_then_find(integration_db: Database) -> None:
    minted = mint_api_key_token(salt="s")
    async with integration_db.sessionmaker() as session:
        row = await create_api_key(
            session,
            name="primary",
            prefix=minted.prefix,
            key_hash=minted.key_hash,
            owner="ops@example.com",
            scopes=("read:alerts", "read:mrms"),
        )
        await session.commit()
        assert row.status == "active"
        assert row.scopes == ["read:alerts", "read:mrms"]

        found = await find_active_api_key(session, prefix=minted.prefix)
        assert found is not None
        assert found.id == row.id


async def test_find_active_skips_revoked(integration_db: Database) -> None:
    minted = mint_api_key_token(salt="s")
    async with integration_db.sessionmaker() as session:
        row = await create_api_key(
            session,
            name="x",
            prefix=minted.prefix,
            key_hash=minted.key_hash,
            owner="o",
        )
        await session.commit()

    async with integration_db.sessionmaker() as session:
        changed = await revoke_api_key(session, key_id=row.id)
        await session.commit()
        assert changed is True

    async with integration_db.sessionmaker() as session:
        assert await find_active_api_key(session, prefix=minted.prefix) is None


async def test_revoke_is_idempotent(integration_db: Database) -> None:
    minted = mint_api_key_token(salt="s")
    async with integration_db.sessionmaker() as session:
        row = await create_api_key(
            session,
            name="x",
            prefix=minted.prefix,
            key_hash=minted.key_hash,
            owner="o",
        )
        await session.commit()
    async with integration_db.sessionmaker() as session:
        first = await revoke_api_key(session, key_id=row.id)
        await session.commit()
    async with integration_db.sessionmaker() as session:
        second = await revoke_api_key(session, key_id=row.id)
        await session.commit()
    assert (first, second) == (True, False)


async def test_revoke_unknown_id_returns_false(integration_db: Database) -> None:
    async with integration_db.sessionmaker() as session:
        changed = await revoke_api_key(session, key_id=uuid.uuid4())
        await session.commit()
    assert changed is False


async def test_list_orders_newest_first(integration_db: Database) -> None:
    """Two seeds → list should return them newest → oldest."""
    minted_a = mint_api_key_token(salt="s")
    minted_b = mint_api_key_token(salt="s")
    async with integration_db.sessionmaker() as session:
        await create_api_key(
            session, name="a", prefix=minted_a.prefix, key_hash=minted_a.key_hash, owner="o"
        )
        await session.commit()
    async with integration_db.sessionmaker() as session:
        await create_api_key(
            session, name="b", prefix=minted_b.prefix, key_hash=minted_b.key_hash, owner="o"
        )
        await session.commit()

    async with integration_db.sessionmaker() as session:
        rows = await list_api_keys(session)
        names = [r.name for r in rows]
    assert names == ["b", "a"]


async def test_list_excludes_revoked_by_default(integration_db: Database) -> None:
    minted = mint_api_key_token(salt="s")
    async with integration_db.sessionmaker() as session:
        row = await create_api_key(
            session, name="x", prefix=minted.prefix, key_hash=minted.key_hash, owner="o"
        )
        await session.commit()
    async with integration_db.sessionmaker() as session:
        await revoke_api_key(session, key_id=row.id)
        await session.commit()

    async with integration_db.sessionmaker() as session:
        active = await list_api_keys(session)
        all_rows = await list_api_keys(session, include_revoked=True)
    assert len(active) == 0
    assert len(all_rows) == 1


async def test_duplicate_prefix_is_rejected(integration_db: Database) -> None:
    minted = mint_api_key_token(salt="s")
    async with integration_db.sessionmaker() as session:
        await create_api_key(
            session, name="a", prefix=minted.prefix, key_hash=minted.key_hash, owner="o"
        )
        await session.commit()

    async with integration_db.sessionmaker() as session:
        with pytest.raises(IntegrityError):
            await create_api_key(
                session,
                name="b",
                prefix=minted.prefix,  # duplicate
                key_hash=minted.key_hash,
                owner="o",
            )
            await session.commit()


async def test_touch_last_used_sets_timestamp(integration_db: Database) -> None:
    minted = mint_api_key_token(salt="s")
    async with integration_db.sessionmaker() as session:
        row = await create_api_key(
            session, name="x", prefix=minted.prefix, key_hash=minted.key_hash, owner="o"
        )
        await session.commit()
        assert row.last_used_at is None

    async with integration_db.sessionmaker() as session:
        await touch_api_key_last_used(session, key_id=row.id)
        await session.commit()

    async with integration_db.sessionmaker() as session:
        refreshed = await find_active_api_key(session, prefix=minted.prefix)
        assert refreshed is not None
        assert refreshed.last_used_at is not None
