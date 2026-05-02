"""DB operations on ``api_keys``.

The auth middleware's hot path (one lookup per request) goes through
:func:`find_active_api_key`. Everything else is operator tooling:
mint a new key, revoke an existing one, list all of them.

Sessions are passed in (the caller manages the transaction boundary)
so the dependency in :mod:`aeroza.auth.dependencies` can read inside
the same session FastAPI uses for the rest of the request.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from datetime import UTC, datetime

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from aeroza.auth.models import API_KEY_DEFAULT_RATE_LIMIT_CLASS, ApiKeyRow


async def create_api_key(
    session: AsyncSession,
    *,
    name: str,
    prefix: str,
    key_hash: str,
    owner: str,
    scopes: Sequence[str] = (),
    rate_limit_class: str = API_KEY_DEFAULT_RATE_LIMIT_CLASS,
) -> ApiKeyRow:
    """Insert one row. Caller commits.

    Raises an integrity error if the prefix is already taken — the
    randomness budget makes collision astronomically unlikely, but if
    it ever happens the CLI's right answer is to mint a fresh token.
    """
    row = ApiKeyRow(
        name=name,
        prefix=prefix,
        key_hash=key_hash,
        owner=owner,
        scopes=list(scopes),
        rate_limit_class=rate_limit_class,
    )
    session.add(row)
    await session.flush()
    return row


async def find_active_api_key(
    session: AsyncSession,
    *,
    prefix: str,
) -> ApiKeyRow | None:
    """Look up by prefix; only returns ``active`` rows.

    A revoked row stays in the table (the audit trail matters) but is
    invisible to the auth middleware — exactly the same effect as if
    the row never existed, without losing the history.
    """
    stmt = select(ApiKeyRow).where(
        ApiKeyRow.prefix == prefix,
        ApiKeyRow.status == "active",
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def list_api_keys(
    session: AsyncSession,
    *,
    include_revoked: bool = False,
) -> Sequence[ApiKeyRow]:
    """All keys, newest first. Opt-in to seeing revoked ones."""
    stmt = select(ApiKeyRow).order_by(ApiKeyRow.created_at.desc())
    if not include_revoked:
        stmt = stmt.where(ApiKeyRow.status == "active")
    result = await session.execute(stmt)
    return result.scalars().all()


async def revoke_api_key(
    session: AsyncSession,
    *,
    key_id: uuid.UUID,
) -> bool:
    """Mark one key revoked. Returns whether the row was changed.

    Idempotent: revoking an already-revoked key is a no-op (returns
    ``False``); the operator can re-run the CLI without worrying.
    """
    stmt = (
        update(ApiKeyRow)
        .where(ApiKeyRow.id == key_id, ApiKeyRow.status == "active")
        .values(status="revoked", updated_at=datetime.now(UTC))
    )
    result = await session.execute(stmt)
    rowcount: int = result.rowcount  # type: ignore[attr-defined]
    return rowcount > 0


async def touch_api_key_last_used(
    session: AsyncSession,
    *,
    key_id: uuid.UUID,
    now: datetime | None = None,
) -> None:
    """Bump ``last_used_at`` to the current time.

    Called from the auth dependency on every authenticated request.
    Cheap (single PK update), idempotent, and safe to run inside the
    same session as the rest of the request.
    """
    stmt = (
        update(ApiKeyRow)
        .where(ApiKeyRow.id == key_id)
        .values(last_used_at=now or datetime.now(UTC))
    )
    await session.execute(stmt)


__all__ = [
    "create_api_key",
    "find_active_api_key",
    "list_api_keys",
    "revoke_api_key",
    "touch_api_key_last_used",
]
