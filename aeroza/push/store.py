"""Persistence helpers for device tokens (kept out of the route handlers)."""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from datetime import UTC, datetime

from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from aeroza.push.models import DeviceTokenRow
from aeroza.push.schemas import DeviceRegistration


async def upsert_device(
    session: AsyncSession,
    registration: DeviceRegistration,
    *,
    api_key_id: uuid.UUID | None = None,
) -> DeviceTokenRow:
    """Insert a device token, or update the existing row for that token.

    Idempotent on ``token`` — a device that re-registers (new location, OS
    upgrade, app reinstall keeping the same token) updates in place rather than
    duplicating.
    """
    now = datetime.now(UTC)
    stmt = (
        pg_insert(DeviceTokenRow)
        .values(
            token=registration.token,
            platform=registration.platform,
            environment=registration.environment,
            location_lat=registration.latitude,
            location_lng=registration.longitude,
            api_key_id=api_key_id,
            updated_at=now,
        )
        .on_conflict_do_update(
            index_elements=["token"],
            set_={
                "platform": registration.platform,
                "environment": registration.environment,
                "location_lat": registration.latitude,
                "location_lng": registration.longitude,
                "api_key_id": api_key_id,
                "updated_at": now,
            },
        )
    )
    await session.execute(stmt)
    await session.commit()

    row = await get_device(session, registration.token)
    if row is None:  # pragma: no cover - we just upserted it
        raise RuntimeError("device upsert did not persist")
    return row


async def get_device(session: AsyncSession, token: str) -> DeviceTokenRow | None:
    result = await session.execute(select(DeviceTokenRow).where(DeviceTokenRow.token == token))
    return result.scalar_one_or_none()


async def delete_device(session: AsyncSession, token: str) -> bool:
    """Delete the row for ``token``; return whether a row was removed."""
    row = await get_device(session, token)
    if row is None:
        return False
    await session.delete(row)
    await session.commit()
    return True


async def prune_devices(session: AsyncSession, tokens: Sequence[str]) -> None:
    """Bulk-delete dead tokens (APNs reported them Unregistered / BadDeviceToken)."""
    if not tokens:
        return
    await session.execute(delete(DeviceTokenRow).where(DeviceTokenRow.token.in_(tokens)))
    await session.commit()
