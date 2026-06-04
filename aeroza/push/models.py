"""SQLAlchemy ORM model for ``device_tokens``.

Mirrors the ``20260603_1200_add_device_tokens`` migration. One row per
registered APNs device token. ``location_lat`` / ``location_lng`` is the user's
saved (primary) location; the dispatch layer point-in-polygon tests it against
each new warning's geometry. Anonymous installs are the norm, so ``api_key_id``
is nullable — it's only set when a BYO-key user registers, and is
``ON DELETE SET NULL`` so revoking a key never drops the device.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Final

from sqlalchemy import CheckConstraint, DateTime, Float, Index, Text, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from aeroza.shared.base import Base

DEVICE_TOKENS_TABLE: Final[str] = "device_tokens"

DEVICE_ENVIRONMENTS: Final[tuple[str, ...]] = ("sandbox", "production")
DEVICE_PLATFORMS: Final[tuple[str, ...]] = ("ios", "ipados", "macos", "watchos", "tvos")


class DeviceTokenRow(Base):
    __tablename__ = DEVICE_TOKENS_TABLE

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=func.gen_random_uuid(),
    )
    # The hex APNs device token. Unique — re-registration upserts on it.
    token: Mapped[str] = mapped_column(Text, nullable=False)
    platform: Mapped[str] = mapped_column(Text, nullable=False)
    # Which APNs host the token belongs to: a debug build yields a sandbox
    # token, a TestFlight / App Store build a production one. The sender routes
    # to api.sandbox.push.apple.com vs api.push.apple.com accordingly.
    environment: Mapped[str] = mapped_column(Text, nullable=False, server_default="production")
    location_lat: Mapped[float | None] = mapped_column(Float)
    location_lng: Mapped[float | None] = mapped_column(Float)
    # Soft reference to api_keys.id for BYO-key installs (informational). NOT a
    # foreign key on purpose (see the migration): a hard reference blocks
    # `TRUNCATE api_keys`, which the auth test suite relies on. A stale id after
    # a key is deleted is harmless — nothing joins on it.
    api_key_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint(
            "environment IN ('sandbox', 'production')",
            name="device_tokens_environment_valid",
        ),
        CheckConstraint(
            "location_lat IS NULL OR (location_lat >= -90 AND location_lat <= 90)",
            name="device_tokens_lat_range",
        ),
        CheckConstraint(
            "location_lng IS NULL OR (location_lng >= -180 AND location_lng <= 180)",
            name="device_tokens_lng_range",
        ),
        Index("uq_device_tokens_token", "token", unique=True),
        Index("ix_device_tokens_api_key_id", "api_key_id"),
    )


__all__ = [
    "DEVICE_ENVIRONMENTS",
    "DEVICE_PLATFORMS",
    "DEVICE_TOKENS_TABLE",
    "DeviceTokenRow",
]
