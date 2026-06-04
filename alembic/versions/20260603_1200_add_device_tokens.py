"""add device_tokens table

Revision ID: 20260603_1200
Revises: 20260503_2200
Create Date: 2026-06-03 12:00:00

Phase 7 — severe-weather push notifications.

One row per registered APNs device token. The app POSTs its token + saved
location to ``/v1/push/devices``; when a new NWS warning is ingested, the
dispatcher point-in-polygon tests each device's saved location against the
warning geometry (PostGIS ``ST_Intersects``) and sends a lean APNs alert. The
iOS Notification Service Extension then hydrates it with a fresh reflectivity
sample at the saved coordinate.

Registration is anonymous by default; ``api_key_id`` is set only when a
BYO-key user authenticates, and is ``ON DELETE SET NULL`` so revoking a key
never drops the device.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260603_1200"
down_revision: str | None = "20260503_2200"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "device_tokens",
        sa.Column(
            "id",
            sa.dialects.postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        # Hex APNs device token. Unique (see index below) — re-register upserts.
        sa.Column("token", sa.Text(), nullable=False),
        sa.Column("platform", sa.Text(), nullable=False),
        # 'sandbox' (debug builds) vs 'production' (TestFlight / App Store) —
        # selects the APNs host the sender posts to.
        sa.Column(
            "environment",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'production'"),
        ),
        # User's saved/primary location, point-in-polygon tested against new
        # warning geometry at dispatch time. Nullable until the app sends one.
        sa.Column("location_lat", sa.Float(), nullable=True),
        sa.Column("location_lng", sa.Float(), nullable=True),
        # Null for anonymous installs; set for BYO-key users. SET NULL on key
        # delete so revoking a key doesn't unregister the device.
        sa.Column(
            "api_key_id",
            sa.dialects.postgresql.UUID(as_uuid=True),
            sa.ForeignKey("api_keys.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.CheckConstraint(
            "environment IN ('sandbox', 'production')",
            name="device_tokens_environment_valid",
        ),
        sa.CheckConstraint(
            "location_lat IS NULL OR (location_lat >= -90 AND location_lat <= 90)",
            name="device_tokens_lat_range",
        ),
        sa.CheckConstraint(
            "location_lng IS NULL OR (location_lng >= -180 AND location_lng <= 180)",
            name="device_tokens_lng_range",
        ),
    )
    op.create_index("uq_device_tokens_token", "device_tokens", ["token"], unique=True)
    op.create_index("ix_device_tokens_api_key_id", "device_tokens", ["api_key_id"])


def downgrade() -> None:
    op.drop_index("ix_device_tokens_api_key_id", table_name="device_tokens")
    op.drop_index("uq_device_tokens_token", table_name="device_tokens")
    op.drop_table("device_tokens")
