"""SQLAlchemy ORM model for the webhook_subscriptions table.

One row per external HTTP endpoint that wants to be notified when
matching events fire. ``events`` is a Postgres text[] of NATS-style
subject strings; the dispatcher (PR #6) uses array containment
(``events @> ARRAY[?]``) backed by a GIN index to cheaply find every
active subscription for a given subject.

``secret`` is the HMAC signing key generated server-side at create
time. Stored in plaintext for v1 — switching to encrypted-at-rest is
a follow-up once we have a KMS / secret-manager story.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Final

from sqlalchemy import CheckConstraint, DateTime, Index, Text, func
from sqlalchemy.dialects.postgresql import ARRAY, UUID
from sqlalchemy.orm import Mapped, mapped_column

from aeroza.shared.base import Base

WEBHOOK_SUBSCRIPTIONS_TABLE: Final[str] = "webhook_subscriptions"

# Allowed values for the ``status`` column. Mirror the CHECK constraint in
# the migration; if these drift, integration tests catch it.
WEBHOOK_STATUSES: Final[tuple[str, ...]] = ("active", "paused", "disabled")

# Event taxonomy on the wire. Same strings as the NATS subjects so logs,
# broker traffic, and webhook payloads all share one vocabulary. New
# events are added here as new publishers come online (e.g.
# ``aeroza.nowcast.grids.new`` once Phase 3 lands).
WEBHOOK_EVENT_TYPES: Final[tuple[str, ...]] = (
    "aeroza.alerts.nws.new",
    "aeroza.mrms.files.new",
    "aeroza.mrms.grids.new",
)


class WebhookSubscriptionRow(Base):
    __tablename__ = WEBHOOK_SUBSCRIPTIONS_TABLE

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        # Server-side default uses ``gen_random_uuid()`` (built into PG
        # 13+); keeping it server-side means the catalog row is fully
        # specified by the caller's INSERT without round-tripping a UUID.
        server_default=func.gen_random_uuid(),
    )
    url: Mapped[str] = mapped_column(Text, nullable=False)
    secret: Mapped[str] = mapped_column(Text, nullable=False)
    events: Mapped[list[str]] = mapped_column(ARRAY(Text), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default="active",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    __table_args__ = (
        CheckConstraint(
            "status IN ('active', 'paused', 'disabled')",
            name="webhook_subscriptions_status_valid",
        ),
        CheckConstraint(
            "cardinality(events) > 0",
            name="webhook_subscriptions_events_nonempty",
        ),
        Index(
            "ix_webhook_subscriptions_events_gin",
            "events",
            postgresql_using="gin",
        ),
        Index(
            "ix_webhook_subscriptions_status_created_at",
            "status",
            "created_at",
        ),
    )


__all__ = [
    "WEBHOOK_EVENT_TYPES",
    "WEBHOOK_STATUSES",
    "WEBHOOK_SUBSCRIPTIONS_TABLE",
    "WebhookSubscriptionRow",
]
