"""SQLAlchemy ORM model for the webhook_deliveries table.

One row per delivery attempt. Per-attempt rows (rather than one row
per logical delivery with a retry counter) make "why did this webhook
keep failing?" boring SQL — `SELECT * FROM webhook_deliveries WHERE
subscription_id = ? ORDER BY created_at DESC LIMIT 20` shows the
exact sequence of attempts and outcomes.

``rule_id`` is nullable: raw event-fan-out deliveries set it to NULL;
rule-triggered deliveries point at the rule that fired.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Final

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from aeroza.shared.base import Base
from aeroza.webhooks.models import WEBHOOK_SUBSCRIPTIONS_TABLE
from aeroza.webhooks.rule_models import ALERT_RULES_TABLE

WEBHOOK_DELIVERIES_TABLE: Final[str] = "webhook_deliveries"

WEBHOOK_DELIVERY_STATUSES: Final[tuple[str, ...]] = ("ok", "failed", "retrying")


class WebhookDeliveryRow(Base):
    __tablename__ = WEBHOOK_DELIVERIES_TABLE

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=func.gen_random_uuid(),
    )
    subscription_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey(
            f"{WEBHOOK_SUBSCRIPTIONS_TABLE}.id",
            ondelete="CASCADE",
        ),
        nullable=False,
    )
    rule_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey(
            f"{ALERT_RULES_TABLE}.id",
            ondelete="SET NULL",
        ),
        nullable=True,
    )
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    attempt: Mapped[int] = mapped_column(Integer, nullable=False)
    response_status: Mapped[int | None] = mapped_column(Integer, nullable=True)
    response_body_preview: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    duration_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    __table_args__ = (
        CheckConstraint(
            "status IN ('ok', 'failed', 'retrying')",
            name="webhook_deliveries_status_valid",
        ),
        CheckConstraint(
            "attempt >= 1",
            name="webhook_deliveries_attempt_positive",
        ),
        Index(
            "ix_webhook_deliveries_subscription_created_at",
            "subscription_id",
            "created_at",
        ),
        Index(
            "ix_webhook_deliveries_subscription_status",
            "subscription_id",
            "status",
            "created_at",
        ),
    )


__all__ = [
    "WEBHOOK_DELIVERIES_TABLE",
    "WEBHOOK_DELIVERY_STATUSES",
    "WebhookDeliveryRow",
]
