"""SQLAlchemy ORM model for the alert_rules table.

One row per named predicate over the latest materialised MRMS grid,
bound to a :class:`WebhookSubscriptionRow` via FK with cascade delete.
The dispatcher worker (slice 3) reads these on every grid event and
POSTs to the bound subscription on a false→true transition.

The shape of ``config`` varies by ``rule_type``; pydantic validates
it at the API boundary (see :mod:`aeroza.webhooks.rule_schemas`). The
DB only guarantees the discriminator's well-formedness via CHECK.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Final

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from aeroza.shared.base import Base
from aeroza.webhooks.models import WEBHOOK_SUBSCRIPTIONS_TABLE

ALERT_RULES_TABLE: Final[str] = "alert_rules"

ALERT_RULE_TYPES: Final[tuple[str, ...]] = ("point", "polygon")
ALERT_RULE_STATUSES: Final[tuple[str, ...]] = ("active", "paused", "disabled")


class AlertRuleRow(Base):
    __tablename__ = ALERT_RULES_TABLE

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
    name: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    rule_type: Mapped[str] = mapped_column(Text, nullable=False)
    config: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    status: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default="active",
    )
    # Dispatcher FSM state — populated by slice 3.
    currently_firing: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        server_default="false",
    )
    last_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    last_evaluated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_fired_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
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
            "rule_type IN ('point', 'polygon')",
            name="alert_rules_type_valid",
        ),
        CheckConstraint(
            "status IN ('active', 'paused', 'disabled')",
            name="alert_rules_status_valid",
        ),
        Index("ix_alert_rules_subscription_id", "subscription_id"),
        Index(
            "ix_alert_rules_status_created_at",
            "status",
            "created_at",
        ),
    )


__all__ = [
    "ALERT_RULES_TABLE",
    "ALERT_RULE_STATUSES",
    "ALERT_RULE_TYPES",
    "AlertRuleRow",
]
