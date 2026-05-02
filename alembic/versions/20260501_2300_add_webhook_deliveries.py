"""add webhook_deliveries table

Revision ID: 20260501_2300
Revises: 20260501_2200
Create Date: 2026-05-01 23:00:00

The third slice of Phase 4 (webhooks + alert rules). Records one row
per delivery attempt: which subscription, what payload, what
HTTP outcome. The dispatcher worker writes these as it works through
its retry policy; the operator UI (future) reads them to surface
delivery health.

A row exists per attempt — a single event that succeeds on the third
retry produces three rows (two ``failed`` + one ``ok``). That keeps
debugging "why does this webhook keep failing" boring SQL instead of
parsing logs.

The ``rule_id`` column is nullable: raw event-fan-out deliveries set
it to NULL; rule-triggered deliveries point at the rule that fired.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260501_2300"
down_revision: str | None = "20260501_2200"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "webhook_deliveries",
        sa.Column(
            "id",
            sa.dialects.postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "subscription_id",
            sa.dialects.postgresql.UUID(as_uuid=True),
            sa.ForeignKey("webhook_subscriptions.id", ondelete="CASCADE"),
            nullable=False,
        ),
        # NULL for raw-fan-out deliveries; points at the rule for
        # rule-triggered deliveries. ON DELETE SET NULL so an operator
        # deleting a rule keeps its delivery history intact.
        sa.Column(
            "rule_id",
            sa.dialects.postgresql.UUID(as_uuid=True),
            sa.ForeignKey("alert_rules.id", ondelete="SET NULL"),
            nullable=True,
        ),
        # NATS subject the delivery is sourced from
        # (``aeroza.alerts.nws.new`` etc.) or
        # ``aeroza.alert_rules.fired`` for rule deliveries.
        sa.Column("event_type", sa.Text(), nullable=False),
        # The signed JSON we POSTed (or attempted to). Stored verbatim
        # so a consumer asking "what exactly did you send me?" gets a
        # truthful answer.
        sa.Column(
            "payload",
            sa.dialects.postgresql.JSONB(),
            nullable=False,
        ),
        sa.Column(
            "status",
            sa.Text(),
            nullable=False,
        ),
        # Attempt counter: 1 = first try, 2 = first retry, etc.
        sa.Column("attempt", sa.Integer(), nullable=False),
        # Populated on success (HTTP 2xx); the response body's first ~1KB
        # for debugging.
        sa.Column("response_status", sa.Integer(), nullable=True),
        sa.Column("response_body_preview", sa.Text(), nullable=True),
        # Populated on failure; the exception message or the non-2xx body
        # excerpt.
        sa.Column("error_reason", sa.Text(), nullable=True),
        # Wall-clock duration of the HTTP attempt in milliseconds.
        sa.Column("duration_ms", sa.Integer(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.CheckConstraint(
            "status IN ('ok', 'failed', 'retrying')",
            name="webhook_deliveries_status_valid",
        ),
        sa.CheckConstraint(
            "attempt >= 1",
            name="webhook_deliveries_attempt_positive",
        ),
    )
    # Operator's "show me deliveries for this subscription" query.
    op.create_index(
        "ix_webhook_deliveries_subscription_created_at",
        "webhook_deliveries",
        ["subscription_id", sa.text("created_at DESC")],
    )
    # Dispatcher's circuit-breaker query: "how many consecutive failures
    # has this subscription had recently?"
    op.create_index(
        "ix_webhook_deliveries_subscription_status",
        "webhook_deliveries",
        ["subscription_id", "status", sa.text("created_at DESC")],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_webhook_deliveries_subscription_status",
        table_name="webhook_deliveries",
    )
    op.drop_index(
        "ix_webhook_deliveries_subscription_created_at",
        table_name="webhook_deliveries",
    )
    op.drop_table("webhook_deliveries")
