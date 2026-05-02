"""add alert_rules table

Revision ID: 20260501_2200
Revises: 20260501_2100
Create Date: 2026-05-01 22:00:00

The second slice of Phase 4 (webhooks + alert rules). Each row is one
named predicate over the latest materialised MRMS grid, bound to a
:class:`WebhookSubscriptionRow` via foreign key. The dispatcher worker
(slice 3) evaluates every active rule on each new grid and POSTs to
the bound subscription when the predicate transitions from false to
true.

Rule types
----------

- ``point``  — sample at a (lat, lng); the predicate is checked
  against that scalar.
- ``polygon`` — reduce the polygon's cells via ``max``/``mean``/
  ``min``/``count_ge``; the predicate is checked against the
  reducer's output.

Both shapes share the same ``predicate`` ({op, threshold}) and live
in the JSONB ``config`` column. Pydantic validators enforce the
shape at the API boundary; the DB CHECK constraints below are the
last-line guard.

Evaluation state
----------------

``currently_firing`` is the load-bearing FSM bit the dispatcher uses
to detect false→true transitions: a rule that stays true across N
consecutive grids fires once, not N times. ``last_value`` /
``last_evaluated_at`` are introspection columns the operator UI
will surface in slice 3.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260501_2200"
down_revision: str | None = "20260501_2100"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "alert_rules",
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
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("rule_type", sa.Text(), nullable=False),
        # Per-rule-type payload. Schema validated by pydantic at the API
        # boundary; the JSONB column accepts anything, the DB CHECK below
        # only guards the discriminator.
        sa.Column(
            "config",
            sa.dialects.postgresql.JSONB(),
            nullable=False,
        ),
        sa.Column(
            "status",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'active'"),
        ),
        # FSM state for the dispatcher.
        sa.Column(
            "currently_firing",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.Column("last_value", sa.Float(), nullable=True),
        sa.Column("last_evaluated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_fired_at", sa.DateTime(timezone=True), nullable=True),
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
            "rule_type IN ('point', 'polygon')",
            name="alert_rules_type_valid",
        ),
        sa.CheckConstraint(
            "status IN ('active', 'paused', 'disabled')",
            name="alert_rules_status_valid",
        ),
    )
    # Per-subscription listing — the operator UI's primary view ("show
    # me all rules under this webhook").
    op.create_index(
        "ix_alert_rules_subscription_id",
        "alert_rules",
        ["subscription_id"],
    )
    # Dispatcher fan-out — "give me every active rule, newest first".
    op.create_index(
        "ix_alert_rules_status_created_at",
        "alert_rules",
        ["status", sa.text("created_at DESC")],
    )


def downgrade() -> None:
    op.drop_index("ix_alert_rules_status_created_at", table_name="alert_rules")
    op.drop_index("ix_alert_rules_subscription_id", table_name="alert_rules")
    op.drop_table("alert_rules")
