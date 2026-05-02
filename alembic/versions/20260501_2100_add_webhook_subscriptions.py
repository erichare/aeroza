"""add webhook_subscriptions table

Revision ID: 20260501_2100
Revises: 20260501_1700
Create Date: 2026-05-01 21:00:00

The first slice of Phase 4 (webhooks + alert rules). Creates the
subscription model only — the dispatcher worker, delivery log, and
alert-rule grammar land in follow-up migrations.

Each row records one external HTTP endpoint that wants to be notified
when one or more event types fire. ``events`` is a text[] of NATS-
style subject strings (e.g. ``aeroza.alerts.nws.new``); the same
taxonomy used inside the broker. ``secret`` is the HMAC signing key
generated server-side at create time.

The simplest possible status surface for now: ``active`` / ``paused``
/ ``disabled``. The dispatcher will move a subscription to ``disabled``
after sustained delivery failures; the operator API will let the user
re-activate it once the destination is healthy.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260501_2100"
down_revision: str | None = "20260501_1700"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "webhook_subscriptions",
        sa.Column(
            "id",
            sa.dialects.postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("secret", sa.Text(), nullable=False),
        # text[] over JSONB — each event matches exactly one NATS subject,
        # and Postgres array containment (`events @> ARRAY[?]`) is the
        # natural fan-out predicate the dispatcher needs in PR #6.
        sa.Column(
            "events",
            sa.dialects.postgresql.ARRAY(sa.Text()),
            nullable=False,
        ),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column(
            "status",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'active'"),
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
            "status IN ('active', 'paused', 'disabled')",
            name="webhook_subscriptions_status_valid",
        ),
        sa.CheckConstraint(
            "cardinality(events) > 0",
            name="webhook_subscriptions_events_nonempty",
        ),
    )
    # Fan-out: "find every active subscription that wants this event".
    # GIN over the events array makes containment queries index-backed.
    op.create_index(
        "ix_webhook_subscriptions_events_gin",
        "webhook_subscriptions",
        ["events"],
        postgresql_using="gin",
    )
    # Listing the operator's subscriptions in reverse chronological order
    # is the default in the API; cover it with an index on (status,
    # created_at desc) so the active set scans cheaply.
    op.create_index(
        "ix_webhook_subscriptions_status_created_at",
        "webhook_subscriptions",
        ["status", sa.text("created_at DESC")],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_webhook_subscriptions_status_created_at",
        table_name="webhook_subscriptions",
    )
    op.drop_index(
        "ix_webhook_subscriptions_events_gin",
        table_name="webhook_subscriptions",
    )
    op.drop_table("webhook_subscriptions")
