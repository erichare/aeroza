"""add categorical (threshold-exceedance) metrics to nowcast_verifications

Revision ID: 20260502_1200
Revises: 20260502_0000
Create Date: 2026-05-02 12:00:00

Phase 6b — operational categorical verification. Adds the
contingency-table columns the verifier needs to compute POD / FAR /
CSI for a configurable dBZ threshold (default 35 dBZ).

Why these and not Brier: Brier needs probabilistic forecasts (an
ensemble), which we don't have yet. POD/FAR/CSI are the
deterministic equivalents — "did we get the threshold crossing
right?" — and they're what every operational forecaster looks at
first.

All four count columns default to 0 so older rows (written before
this migration ran) round-trip cleanly when the route aggregator
sums them. The threshold itself is nullable: existing rows have it
as NULL ("we didn't compute categorical metrics for this row"), new
rows always carry the threshold the row was scored against.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260502_1200"
down_revision: str | None = "20260502_0000"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "nowcast_verifications",
        sa.Column("threshold_dbz", sa.Float(), nullable=True),
    )
    for col in ("hits", "misses", "false_alarms", "correct_negatives"):
        op.add_column(
            "nowcast_verifications",
            sa.Column(col, sa.Integer(), nullable=False, server_default="0"),
        )
    op.create_check_constraint(
        "nowcast_verifications_contingency_nonneg",
        "nowcast_verifications",
        "hits >= 0 AND misses >= 0 AND false_alarms >= 0 AND correct_negatives >= 0",
    )


def downgrade() -> None:
    op.drop_constraint(
        "nowcast_verifications_contingency_nonneg",
        "nowcast_verifications",
        type_="check",
    )
    for col in ("correct_negatives", "false_alarms", "misses", "hits", "threshold_dbz"):
        op.drop_column("nowcast_verifications", col)
