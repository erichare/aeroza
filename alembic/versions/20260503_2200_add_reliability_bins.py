"""add reliability_bins JSONB to nowcast_verifications

Revision ID: 20260503_2200
Revises: 20260503_0900
Create Date: 2026-05-03 22:00:00

Phase 6f.1 — reliability diagrams.

The Brier score Phase 6f shipped is the *aggregate* of probabilistic
calibration. The reliability diagram is the breakdown: bin the
ensemble's event-probability forecasts, plot observed frequency per
bin, compare to the diagonal. Visually it's the canonical "how
calibrated is this forecast?" picture every weather paper ships.

Schema choice: JSONB ``reliability_bins`` rather than a side table.
Each bin carries 4 fields (lower-edge probability, count of cells in
this probability bucket, count of observed events in those cells,
average forecast prob in those cells), 10 bins per row, ~400 bytes
per verification. Verifications are already a dense write path; a
side table would multiply joins on the aggregator's hot query
without paying for itself.

Forward-only. Existing rows get NULL — the aggregator surfaces the
reliability response as ``null`` when no bin data has been written
yet, matching the same convention Brier/CRPS use.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

revision: str = "20260503_2200"
down_revision: str | None = "20260503_0900"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "nowcast_verifications",
        sa.Column("reliability_bins", JSONB(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("nowcast_verifications", "reliability_bins")
