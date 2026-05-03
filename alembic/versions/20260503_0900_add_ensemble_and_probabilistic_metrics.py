"""add ensemble_size to mrms_nowcasts and probabilistic metrics to nowcast_verifications

Revision ID: 20260503_0900
Revises: 20260502_1400
Create Date: 2026-05-03 09:00:00

Phase 6f — ensemble-aware nowcasts and probabilistic verification.

Two paired changes:

1. ``mrms_nowcasts.ensemble_size`` (NOT NULL, default 1). Records the
   number of ensemble members the prediction Zarr stores along its
   leading ``member`` dim. Default 1 keeps deterministic forecasters
   (``persistence``, ``pysteps``) round-tripping unchanged — their
   rows look like an ensemble of one. The check constraint
   ``ensemble_size >= 1`` rules out the meaningless zero case.

2. ``nowcast_verifications`` gains nullable ``ensemble_size``,
   ``brier_score``, and ``crps`` columns. Nullable because they only
   make sense for genuine ensembles; older deterministic rows stay
   as NULL and the calibration aggregator surfaces null in the API.

Forward-only migration: no data backfill needed. The existing
verifier path that scores deterministic forecasts continues to write
NULL into the new columns.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260503_0900"
down_revision: str | None = "20260502_1400"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "mrms_nowcasts",
        sa.Column("ensemble_size", sa.Integer(), nullable=False, server_default="1"),
    )
    op.create_check_constraint(
        "mrms_nowcasts_ensemble_size_positive",
        "mrms_nowcasts",
        "ensemble_size >= 1",
    )

    op.add_column(
        "nowcast_verifications",
        sa.Column("ensemble_size", sa.Integer(), nullable=True),
    )
    op.add_column(
        "nowcast_verifications",
        sa.Column("brier_score", sa.Float(), nullable=True),
    )
    op.add_column(
        "nowcast_verifications",
        sa.Column("crps", sa.Float(), nullable=True),
    )
    op.create_check_constraint(
        "nowcast_verifications_ensemble_size_positive",
        "nowcast_verifications",
        "ensemble_size IS NULL OR ensemble_size >= 1",
    )


def downgrade() -> None:
    op.drop_constraint(
        "nowcast_verifications_ensemble_size_positive",
        "nowcast_verifications",
        type_="check",
    )
    op.drop_column("nowcast_verifications", "crps")
    op.drop_column("nowcast_verifications", "brier_score")
    op.drop_column("nowcast_verifications", "ensemble_size")

    op.drop_constraint(
        "mrms_nowcasts_ensemble_size_positive",
        "mrms_nowcasts",
        type_="check",
    )
    op.drop_column("mrms_nowcasts", "ensemble_size")
