"""add mrms_nowcasts and nowcast_verifications tables

Revision ID: 20260502_0000
Revises: 20260501_2300
Create Date: 2026-05-02 00:00:00

Phase 3 — predictive nowcasting + verification scaffolding. Two
tables landing together because the verification table's foreign
keys reference the nowcasts table; splitting them across migrations
would buy us nothing.

mrms_nowcasts
-------------
One row per (source grid, forecast horizon) pair. The pipeline runs
on every materialised grid event: given the observation grid at time
T, produce N predictions valid at T + 10/30/60 minutes (the
horizons Phase 5's UI will surface as a scrubbable timeline).

The v1 algorithm is *persistence* (forecast == observation) — the
plan's documented baseline that real nowcasting (pySTEPS, NowcastNet)
must beat. Storing predictions even from a trivial baseline means
the verification pipeline has data to score from day one, which is
the §3.3 moat: a public reliability diagram you can point at.

nowcast_verifications
---------------------
One row per (nowcast, observation) pair. Populated when an
observation grid arrives whose ``valid_at`` matches a previously
generated nowcast's ``valid_at``: we look up every forecast that
predicted this moment and score them against the truth. MAE / bias /
RMSE are the deterministic-forecast scoring metrics; Brier
/ reliability bins land once we have probabilistic forecasts (which
v1 persistence doesn't produce).
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260502_0000"
down_revision: str | None = "20260501_2300"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "mrms_nowcasts",
        sa.Column(
            "id",
            sa.dialects.postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        # The observation that this forecast was issued from. CASCADE
        # so deleting the source grid (e.g. via a backfill rerun)
        # cleans the nowcasts derived from it.
        sa.Column(
            "source_file_key",
            sa.Text(),
            sa.ForeignKey("mrms_files.key", ondelete="CASCADE"),
            nullable=False,
        ),
        # Mirrors the source grid's product/level so the
        # ``/v1/calibration`` aggregator can group without a join.
        sa.Column("product", sa.String(length=128), nullable=False),
        sa.Column("level", sa.String(length=16), nullable=False),
        # Algorithm tag — "persistence" today, "pysteps" / "nowcastnet"
        # in future. Stored on the row so a single calibration query
        # can compare algorithms head-to-head.
        sa.Column("algorithm", sa.String(length=64), nullable=False),
        # Lead time in minutes (10, 30, 60).
        sa.Column("forecast_horizon_minutes", sa.Integer(), nullable=False),
        # The wall-clock moment this forecast is *for*. Equal to the
        # source grid's valid_at + horizon.
        sa.Column("valid_at", sa.DateTime(timezone=True), nullable=False),
        # Same locator shape as mrms_grids — Zarr URI, dims/shape/dtype/nbytes.
        sa.Column("zarr_uri", sa.Text(), nullable=False),
        sa.Column("variable", sa.String(length=64), nullable=False),
        sa.Column(
            "dims_json",
            sa.dialects.postgresql.JSONB(),
            nullable=False,
        ),
        sa.Column(
            "shape_json",
            sa.dialects.postgresql.JSONB(),
            nullable=False,
        ),
        sa.Column("dtype", sa.String(length=32), nullable=False),
        sa.Column("nbytes", sa.BigInteger(), nullable=False),
        sa.Column(
            "generated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.CheckConstraint(
            "forecast_horizon_minutes > 0",
            name="mrms_nowcasts_horizon_positive",
        ),
        sa.UniqueConstraint(
            "source_file_key",
            "algorithm",
            "forecast_horizon_minutes",
            name="uq_mrms_nowcasts_source_algorithm_horizon",
        ),
    )
    op.create_index(
        "ix_mrms_nowcasts_valid_at",
        "mrms_nowcasts",
        ["valid_at"],
    )
    # Per-algorithm freshness query for `/v1/calibration` and the UI.
    op.create_index(
        "ix_mrms_nowcasts_algorithm_generated_at",
        "mrms_nowcasts",
        ["algorithm", sa.text("generated_at DESC")],
    )

    op.create_table(
        "nowcast_verifications",
        sa.Column(
            "id",
            sa.dialects.postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "nowcast_id",
            sa.dialects.postgresql.UUID(as_uuid=True),
            sa.ForeignKey("mrms_nowcasts.id", ondelete="CASCADE"),
            nullable=False,
        ),
        # The observation grid we verified against. CASCADE so a
        # source-data rerun also clears the verifications it would
        # invalidate.
        sa.Column(
            "observation_file_key",
            sa.Text(),
            sa.ForeignKey("mrms_files.key", ondelete="CASCADE"),
            nullable=False,
        ),
        # Convenience denormalisation — same product/level/horizon as
        # the nowcast row. Lets the calibration aggregator group
        # without joining nowcasts.
        sa.Column("product", sa.String(length=128), nullable=False),
        sa.Column("level", sa.String(length=16), nullable=False),
        sa.Column("algorithm", sa.String(length=64), nullable=False),
        sa.Column("forecast_horizon_minutes", sa.Integer(), nullable=False),
        sa.Column("valid_at", sa.DateTime(timezone=True), nullable=False),
        # Deterministic scoring metrics. Float, all in the same units
        # as the observation variable (dBZ for reflectivity).
        sa.Column("mae", sa.Float(), nullable=False),
        sa.Column("bias", sa.Float(), nullable=False),
        sa.Column("rmse", sa.Float(), nullable=False),
        sa.Column("sample_count", sa.Integer(), nullable=False),
        sa.Column(
            "verified_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.CheckConstraint(
            "sample_count >= 0",
            name="nowcast_verifications_sample_count_nonneg",
        ),
        sa.UniqueConstraint(
            "nowcast_id",
            "observation_file_key",
            name="uq_nowcast_verifications_nowcast_observation",
        ),
    )
    # Aggregator path: "every verification, latest first, optionally
    # filtered by algorithm/horizon".
    op.create_index(
        "ix_nowcast_verifications_algorithm_horizon_verified_at",
        "nowcast_verifications",
        [
            "algorithm",
            "forecast_horizon_minutes",
            sa.text("verified_at DESC"),
        ],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_nowcast_verifications_algorithm_horizon_verified_at",
        table_name="nowcast_verifications",
    )
    op.drop_table("nowcast_verifications")
    op.drop_index("ix_mrms_nowcasts_algorithm_generated_at", table_name="mrms_nowcasts")
    op.drop_index("ix_mrms_nowcasts_valid_at", table_name="mrms_nowcasts")
    op.drop_table("mrms_nowcasts")
