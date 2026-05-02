"""add metar_observations table

Revision ID: 20260502_1400
Revises: 20260502_1300
Create Date: 2026-05-02 14:00:00

Phase 6d — METAR (surface-station observations) ingest.

The first ingest source outside MRMS / NWS alerts. Each row is one
observation from one ICAO-identified airport, fetched from the
Aviation Weather Center JSON endpoint. AWC's API already returns
parsed values, so we don't carry a raw-METAR parser; the ``raw_text``
column preserves the original string for callers who want one.

All measurement columns are nullable: AWC drops sensor readings that
weren't reporting, but we still want the row keyed by
``(station_id, observation_time)`` so downstream "last seen" /
"latest at airport X" queries always hit a real index.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260502_1400"
down_revision: str | None = "20260502_1300"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "metar_observations",
        sa.Column(
            "id",
            sa.dialects.postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        # ICAO 4-letter id (e.g. KIAH, EGLL). Always uppercase.
        sa.Column("station_id", sa.Text(), nullable=False),
        sa.Column("observation_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("latitude", sa.Float(), nullable=False),
        sa.Column("longitude", sa.Float(), nullable=False),
        # The original METAR string. Preserves anything the parsed
        # fields drop, including remarks and runway visual ranges.
        sa.Column("raw_text", sa.Text(), nullable=False),
        sa.Column("temp_c", sa.Float(), nullable=True),
        sa.Column("dewpoint_c", sa.Float(), nullable=True),
        sa.Column("wind_speed_kt", sa.Float(), nullable=True),
        sa.Column("wind_direction_deg", sa.Integer(), nullable=True),
        sa.Column("wind_gust_kt", sa.Float(), nullable=True),
        sa.Column("visibility_sm", sa.Float(), nullable=True),
        # Altimeter in hPa (the AWC JSON uses hPa even though raw
        # METAR uses inches of mercury). Document on the wire which it
        # is; converting at the edge is the consumer's job.
        sa.Column("altimeter_hpa", sa.Float(), nullable=True),
        # Flight category — AWC returns one of VFR / MVFR / IFR / LIFR
        # or null. We don't add a CHECK constraint because a future
        # category from AWC shouldn't break ingest.
        sa.Column("flight_category", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        # Each station publishes one observation per cycle (~hourly,
        # plus SPECI updates between cycles). The unique constraint
        # makes upserts idempotent and keeps duplicate-fetch protection
        # in one place.
        sa.UniqueConstraint(
            "station_id",
            "observation_time",
            name="uq_metar_observations_station_observation_time",
        ),
        sa.CheckConstraint(
            "wind_speed_kt IS NULL OR wind_speed_kt >= 0",
            name="metar_observations_wind_speed_nonneg",
        ),
        sa.CheckConstraint(
            "wind_gust_kt IS NULL OR wind_gust_kt >= 0",
            name="metar_observations_wind_gust_nonneg",
        ),
        sa.CheckConstraint(
            "visibility_sm IS NULL OR visibility_sm >= 0",
            name="metar_observations_visibility_nonneg",
        ),
        sa.CheckConstraint(
            "wind_direction_deg IS NULL OR (wind_direction_deg >= 0 AND wind_direction_deg <= 360)",
            name="metar_observations_wind_direction_range",
        ),
    )
    # The hot read: "show me everything from the last hour" or "everything
    # for KIAH this week". (station_id, observation_time DESC) covers both
    # without a second index.
    op.create_index(
        "ix_metar_observations_station_time",
        "metar_observations",
        ["station_id", sa.text("observation_time DESC")],
    )
    # The other hot read: "everything in this bbox right now" — driven
    # by observation_time alone. Indexed DESC to support the "latest"
    # ordering with no extra sort.
    op.create_index(
        "ix_metar_observations_observation_time",
        "metar_observations",
        [sa.text("observation_time DESC")],
    )


def downgrade() -> None:
    op.drop_index("ix_metar_observations_observation_time", table_name="metar_observations")
    op.drop_index("ix_metar_observations_station_time", table_name="metar_observations")
    op.drop_table("metar_observations")
