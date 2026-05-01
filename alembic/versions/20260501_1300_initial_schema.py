"""initial schema: postgis extension + nws_alerts table

Revision ID: 20260501_1300
Revises:
Create Date: 2026-05-01 13:00:00

"""

from __future__ import annotations

from collections.abc import Sequence

import geoalchemy2
import sqlalchemy as sa

from alembic import op

revision: str = "20260501_1300"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # PostGIS must exist before any GEOMETRY column can be created.
    op.execute("CREATE EXTENSION IF NOT EXISTS postgis")

    op.create_table(
        "nws_alerts",
        sa.Column("id", sa.Text(), primary_key=True),
        sa.Column("event", sa.Text(), nullable=False),
        sa.Column("headline", sa.Text(), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("instruction", sa.Text(), nullable=True),
        sa.Column(
            "severity", sa.String(length=16), nullable=False, server_default=sa.text("'Unknown'")
        ),
        sa.Column(
            "urgency", sa.String(length=16), nullable=False, server_default=sa.text("'Unknown'")
        ),
        sa.Column(
            "certainty", sa.String(length=16), nullable=False, server_default=sa.text("'Unknown'")
        ),
        sa.Column("sender_name", sa.Text(), nullable=True),
        sa.Column("area_desc", sa.Text(), nullable=True),
        sa.Column("effective", sa.DateTime(timezone=True), nullable=True),
        sa.Column("onset", sa.DateTime(timezone=True), nullable=True),
        sa.Column("expires", sa.DateTime(timezone=True), nullable=True),
        sa.Column("ends", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "geometry",
            geoalchemy2.types.Geometry(geometry_type="GEOMETRY", srid=4326),
            nullable=True,
        ),
        sa.Column(
            "inserted_at",
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
    )

    op.create_index("ix_nws_alerts_event", "nws_alerts", ["event"])
    op.create_index("ix_nws_alerts_expires", "nws_alerts", ["expires"])
    op.create_index("ix_nws_alerts_severity", "nws_alerts", ["severity"])


def downgrade() -> None:
    op.drop_index("ix_nws_alerts_severity", table_name="nws_alerts")
    op.drop_index("ix_nws_alerts_expires", table_name="nws_alerts")
    op.drop_index("ix_nws_alerts_event", table_name="nws_alerts")
    # GeoAlchemy2 registers a GIST index on the geometry column automatically;
    # drop_table removes it implicitly.
    op.drop_table("nws_alerts")
    # We intentionally do not drop the postgis extension on downgrade — other
    # schema changes might rely on it, and reinstalling is a privileged op.
