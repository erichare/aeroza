"""add mrms_grids materialisation table

Revision ID: 20260501_1700
Revises: 20260501_1500
Create Date: 2026-05-01 17:00:00

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

revision: str = "20260501_1700"
down_revision: str | None = "20260501_1500"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "mrms_grids",
        sa.Column(
            "file_key",
            sa.Text(),
            sa.ForeignKey("mrms_files.key", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("zarr_uri", sa.Text(), nullable=False),
        sa.Column("variable", sa.String(length=64), nullable=False),
        sa.Column("dims_json", JSONB(), nullable=False),
        sa.Column("shape_json", JSONB(), nullable=False),
        sa.Column("dtype", sa.String(length=32), nullable=False),
        sa.Column("nbytes", sa.BigInteger(), nullable=False),
        sa.Column(
            "materialised_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.create_index("ix_mrms_grids_materialised_at", "mrms_grids", ["materialised_at"])
    op.create_index("ix_mrms_grids_variable", "mrms_grids", ["variable"])


def downgrade() -> None:
    op.drop_index("ix_mrms_grids_variable", table_name="mrms_grids")
    op.drop_index("ix_mrms_grids_materialised_at", table_name="mrms_grids")
    op.drop_table("mrms_grids")
