"""add mrms_files catalog table

Revision ID: 20260501_1500
Revises: 20260501_1300
Create Date: 2026-05-01 15:00:00

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260501_1500"
down_revision: str | None = "20260501_1300"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "mrms_files",
        sa.Column("key", sa.Text(), primary_key=True),
        sa.Column("product", sa.String(length=64), nullable=False),
        sa.Column("level", sa.String(length=16), nullable=False),
        sa.Column("valid_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
        sa.Column("etag", sa.String(length=64), nullable=True),
        sa.Column(
            "discovered_at",
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
    op.create_index("ix_mrms_files_product_valid_at", "mrms_files", ["product", "valid_at"])
    op.create_index("ix_mrms_files_valid_at", "mrms_files", ["valid_at"])


def downgrade() -> None:
    op.drop_index("ix_mrms_files_valid_at", table_name="mrms_files")
    op.drop_index("ix_mrms_files_product_valid_at", table_name="mrms_files")
    op.drop_table("mrms_files")
