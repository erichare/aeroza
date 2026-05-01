"""SQLAlchemy ORM model for the materialised MRMS grids table.

Catalog-of-grids: each row records one MRMS file that has been decoded
and written to a Zarr store. The S3 key (foreign-keyed to
``mrms_files``) is the natural primary key — at most one materialisation
per source file.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import BigInteger, DateTime, ForeignKey, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from aeroza.shared.base import Base

MRMS_GRIDS_TABLE: str = "mrms_grids"


class MrmsGridRow(Base):
    """One materialised MRMS grid stored as Zarr.

    ``dims_json`` and ``shape_json`` are JSONB so the schema doesn't bake
    in a fixed dimensionality — useful when a future variable comes
    with a third (e.g. height) dimension.
    """

    __tablename__ = MRMS_GRIDS_TABLE

    file_key: Mapped[str] = mapped_column(
        Text,
        ForeignKey("mrms_files.key", ondelete="CASCADE"),
        primary_key=True,
    )
    zarr_uri: Mapped[str] = mapped_column(Text, nullable=False)
    variable: Mapped[str] = mapped_column(String(64), nullable=False)
    dims_json: Mapped[Any] = mapped_column(JSONB, nullable=False)
    shape_json: Mapped[Any] = mapped_column(JSONB, nullable=False)
    dtype: Mapped[str] = mapped_column(String(32), nullable=False)
    nbytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    materialised_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
