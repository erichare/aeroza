"""SQLAlchemy ORM model for the MRMS file catalog table.

Catalog rows record the *existence* of an MRMS object on S3 — they are
metadata only. Decoded grids will live elsewhere (Zarr in a follow-up
slice); this table is what tells the rest of the system "an
``MergedReflectivityComposite`` grid for 12:20 UTC exists at this key".
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import BigInteger, DateTime, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from aeroza.shared.base import Base

MRMS_FILES_TABLE: str = "mrms_files"


class MrmsFileRow(Base):
    """One discovered MRMS object on AWS Open Data.

    The S3 key is the natural primary key — globally unique, stable across
    re-listings, encodes the product and timestamp.
    """

    __tablename__ = MRMS_FILES_TABLE

    key: Mapped[str] = mapped_column(Text, primary_key=True)
    product: Mapped[str] = mapped_column(String(64), nullable=False)
    level: Mapped[str] = mapped_column(String(16), nullable=False)
    valid_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    etag: Mapped[str | None] = mapped_column(String(64))
    discovered_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
