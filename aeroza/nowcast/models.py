"""SQLAlchemy ORM model for the mrms_nowcasts table.

One row per (source observation grid, algorithm, horizon) tuple.
Same locator shape as :class:`aeroza.ingest.mrms_grids_models.MrmsGridRow`
— the field names are deliberately identical so the read-side wire
schema for nowcasts can mirror the materialised-grids schema with
just an added ``algorithm`` + ``forecastHorizonMinutes``.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Final

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from aeroza.shared.base import Base

MRMS_NOWCASTS_TABLE: Final[str] = "mrms_nowcasts"


class NowcastRow(Base):
    __tablename__ = MRMS_NOWCASTS_TABLE

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=func.gen_random_uuid(),
    )
    source_file_key: Mapped[str] = mapped_column(
        Text,
        ForeignKey("mrms_files.key", ondelete="CASCADE"),
        nullable=False,
    )
    product: Mapped[str] = mapped_column(String(128), nullable=False)
    level: Mapped[str] = mapped_column(String(16), nullable=False)
    algorithm: Mapped[str] = mapped_column(String(64), nullable=False)
    forecast_horizon_minutes: Mapped[int] = mapped_column(Integer, nullable=False)
    # Number of ensemble members the prediction Zarr stores along its
    # leading ``member`` dim. Defaults to 1 so deterministic forecasters
    # (``persistence``, ``pysteps``) write rows that look like an
    # ensemble of one — keeps the verifier's read path uniform.
    ensemble_size: Mapped[int] = mapped_column(Integer, nullable=False, server_default="1")
    valid_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    zarr_uri: Mapped[str] = mapped_column(Text, nullable=False)
    variable: Mapped[str] = mapped_column(String(64), nullable=False)
    dims_json: Mapped[Any] = mapped_column(JSONB, nullable=False)
    shape_json: Mapped[Any] = mapped_column(JSONB, nullable=False)
    dtype: Mapped[str] = mapped_column(String(32), nullable=False)
    nbytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    generated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    __table_args__ = (
        CheckConstraint(
            "forecast_horizon_minutes > 0",
            name="mrms_nowcasts_horizon_positive",
        ),
        CheckConstraint(
            "ensemble_size >= 1",
            name="mrms_nowcasts_ensemble_size_positive",
        ),
        UniqueConstraint(
            "source_file_key",
            "algorithm",
            "forecast_horizon_minutes",
            name="uq_mrms_nowcasts_source_algorithm_horizon",
        ),
        Index("ix_mrms_nowcasts_valid_at", "valid_at"),
        Index(
            "ix_mrms_nowcasts_algorithm_generated_at",
            "algorithm",
            "generated_at",
        ),
    )


__all__ = [
    "MRMS_NOWCASTS_TABLE",
    "NowcastRow",
]
