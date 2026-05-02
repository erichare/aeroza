"""SQLAlchemy ORM model for the nowcast_verifications table.

One row per (nowcast, observation) pair — the verification worker
populates these as observations arrive whose ``valid_at`` matches a
previously-issued forecast.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Final

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from aeroza.shared.base import Base

NOWCAST_VERIFICATIONS_TABLE: Final[str] = "nowcast_verifications"


class VerificationRow(Base):
    __tablename__ = NOWCAST_VERIFICATIONS_TABLE

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=func.gen_random_uuid(),
    )
    nowcast_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("mrms_nowcasts.id", ondelete="CASCADE"),
        nullable=False,
    )
    observation_file_key: Mapped[str] = mapped_column(
        Text,
        ForeignKey("mrms_files.key", ondelete="CASCADE"),
        nullable=False,
    )
    product: Mapped[str] = mapped_column(String(128), nullable=False)
    level: Mapped[str] = mapped_column(String(16), nullable=False)
    algorithm: Mapped[str] = mapped_column(String(64), nullable=False)
    forecast_horizon_minutes: Mapped[int] = mapped_column(Integer, nullable=False)
    valid_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    mae: Mapped[float] = mapped_column(Float, nullable=False)
    bias: Mapped[float] = mapped_column(Float, nullable=False)
    rmse: Mapped[float] = mapped_column(Float, nullable=False)
    sample_count: Mapped[int] = mapped_column(Integer, nullable=False)
    verified_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    __table_args__ = (
        CheckConstraint(
            "sample_count >= 0",
            name="nowcast_verifications_sample_count_nonneg",
        ),
        UniqueConstraint(
            "nowcast_id",
            "observation_file_key",
            name="uq_nowcast_verifications_nowcast_observation",
        ),
        Index(
            "ix_nowcast_verifications_algorithm_horizon_verified_at",
            "algorithm",
            "forecast_horizon_minutes",
            "verified_at",
        ),
    )


__all__ = [
    "NOWCAST_VERIFICATIONS_TABLE",
    "VerificationRow",
]
