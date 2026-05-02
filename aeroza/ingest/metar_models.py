"""SQLAlchemy ORM mapping for ``metar_observations``.

Mirrors the ``20260502_1400`` migration. Optional fields use
``Mapped[T | None]`` so the ORM emits NULLABLE columns; the unique
constraint on ``(station_id, observation_time)`` powers the upsert
in :mod:`aeroza.ingest.metar_store`.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Final

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Float,
    Index,
    Integer,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from aeroza.shared.base import Base

METAR_OBSERVATIONS_TABLE: Final[str] = "metar_observations"


class MetarObservationRow(Base):
    __tablename__ = METAR_OBSERVATIONS_TABLE

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=func.gen_random_uuid(),
    )
    station_id: Mapped[str] = mapped_column(Text, nullable=False)
    observation_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    latitude: Mapped[float] = mapped_column(Float, nullable=False)
    longitude: Mapped[float] = mapped_column(Float, nullable=False)
    raw_text: Mapped[str] = mapped_column(Text, nullable=False)
    temp_c: Mapped[float | None] = mapped_column(Float, nullable=True)
    dewpoint_c: Mapped[float | None] = mapped_column(Float, nullable=True)
    wind_speed_kt: Mapped[float | None] = mapped_column(Float, nullable=True)
    wind_direction_deg: Mapped[int | None] = mapped_column(Integer, nullable=True)
    wind_gust_kt: Mapped[float | None] = mapped_column(Float, nullable=True)
    visibility_sm: Mapped[float | None] = mapped_column(Float, nullable=True)
    altimeter_hpa: Mapped[float | None] = mapped_column(Float, nullable=True)
    flight_category: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    __table_args__ = (
        UniqueConstraint(
            "station_id",
            "observation_time",
            name="uq_metar_observations_station_observation_time",
        ),
        CheckConstraint(
            "wind_speed_kt IS NULL OR wind_speed_kt >= 0",
            name="metar_observations_wind_speed_nonneg",
        ),
        CheckConstraint(
            "wind_gust_kt IS NULL OR wind_gust_kt >= 0",
            name="metar_observations_wind_gust_nonneg",
        ),
        CheckConstraint(
            "visibility_sm IS NULL OR visibility_sm >= 0",
            name="metar_observations_visibility_nonneg",
        ),
        CheckConstraint(
            "wind_direction_deg IS NULL OR (wind_direction_deg >= 0 AND wind_direction_deg <= 360)",
            name="metar_observations_wind_direction_range",
        ),
        Index(
            "ix_metar_observations_station_time",
            "station_id",
            "observation_time",
        ),
        Index(
            "ix_metar_observations_observation_time",
            "observation_time",
        ),
    )


__all__ = [
    "METAR_OBSERVATIONS_TABLE",
    "MetarObservationRow",
]
