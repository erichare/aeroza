"""SQLAlchemy ORM model for the persisted NWS alerts table.

Kept separate from :mod:`aeroza.ingest.nws_alerts` so the pure async fetcher
(no DB dep) and the persistence layer can evolve independently — matches the
``shared.base`` / ``shared.db`` split.
"""

from __future__ import annotations

from datetime import datetime

from geoalchemy2 import Geometry, WKBElement
from sqlalchemy import DateTime, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from aeroza.shared.base import Base

NWS_ALERTS_TABLE: str = "nws_alerts"
NWS_ALERTS_GEOMETRY_TYPE: str = "GEOMETRY"
NWS_ALERTS_SRID: int = 4326


class NwsAlertRow(Base):
    """Persistent row backing one active NWS alert.

    Severity / urgency / certainty are stored as plain strings (rather than a
    Postgres ENUM) so adding a new value upstream doesn't require a migration.
    The geometry column is intentionally typed ``GEOMETRY`` (not ``POLYGON``)
    because NWS occasionally emits ``MultiPolygon`` and even ``GeometryCollection``.
    """

    __tablename__ = NWS_ALERTS_TABLE

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    event: Mapped[str] = mapped_column(Text, nullable=False)
    headline: Mapped[str | None] = mapped_column(Text)
    description: Mapped[str | None] = mapped_column(Text)
    instruction: Mapped[str | None] = mapped_column(Text)
    severity: Mapped[str] = mapped_column(String(16), nullable=False, default="Unknown")
    urgency: Mapped[str] = mapped_column(String(16), nullable=False, default="Unknown")
    certainty: Mapped[str] = mapped_column(String(16), nullable=False, default="Unknown")
    sender_name: Mapped[str | None] = mapped_column(Text)
    area_desc: Mapped[str | None] = mapped_column(Text)
    effective: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    onset: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    expires: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    ends: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    geometry: Mapped[WKBElement | None] = mapped_column(
        Geometry(geometry_type=NWS_ALERTS_GEOMETRY_TYPE, srid=NWS_ALERTS_SRID)
    )
    inserted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
