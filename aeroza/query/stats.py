"""Compact "what does the system know right now?" snapshot.

A few cheap aggregate counts that the dev console (and anyone running
``curl /v1/stats``) can use to confirm the platform is breathing:

- ``alerts.active`` / ``alerts.total`` — counts of NWS alerts.
- ``mrms.files`` / ``mrms.gridsMaterialised`` / ``mrms.filesPending`` —
  catalog and materialisation queue depth.

All numbers come from a single ``SELECT`` with grouped subqueries, so
the route is one round-trip regardless of how many counters we add.
Field names are camelCased to match the rest of the v1 wire surface.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Final, Literal

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from aeroza.ingest.mrms_grids_models import MrmsGridRow
from aeroza.ingest.mrms_models import MrmsFileRow
from aeroza.ingest.nws_alerts_models import NwsAlertRow

__all__ = [
    "AlertsStats",
    "MrmsStats",
    "Stats",
    "StatsView",
    "compute_stats",
    "stats_view_to_model",
]


# --------------------------------------------------------------------------- #
# Internal projection                                                          #
# --------------------------------------------------------------------------- #


@dataclass(frozen=True, slots=True)
class StatsView:
    alerts_total: int
    alerts_active: int
    mrms_files: int
    mrms_grids_materialised: int
    mrms_files_pending: int
    latest_alert_expires: datetime | None
    latest_mrms_valid_at: datetime | None
    latest_grid_materialised_at: datetime | None


# --------------------------------------------------------------------------- #
# Repository                                                                   #
# --------------------------------------------------------------------------- #


_DEFAULT_NOW: Final[None] = None


async def compute_stats(
    session: AsyncSession,
    *,
    now: datetime | None = _DEFAULT_NOW,
) -> StatsView:
    """Compute the live stats snapshot.

    ``now`` is injectable for tests; production callers leave it ``None``
    and the function uses ``datetime.now(UTC)``.
    """
    moment = now if now is not None else datetime.now(UTC)

    alerts_total = (
        await session.execute(select(func.count()).select_from(NwsAlertRow))
    ).scalar_one()
    alerts_active = (
        await session.execute(
            select(func.count())
            .select_from(NwsAlertRow)
            .where((NwsAlertRow.expires.is_(None)) | (NwsAlertRow.expires > moment))
        )
    ).scalar_one()
    latest_alert_expires = (
        await session.execute(select(func.max(NwsAlertRow.expires)))
    ).scalar_one()

    mrms_files = (await session.execute(select(func.count()).select_from(MrmsFileRow))).scalar_one()
    mrms_grids = (await session.execute(select(func.count()).select_from(MrmsGridRow))).scalar_one()
    latest_mrms_valid_at = (
        await session.execute(select(func.max(MrmsFileRow.valid_at)))
    ).scalar_one()
    latest_grid_materialised_at = (
        await session.execute(select(func.max(MrmsGridRow.materialised_at)))
    ).scalar_one()

    return StatsView(
        alerts_total=int(alerts_total),
        alerts_active=int(alerts_active),
        mrms_files=int(mrms_files),
        mrms_grids_materialised=int(mrms_grids),
        mrms_files_pending=int(mrms_files) - int(mrms_grids),
        latest_alert_expires=latest_alert_expires,
        latest_mrms_valid_at=latest_mrms_valid_at,
        latest_grid_materialised_at=latest_grid_materialised_at,
    )


# --------------------------------------------------------------------------- #
# Wire schemas                                                                 #
# --------------------------------------------------------------------------- #


class AlertsStats(BaseModel):
    """Per-domain rollup for NWS alerts."""

    model_config = ConfigDict(populate_by_name=True, frozen=True)

    total: int
    active: int
    latest_expires: datetime | None = Field(default=None, serialization_alias="latestExpires")


class MrmsStats(BaseModel):
    """Per-domain rollup for the MRMS catalog + materialised grids."""

    model_config = ConfigDict(populate_by_name=True, frozen=True)

    files: int
    grids_materialised: int = Field(serialization_alias="gridsMaterialised")
    files_pending: int = Field(serialization_alias="filesPending")
    latest_valid_at: datetime | None = Field(
        default=None,
        serialization_alias="latestValidAt",
    )
    latest_grid_materialised_at: datetime | None = Field(
        default=None,
        serialization_alias="latestGridMaterialisedAt",
    )


class Stats(BaseModel):
    """Top-level envelope returned by ``GET /v1/stats``."""

    type: Literal["Stats"] = "Stats"
    generated_at: datetime = Field(serialization_alias="generatedAt")
    alerts: AlertsStats
    mrms: MrmsStats


def stats_view_to_model(view: StatsView, *, generated_at: datetime) -> Stats:
    return Stats(
        generated_at=generated_at,
        alerts=AlertsStats(
            total=view.alerts_total,
            active=view.alerts_active,
            latest_expires=view.latest_alert_expires,
        ),
        mrms=MrmsStats(
            files=view.mrms_files,
            grids_materialised=view.mrms_grids_materialised,
            files_pending=view.mrms_files_pending,
            latest_valid_at=view.latest_mrms_valid_at,
            latest_grid_materialised_at=view.latest_grid_materialised_at,
        ),
    )
