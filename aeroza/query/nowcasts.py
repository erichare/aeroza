"""Read-side query repository + wire schemas for nowcasts.

Mirrors :mod:`aeroza.query.mrms_grids` (the materialised-grid catalog)
with the nowcast-specific ``algorithm`` + ``forecastHorizonMinutes``
fields added. The wire shape is deliberately parallel so a UI rendering
"observation grids" + "predicted grids" can reuse one component.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Final, Literal

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aeroza.nowcast.models import NowcastRow

DEFAULT_LIMIT: Final[int] = 100
MAX_LIMIT: Final[int] = 500


@dataclass(frozen=True, slots=True)
class NowcastView:
    """Read projection of one nowcast row. Same shape as
    :class:`aeroza.query.mrms_grids.MrmsGridView` plus algorithm /
    horizon fields."""

    id: str
    source_file_key: str
    product: str
    level: str
    algorithm: str
    forecast_horizon_minutes: int
    valid_at: datetime
    zarr_uri: str
    variable: str
    dims: tuple[str, ...]
    shape: tuple[int, ...]
    dtype: str
    nbytes: int
    generated_at: datetime


async def find_nowcasts(
    session: AsyncSession,
    *,
    product: str | None = None,
    level: str | None = None,
    algorithm: str | None = None,
    horizon_minutes: int | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    limit: int = DEFAULT_LIMIT,
) -> tuple[NowcastView, ...]:
    """Return nowcasts ordered by ``valid_at`` descending."""
    bounded_limit = min(max(limit, 1), MAX_LIMIT)
    stmt = select(NowcastRow).order_by(NowcastRow.valid_at.desc()).limit(bounded_limit)
    if product is not None:
        stmt = stmt.where(NowcastRow.product == product)
    if level is not None:
        stmt = stmt.where(NowcastRow.level == level)
    if algorithm is not None:
        stmt = stmt.where(NowcastRow.algorithm == algorithm)
    if horizon_minutes is not None:
        stmt = stmt.where(NowcastRow.forecast_horizon_minutes == horizon_minutes)
    if since is not None:
        stmt = stmt.where(NowcastRow.valid_at >= since)
    if until is not None:
        stmt = stmt.where(NowcastRow.valid_at < until)
    result = await session.execute(stmt)
    rows = result.scalars().all()
    return tuple(_row_to_view(row) for row in rows)


def _row_to_view(row: NowcastRow) -> NowcastView:
    return NowcastView(
        id=str(row.id),
        source_file_key=row.source_file_key,
        product=row.product,
        level=row.level,
        algorithm=row.algorithm,
        forecast_horizon_minutes=row.forecast_horizon_minutes,
        valid_at=row.valid_at,
        zarr_uri=row.zarr_uri,
        variable=row.variable,
        dims=_jsonb_strings(row.dims_json),
        shape=_jsonb_ints(row.shape_json),
        dtype=row.dtype,
        nbytes=row.nbytes,
        generated_at=row.generated_at,
    )


def _jsonb_strings(raw: Any) -> tuple[str, ...]:
    if isinstance(raw, str):
        raw = json.loads(raw)
    return tuple(str(x) for x in raw)


def _jsonb_ints(raw: Any) -> tuple[int, ...]:
    if isinstance(raw, str):
        raw = json.loads(raw)
    return tuple(int(x) for x in raw)


# --------------------------------------------------------------------------- #
# Wire schemas


class NowcastItem(BaseModel):
    """One nowcast row, formatted for the wire."""

    model_config = ConfigDict(populate_by_name=True, frozen=True)

    id: str
    source_file_key: str = Field(serialization_alias="sourceFileKey")
    product: str
    level: str
    algorithm: str
    forecast_horizon_minutes: int = Field(serialization_alias="forecastHorizonMinutes")
    valid_at: datetime = Field(serialization_alias="validAt")
    zarr_uri: str = Field(serialization_alias="zarrUri")
    variable: str
    dims: tuple[str, ...]
    shape: tuple[int, ...]
    dtype: str
    nbytes: int
    generated_at: datetime = Field(serialization_alias="generatedAt")


class NowcastList(BaseModel):
    """Envelope returned by ``GET /v1/nowcasts``."""

    type: Literal["NowcastList"] = "NowcastList"
    items: list[NowcastItem]


def nowcast_view_to_item(view: NowcastView) -> NowcastItem:
    return NowcastItem(
        id=view.id,
        source_file_key=view.source_file_key,
        product=view.product,
        level=view.level,
        algorithm=view.algorithm,
        forecast_horizon_minutes=view.forecast_horizon_minutes,
        valid_at=view.valid_at,
        zarr_uri=view.zarr_uri,
        variable=view.variable,
        dims=view.dims,
        shape=view.shape,
        dtype=view.dtype,
        nbytes=view.nbytes,
        generated_at=view.generated_at,
    )


__all__ = [
    "DEFAULT_LIMIT",
    "MAX_LIMIT",
    "NowcastItem",
    "NowcastList",
    "NowcastView",
    "find_nowcasts",
    "nowcast_view_to_item",
]
