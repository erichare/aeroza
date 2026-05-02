"""Read-side query repository + wire schemas for the materialised MRMS grids.

Mirrors :mod:`aeroza.query.mrms` (the file catalog) but joins through
``mrms_grids`` so the result shape is locator-centric: every row is one
materialised Zarr store, with the catalog's product/level/valid_at
projected in alongside the locator metadata.

Wire shape — flat envelope, camelCase aliases — matches the alerts and
file-catalog conventions:

    {"items": [{"fileKey": …, "zarrUri": …, "validAt": …, "shape": [3500,7000], …}, …]}

Single-grid detail uses the same item shape (no extra detail fields
beyond what the list returns) — keeping list and detail in lockstep
means the dev console can render either with one component.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Final, Literal

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aeroza.ingest.mrms_grids_models import MrmsGridRow
from aeroza.ingest.mrms_models import MrmsFileRow

DEFAULT_LIMIT: Final[int] = 100
MAX_LIMIT: Final[int] = 500


# --------------------------------------------------------------------------- #
# Internal projection                                                          #
# --------------------------------------------------------------------------- #


@dataclass(frozen=True, slots=True)
class MrmsGridView:
    """Read-projection of one materialised grid joined to its catalog row.

    ``product``/``level``/``valid_at`` come from ``mrms_files`` (the source
    of truth for "what" a grid represents); the rest come from
    ``mrms_grids`` (where it lives, what shape it has).
    """

    file_key: str
    product: str
    level: str
    valid_at: datetime
    zarr_uri: str
    variable: str
    dims: tuple[str, ...]
    shape: tuple[int, ...]
    dtype: str
    nbytes: int
    materialised_at: datetime


# --------------------------------------------------------------------------- #
# Repository                                                                   #
# --------------------------------------------------------------------------- #


_BASE_COLUMNS = (
    MrmsGridRow.file_key,
    MrmsFileRow.product,
    MrmsFileRow.level,
    MrmsFileRow.valid_at,
    MrmsGridRow.zarr_uri,
    MrmsGridRow.variable,
    MrmsGridRow.dims_json,
    MrmsGridRow.shape_json,
    MrmsGridRow.dtype,
    MrmsGridRow.nbytes,
    MrmsGridRow.materialised_at,
)


async def find_mrms_grids(
    session: AsyncSession,
    *,
    product: str | None = None,
    level: str | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    limit: int = DEFAULT_LIMIT,
) -> tuple[MrmsGridView, ...]:
    """Return materialised grids matching the supplied filters.

    Joins ``mrms_grids`` to ``mrms_files`` on ``file_key`` so callers can
    filter on product/level/valid_at without repeating the catalog schema
    here. Results are ordered by ``valid_at`` descending (newest first)
    and clamped to :data:`MAX_LIMIT`.
    """
    bounded_limit = min(max(limit, 1), MAX_LIMIT)
    stmt = (
        select(*_BASE_COLUMNS)
        .join(MrmsFileRow, MrmsFileRow.key == MrmsGridRow.file_key)
        .order_by(MrmsFileRow.valid_at.desc())
        .limit(bounded_limit)
    )
    if product is not None:
        stmt = stmt.where(MrmsFileRow.product == product)
    if level is not None:
        stmt = stmt.where(MrmsFileRow.level == level)
    if since is not None:
        stmt = stmt.where(MrmsFileRow.valid_at >= since)
    if until is not None:
        stmt = stmt.where(MrmsFileRow.valid_at < until)

    result = await session.execute(stmt)
    return tuple(_row_to_view(row) for row in result.mappings())


async def find_mrms_grid_by_key(
    session: AsyncSession,
    file_key: str,
) -> MrmsGridView | None:
    """Return the single materialised grid for ``file_key`` or ``None``.

    Same join as :func:`find_mrms_grids`. The catalog row is required —
    a grid without a parent file row is impossible (FK + cascade), so the
    join doesn't change the semantics.
    """
    stmt = (
        select(*_BASE_COLUMNS)
        .join(MrmsFileRow, MrmsFileRow.key == MrmsGridRow.file_key)
        .where(MrmsGridRow.file_key == file_key)
    )
    result = await session.execute(stmt)
    row = result.mappings().first()
    if row is None:
        return None
    return _row_to_view(row)


def _row_to_view(row: Any) -> MrmsGridView:
    return MrmsGridView(
        file_key=row["file_key"],
        product=row["product"],
        level=row["level"],
        valid_at=row["valid_at"],
        zarr_uri=row["zarr_uri"],
        variable=row["variable"],
        dims=_parse_jsonb_strings(row["dims_json"]),
        shape=_parse_jsonb_ints(row["shape_json"]),
        dtype=row["dtype"],
        nbytes=row["nbytes"],
        materialised_at=row["materialised_at"],
    )


def _parse_jsonb_strings(raw: Any) -> tuple[str, ...]:
    """JSONB columns come back from asyncpg as ``list``; strings come back
    as ``list[str]``. Older rows could also be plain JSON text. Handle both."""
    if isinstance(raw, str):
        raw = json.loads(raw)
    return tuple(str(x) for x in raw)


def _parse_jsonb_ints(raw: Any) -> tuple[int, ...]:
    if isinstance(raw, str):
        raw = json.loads(raw)
    return tuple(int(x) for x in raw)


# --------------------------------------------------------------------------- #
# Wire schemas                                                                 #
# --------------------------------------------------------------------------- #


class MrmsGridItem(BaseModel):
    """One materialised grid, formatted for the wire."""

    model_config = ConfigDict(populate_by_name=True, frozen=True)

    file_key: str = Field(serialization_alias="fileKey")
    product: str
    level: str
    valid_at: datetime = Field(serialization_alias="validAt")
    zarr_uri: str = Field(serialization_alias="zarrUri")
    variable: str
    dims: tuple[str, ...]
    shape: tuple[int, ...]
    dtype: str
    nbytes: int
    materialised_at: datetime = Field(serialization_alias="materialisedAt")


class MrmsGridList(BaseModel):
    """Envelope returned by the list route."""

    type: Literal["MrmsGridList"] = "MrmsGridList"
    items: list[MrmsGridItem]


def mrms_grid_view_to_item(view: MrmsGridView) -> MrmsGridItem:
    return MrmsGridItem(
        file_key=view.file_key,
        product=view.product,
        level=view.level,
        valid_at=view.valid_at,
        zarr_uri=view.zarr_uri,
        variable=view.variable,
        dims=view.dims,
        shape=view.shape,
        dtype=view.dtype,
        nbytes=view.nbytes,
        materialised_at=view.materialised_at,
    )
