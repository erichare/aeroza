"""Read-side query repository + wire schemas for the MRMS file catalog.

Unlike alerts (which are inherently geographic and emit GeoJSON), MRMS
catalog entries are pure metadata — one row per discovered S3 object —
so the wire shape is a flat envelope:

    {"items": [{"key": …, "product": …, "validAt": …, …}, …]}

Field names use NWS-style camelCase (``validAt``, ``sizeBytes``) to
match the alerts convention and the NATS event payload encoded by
:func:`aeroza.stream.nats._encode_mrms_file`.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Final, Literal

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aeroza.ingest.mrms_models import MrmsFileRow

DEFAULT_LIMIT: Final[int] = 100
MAX_LIMIT: Final[int] = 500


# --------------------------------------------------------------------------- #
# Internal projection                                                          #
# --------------------------------------------------------------------------- #


@dataclass(frozen=True, slots=True)
class MrmsFileView:
    """Read-projection of one catalog row, decoupled from the ORM."""

    key: str
    product: str
    level: str
    valid_at: datetime
    size_bytes: int
    etag: str | None


# --------------------------------------------------------------------------- #
# Repository                                                                   #
# --------------------------------------------------------------------------- #


async def find_mrms_files(
    session: AsyncSession,
    *,
    product: str | None = None,
    level: str | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    limit: int = DEFAULT_LIMIT,
) -> tuple[MrmsFileView, ...]:
    """Return MRMS catalog rows matching the supplied filters.

    Results are ordered by ``valid_at`` descending (most-recent first) and
    clamped to :data:`MAX_LIMIT`.
    """
    bounded_limit = min(max(limit, 1), MAX_LIMIT)
    stmt = (
        select(
            MrmsFileRow.key,
            MrmsFileRow.product,
            MrmsFileRow.level,
            MrmsFileRow.valid_at,
            MrmsFileRow.size_bytes,
            MrmsFileRow.etag,
        )
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


def _row_to_view(row: Any) -> MrmsFileView:
    return MrmsFileView(
        key=row["key"],
        product=row["product"],
        level=row["level"],
        valid_at=row["valid_at"],
        size_bytes=row["size_bytes"],
        etag=row["etag"],
    )


# --------------------------------------------------------------------------- #
# Wire schemas                                                                 #
# --------------------------------------------------------------------------- #


class MrmsFileItem(BaseModel):
    """One file in the catalog, formatted for the wire."""

    model_config = ConfigDict(populate_by_name=True, frozen=True)

    key: str
    product: str
    level: str
    valid_at: datetime = Field(serialization_alias="validAt")
    size_bytes: int = Field(serialization_alias="sizeBytes")
    etag: str | None = None


class MrmsFileList(BaseModel):
    """Envelope returned by the list route."""

    type: Literal["MrmsFileList"] = "MrmsFileList"
    items: list[MrmsFileItem]


def mrms_view_to_item(view: MrmsFileView) -> MrmsFileItem:
    return MrmsFileItem(
        key=view.key,
        product=view.product,
        level=view.level,
        valid_at=view.valid_at,
        size_bytes=view.size_bytes,
        etag=view.etag,
    )
