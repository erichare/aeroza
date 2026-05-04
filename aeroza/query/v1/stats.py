"""``/v1/stats`` route — system freshness snapshot."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from aeroza.query.dependencies import get_session
from aeroza.query.stats import Stats, compute_stats, stats_view_to_model

router = APIRouter(tags=["meta"])


@router.get(
    "/stats",
    response_model=Stats,
    response_model_by_alias=True,
    response_model_exclude_none=False,
    summary="Live system stats: alerts + MRMS counts and freshness",
    description=(
        "Compact 'what does the system know right now?' snapshot. Cheap "
        "aggregate counts (alerts active/total, MRMS files vs grids "
        "materialised) plus the latest ``valid_at`` and ``materialised_at`` "
        "timestamps so callers can confirm data is flowing without scanning "
        "the catalogs themselves."
    ),
)
async def get_stats_route(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> Stats:
    now = datetime.now(UTC)
    view = await compute_stats(session, now=now)
    return stats_view_to_model(view, generated_at=now)
