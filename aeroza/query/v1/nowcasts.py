"""``/v1/nowcasts`` route — predicted-grid catalog."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from aeroza.query.dependencies import get_session
from aeroza.query.nowcasts import (
    DEFAULT_LIMIT as NOWCASTS_DEFAULT_LIMIT,
)
from aeroza.query.nowcasts import (
    MAX_LIMIT as NOWCASTS_MAX_LIMIT,
)
from aeroza.query.nowcasts import (
    NowcastList,
    find_nowcasts,
    nowcast_view_to_item,
)

router = APIRouter(tags=["nowcasts"])


@router.get(
    "/nowcasts",
    response_model=NowcastList,
    response_model_by_alias=True,
    response_model_exclude_none=False,
    summary="List nowcasts (predicted grids)",
    description=(
        "Returns the most-recent rows of the nowcast catalog populated by "
        "the ``aeroza-nowcast-mrms`` worker. Each row is one (algorithm, "
        "horizon) prediction derived from a source observation grid. "
        "Filters: ``product``, ``level``, ``algorithm`` (e.g. "
        "``persistence``), ``horizon_minutes``, and a half-open "
        "``[since, until)`` window on ``valid_at``."
    ),
)
async def list_nowcasts_route(
    session: Annotated[AsyncSession, Depends(get_session)],
    product: Annotated[
        str | None,
        Query(description="Filter to a single product (e.g. 'MergedReflectivityComposite')"),
    ] = None,
    level: Annotated[
        str | None,
        Query(description="Filter to a single product level (e.g. '00.50')"),
    ] = None,
    algorithm: Annotated[
        str | None,
        Query(description="Filter to one algorithm tag (e.g. 'persistence', 'pysteps')"),
    ] = None,
    horizon_minutes: Annotated[
        int | None,
        Query(
            alias="horizonMinutes",
            description="Filter to one forecast horizon (e.g. 10, 30, 60).",
        ),
    ] = None,
    since: Annotated[
        datetime | None,
        Query(description="Inclusive lower bound on valid_at (ISO-8601 timestamp)"),
    ] = None,
    until: Annotated[
        datetime | None,
        Query(description="Exclusive upper bound on valid_at (ISO-8601 timestamp)"),
    ] = None,
    limit: Annotated[
        int,
        Query(
            ge=1,
            le=NOWCASTS_MAX_LIMIT,
            description=f"Max results to return (default {NOWCASTS_DEFAULT_LIMIT})",
        ),
    ] = NOWCASTS_DEFAULT_LIMIT,
) -> NowcastList:
    if since is not None and until is not None and since >= until:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="since must be strictly before until",
        )
    views = await find_nowcasts(
        session,
        product=product,
        level=level,
        algorithm=algorithm,
        horizon_minutes=horizon_minutes,
        since=since,
        until=until,
        limit=limit,
    )
    return NowcastList(items=[nowcast_view_to_item(v) for v in views])
