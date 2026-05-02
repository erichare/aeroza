"""HTTP CRUD over alert rules.

Mounted under ``/v1/alert-rules`` from :mod:`aeroza.main`. The
dispatcher worker (slice 3) reads rows directly via the store.

Surface:

- ``POST /v1/alert-rules`` — create. Validates ``subscriptionId``
  exists.
- ``GET /v1/alert-rules`` — list, newest first. Optional ``status`` /
  ``subscriptionId`` filters.
- ``GET /v1/alert-rules/{id}`` — single rule detail.
- ``PATCH /v1/alert-rules/{id}`` — partial update. ``config`` is
  replaced wholesale (sub-field deltas not on the wire).
- ``DELETE /v1/alert-rules/{id}`` — idempotent: second delete is 404.
"""

from __future__ import annotations

import uuid
from typing import Annotated

import structlog
from fastapi import APIRouter, Depends, HTTPException, Path, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from aeroza.query.dependencies import get_session
from aeroza.webhooks.rule_schemas import (
    AlertRule,
    AlertRuleCreate,
    AlertRuleList,
    AlertRulePatch,
    alert_rule_from_row,
)
from aeroza.webhooks.rule_store import (
    DEFAULT_LIST_LIMIT,
    MAX_LIST_LIMIT,
    create_rule,
    delete_rule,
    get_rule,
    list_rules,
    update_rule,
)
from aeroza.webhooks.store import get_subscription

log = structlog.get_logger(__name__)

router = APIRouter(prefix="/v1/alert-rules", tags=["webhooks"])


@router.post(
    "",
    response_model=AlertRule,
    response_model_by_alias=True,
    response_model_exclude_none=False,
    status_code=status.HTTP_201_CREATED,
    summary="Create an alert rule",
    description=(
        "Creates a rule bound to an existing webhook subscription. The "
        "dispatcher worker evaluates every active rule on each new "
        "MRMS grid; on a false→true predicate transition it POSTs the "
        "evaluation to the bound subscription's URL with the standard "
        "Aeroza HMAC headers."
    ),
)
async def create_alert_rule_route(
    payload: AlertRuleCreate,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> AlertRule:
    # Validate the FK target exists upfront so callers get a 404 instead
    # of a Postgres FK violation translated to an opaque 500.
    if await get_subscription(session, payload.subscription_id) is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"webhook subscription {payload.subscription_id} not found",
        )
    row = await create_rule(session, payload)
    await session.commit()
    return alert_rule_from_row(row)


@router.get(
    "",
    response_model=AlertRuleList,
    response_model_by_alias=True,
    response_model_exclude_none=False,
    summary="List alert rules (newest first)",
)
async def list_alert_rules_route(
    session: Annotated[AsyncSession, Depends(get_session)],
    status_filter: Annotated[
        str | None,
        Query(
            alias="status",
            description="Filter to a single status (active / paused / disabled).",
        ),
    ] = None,
    subscription_id: Annotated[
        uuid.UUID | None,
        Query(
            alias="subscriptionId",
            description="Filter to rules bound to this subscription.",
        ),
    ] = None,
    limit: Annotated[
        int,
        Query(
            ge=1,
            le=MAX_LIST_LIMIT,
            description=f"Max results to return (default {DEFAULT_LIST_LIMIT}).",
        ),
    ] = DEFAULT_LIST_LIMIT,
) -> AlertRuleList:
    rows = await list_rules(
        session,
        status=status_filter,
        subscription_id=subscription_id,
        limit=limit,
    )
    return AlertRuleList(items=[alert_rule_from_row(row) for row in rows])


@router.get(
    "/{rule_id}",
    response_model=AlertRule,
    response_model_by_alias=True,
    response_model_exclude_none=False,
    summary="Get an alert rule by id",
    responses={404: {"description": "Rule not found."}},
)
async def get_alert_rule_route(
    rule_id: Annotated[uuid.UUID, Path(description="Rule id (UUID).")],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> AlertRule:
    row = await get_rule(session, rule_id)
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"alert rule {rule_id} not found",
        )
    return alert_rule_from_row(row)


@router.patch(
    "/{rule_id}",
    response_model=AlertRule,
    response_model_by_alias=True,
    response_model_exclude_none=False,
    summary="Update an alert rule",
    responses={404: {"description": "Rule not found."}},
)
async def update_alert_rule_route(
    rule_id: Annotated[uuid.UUID, Path(description="Rule id (UUID).")],
    patch: AlertRulePatch,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> AlertRule:
    row = await update_rule(session, rule_id, patch)
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"alert rule {rule_id} not found",
        )
    await session.commit()
    return alert_rule_from_row(row)


@router.delete(
    "/{rule_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete an alert rule",
    description="Idempotent — second delete returns 404.",
    responses={404: {"description": "Rule not found."}},
)
async def delete_alert_rule_route(
    rule_id: Annotated[uuid.UUID, Path(description="Rule id (UUID).")],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> None:
    deleted = await delete_rule(session, rule_id)
    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"alert rule {rule_id} not found",
        )
    await session.commit()


__all__ = ["router"]
