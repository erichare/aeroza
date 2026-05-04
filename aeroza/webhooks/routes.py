"""HTTP CRUD over webhook subscriptions.

Mounted under ``/v1/webhooks`` from :mod:`aeroza.main` alongside the
existing ``/v1/...`` query routes. The dispatcher worker doesn't ride
this router — it talks directly to the store. These routes are the
operator-facing surface only.

Surface:

- ``POST /v1/webhooks`` — create a subscription. Returns the full
  :class:`WebhookSubscription` shape (including the freshly minted
  ``secret``); subsequent reads omit the secret.
- ``GET /v1/webhooks`` — list subscriptions, newest first. Optional
  ``status`` filter.
- ``GET /v1/webhooks/{id}`` — single subscription detail (no secret).
- ``PATCH /v1/webhooks/{id}`` — partial update (url / events /
  description / status).
- ``DELETE /v1/webhooks/{id}`` — remove. Idempotent — second delete
  returns 404.
- ``GET /v1/webhooks/{id}/deliveries`` — recent delivery attempts.
  Read-only audit trail (write path lives in ``delivery.py``).
"""

from __future__ import annotations

import uuid
from typing import Annotated

import structlog
from fastapi import APIRouter, Depends, HTTPException, Path, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from aeroza.query.dependencies import get_session
from aeroza.webhooks.delivery_schemas import (
    WebhookDeliveryList,
    webhook_delivery_from_row,
)
from aeroza.webhooks.delivery_store import (
    DEFAULT_LIST_LIMIT as DELIVERIES_DEFAULT_LIST_LIMIT,
)
from aeroza.webhooks.delivery_store import (
    MAX_LIST_LIMIT as DELIVERIES_MAX_LIST_LIMIT,
)
from aeroza.webhooks.delivery_store import (
    list_deliveries_for_subscription,
)
from aeroza.webhooks.schemas import (
    WebhookSubscription,
    WebhookSubscriptionCreate,
    WebhookSubscriptionList,
    WebhookSubscriptionPatch,
    WebhookSubscriptionRedacted,
)
from aeroza.webhooks.store import (
    DEFAULT_LIST_LIMIT,
    MAX_LIST_LIMIT,
    create_subscription,
    delete_subscription,
    get_subscription,
    list_subscriptions,
    update_subscription,
)

log = structlog.get_logger(__name__)

router = APIRouter(prefix="/v1/webhooks", tags=["webhooks"])


@router.post(
    "",
    response_model=WebhookSubscription,
    response_model_by_alias=True,
    response_model_exclude_none=False,
    status_code=status.HTTP_201_CREATED,
    summary="Create a webhook subscription",
    description=(
        "Creates a subscription that the dispatcher worker will fan "
        "matching events out to. The ``secret`` field on the response "
        "is the HMAC signing key — it is shown **once** on creation and "
        "is omitted from every subsequent read; store it on your side."
    ),
)
async def create_webhook_route(
    payload: WebhookSubscriptionCreate,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> WebhookSubscription:
    row = await create_subscription(session, payload)
    await session.commit()
    return WebhookSubscription.model_validate(row, from_attributes=True)


@router.get(
    "",
    response_model=WebhookSubscriptionList,
    response_model_by_alias=True,
    response_model_exclude_none=False,
    summary="List webhook subscriptions (newest first)",
    description=(
        "Returns subscriptions ordered by ``created_at`` descending. "
        "The signing ``secret`` is omitted from every item — it is "
        "only ever returned on the create response."
    ),
)
async def list_webhooks_route(
    session: Annotated[AsyncSession, Depends(get_session)],
    status_filter: Annotated[
        str | None,
        Query(
            alias="status",
            description="Filter to a single status (active / paused / disabled).",
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
) -> WebhookSubscriptionList:
    rows = await list_subscriptions(session, status=status_filter, limit=limit)
    items = [WebhookSubscriptionRedacted.model_validate(row, from_attributes=True) for row in rows]
    return WebhookSubscriptionList(items=items)


@router.get(
    "/{sub_id}",
    response_model=WebhookSubscriptionRedacted,
    response_model_by_alias=True,
    response_model_exclude_none=False,
    summary="Get a webhook subscription by id",
    responses={404: {"description": "Subscription not found."}},
)
async def get_webhook_route(
    sub_id: Annotated[uuid.UUID, Path(description="Subscription id (UUID).")],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> WebhookSubscriptionRedacted:
    row = await get_subscription(session, sub_id)
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"webhook subscription {sub_id} not found",
        )
    return WebhookSubscriptionRedacted.model_validate(row, from_attributes=True)


@router.patch(
    "/{sub_id}",
    response_model=WebhookSubscriptionRedacted,
    response_model_by_alias=True,
    response_model_exclude_none=False,
    summary="Update a webhook subscription",
    description=(
        "Partial update. Every field is optional; absent fields are "
        "left untouched. ``events`` overwrites the full list — "
        "add/remove deltas are not supported on the wire."
    ),
    responses={404: {"description": "Subscription not found."}},
)
async def update_webhook_route(
    sub_id: Annotated[uuid.UUID, Path(description="Subscription id (UUID).")],
    patch: WebhookSubscriptionPatch,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> WebhookSubscriptionRedacted:
    row = await update_subscription(session, sub_id, patch)
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"webhook subscription {sub_id} not found",
        )
    await session.commit()
    return WebhookSubscriptionRedacted.model_validate(row, from_attributes=True)


@router.delete(
    "/{sub_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete a webhook subscription",
    description="Idempotent — second delete returns 404.",
    responses={404: {"description": "Subscription not found."}},
)
async def delete_webhook_route(
    sub_id: Annotated[uuid.UUID, Path(description="Subscription id (UUID).")],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> None:
    deleted = await delete_subscription(session, sub_id)
    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"webhook subscription {sub_id} not found",
        )
    await session.commit()


@router.get(
    "/{sub_id}/deliveries",
    response_model=WebhookDeliveryList,
    response_model_by_alias=True,
    response_model_exclude_none=False,
    summary="Recent delivery attempts for a subscription",
    description=(
        "Returns delivery rows ordered by ``created_at`` descending — "
        "one row per attempt the dispatcher made (initial + retries). "
        'Newest-first matches "why did it just fail?" debugging.\n\n'
        "Optional ``status`` filter narrows to a single outcome "
        "(``ok`` / ``failed`` / ``retrying``). The signed payload itself "
        "is omitted from the wire — operators that need it can read "
        "``webhook_deliveries.payload`` directly."
    ),
    responses={404: {"description": "Subscription not found."}},
)
async def list_webhook_deliveries_route(
    sub_id: Annotated[uuid.UUID, Path(description="Subscription id (UUID).")],
    session: Annotated[AsyncSession, Depends(get_session)],
    status_filter: Annotated[
        str | None,
        Query(
            alias="status",
            description="Filter to a single status (ok / failed / retrying).",
        ),
    ] = None,
    limit: Annotated[
        int,
        Query(
            ge=1,
            le=DELIVERIES_MAX_LIST_LIMIT,
            description=f"Max attempts to return (default {DELIVERIES_DEFAULT_LIST_LIMIT}).",
        ),
    ] = DELIVERIES_DEFAULT_LIST_LIMIT,
) -> WebhookDeliveryList:
    # 404 for unknown subscriptions so callers don't conflate "no
    # attempts yet" with "you typo'd the id."
    if await get_subscription(session, sub_id) is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"webhook subscription {sub_id} not found",
        )
    rows = await list_deliveries_for_subscription(
        session,
        sub_id,
        status=status_filter,
        limit=limit,
    )
    return WebhookDeliveryList(
        items=[webhook_delivery_from_row(row) for row in rows],
    )


__all__ = ["router"]
