"""Persistence for the webhook_subscriptions table.

Plain CRUD + one fan-out helper (:func:`find_active_subscriptions_for_event`)
that the dispatcher worker will use to look up every active row whose
``events`` array contains a given subject. The helper is here, not on
the dispatcher, so it can be unit-tested against Postgres independently.

The session is **not** committed by any function in this module —
callers own the transaction, matching the convention used by the
alerts and MRMS stores.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from typing import Final

import structlog
from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from aeroza.webhooks.models import WebhookSubscriptionRow
from aeroza.webhooks.schemas import (
    WebhookSubscriptionCreate,
    WebhookSubscriptionPatch,
)
from aeroza.webhooks.signing import generate_secret

log = structlog.get_logger(__name__)

DEFAULT_LIST_LIMIT: Final[int] = 100
MAX_LIST_LIMIT: Final[int] = 500


async def create_subscription(
    session: AsyncSession,
    payload: WebhookSubscriptionCreate,
    *,
    secret: str | None = None,
) -> WebhookSubscriptionRow:
    """Insert a new subscription. Returns the persisted ORM row.

    ``secret`` defaults to a fresh CSPRNG-backed token; tests may inject
    a deterministic value. The row's ``id`` / timestamps come from the
    server-side defaults.
    """
    row = WebhookSubscriptionRow(
        url=payload.url,
        secret=secret if secret is not None else generate_secret(),
        events=list(payload.events),
        description=payload.description,
    )
    session.add(row)
    await session.flush()
    log.info(
        "webhooks.store.create",
        id=str(row.id),
        url=row.url,
        events=row.events,
    )
    return row


async def list_subscriptions(
    session: AsyncSession,
    *,
    status: str | None = None,
    limit: int = DEFAULT_LIST_LIMIT,
) -> tuple[WebhookSubscriptionRow, ...]:
    """Return subscriptions ordered by ``created_at`` descending.

    ``status`` filters to a single status (typically ``"active"``);
    ``None`` returns every row regardless of state.
    """
    bounded_limit = min(max(limit, 1), MAX_LIST_LIMIT)
    stmt = (
        select(WebhookSubscriptionRow)
        .order_by(WebhookSubscriptionRow.created_at.desc())
        .limit(bounded_limit)
    )
    if status is not None:
        stmt = stmt.where(WebhookSubscriptionRow.status == status)
    result = await session.execute(stmt)
    return tuple(result.scalars().all())


async def get_subscription(
    session: AsyncSession, sub_id: uuid.UUID
) -> WebhookSubscriptionRow | None:
    """Return one subscription by id, or ``None`` if it doesn't exist."""
    result = await session.execute(
        select(WebhookSubscriptionRow).where(WebhookSubscriptionRow.id == sub_id)
    )
    return result.scalar_one_or_none()


async def update_subscription(
    session: AsyncSession,
    sub_id: uuid.UUID,
    patch: WebhookSubscriptionPatch,
) -> WebhookSubscriptionRow | None:
    """Apply a partial update. Returns the refreshed row, or ``None``
    if the id doesn't exist.

    A patch with no fields set is a no-op (no DB write); the row is
    still returned. ``updated_at`` is bumped via ``func.now()`` only
    when at least one column actually changes.
    """
    values: dict[str, object] = {}
    if patch.url is not None:
        values["url"] = patch.url
    if patch.events is not None:
        values["events"] = list(patch.events)
    if patch.description is not None:
        values["description"] = patch.description
    if patch.status is not None:
        values["status"] = patch.status

    if not values:
        return await get_subscription(session, sub_id)

    values["updated_at"] = func.now()
    result = await session.execute(
        update(WebhookSubscriptionRow)
        .where(WebhookSubscriptionRow.id == sub_id)
        .values(**values)
        .returning(WebhookSubscriptionRow)
    )
    row = result.scalar_one_or_none()
    if row is not None:
        await session.refresh(row)
    log.info(
        "webhooks.store.update",
        id=str(sub_id),
        fields=list(values.keys()),
        found=row is not None,
    )
    return row


async def delete_subscription(session: AsyncSession, sub_id: uuid.UUID) -> bool:
    """Remove the subscription. Returns ``True`` if a row was deleted."""
    result = await session.execute(
        delete(WebhookSubscriptionRow)
        .where(WebhookSubscriptionRow.id == sub_id)
        .returning(WebhookSubscriptionRow.id)
    )
    deleted = result.scalar_one_or_none() is not None
    log.info("webhooks.store.delete", id=str(sub_id), deleted=deleted)
    return deleted


async def find_active_subscriptions_for_event(
    session: AsyncSession, event: str
) -> Sequence[WebhookSubscriptionRow]:
    """Return every ``active`` subscription whose ``events`` contains ``event``.

    Used by the dispatcher worker (PR #6) to fan a NATS event out to
    every interested HTTP destination. The query is index-backed via
    :data:`ix_webhook_subscriptions_events_gin` (GIN over the array)
    and :data:`ix_webhook_subscriptions_status_created_at`.
    """
    result = await session.execute(
        select(WebhookSubscriptionRow)
        .where(WebhookSubscriptionRow.status == "active")
        .where(WebhookSubscriptionRow.events.contains([event]))
        .order_by(WebhookSubscriptionRow.created_at.desc())
    )
    return tuple(result.scalars().all())


__all__ = [
    "DEFAULT_LIST_LIMIT",
    "MAX_LIST_LIMIT",
    "create_subscription",
    "delete_subscription",
    "find_active_subscriptions_for_event",
    "get_subscription",
    "list_subscriptions",
    "update_subscription",
]
