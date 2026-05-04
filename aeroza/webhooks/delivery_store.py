"""Read-side queries over the webhook_deliveries table.

The write path lives in :mod:`aeroza.webhooks.delivery` (per-attempt
row writes during :func:`deliver_payload`). This module owns the
read path the audit-trail API uses — ``ORDER BY created_at DESC``
with optional status filter, hitting
``ix_webhook_deliveries_subscription_created_at``.

Like the other stores, this module never commits — read-only queries
don't need to.
"""

from __future__ import annotations

import uuid
from typing import Final

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aeroza.webhooks.delivery_models import WebhookDeliveryRow

DEFAULT_LIST_LIMIT: Final[int] = 50
MAX_LIST_LIMIT: Final[int] = 200


async def list_deliveries_for_subscription(
    session: AsyncSession,
    subscription_id: uuid.UUID,
    *,
    status: str | None = None,
    limit: int = DEFAULT_LIST_LIMIT,
) -> tuple[WebhookDeliveryRow, ...]:
    """Recent delivery attempts for one subscription, newest first.

    ``status`` filters to ``"ok"``, ``"failed"``, or ``"retrying"``;
    ``None`` returns rows regardless of status.
    """
    bounded_limit = min(max(limit, 1), MAX_LIST_LIMIT)
    stmt = (
        select(WebhookDeliveryRow)
        .where(WebhookDeliveryRow.subscription_id == subscription_id)
        .order_by(WebhookDeliveryRow.created_at.desc())
        .limit(bounded_limit)
    )
    if status is not None:
        stmt = stmt.where(WebhookDeliveryRow.status == status)
    result = await session.execute(stmt)
    return tuple(result.scalars().all())


__all__ = [
    "DEFAULT_LIST_LIMIT",
    "MAX_LIST_LIMIT",
    "list_deliveries_for_subscription",
]
