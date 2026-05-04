"""Wire shapes for the webhook delivery log.

The dispatcher writes one ``webhook_deliveries`` row per attempt;
:class:`WebhookDelivery` is the read-side projection of that row for
the audit-trail API. ``payload`` is intentionally omitted from the
public surface — it's stored for forensic replay but not safe to
hand back over the wire (it can carry user-supplied geometry from
alert rules and would balloon list responses). Operators that need
the full payload can reach into the table directly.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

WebhookDeliveryStatus = Literal["ok", "failed", "retrying"]


class WebhookDelivery(BaseModel):
    """One row of the audit trail.

    Mirrors :class:`WebhookDeliveryRow` minus ``payload`` (see module
    docstring). ``responseBodyPreview`` is already a server-side
    truncated copy (see :data:`RESPONSE_BODY_PREVIEW_BYTES`), so it's
    safe to surface as-is.
    """

    model_config = ConfigDict(populate_by_name=True, frozen=True, from_attributes=True)

    type: Literal["WebhookDelivery"] = "WebhookDelivery"
    id: UUID
    subscription_id: UUID = Field(serialization_alias="subscriptionId")
    rule_id: UUID | None = Field(default=None, serialization_alias="ruleId")
    event_type: str = Field(serialization_alias="eventType")
    status: WebhookDeliveryStatus
    attempt: int
    response_status: int | None = Field(default=None, serialization_alias="responseStatus")
    response_body_preview: str | None = Field(
        default=None, serialization_alias="responseBodyPreview"
    )
    error_reason: str | None = Field(default=None, serialization_alias="errorReason")
    duration_ms: int | None = Field(default=None, serialization_alias="durationMs")
    created_at: datetime = Field(serialization_alias="createdAt")


class WebhookDeliveryList(BaseModel):
    """Envelope for ``GET /v1/webhooks/{id}/deliveries``."""

    type: Literal["WebhookDeliveryList"] = "WebhookDeliveryList"
    items: list[WebhookDelivery]


def webhook_delivery_from_row(row: Any) -> WebhookDelivery:
    """Project a :class:`WebhookDeliveryRow` into the wire shape.

    A small helper that mirrors :func:`alert_rule_from_row`; routes
    that build lists of these can then map without the
    ``model_validate(... from_attributes=True)`` boilerplate.
    """
    return WebhookDelivery.model_validate(row, from_attributes=True)


__all__ = [
    "WebhookDelivery",
    "WebhookDeliveryList",
    "WebhookDeliveryStatus",
    "webhook_delivery_from_row",
]
