"""Wire shapes for the webhook subscription API.

Three flavours, all camelCase on the wire to match the rest of v1:

- :class:`WebhookSubscriptionCreate` — request body for ``POST``.
  Caller supplies ``url``, ``events``, optional ``description``. The
  ``secret`` is generated server-side; the ``id``/``status``/timestamps
  fall out of the database.
- :class:`WebhookSubscriptionPatch` — partial update body. Every
  field is optional; the route applies whatever is present.
- :class:`WebhookSubscription` — public response shape. Includes
  ``secret`` only on the ``POST`` response (the create-time payload);
  list/get responses omit it via :class:`WebhookSubscriptionRedacted`
  so a leaked log line can't replay signatures.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Final, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

from aeroza.webhooks.models import WEBHOOK_EVENT_TYPES, WEBHOOK_STATUSES

WebhookStatus = Literal["active", "paused", "disabled"]

# Allowed schemes for ``url``. ``http://`` is permitted so the dev
# console can register a localhost destination without TLS plumbing;
# the dispatcher will warn-log on http in production environments.
_URL_SCHEME_RE: Final[re.Pattern[str]] = re.compile(r"^https?://", re.IGNORECASE)
# Cap so a malicious operator can't store megabyte URLs.
_URL_MAX_LEN: Final[int] = 2048
_DESCRIPTION_MAX_LEN: Final[int] = 512


class WebhookSubscriptionCreate(BaseModel):
    """Request body for ``POST /v1/webhooks``."""

    model_config = ConfigDict(populate_by_name=True, frozen=True)

    url: str = Field(
        ...,
        max_length=_URL_MAX_LEN,
        description="Absolute https:// (or http:// in dev) URL.",
    )
    events: list[str] = Field(
        ...,
        min_length=1,
        description=(
            f"List of event types to subscribe to. Each must be one of {list(WEBHOOK_EVENT_TYPES)}."
        ),
    )
    description: str | None = Field(
        default=None,
        max_length=_DESCRIPTION_MAX_LEN,
        description="Human-readable label, surfaced in the dashboard / logs.",
    )

    @field_validator("url")
    @classmethod
    def _validate_url(cls, value: str) -> str:
        if not _URL_SCHEME_RE.match(value):
            raise ValueError("url must start with http:// or https://")
        return value

    @field_validator("events")
    @classmethod
    def _validate_events(cls, value: list[str]) -> list[str]:
        unknown = [e for e in value if e not in WEBHOOK_EVENT_TYPES]
        if unknown:
            raise ValueError(f"unknown event types {unknown}; allowed: {list(WEBHOOK_EVENT_TYPES)}")
        # De-dup while preserving order so re-queries from the dashboard
        # don't churn the row's ``events`` array.
        return list(dict.fromkeys(value))


class WebhookSubscriptionPatch(BaseModel):
    """Request body for ``PATCH /v1/webhooks/{id}``.

    Every field is optional; absent fields are left untouched. Setting
    ``events`` to a list overwrites; the API does not support add/remove
    deltas (the round-trip is small enough that the client can read,
    edit, and write).
    """

    model_config = ConfigDict(populate_by_name=True, frozen=True)

    url: str | None = Field(default=None, max_length=_URL_MAX_LEN)
    events: list[str] | None = Field(default=None, min_length=1)
    description: str | None = Field(default=None, max_length=_DESCRIPTION_MAX_LEN)
    status: WebhookStatus | None = None

    @field_validator("url")
    @classmethod
    def _validate_url(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if not _URL_SCHEME_RE.match(value):
            raise ValueError("url must start with http:// or https://")
        return value

    @field_validator("events")
    @classmethod
    def _validate_events(cls, value: list[str] | None) -> list[str] | None:
        if value is None:
            return None
        unknown = [e for e in value if e not in WEBHOOK_EVENT_TYPES]
        if unknown:
            raise ValueError(f"unknown event types {unknown}; allowed: {list(WEBHOOK_EVENT_TYPES)}")
        return list(dict.fromkeys(value))

    @field_validator("status")
    @classmethod
    def _validate_status(cls, value: str | None) -> str | None:
        # pydantic's Literal already guards the wire type, but a defensive
        # double-check keeps the row never able to drift past the DB CHECK.
        if value is None:
            return None
        if value not in WEBHOOK_STATUSES:
            raise ValueError(f"status must be one of {list(WEBHOOK_STATUSES)}")
        return value


class WebhookSubscription(BaseModel):
    """Full response shape, including the signing secret.

    Returned only on the create response. Subsequent reads use
    :class:`WebhookSubscriptionRedacted` so the secret isn't exposed
    after creation.
    """

    model_config = ConfigDict(populate_by_name=True, frozen=True, from_attributes=True)

    type: Literal["WebhookSubscription"] = "WebhookSubscription"
    id: UUID
    url: str
    events: tuple[str, ...]
    description: str | None
    status: WebhookStatus
    secret: str
    created_at: datetime = Field(serialization_alias="createdAt")
    updated_at: datetime = Field(serialization_alias="updatedAt")


class WebhookSubscriptionRedacted(BaseModel):
    """Response shape that omits ``secret``.

    Used by every read endpoint after creation. The secret is shown
    once at creation time (à la Stripe webhook signing keys); operators
    that need it again rotate it via a future ``POST
    /v1/webhooks/{id}/rotate-secret`` route (out of scope for this PR).
    """

    model_config = ConfigDict(populate_by_name=True, frozen=True, from_attributes=True)

    type: Literal["WebhookSubscriptionRedacted"] = "WebhookSubscriptionRedacted"
    id: UUID
    url: str
    events: tuple[str, ...]
    description: str | None
    status: WebhookStatus
    created_at: datetime = Field(serialization_alias="createdAt")
    updated_at: datetime = Field(serialization_alias="updatedAt")


class WebhookSubscriptionList(BaseModel):
    """Envelope for ``GET /v1/webhooks``."""

    type: Literal["WebhookSubscriptionList"] = "WebhookSubscriptionList"
    items: list[WebhookSubscriptionRedacted]


__all__ = [
    "WebhookStatus",
    "WebhookSubscription",
    "WebhookSubscriptionCreate",
    "WebhookSubscriptionList",
    "WebhookSubscriptionPatch",
    "WebhookSubscriptionRedacted",
]
