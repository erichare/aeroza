"""Wire shapes for ``GET /v1/me``.

Other auth routes (admin CRUD over keys) intentionally don't exist in
this slice — the CLI is the management plane for v1. ``/v1/me`` is
the only thing a key holder can call about their own credential.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from aeroza.auth.dependencies import AuthenticatedKey


class MeResponse(BaseModel):
    """The calling key's metadata, redacted to what the caller already knows.

    Notably absent: ``key_hash`` (server-only), the full token (only
    ever shown at mint time), and ``created_at`` /
    ``updated_at`` (administrative metadata the caller hasn't asked
    for). Add fields here as concrete needs surface.
    """

    model_config = ConfigDict(populate_by_name=True, frozen=True)

    type: Literal["Me"] = "Me"
    name: str
    prefix: str
    owner: str
    scopes: list[str]
    rate_limit_class: str = Field(serialization_alias="rateLimitClass")
    last_used_at: datetime | None = Field(default=None, serialization_alias="lastUsedAt")


def authenticated_key_to_me_response(
    key: AuthenticatedKey,
    *,
    last_used_at: datetime | None,
) -> MeResponse:
    return MeResponse(
        name=key.name,
        prefix=key.prefix,
        owner=key.owner,
        scopes=list(key.scopes),
        rate_limit_class=key.rate_limit_class,
        last_used_at=last_used_at,
    )


__all__ = [
    "MeResponse",
    "authenticated_key_to_me_response",
]
