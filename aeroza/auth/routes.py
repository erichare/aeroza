"""Public auth routes (``/v1/me``).

Wired into the app at the same level as the v1 query router. Anything
here is enforce-required: an anonymous caller gets 401 even when the
global ``AEROZA_AUTH_REQUIRED`` flag is off, because there is no
useful answer to "who am I?" without a key.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aeroza.auth.dependencies import AuthenticatedKey, require_api_key
from aeroza.auth.models import ApiKeyRow
from aeroza.auth.schemas import MeResponse, authenticated_key_to_me_response
from aeroza.query.dependencies import get_session

router: APIRouter = APIRouter(prefix="/v1", tags=["auth"])


@router.get("/me", response_model=MeResponse, response_model_by_alias=True)
async def get_me(
    key: Annotated[AuthenticatedKey, Depends(require_api_key)],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> MeResponse:
    """Introspect the calling API key.

    The bearer token already authenticated us, so the answer is
    derived from :class:`AuthenticatedKey` plus a single read for
    ``last_used_at`` (which the auth dependency just bumped — but the
    in-memory snapshot only has the *previous* value).
    """
    last_used_at = await session.scalar(
        select(ApiKeyRow.last_used_at).where(ApiKeyRow.id == key.id)
    )
    return authenticated_key_to_me_response(key, last_used_at=last_used_at)


__all__ = ["router"]
