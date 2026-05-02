"""FastAPI dependencies: optional auth + required auth.

The two-mode design lets us land auth without breaking anonymous
traffic on day one. Both modes look up the bearer token if present;
the difference is whether a missing/invalid token returns 401 or
silently lets the request through.

Mode is controlled by the ``AEROZA_AUTH_REQUIRED`` env flag (read
once at startup, cached on :class:`AuthSettings`). The flag defaults
to ``false`` so a fresh checkout still serves the anonymous landing
page; flipping it to ``true`` makes ``require_api_key`` enforce on
every request that depends on it.
"""

from __future__ import annotations

import hmac
import os
from dataclasses import dataclass
from typing import Annotated, Final

from fastapi import Depends, Header, HTTPException, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from aeroza.auth.hashing import hash_api_key_secret, parse_bearer_token
from aeroza.auth.models import ApiKeyRow
from aeroza.auth.store import find_active_api_key, touch_api_key_last_used
from aeroza.config import get_settings
from aeroza.query.dependencies import get_session

AUTH_REQUIRED_ENV_FLAG: Final[str] = "AEROZA_AUTH_REQUIRED"


@dataclass(frozen=True, slots=True)
class AuthenticatedKey:
    """A successfully-authenticated API key.

    Stored on ``request.state`` so anything downstream (logging,
    rate-limit middleware, route handlers) can read who is calling
    without re-running the bearer check.
    """

    id: str
    name: str
    prefix: str
    owner: str
    scopes: tuple[str, ...]
    rate_limit_class: str

    @classmethod
    def from_row(cls, row: ApiKeyRow) -> AuthenticatedKey:
        return cls(
            id=str(row.id),
            name=row.name,
            prefix=row.prefix,
            owner=row.owner,
            scopes=tuple(row.scopes or ()),
            rate_limit_class=row.rate_limit_class,
        )


def _auth_required() -> bool:
    """Read the env flag once per call.

    Tests can monkeypatch the env variable between requests without
    rebuilding the app — :func:`get_settings` is cached, but this
    flag deliberately is not, so flipping it during a session works.
    """
    return os.environ.get(AUTH_REQUIRED_ENV_FLAG, "false").lower() in ("1", "true", "yes")


async def _resolve_bearer_key(
    *,
    session: AsyncSession,
    authorization: str | None,
    request: Request,
) -> AuthenticatedKey | None:
    """Common lookup path. Returns ``None`` for anonymous traffic.

    A malformed header, an unknown prefix, or a hash mismatch all
    return ``None`` — the caller decides whether that triggers a 401
    (required mode) or silent pass-through (optional mode). The
    distinction between "no header" and "bad header" only matters for
    error messaging, so we collapse them here.
    """
    parsed = parse_bearer_token(authorization)
    if parsed is None:
        return None
    prefix, random_part = parsed

    row = await find_active_api_key(session, prefix=prefix)
    if row is None:
        return None

    expected_hash = hash_api_key_secret(random_part, salt=get_settings().api_key_salt)
    if not hmac.compare_digest(expected_hash, row.key_hash):
        # Constant-time compare — the prefix is public, but the secret
        # isn't, and a timing channel that leaks "was a hash close" is
        # the kind of bug you only notice in retrospect.
        return None

    # Touch happens in the request's session; the caller's transaction
    # scope (FastAPI's per-request session) handles the commit.
    await touch_api_key_last_used(session, key_id=row.id)

    authed = AuthenticatedKey.from_row(row)
    request.state.api_key = authed
    return authed


async def get_optional_api_key(
    request: Request,
    session: Annotated[AsyncSession, Depends(get_session)],
    authorization: Annotated[str | None, Header()] = None,
) -> AuthenticatedKey | None:
    """Pass-through auth.

    Returns the resolved key if present; ``None`` otherwise. Never
    raises — anonymous traffic is allowed.
    """
    return await _resolve_bearer_key(
        session=session,
        authorization=authorization,
        request=request,
    )


async def require_api_key(
    request: Request,
    session: Annotated[AsyncSession, Depends(get_session)],
    authorization: Annotated[str | None, Header()] = None,
) -> AuthenticatedKey:
    """Strict auth.

    Behaviour depends on the :data:`AUTH_REQUIRED_ENV_FLAG`:

    - When the flag is **on**, an unresolved key raises 401.
    - When the flag is **off**, an unresolved key still raises 401
      (this dependency is "required" by name; callers wanting the
      soft mode use :func:`get_optional_api_key`).

    The flag affects which routes *use* this dependency, not what it
    does once invoked. The recommended pattern is to wire
    ``require_api_key`` into a router's ``dependencies=[]`` only when
    the flag flips on; until then most routes use the optional
    dependency for telemetry and leave anonymous traffic alone.
    """
    key = await _resolve_bearer_key(
        session=session,
        authorization=authorization,
        request=request,
    )
    if key is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="missing or invalid API key",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return key


__all__ = [
    "AUTH_REQUIRED_ENV_FLAG",
    "AuthenticatedKey",
    "get_optional_api_key",
    "require_api_key",
]
