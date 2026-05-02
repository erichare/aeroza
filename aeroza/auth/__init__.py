"""Bearer-token API-key authentication.

Public surface:

- :func:`mint_api_key_token` — generate a new ``aza_live_<random>``
  token and return its plaintext + the prefix/hash to persist.
- :func:`hash_api_key_secret` — recompute the storage hash of a
  presented secret so the middleware can compare against ``key_hash``.
- :class:`ApiKeyRow` — ORM mapping for the ``api_keys`` table.
- :func:`get_optional_api_key` / :func:`require_api_key` — FastAPI
  dependencies. The first records who is calling without rejecting;
  the second raises 401 when the request is anonymous or the key is
  invalid / revoked.

The router that exposes ``GET /v1/me`` lives in :mod:`aeroza.auth.routes`.
"""

from __future__ import annotations

from aeroza.auth.dependencies import (
    AUTH_REQUIRED_ENV_FLAG,
    AuthenticatedKey,
    get_optional_api_key,
    require_api_key,
)
from aeroza.auth.hashing import (
    API_KEY_TOKEN_PREFIX,
    MintedApiKey,
    hash_api_key_secret,
    mint_api_key_token,
    parse_bearer_token,
)
from aeroza.auth.models import ApiKeyRow
from aeroza.auth.store import (
    create_api_key,
    find_active_api_key,
    list_api_keys,
    revoke_api_key,
    touch_api_key_last_used,
)

__all__ = [
    "API_KEY_TOKEN_PREFIX",
    "AUTH_REQUIRED_ENV_FLAG",
    "ApiKeyRow",
    "AuthenticatedKey",
    "MintedApiKey",
    "create_api_key",
    "find_active_api_key",
    "get_optional_api_key",
    "hash_api_key_secret",
    "list_api_keys",
    "mint_api_key_token",
    "parse_bearer_token",
    "require_api_key",
    "revoke_api_key",
    "touch_api_key_last_used",
]
