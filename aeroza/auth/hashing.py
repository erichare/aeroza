"""Token shape, generation, and hashing.

The bearer-token format is ``aza_live_<random>``, where ``<random>`` is
a URL-safe base64 encoding of 32 random bytes (so ~43 chars after
stripping ``=`` padding). The first 8 chars of ``<random>`` are
persisted as the ``prefix`` (visible) and the full ``<random>`` is
HMAC-SHA-256-hashed against the configured ``api_key_salt`` to produce
``key_hash`` (64 hex chars).

We use HMAC rather than a bare SHA-256 so the salt provides domain
separation: a token leaked from another system that happens to land
in this database can't be brute-forced by recomputing SHA-256 of
random strings — the attacker would also need the salt.

We do **not** use bcrypt / argon2 here. Those algorithms exist to
make low-entropy passwords expensive to brute-force; our tokens are
256 bits of cryptographic randomness, so there is nothing to
brute-force, and a plain HMAC keeps the per-request auth check at
microsecond cost.
"""

from __future__ import annotations

import hmac
import secrets
from dataclasses import dataclass
from hashlib import sha256
from typing import Final

API_KEY_TOKEN_PREFIX: Final[str] = "aza_live_"

# Length of the visible prefix portion. Stripe uses 8 chars after the
# brand for `sk_live_`, and 8 is enough to be human-glanceable while
# keeping the brute-force-collision space large.
PREFIX_LENGTH: Final[int] = 8

# 32 bytes = 256 bits of entropy. base64-url-without-padding renders
# this as 43 chars, well above the 12-15 chars where guessing remains
# theoretical even at modern attacker hardware.
SECRET_BYTES: Final[int] = 32


@dataclass(frozen=True, slots=True)
class MintedApiKey:
    """The materials produced by :func:`mint_api_key_token`.

    ``token`` is the only place the plaintext bearer string ever
    appears — return it to the caller once and never store or log it.
    """

    token: str
    prefix: str
    key_hash: str


def mint_api_key_token(*, salt: str) -> MintedApiKey:
    """Generate a fresh bearer token and the persistence-ready hash.

    The plaintext token returned in :attr:`MintedApiKey.token` is the
    only place the secret exists in cleartext. The caller is
    responsible for handing it to the user once and persisting only
    ``prefix`` and ``key_hash``.
    """
    random_part = secrets.token_urlsafe(SECRET_BYTES)
    token = f"{API_KEY_TOKEN_PREFIX}{random_part}"
    prefix = random_part[:PREFIX_LENGTH]
    key_hash = hash_api_key_secret(random_part, salt=salt)
    return MintedApiKey(token=token, prefix=prefix, key_hash=key_hash)


def hash_api_key_secret(random_part: str, *, salt: str) -> str:
    """HMAC-SHA-256 the random portion of a token against ``salt``.

    Returns 64 lowercase hex characters. Inputs are encoded as UTF-8.
    """
    digest = hmac.new(
        salt.encode("utf-8"),
        random_part.encode("utf-8"),
        sha256,
    ).hexdigest()
    return digest


def parse_bearer_token(authorization: str | None) -> tuple[str, str] | None:
    """Pull (prefix, random_part) out of an ``Authorization`` header.

    Accepts ``Bearer aza_live_<random>``. Returns ``None`` for anything
    else — missing header, wrong scheme, wrong token brand, or
    malformed (too short to even contain a prefix). The caller decides
    how to react: anonymous traffic when in optional-auth mode, 401
    when in required-auth mode.
    """
    if not authorization:
        return None
    parts = authorization.split(" ", 1)
    if len(parts) != 2:
        return None
    scheme, value = parts[0], parts[1].strip()
    if scheme.lower() != "bearer":
        return None
    if not value.startswith(API_KEY_TOKEN_PREFIX):
        return None
    random_part = value[len(API_KEY_TOKEN_PREFIX) :]
    if len(random_part) < PREFIX_LENGTH:
        return None
    prefix = random_part[:PREFIX_LENGTH]
    return prefix, random_part


__all__ = [
    "API_KEY_TOKEN_PREFIX",
    "PREFIX_LENGTH",
    "MintedApiKey",
    "hash_api_key_secret",
    "mint_api_key_token",
    "parse_bearer_token",
]
