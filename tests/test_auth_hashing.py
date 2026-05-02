"""Unit tests for token mint, hash, and bearer-header parsing.

Pure-function tests — no DB, no FastAPI, no env state. Cover the
contract every other layer relies on:
- a minted token round-trips through the hash check;
- changing the salt makes the round-trip fail (HMAC, not raw SHA);
- malformed/missing/wrong-scheme bearer headers all return ``None``.
"""

from __future__ import annotations

import pytest

from aeroza.auth.hashing import (
    API_KEY_TOKEN_PREFIX,
    PREFIX_LENGTH,
    hash_api_key_secret,
    mint_api_key_token,
    parse_bearer_token,
)

pytestmark = pytest.mark.unit


def test_minted_token_round_trips_through_hash() -> None:
    minted = mint_api_key_token(salt="s1")
    assert minted.token.startswith(API_KEY_TOKEN_PREFIX)
    random_part = minted.token[len(API_KEY_TOKEN_PREFIX) :]
    assert minted.prefix == random_part[:PREFIX_LENGTH]
    assert hash_api_key_secret(random_part, salt="s1") == minted.key_hash


def test_different_salts_produce_different_hashes() -> None:
    minted = mint_api_key_token(salt="s1")
    random_part = minted.token[len(API_KEY_TOKEN_PREFIX) :]
    assert hash_api_key_secret(random_part, salt="s2") != minted.key_hash


def test_minted_keys_are_unique() -> None:
    """256 bits of entropy means a duplicate is astronomically unlikely.

    A small loop confirms the generator isn't accidentally seeded with
    a constant — common bug source when porting between modules.
    """
    seen = {mint_api_key_token(salt="s").token for _ in range(50)}
    assert len(seen) == 50


def test_hash_is_64_hex_chars() -> None:
    """The migration's CHECK constraint enforces this; tests pin it."""
    minted = mint_api_key_token(salt="s")
    assert len(minted.key_hash) == 64
    int(minted.key_hash, 16)  # raises if not pure hex


@pytest.mark.parametrize(
    "header",
    [
        None,
        "",
        "Basic abc",
        "Bearer",  # no value
        "Bearer not_an_aza_token",
        "Bearer aza_live_",  # nothing after the brand
        "Bearer aza_live_a",  # too short to fill the prefix
    ],
)
def test_parse_bearer_returns_none_for_bad_input(header: str | None) -> None:
    assert parse_bearer_token(header) is None


def test_parse_bearer_extracts_prefix_and_random_part() -> None:
    minted = mint_api_key_token(salt="s")
    parsed = parse_bearer_token(f"Bearer {minted.token}")
    assert parsed is not None
    prefix, random_part = parsed
    assert prefix == minted.prefix
    assert minted.token == f"{API_KEY_TOKEN_PREFIX}{random_part}"


def test_parse_bearer_is_scheme_case_insensitive() -> None:
    minted = mint_api_key_token(salt="s")
    parsed = parse_bearer_token(f"bearer {minted.token}")
    assert parsed is not None
    assert parsed[0] == minted.prefix
