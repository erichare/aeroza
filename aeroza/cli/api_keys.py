"""Operator CLI: ``aeroza-api-keys create | list | revoke``.

The management plane for v1. Until we have an admin scope to gate an
HTTP CRUD on, the CLI is the only way to mint or retire keys. That's
deliberate — bootstrapping a key-management API that itself needs a
key is awkward, and "you have to be on the box to mint keys" is a
useful security property at this scale.

``create`` returns the plaintext token exactly once. After that only
the prefix is visible (in ``list`` output and in ``GET /v1/me``
responses); the secret is HMAC'd against ``api_key_salt`` and only
the digest is persisted.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import uuid
from typing import Final

import structlog

from aeroza.auth.hashing import mint_api_key_token
from aeroza.auth.models import API_KEY_DEFAULT_RATE_LIMIT_CLASS, ApiKeyRow
from aeroza.auth.store import create_api_key, list_api_keys, revoke_api_key
from aeroza.config import get_settings
from aeroza.shared.db import create_engine_and_session

log = structlog.get_logger(__name__)

EXIT_OK: Final[int] = 0
EXIT_NOT_FOUND: Final[int] = 1
EXIT_USAGE: Final[int] = 2


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="aeroza-api-keys",
        description="Manage Aeroza API keys.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    create = sub.add_parser("create", help="Mint a new API key (token printed once).")
    create.add_argument("--name", required=True, help="Human label for the key.")
    create.add_argument(
        "--owner",
        required=True,
        help="Free-form owner string (email, service name, internal id).",
    )
    create.add_argument(
        "--scope",
        action="append",
        default=[],
        help="Repeatable; may be specified zero or more times.",
    )
    create.add_argument(
        "--rate-limit-class",
        default=API_KEY_DEFAULT_RATE_LIMIT_CLASS,
        help=f"Rate-limit bucket name (default: {API_KEY_DEFAULT_RATE_LIMIT_CLASS}).",
    )

    sub.add_parser("list", help="List active keys (newest first).")

    revoke = sub.add_parser("revoke", help="Revoke a key by id.")
    revoke.add_argument("key_id", help="UUID of the key to revoke.")

    return parser


async def _run_create(args: argparse.Namespace) -> int:
    settings = get_settings()
    minted = mint_api_key_token(salt=settings.api_key_salt)
    db = create_engine_and_session(settings.database_url)
    try:
        async with db.sessionmaker() as session:
            row = await create_api_key(
                session,
                name=args.name,
                prefix=minted.prefix,
                key_hash=minted.key_hash,
                owner=args.owner,
                scopes=args.scope,
                rate_limit_class=args.rate_limit_class,
            )
            await session.commit()
        # Output is intentionally plain so it's friendly to copy-paste.
        # The token is printed once; we tell the user this loudly.
        print(f"id: {row.id}")
        print(f"name: {row.name}")
        print(f"owner: {row.owner}")
        print(f"prefix: {row.prefix}")
        print(f"scopes: {','.join(row.scopes) if row.scopes else '(none)'}")
        print(f"rate_limit_class: {row.rate_limit_class}")
        print()
        print("Save this token now — it will not be shown again:")
        print(f"  {minted.token}")
        return EXIT_OK
    finally:
        await db.dispose()


async def _run_list(_args: argparse.Namespace) -> int:
    settings = get_settings()
    db = create_engine_and_session(settings.database_url)
    try:
        async with db.sessionmaker() as session:
            rows = await list_api_keys(session)
        if not rows:
            print("(no active keys)")
            return EXIT_OK
        for row in rows:
            _print_row(row)
        return EXIT_OK
    finally:
        await db.dispose()


async def _run_revoke(args: argparse.Namespace) -> int:
    try:
        key_id = uuid.UUID(args.key_id)
    except ValueError:
        print(f"error: not a valid UUID: {args.key_id!r}", file=sys.stderr)
        return EXIT_USAGE

    settings = get_settings()
    db = create_engine_and_session(settings.database_url)
    try:
        async with db.sessionmaker() as session:
            changed = await revoke_api_key(session, key_id=key_id)
            await session.commit()
        if changed:
            print(f"revoked: {key_id}")
            return EXIT_OK
        print(f"no active key with id {key_id}", file=sys.stderr)
        return EXIT_NOT_FOUND
    finally:
        await db.dispose()


def _print_row(row: ApiKeyRow) -> None:
    last_used = row.last_used_at.isoformat() if row.last_used_at else "(never)"
    print(
        f"{row.id}  {row.prefix}  {row.name!r}  owner={row.owner}  "
        f"scopes={','.join(row.scopes) or '(none)'}  "
        f"class={row.rate_limit_class}  last_used={last_used}"
    )


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    runner = {
        "create": _run_create,
        "list": _run_list,
        "revoke": _run_revoke,
    }[args.command]
    return asyncio.run(runner(args))


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
