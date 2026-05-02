"""add api_keys table

Revision ID: 20260502_1300
Revises: 20260502_1200
Create Date: 2026-05-02 13:00:00

Phase 6 — operator-managed API keys.

Each row stores one bearer-token credential. Keys are minted as
``aza_live_<random>`` strings; only the random portion's HMAC-SHA-256
hash is persisted (``key_hash``), keyed by the ``api_key_salt``
configured in env. The first 8 chars of the random portion (``prefix``)
are stored visible so an operator can tell which key is which without
ever needing the full secret again — this is the same idea as Stripe's
``sk_live_*`` prefixes.

Auth is opt-in: when ``AEROZA_AUTH_REQUIRED=false`` (the default), the
middleware records who is calling but lets anonymous traffic through.
Flip the env flag once you have a few keys minted and a UI / CLI to
manage them.

This PR ships:
- the schema + ORM model
- the hash + token utilities
- the bearer-token dependency
- the ``aeroza-api-keys`` CLI for create/revoke/list
- ``GET /v1/me`` to introspect the calling key

CRUD over HTTP (``/v1/api-keys``) is intentionally **not** in this
slice — bootstrapping a key-management surface that itself requires a
key is awkward, and the CLI is good enough for v1. The HTTP CRUD lands
once we have an admin scope to gate it on.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260502_1300"
down_revision: str | None = "20260502_1200"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "api_keys",
        sa.Column(
            "id",
            sa.dialects.postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("name", sa.Text(), nullable=False),
        # Visible identifier ("aza_live_xxxxxxxx") — first 8 chars of the
        # random portion. Indexed so the auth middleware's per-request
        # lookup is one B-tree probe.
        sa.Column("prefix", sa.Text(), nullable=False),
        # HMAC-SHA-256 hex of the random portion, keyed by api_key_salt.
        # 64 hex chars; never the secret itself.
        sa.Column("key_hash", sa.Text(), nullable=False),
        # Free-form scope strings (e.g. "read:alerts", "write:webhooks").
        # text[] over a separate scopes table — these are static labels
        # checked on every request, and a join would be wasteful. Defaults
        # to an empty array; a key with no scopes can call anonymous-tier
        # routes only.
        sa.Column(
            "scopes",
            sa.dialects.postgresql.ARRAY(sa.Text()),
            nullable=False,
            server_default=sa.text("ARRAY[]::text[]"),
        ),
        # Rate-limit bucket name (e.g. "default", "premium"). Looked up
        # against a static config map at request time. Per-class buckets
        # are still off-by-default; turning them on lands once we have
        # the Redis token-bucket plumbed into the dependency.
        sa.Column(
            "rate_limit_class",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'default'"),
        ),
        sa.Column(
            "status",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'active'"),
        ),
        # Free-text label for who owns this key — usually an email, but
        # we don't validate the shape so a user-id, a service name, or
        # any internal identifier all work.
        sa.Column("owner", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "last_used_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.UniqueConstraint("prefix", name="uq_api_keys_prefix"),
        sa.CheckConstraint(
            "status IN ('active', 'revoked')",
            name="api_keys_status_valid",
        ),
        sa.CheckConstraint(
            "char_length(prefix) >= 4",
            name="api_keys_prefix_min_length",
        ),
        sa.CheckConstraint(
            "char_length(key_hash) = 64",
            name="api_keys_key_hash_sha256_length",
        ),
    )


def downgrade() -> None:
    op.drop_table("api_keys")
