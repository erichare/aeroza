"""enable RLS on all public application tables

Revision ID: 20260610_0900
Revises: 20260610_0800
Create Date: 2026-06-10 09:00:00

Resolves the Supabase security advisor lint ``rls_disabled_in_public`` for
the remaining 11 application tables in ``public``. On Supabase, every table
in ``public`` is exposed through PostgREST, and the platform grants the
``anon``/``authenticated`` API roles full privileges by default — so all of
these tables (including ``api_keys``, ``device_tokens``, and
``webhook_subscriptions``, which hold secrets) were readable and writable
with just the publishable anon key.

Nothing in the stack uses the Supabase Data API: the FastAPI backend
connects via asyncpg as the table owner, which bypasses RLS. So enabling
RLS with no policies gives the API roles deny-by-default without affecting
the application. The REVOKE strips the default PostgREST grants as defense
in depth; it is guarded because plain Postgres (local dev, CI) has no
``anon``/``authenticated`` roles. On plain Postgres enabling RLS is a
behavioral no-op for the owning role, so this is safe everywhere.

``public.spatial_ref_sys`` also trips this lint but is owned by the
PostGIS extension, and ``postgres`` cannot enable RLS on a table it does
not own — that one is handled by ``20260610_1000``, which relocates the
whole extension to the ``extensions`` schema (see
docs/POSTGIS-SCHEMA-RELOCATION.md for the Supabase runbook).
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "20260610_0900"
down_revision: str | None = "20260610_0800"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

TABLES: tuple[str, ...] = (
    "nws_alerts",
    "mrms_files",
    "mrms_grids",
    "mrms_nowcasts",
    "nowcast_verifications",
    "metar_observations",
    "webhook_subscriptions",
    "webhook_deliveries",
    "alert_rules",
    "api_keys",
    "device_tokens",
)

_TABLE_LIST = ", ".join(TABLES)


def upgrade() -> None:
    for table in TABLES:
        op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")
    op.execute(
        f"""
        DO $$
        BEGIN
            IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'anon')
               AND EXISTS (SELECT FROM pg_roles WHERE rolname = 'authenticated')
            THEN
                REVOKE ALL ON TABLE {_TABLE_LIST} FROM anon, authenticated;
            END IF;
        END
        $$
        """
    )


def downgrade() -> None:
    for table in TABLES:
        op.execute(f"ALTER TABLE {table} DISABLE ROW LEVEL SECURITY")
    op.execute(
        f"""
        DO $$
        BEGIN
            IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'anon')
               AND EXISTS (SELECT FROM pg_roles WHERE rolname = 'authenticated')
            THEN
                GRANT ALL ON TABLE {_TABLE_LIST} TO anon, authenticated;
            END IF;
        END
        $$
        """
    )
