"""enable RLS on alembic_version

Revision ID: 20260610_0800
Revises: 20260603_1200
Create Date: 2026-06-10 08:00:00

Resolves the Supabase security advisor lint ``rls_disabled_in_public`` for
``public.alembic_version``. On Supabase, every table in ``public`` is exposed
through PostgREST, and the platform grants the ``anon``/``authenticated`` API
roles full privileges by default — so the Alembic bookkeeping table was
readable (and writable) with just the publishable anon key.

No policies are created. Alembic and the application connect as the table
owner, which bypasses RLS, while the API roles get deny-by-default. On plain
Postgres (local dev, CI) enabling RLS is a behavioral no-op for the owning
role, so this is safe everywhere.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "20260610_0800"
down_revision: str | None = "20260603_1200"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("ALTER TABLE alembic_version ENABLE ROW LEVEL SECURITY")


def downgrade() -> None:
    op.execute("ALTER TABLE alembic_version DISABLE ROW LEVEL SECURITY")
