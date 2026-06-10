"""relocate the postgis extension from public to the extensions schema

Revision ID: 20260610_1000
Revises: 20260610_0900
Create Date: 2026-06-10 10:00:00

Resolves the last two Supabase security advisor findings: ``public.
spatial_ref_sys`` tripping ``rls_disabled_in_public`` (0013) and postgis
itself tripping ``extension_in_public`` (0014). ``spatial_ref_sys`` is owned
by the extension, so RLS can never be enabled on it by ``postgres``; the fix
is moving the whole extension out of ``public``. Once postgis lives in
``extensions``, both lints stop firing (that schema is hard-excluded by the
linter and not exposed through PostgREST).

PostGIS >= 2.3 is marked non-relocatable, so a plain ``ALTER EXTENSION
postgis SET SCHEMA`` is refused. The PostGIS project publishes a sanctioned
superuser recipe instead (https://postgis.net/documentation/tips/
tip-move-postgis-schema/): temporarily flip ``pg_extension.extrelocatable``,
move the schema, then run a dummy extension upgrade so postgis rewrites its
internal schema-qualified references. Supabase reproduces the same SQL in
their postgis docs and asks that *their support team* run it, because the
``postgres`` role on Supabase is not a superuser.

Accordingly this migration takes one of three branches:

- postgis not installed, or already outside ``public`` → no-op. This is the
  prod path after Supabase support performs the relocation (and the path for
  fresh Supabase projects where the dashboard installs into ``extensions``).
- postgis in ``public`` and we are superuser (local docker compose, CI
  postgis/postgis:16-3.5 containers) → run the recipe, then put
  ``extensions`` on the database-level search_path so unqualified runtime
  calls (``ST_Intersects``, ``ST_AsGeoJSON``, …) and future ``geometry``
  column DDL keep resolving. Supabase already ships ``"$user", public,
  extensions`` as the default search_path, so no equivalent step is needed
  there.
- postgis in ``public`` but not superuser (Supabase before the support
  ticket) → warn loudly and no-op, so deploys never break. The runbook for
  the out-of-band prod step is docs/POSTGIS-SCHEMA-RELOCATION.md.

Existing ``geometry`` columns (``nws_alerts.geometry``), their GIST index,
and stored data reference the type by OID and survive the move untouched.
Connections opened before the move keep their old search_path — restart the
API / recycle pools after migrating a live environment.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence

from sqlalchemy import text

from alembic import op

revision: str = "20260610_1000"
down_revision: str | None = "20260610_0900"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

log = logging.getLogger("alembic.runtime.migration")

APP_SEARCH_PATH = '"$user", public, extensions'


def _postgis_schema() -> str | None:
    """Schema the postgis extension currently lives in, or None if absent."""
    return (
        op.get_bind()
        .execute(
            text(
                "SELECT n.nspname FROM pg_extension e"
                " JOIN pg_namespace n ON n.oid = e.extnamespace"
                " WHERE e.extname = 'postgis'"
            )
        )
        .scalar()
    )


def _is_superuser() -> bool:
    return bool(
        op.get_bind()
        .execute(text("SELECT rolsuper FROM pg_roles WHERE rolname = current_user"))
        .scalar()
    )


def _relocate_postgis(target_schema: str) -> None:
    """The PostGIS-project relocation recipe. Requires superuser.

    ``UPDATE TO "ANY"`` + ``UPDATE`` is the documented dummy-upgrade pair for
    PostGIS >= 3.3 (our pinned image is 16-3.5); it re-runs the extension
    upgrade script, which recreates every postgis function with references
    resolved against the new schema. ``SET SCHEMA`` alone would leave the
    extension half-broken.
    """
    op.execute("UPDATE pg_extension SET extrelocatable = true WHERE extname = 'postgis'")
    op.execute(f'ALTER EXTENSION postgis SET SCHEMA "{target_schema}"')
    op.execute('ALTER EXTENSION postgis UPDATE TO "ANY"')
    op.execute("ALTER EXTENSION postgis UPDATE")
    op.execute("UPDATE pg_extension SET extrelocatable = false WHERE extname = 'postgis'")


def _set_database_search_path(search_path: str) -> None:
    """Persist ``search_path`` for new connections, and adopt it now.

    ``ALTER DATABASE … SET`` only affects connections opened afterwards; the
    session-level ``SET`` covers the rest of this alembic run (e.g. a later
    migration creating a ``geometry`` column on a fresh database).
    """
    dbname = op.get_bind().execute(text("SELECT current_database()")).scalar()
    op.execute(f'ALTER DATABASE "{dbname}" SET search_path TO {search_path}')
    op.execute(f"SET search_path TO {search_path}")


def upgrade() -> None:
    schema = _postgis_schema()
    if schema != "public":
        return
    if not _is_superuser():
        log.warning(
            "postgis is installed in 'public' but %s is not a superuser, so it "
            "cannot be relocated here (PostGIS >= 2.3 refuses SET SCHEMA, and the "
            "extrelocatable workaround needs superuser). On Supabase, ask support "
            "to run the relocation recipe — see docs/POSTGIS-SCHEMA-RELOCATION.md. "
            "Skipping; this migration will no-op once postgis is in 'extensions'.",
            op.get_bind().execute(text("SELECT current_user")).scalar(),
        )
        return
    op.execute("CREATE SCHEMA IF NOT EXISTS extensions")
    op.execute("GRANT USAGE ON SCHEMA extensions TO PUBLIC")
    _relocate_postgis("extensions")
    _set_database_search_path(APP_SEARCH_PATH)


def downgrade() -> None:
    schema = _postgis_schema()
    if schema != "extensions" or not _is_superuser():
        return
    _relocate_postgis("public")
    dbname = op.get_bind().execute(text("SELECT current_database()")).scalar()
    op.execute(f'ALTER DATABASE "{dbname}" RESET search_path')
    op.execute('SET search_path TO "$user", public')
    # The extensions schema is left in place: cheap, and other extensions may
    # have been installed into it meanwhile.
