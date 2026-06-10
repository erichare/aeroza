# Relocating PostGIS out of `public`

**Status:** migration `20260610_1000` shipped; prod (Supabase) step pending a
support ticket.

## Problem

The Supabase security advisor flags two findings on project
`qijajhckwxsclvyzeaez`:

- `rls_disabled_in_public` (0013) on `public.spatial_ref_sys`
- `extension_in_public` (0014) on `postgis`

`spatial_ref_sys` is a lookup table owned by the PostGIS extension (created
as `supabase_admin` on Supabase). The `postgres` role cannot `ALTER TABLE …
ENABLE ROW LEVEL SECURITY` on it ("must be owner"), and a `REVOKE … FROM
anon, authenticated` run as `postgres` is a silent no-op because the grants
were made by `supabase_admin`. So unlike the 11 app tables fixed in
`20260610_0900`, this finding cannot be cleared in place — the supported fix
is moving the whole extension to the `extensions` schema. The advisor
hard-excludes that schema, and it is not exposed through PostgREST, so both
lints clear permanently. Supabase offers no per-finding mute, so this is the
only clean resolution.

## Why a plain `ALTER EXTENSION … SET SCHEMA` doesn't work

PostGIS ≥ 2.3 marks itself non-relocatable; `ALTER EXTENSION postgis SET
SCHEMA extensions` fails with `extension "postgis" does not support SET
SCHEMA`. The PostGIS project publishes the sanctioned workaround
([tip-move-postgis-schema](https://postgis.net/documentation/tips/tip-move-postgis-schema/)),
which Supabase's own [postgis docs](https://supabase.com/docs/guides/database/extensions/postgis)
reproduce in their Troubleshooting section:

```sql
BEGIN;
UPDATE pg_extension SET extrelocatable = true WHERE extname = 'postgis';
ALTER EXTENSION postgis SET SCHEMA extensions;
ALTER EXTENSION postgis UPDATE TO "ANY";  -- "<version>next" on PostGIS <= 3.2
ALTER EXTENSION postgis UPDATE;           -- dummy upgrade rewrites internal refs
UPDATE pg_extension SET extrelocatable = false WHERE extname = 'postgis';
COMMIT;
```

This needs **superuser** (it writes `pg_catalog`). On Supabase, `postgres` is
not a superuser and the extension is owned by `supabase_admin`, so Supabase's
documented path for an existing project is: *contact Supabase support and ask
them to run this SQL*. (The self-service alternative — `DROP EXTENSION
postgis CASCADE` and re-create in `extensions` — would drop
`nws_alerts.geometry` and its GIST index; not worth it when support can do a
non-destructive move.)

## Audit findings (what depends on PostGIS here)

| Reference | Location | Affected by the move? |
|---|---|---|
| `CREATE EXTENSION IF NOT EXISTS postgis` | `alembic/versions/20260501_1300_initial_schema.py` | No-op on DBs where postgis exists; on a fresh vanilla DB it installs into `public`, and `20260610_1000` relocates it later in the same `upgrade head` run |
| `geometry(GEOMETRY,4326)` column | `nws_alerts.geometry` (only geometry column; `mrms_grids` stores arrays/metadata, no geometry type) | Safe — columns, GIST index, typmods and data reference the type by OID and survive relocation |
| `func.ST_AsGeoJSON`, `ST_GeomFromText`, `ST_MakeEnvelope`, `ST_Intersects` | `aeroza/query/alerts.py` | Runtime name resolution via `search_path` — covered (see below) |
| `func.ST_SetSRID`, `ST_MakePoint`, `ST_Intersects` | `aeroza/push/dispatch.py` | Same |
| Raw `ST_AsGeoJSON` / `ST_SetSRID(ST_GeomFromText(…))` in tests | `tests/test_nws_alerts_store.py`, `tests/test_push_dispatch_db.py` | Same; `tests/conftest.py` now disposes the engine pool after `upgrade head` so no pooled connection keeps a pre-relocation `search_path` |
| `spatial_ref_sys` autogen exclusion | `alembic/env.py` | Kept — harmless either way |
| Views / functions / generated columns calling `ST_*` | none exist | n/a |

`search_path`: Supabase ships `"$user", public, extensions` as the database
default, so the app's unqualified `ST_*` calls keep working after support
relocates the extension — no app change needed. Plain Postgres does **not**
include `extensions`, so the migration's superuser branch runs `ALTER
DATABASE … SET search_path TO "$user", public, extensions` itself.

## What migration `20260610_1000` does

1. postgis absent or already outside `public` → no-op (prod after the
   support ticket; fresh Supabase projects where the dashboard default
   schema is `extensions`).
2. postgis in `public` + superuser (local docker compose, both CI postgis
   service containers) → runs the recipe above, creates/grants the
   `extensions` schema, and sets the database+session `search_path`.
3. postgis in `public` + not superuser (Supabase today) → logs a warning and
   no-ops so `alembic upgrade head` in `scripts/railway-start.sh` never
   breaks a deploy.

The migration is convergent: it keeps no-opping on every deploy until the
prod relocation happens out-of-band, then branch 1 applies forever.

## Prod runbook (Supabase)

1. Confirm a recent backup / note the PITR timestamp.
2. Open a Supabase support ticket: *"Please run the PostGIS schema
   relocation SQL from your postgis docs (Troubleshooting → moving postgis
   to the extensions schema) on project `qijajhckwxsclvyzeaez`, database
   `postgres`."* Quote the SQL block above. Do this in a quiet window.
3. After support confirms, verify as `postgres`:

   ```sql
   SELECT extname, n.nspname FROM pg_extension e
   JOIN pg_namespace n ON n.oid = e.extnamespace WHERE extname = 'postgis';
   -- expect: postgis | extensions
   SELECT count(*) FROM extensions.spatial_ref_sys;   -- expect 8500
   SELECT ST_AsGeoJSON(geometry) FROM nws_alerts LIMIT 1;  -- unqualified call resolves
   ```

4. `NOTIFY pgrst, 'reload schema';` (or restart the API settings) so
   PostgREST drops `spatial_ref_sys` from its schema cache.
5. Redeploy / restart the Railway API service so pooled connections are
   re-established (old ones keep working here since Supabase's search_path
   already contained `extensions`, but recycling is cheap insurance).
6. Re-run the Security Advisor: 0013 (`spatial_ref_sys`) and 0014
   (`postgis`) should both clear.

## Local dev note

Existing `aeroza-postgres-data` volumes get relocated automatically on the
next `make db-upgrade` (the compose user is the bootstrap superuser).
Restart the API afterwards — live connections keep the old `search_path`.

## Rollback

`alembic downgrade -1` moves the extension back to `public` and resets the
database `search_path` (superuser environments only; it no-ops elsewhere).
On Supabase, rollback would again be a support ticket with the inverse SQL.

## Validation performed

Against an ephemeral `postgis/postgis:16-3.5` container (linux/amd64, port
55432), both provisioning paths:

- image-default database (postgis **+ topology + tiger preinstalled in
  `public`**, matching local compose / the CI web-e2e service): `alembic
  upgrade head` relocates; extension reports `extensions`,
  `spatial_ref_sys` row count intact, unqualified `ST_*` resolves on a new
  connection, `nws_alerts.geometry` typmod + GIST index intact;
  `downgrade -1` returns postgis to `public`; re-upgrade relocates again.
- fresh database created from `template1` (no postgis, matching the CI
  integration job): full `upgrade head` installs into `public` then
  relocates in the same run.
- repo integration test suite (`pytest -m integration`) green against the
  relocated database.
