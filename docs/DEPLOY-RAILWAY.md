# Railway deploy

Step-by-step for putting the Aeroza API + workers on Railway, paired
with a Vercel-hosted `web/` frontend. ~30 minutes start to finish if
you already have Railway and Supabase accounts.

## Architecture

```
                             ┌─ Railway ─────────────────┐
 https://aeroza.vercel.app   │  api+workers              │
       (Vercel, free)        │  ├─ uvicorn (port $PORT)  │
            │                │  ├─ ingest-alerts         │
            │   API calls    │  ├─ ingest-mrms           │
            └───────────────►│  ├─ materialise-mrms      │
                             │  ├─ dispatch-webhooks     │
                             │  └─ volume → /app/data    │
                             └──────────┬────────────────┘
                                        │ Postgres DSN
                                        ▼
                             ┌─ Supabase (free tier) ────┐
                             │  Postgres 16 + PostGIS     │
                             └────────────────────────────┘
```

The whole API + worker fleet runs as **one Railway service** — they
share a volume (`/app/data`) so the materialiser worker can write Zarr
grids the API serves as raster tiles. Railway volumes can't be shared
across services, so splitting the workers into their own services would
break the radar replay.

NATS is intentionally omitted — only the SSE alerts stream depends on
it, and the demo UI doesn't need it. Add it later if you wire up
streaming alerts.

## What you'll create

1. **Supabase project** — Postgres 16 + PostGIS, free tier.
2. **Railway project** — one service from this repo's Dockerfile, one
   volume.
3. **Vercel env var** — point `NEXT_PUBLIC_AEROZA_API_URL` at the
   Railway URL.

Total cost: ~$5-15/mo on Railway depending on uptime + worker activity.
Vercel hobby and Supabase free tier are $0.

## 1. Supabase: spin up Postgres+PostGIS

1. Go to [supabase.com](https://supabase.com) → **New project**.
   Name it `aeroza`, pick a region near your Railway region (US East
   if you'll deploy to AWS us-east-1).
2. Set a strong DB password, save it somewhere — you'll need the full
   connection string in step 2.
3. **Database → Extensions** → enable **postgis**. (PostGIS ships in
   Supabase out of the box; you just have to flip the toggle.)
4. **Database → Connection string** → switch to **URI** format. You
   want the **direct connection** string, not the pooler — alembic
   migrations need session-level DDL. Looks like:
   ```
   postgresql://postgres.PROJECT:PASSWORD@aws-0-us-east-1.pooler.supabase.com:5432/postgres
   ```
   Convert to async by swapping the scheme:
   ```
   postgresql+asyncpg://postgres.PROJECT:PASSWORD@aws-0-us-east-1.pooler.supabase.com:5432/postgres
   ```

## 2. Railway: deploy the API + workers

1. Go to [railway.app](https://railway.app) → **New project** → **Deploy
   from GitHub repo** → select this repo.
2. Railway auto-detects the `railway.json` and the Dockerfile. The
   build will install eccodes + the `grib` extra, taking ~3-5 minutes.
3. Once the first build finishes, the service will fail healthchecks
   (no DB yet). That's expected. Open the service settings.
4. **Variables** — add these:
   ```
   AEROZA_ENV=production
   AEROZA_DATABASE_URL=postgresql+asyncpg://postgres.PROJECT:PASSWORD@…
   AEROZA_REDIS_URL=redis://stub-not-used   # see note below
   AEROZA_NATS_URL=nats://stub-not-used     # see note below
   AEROZA_API_KEY_SALT=GENERATE_ME_WITH_OPENSSL_RAND_BASE64_32
   AEROZA_CORS_ALLOW_ORIGINS=https://aeroza.vercel.app
   AEROZA_DATA_DIR=/app/data
   ```
   Generate the salt:
   ```bash
   openssl rand -base64 32
   ```
   The Redis / NATS URLs can stay as stubs because the API has graceful
   fallbacks for both. If you want SSE streaming, deploy a `redis:7-alpine`
   and `nats:2-alpine` as separate Railway services and set the URLs to
   their internal `${{redis.RAILWAY_PRIVATE_DOMAIN}}:6379` shapes.
5. **Settings → Volumes** — add one volume mounted at `/app/data`. 5 GB
   is plenty for a few featured events; bump if you intend to keep many
   weeks of grids.
6. **Settings → Networking** — generate a public domain (Railway will
   give you something like `aeroza-api-production.up.railway.app`).
7. Trigger a redeploy. Watch the logs — you should see:
   ```
   [boot] applying database migrations…
   …alembic upgrade head…
   [boot] starting api + workers via honcho…
   13:42:11 api.1     | INFO:     Uvicorn running on http://0.0.0.0:8000
   13:42:11 alerts.1  | INFO ingest_alerts:starting…
   13:42:11 mrms.1    | INFO ingest_mrms:starting…
   13:42:11 materialise.1 | INFO materialise_mrms:starting…
   ```
8. Hit `https://aeroza-api-production.up.railway.app/health` — should
   return `{"status":"ok","version":"…"}`.

## 3. Vercel: point the web frontend at the Railway API

1. Vercel project settings → **Environment variables**.
2. Add `NEXT_PUBLIC_AEROZA_API_URL` = `https://YOUR-RAILWAY-DOMAIN`
   for **all environments** (Production, Preview, Development).
3. **Redeploy** the latest commit so the new env var bakes into the
   client bundle (`NEXT_PUBLIC_*` is read at build time).

## 4. Seed a few featured events so visitors see content

Without seeding, `/demo` shows "no grids in this window" until the
ingest worker pulls live MRMS — which only covers the last few hours.
Featured events from 2021–2024 require manual backfill:

```bash
# Run from your laptop, not the Railway box.
RAILWAY_API=https://aeroza-api-production.up.railway.app

# Houston Derecho (May 16, 2024)
curl -X POST "$RAILWAY_API/v1/admin/seed-event?since=2024-05-16T22:00:00Z&until=2024-05-17T02:30:00Z"

# Mid-January Plains Blizzard (Jan 12-13, 2024)
curl -X POST "$RAILWAY_API/v1/admin/seed-event?since=2024-01-12T18:00:00Z&until=2024-01-13T18:00:00Z"

# Add more — the FEATURED_EVENTS catalog in
# web/lib/featuredEvents.ts has the canonical (start, end) pairs.
```

Each seed runs server-side and takes 1-3 minutes per event window
depending on how many MRMS files NOAA's bucket holds for that window.

## What to verify before sharing

- `https://aeroza.vercel.app/` — landing page numbers populate
  from the API (no "Live numbers unavailable" banner).
- `https://aeroza.vercel.app/map` — radar tiles paint (fresh CONUS
  reflectivity from the live ingest).
- `https://aeroza.vercel.app/demo` — pick "Houston Derecho", radar
  + amber alert polygons render in sync as the scrubber advances.

## Operational notes

- **RAM**: Railway Hobby starts at 512 MB; the materialiser per-grid
  can spike to 1.5 GB. Bump to a plan that gives you 2-4 GB or you'll
  see OOM-kills on the materialiser process.
- **Cost ceiling**: enable Railway's monthly spend limit. With the
  volume + always-on workers you'll burn ~$10-20/mo on Hobby.
- **Logs**: Railway logs are prefixed by honcho process name
  (`api.1`, `alerts.1`, `mrms.1`, `materialise.1`, `webhooks.1`). Use
  the search box in the Railway log UI to scope.
- **Updates**: pushing to `main` triggers a Railway redeploy
  automatically (Railway's GitHub integration is wired by default).
  Migrations run before the new image takes traffic.
