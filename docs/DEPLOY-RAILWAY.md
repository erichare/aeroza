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
                             │  ├─ prewarm-tiles ───────►│──► Cloudflare R2
                             │  ├─ dispatch-webhooks     │    (tiles.aeroza.app)
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

NATS is **required** in production now: the `prewarm` worker (see
`Procfile.railway`) subscribes to `aeroza.mrms.grids.new` and renders
the CONUS tile pyramid into Cloudflare R2 — without a reachable NATS
service the worker connects but never receives an event, and
`tiles.aeroza.app` 404s every tile until the on-demand FastAPI
write-through catches up grid-by-grid. Deploy a `nats:2-alpine` Railway
service and point `AEROZA_NATS_URL` at its internal hostname.

Cloudflare R2 is **required** for the tile CDN. The `prewarm` worker
uploads every rendered tile under `{file_key}/{z}/{x}/{y}.webp`, and
the deployed dashboard fetches tiles from `https://tiles.aeroza.app`
(a custom domain on the R2 bucket). Without the four R2 env vars the
prewarm worker logs a one-shot warning and idles — no crash loop, but
no tile prewarming either.

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
3. **Database → Extensions** → enable **postgis**, keeping the schema
   at the dashboard default **`extensions`** — do NOT install it into
   `public`, or the security advisor flags `spatial_ref_sys`
   (see docs/POSTGIS-SCHEMA-RELOCATION.md). Do this BEFORE the first
   `alembic upgrade head`: the initial migration's
   `CREATE EXTENSION IF NOT EXISTS postgis` would otherwise install it
   into `public`, and the `postgres` role cannot relocate it afterwards.
   (PostGIS ships in Supabase out of the box; you just have to flip the
   toggle.)
4. **Database → Connection string** → URI format → choose **Session
   pooler** (NOT *Direct connection*, NOT *Transaction pooler*). Why:
   - *Direct connection* (`db.PROJECT.supabase.co`) resolves to IPv6
     only on the Free tier, and Railway has no IPv6 egress, so every
     connect attempt instantly fails with `Network is unreachable`.
   - *Transaction pooler* (port `6543`) breaks asyncpg's prepared
     statements and breaks alembic migrations.
   - *Session pooler* (port `5432` on the `pooler.supabase.com` host)
     is IPv4 and supports session-level features. This is the one.

   It looks like:
   ```
   postgresql://postgres.PROJECT_REF:PASSWORD@aws-0-us-east-1.pooler.supabase.com:5432/postgres
   ```
   Convert the scheme for asyncpg:
   ```
   postgresql+asyncpg://postgres.PROJECT_REF:PASSWORD@aws-0-us-east-1.pooler.supabase.com:5432/postgres
   ```
   Note the username is `postgres.PROJECT_REF` (with the project ref
   suffix), not just `postgres` — the pooler uses that to route to
   your tenant.

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
   AEROZA_REDIS_URL=redis://stub-not-used   # optional, see note below
   AEROZA_NATS_URL=nats://${{nats.RAILWAY_PRIVATE_DOMAIN}}:4222
   AEROZA_API_KEY_SALT=GENERATE_ME_WITH_OPENSSL_RAND_BASE64_32
   AEROZA_CORS_ALLOW_ORIGINS=https://aeroza.vercel.app
   AEROZA_DATA_DIR=/app/data

   # Cloudflare R2 — feeds the tile prewarm worker. All four required
   # in production or tiles.aeroza.app stays empty. See section 5 below
   # for how to provision the bucket + access keys.
   AEROZA_R2_ENDPOINT=https://<account-id>.r2.cloudflarestorage.com
   AEROZA_R2_BUCKET=aeroza-tiles
   AEROZA_R2_ACCESS_KEY_ID=…
   AEROZA_R2_SECRET_ACCESS_KEY=…
   AEROZA_R2_PUBLIC_BASE_URL=https://tiles.aeroza.app
   ```
   Generate the salt:
   ```bash
   openssl rand -base64 32
   ```
   The Redis URL can stay as a stub because the API has a graceful
   fallback. NATS is required (the prewarm worker depends on it) —
   deploy a `nats:2-alpine` Railway service and reference its internal
   hostname above. If you also want SSE streaming, deploy `redis:7-alpine`
   and swap the Redis stub for `redis://${{redis.RAILWAY_PRIVATE_DOMAIN}}:6379`.
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

## 2b. Cloudflare R2: provision the tile bucket

The `prewarm` worker uploads every CONUS tile to a Cloudflare R2 bucket
that the frontend reads via a custom domain. Without R2, every tile on
`tiles.aeroza.app` returns 404.

1. Cloudflare dashboard → **R2** → **Create bucket** → name it
   `aeroza-tiles`. Region: `Automatic`.
2. **Settings → Public access → Custom domain** → add
   `tiles.aeroza.app` (or whatever subdomain you want). Cloudflare
   provisions an HTTPS endpoint that fronts the bucket with their
   global CDN. The frontend reads `tiles.aeroza.app/{fileKey}/{z}/{x}/{y}.webp`.
3. **R2 → Manage R2 API Tokens → Create API token** →
   `aeroza-tiles-writer`, permissions `Object Read & Write`, specify
   `aeroza-tiles` as the only bucket. Save the four values it shows you:
   - **Access Key ID** → `AEROZA_R2_ACCESS_KEY_ID`
   - **Secret Access Key** → `AEROZA_R2_SECRET_ACCESS_KEY`
   - **Endpoint** (`https://<account>.r2.cloudflarestorage.com`) →
     `AEROZA_R2_ENDPOINT`
   - Bucket name (`aeroza-tiles`) → `AEROZA_R2_BUCKET`
4. Drop them into the Railway service Variables panel (see step 2.4
   above), redeploy, and check the prewarm worker logs for
   `tiles.prewarm.consumer.start … target=r2`. The first new MRMS grid
   after that should populate ≈680 tiles in R2 within ~30 seconds.

Backfill seed (after first deploy): see `scripts/backfill-prewarm.py`
— a one-shot script that walks the pyramid for the N most-recent
fileKeys so the CDN is hot the moment the frontend starts polling.

## 3. Vercel: point the web frontend at the Railway API

1. Vercel project settings → **Environment variables**.
2. Add `NEXT_PUBLIC_AEROZA_API_URL` = `https://YOUR-RAILWAY-DOMAIN`
   for **all environments** (Production, Preview, Development).
3. **Redeploy** the latest commit so the new env var bakes into the
   client bundle (`NEXT_PUBLIC_*` is read at build time).

## 4. Seed a few featured events so visitors see content

Without seeding, `/replay` shows "no grids in this window" until the
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
- `https://aeroza.vercel.app/replay` — pick "Houston Derecho", radar
  + amber alert polygons render in sync as the scrubber advances.

## Operational notes

- **RAM**: Railway Hobby starts at 512 MB; the materialiser per-grid
  can spike to 1.5 GB. Bump to a plan that gives you 2-4 GB or you'll
  see OOM-kills on the materialiser process.
- **Cost ceiling**: enable Railway's monthly spend limit. With the
  volume + always-on workers you'll burn ~$10-20/mo on Hobby.
- **Logs**: Railway logs are prefixed by honcho process name
  (`api.1`, `alerts.1`, `mrms.1`, `materialise.1`, `prewarm.1`,
  `webhooks.1`). Use the search box in the Railway log UI to scope.
  For tile 404 debugging, filter on `prewarm` and look for
  `tiles.prewarm.consumer.event_done` — the `rendered` / `skipped_existing`
  counts tell you whether R2 is being populated per grid.
- **Updates**: pushing to `main` triggers a Railway redeploy
  automatically (Railway's GitHub integration is wired by default).
  Migrations run before the new image takes traffic.
