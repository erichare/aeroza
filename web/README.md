# Aeroza · web

The Next.js 15 app that ships the public face of Aeroza. Five surfaces:

- **`/`** — landing page (hero, three feature cards, primary CTA into `/map`).
- **`/map`** — interactive map. MapLibre raster basemap (CARTO Voyager light) + alert GeoJSON polygons coloured by severity + MRMS reflectivity raster overlay (toggleable) + scrubbable 6-hour timeline that filters alerts client-side by their `effective` / `expires` window.
- **`/calibration`** — `(algorithm × forecast horizon)` MAE / bias / RMSE matrix with a per-row sparkline trending the metric over the requested window (1h / 6h / 24h / 7d / 30d).
- **`/console`** — dev panels: live alerts SSE stream, MRMS file catalog, materialised grids, point sample, system health.
- **`/docs`** — overview / quickstart / concepts / API reference, with editorial typography.

Every fetch flows through `@aeroza/sdk` (the workspace's TypeScript SDK), so any awkwardness in the API contract surfaces here first.

## Run it

```bash
# From the repo root: brings up Postgres + Redis + NATS, runs FastAPI on :8000.
make dev

# In separate terminals, start the long-running ingesters so the web has
# real data to render.
make ingest-alerts
make ingest-mrms
make materialise-mrms

# In another terminal:
make web-dev      # http://localhost:3000
```

The default API base is `http://localhost:8000`. Override with:

```bash
NEXT_PUBLIC_AEROZA_API_URL=https://staging.example.com npm run dev
```

## End-to-end smoke

A Playwright spec at `e2e/map.spec.ts` loads `/map`, waits for the MapLibre style + first frame, then screenshots the canvas and asserts non-zero alpha pixels — robust across drivers because it goes through the page compositor (not WebGL `readPixels`, whose back buffer is undefined after the swap).

```bash
make web-dev               # one terminal — keep it running
npm --prefix web run test:e2e
```

CI runs the same spec in the `web · Playwright /map smoke` job.

## Stack

- Next.js 15 (App Router) + React 19, TypeScript strict.
- Tailwind CSS with a hand-rolled Meridian palette (pale glacier base + prussian-ink text + aged-brass accent).
- `next/font/google` for Inter (sans), Fraunces (display), JetBrains Mono.
- MapLibre GL JS for the map surface; raster tiles only — vector MVT is roadmap.
- Native `EventSource` for SSE, native `fetch` for REST.
- `@aeroza/sdk` from the workspace — typed wrapper over the v1 REST API.

No SWR, no axios, no shadcn. The surface is small and the dependency budget is intentionally tight.
