# Aeroza · dev console

A minimal Next.js 15 app that visualises the Aeroza FastAPI surface. Three panels:

- **Alerts · live stream** — `/v1/alerts/stream` (SSE) plus a polled `/v1/alerts` list.
- **MRMS · file catalog** — `/v1/mrms/files` with a sliver-timeline of `valid_at` and per-row stats.
- **System · health** — `/health` polled every 10 s.

This is the dev/demo client. The polished Phase 5 reference web app
(MapLibre + scrubbable timeline + landing page) will replace it later.

## Run it

```bash
# From the repo root: brings up Postgres + Redis + NATS, runs FastAPI on :8000.
make dev

# In a second terminal:
cd web
npm install
npm run dev
# Console: http://localhost:3000
# FastAPI: http://localhost:8000
```

The default API base is `http://localhost:8000`. Override with:

```bash
NEXT_PUBLIC_AEROZA_API_URL=https://staging.example.com npm run dev
```

## Why no MapLibre / GraphQL / SDK yet?

Scope. This console exists to make the *current* backend feel real today —
the SSE stream is the hardest thing to demo without a UI, and the MRMS
catalog goes from "rows in Postgres" to "files arriving every 2 minutes"
much faster when you can see the timeline tick.

When `/v1/mrms/grids` and the read API for materialised grids ship, this
console will gain a fourth panel rendering the most-recent reflectivity
grid as a quick canvas heatmap. The map (MapLibre + vector tiles) belongs
to the Phase 5 polished client.

## Stack

- Next.js 15 (App Router) + React 19
- TypeScript strict
- Tailwind CSS for styling (no component library)
- Native `EventSource` for SSE; native `fetch` for REST

No SWR, no axios, no shadcn — the surface is small and the dependency
budget is intentionally tight.
