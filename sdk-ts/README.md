# @aeroza/sdk

TypeScript client for the Aeroza weather-intelligence API.

> **Status:** v0.1, in-tree only. Not published to npm yet — this package
> is the forcing function for API contract design while the surface
> stabilises. It's used as a workspace dep by the dev console
> (`web/`) so every change is dogfooded as it lands.

## Quickstart

```ts
import { AerozaClient } from "@aeroza/sdk";

const client = new AerozaClient({ apiBase: "http://localhost:8000" });

// Health
const health = await client.getHealth();
// → { status: "ok", version: "0.1.0" }

// Latest reflectivity at a point
const sample = await client.sampleGrid({ lat: 29.76, lng: -95.37 });
// → { type: "MrmsGridSample", value: 39.19, matchedLatitude: 29.8, ... }

// Polygon reducer — "is anything ≥ 40 dBZ in this region?"
const polygon = await client.reduceGridOverPolygon({
  polygon: "-95.7,29.5,-95.0,29.5,-95.0,30.0,-95.7,30.0",
  reducer: "count_ge",
  threshold: 40,
});
// → { type: "MrmsGridPolygonSample", value: 17, cellCount: 35, ... }
```

## What the client wraps

| Method | Route |
|---|---|
| `getHealth()` | `GET /health` |
| `getStats()` | `GET /v1/stats` |
| `listAlerts(query)` | `GET /v1/alerts` |
| `getAlert(id)` | `GET /v1/alerts/{id}` |
| `alertsStreamUrl()` | URL builder for `GET /v1/alerts/stream` (use with `EventSource`) |
| `listMrmsFiles(query)` | `GET /v1/mrms/files` |
| `listMrmsGrids(query)` | `GET /v1/mrms/grids` |
| `getMrmsGrid(fileKey)` | `GET /v1/mrms/grids/{file_key}` |
| `sampleGrid(query)` | `GET /v1/mrms/grids/sample` |
| `reduceGridOverPolygon(query)` | `GET /v1/mrms/grids/polygon` |

## Streaming alerts (SSE)

The client deliberately does not wrap `EventSource`. Browser code uses
the platform API directly; that keeps reconnection / error handling
under your control:

```ts
const client = new AerozaClient({ apiBase: "http://localhost:8000" });
const source = new EventSource(client.alertsStreamUrl());

source.addEventListener("alert", (e) => {
  const alert = JSON.parse((e as MessageEvent).data);
  console.log(alert.event, alert.severity);
});
```

A higher-level helper may land later — the contract should grow upward
once we know what consumers actually want.

## Errors

Non-2xx responses throw `AerozaApiError`. The `status` is the HTTP code;
`detail` is FastAPI's `detail` field when the body is JSON, or `null`
otherwise:

```ts
import { AerozaApiError } from "@aeroza/sdk";

try {
  await client.sampleGrid({ lat: 50, lng: -50 });
} catch (err) {
  if (err instanceof AerozaApiError && err.status === 404) {
    console.log("Out of domain:", err.detail);
  } else {
    throw err;
  }
}
```

## Conventions

- **`camelCase` on the wire.** The Python is `snake_case` internally;
  pydantic re-aliases at the boundary. This SDK preserves the wire
  shape exactly.
- **Geospatial ordering follows GeoJSON / OGC.** `bbox` and `polygon`
  use `lng,lat,lng,lat,…`; `point` uses `lat,lng`.
- **Times are ISO-8601 UTC.** `validAt`, `materialisedAt`, etc. all
  carry a `Z` suffix.

## Development

```sh
# From the repo root (npm workspaces).
npm install --workspaces
npm test --workspace=@aeroza/sdk
npm run typecheck --workspace=@aeroza/sdk
```
