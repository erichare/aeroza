# Aeroza roadmap

A history of what's shipped and a credible plan for what's next. Specific items are commitments; broader phases are aspirational and likely to reshape as we learn.

This file is the source of truth for "what state is the project in?" — the README links here, the dev console points users here, and PRs that close out a phase update this file alongside the code.

## Shipped (phases 0 – 5)

### Phase 0 — Scaffold

FastAPI app skeleton, Postgres + PostGIS migrations via Alembic, Redis, NATS JetStream, Docker Compose, GitHub Actions CI (`make check` + integration tests), `uv`-managed Python deps. Plus `/health` and the v1 API stub.

### Phase 1 — Alerts

NWS active-alerts ingest via the public API. Alerts are persisted to `nws_alerts` with severity / urgency / certainty / area description / lifecycle timestamps, normalised geometry, and a derived `expires_at` for "active" filters.

Public surface:

- `GET /v1/alerts` — GeoJSON `FeatureCollection`. Filterable by `point`, `bbox`, minimum `severity`.
- `GET /v1/alerts/{alert_id}` — single-alert detail (with the long `description` and `instruction` fields that the list omits).
- `GET /v1/alerts/stream` — Server-Sent Events for newly-observed alerts (re-emits the `aeroza.alerts.nws.new` NATS subject).

### Phase 2 — MRMS

Multi-Radar / Multi-Sensor reflectivity ingest from NOAA Open Data. Two-stage pipeline: catalog (`mrms_files`, populated by `aeroza-ingest-mrms`) and Zarr materialisation (`mrms_grids`, populated by `aeroza-materialise-mrms`). The materialise worker is event-triggered (subscribes to `aeroza.mrms.files.new`) with a 60 s scheduler backstop.

Public surface:

- `GET /v1/mrms/files` — file catalog ("what data exists right now").
- `GET /v1/mrms/grids` — materialised-grid catalog ("what data is queryable right now").
- `GET /v1/mrms/grids/{file_key}` — single-grid detail.
- `GET /v1/mrms/grids/sample?lat=&lng=` — nearest-cell value at a point.
- `GET /v1/mrms/grids/polygon?polygon=…&reducer=…` — `max` / `mean` / `min` / `count_ge` over the cells inside a polygon.
- `GET /v1/mrms/tiles/{z}/{x}/{y}.png` — Web-Mercator XYZ raster tiles (NWS dBZ ramp, transparent fallback).
- `GET /v1/stats` — system-wide freshness snapshot.

### Phase 3 — Nowcast + verify + calibration

`Forecaster` Protocol with a persistence baseline (`PersistenceForecaster`). Per-tick the worker reads the latest observation grid, copies it forward at 10 / 30 / 60-minute horizons, persists each prediction to `mrms_nowcasts`, and publishes `aeroza.nowcast.grids.new`. The verifier scores each forecast against the matching observation as soon as truth lands; sample-weighted MAE / bias / RMSE go to `nowcast_verifications`.

Public surface:

- `GET /v1/nowcasts` — prediction catalog (algorithm × horizon × valid_at).
- `GET /v1/calibration` — current matrix per `(algorithm, forecastHorizonMinutes)`.
- `GET /v1/calibration/series` — time-bucketed sparkline data (5 min – 1 day buckets).

### Phase 4 — Webhooks

Subscription model with HMAC-SHA256 signed payloads (Stripe-style headers), a delivery worker with a retry queue + circuit breaker, an alert-rule DSL for point and polygon predicates, and a dispatcher that fans out NATS events to webhook subscriptions. Discriminated pydantic unions for rule configs; `webhook_deliveries` rows track every attempt.

Public surface:

- `GET / POST / PATCH / DELETE /v1/webhooks` and `/v1/webhooks/{id}` — subscription CRUD.
- `GET / POST / PATCH / DELETE /v1/alert-rules` and `/v1/alert-rules/{id}` — rule CRUD.

### Phase 5 — Polished UI

Public-facing landing page (`/`), interactive `/map` (MapLibre raster basemap + alert GeoJSON polygons + MRMS reflectivity raster overlay + scrubbable 6-hour timeline), `/calibration` dashboard (matrix + per-row sparkline), warm parchment theme with Fraunces display serif, refreshed `/docs` hub, `@aeroza/sdk` TypeScript client (the dev console dogfoods it), Playwright E2E smoke for `/map`'s WebGL canvas, `make ingest-*` targets so day-zero contributors are one command from live data.

## Up next (phase 6)

Three workstreams roughly equal in priority. Reordering to taste; nothing is locked in.

### Real nowcasting (pySTEPS)

The §3.3 calibration moat is more interesting once a real algorithm beats persistence. pySTEPS is the obvious first non-trivial forecaster — Lagrangian extrapolation, S-PROG / STEPS modes, mature scientific lineage. Drop-in via the existing `Forecaster` Protocol; the verifier and calibration aggregates work unchanged.

Open questions:

- libomp / OpenMP install pain on macOS — same pattern we used for `eccodes` should work.
- Zarr → numpy → pysteps array conversion: pysteps wants `(time, y, x)` and we have `(latitude, longitude)` — minor adapter.
- Output cadence: pysteps wants a sequence of past observations, not just one. The worker's tick needs a small lookback fetch.

Once pySTEPS is in, ensemble runs unlock Brier scores + CRPS in `verify/`, which makes the calibration page interesting enough to ship publicly.

### Auth + API keys

Currently every route is anonymous. Before any external user, we need:

- `api_keys` table with hashed secret + scopes + rate-limit class.
- Bearer-token middleware (FastAPI dependency).
- Per-key rate limiting (Redis token-bucket).
- A `/v1/me` introspection route so consumers can check their own key.

The webhooks subsystem already understands signed payloads — auth on the read side fits the same shape.

### More ingest sources

- **NEXRAD Level II** (`s3://noaa-nexrad-level2`). Single-radar Cartesian products via `pyart`. Higher resolution than MRMS, narrower coverage. Useful for hyperlocal reflectivity / velocity.
- **HRRR / NBM model data** (`s3://noaa-hrrr-bdp-pds`). 3 km / 2.5 km gridded forecasts. The input for any "real" nowcaster beyond extrapolation; also the input for hours-out forecast data the API doesn't yet expose.
- **METAR** surface observations from `aviationweather.gov`. Simple JSON, useful for ground truth + station-resolved queries.

## Later (phase 7+)

Things we know we want, but not next:

- **Vector tiles (MVT)** for alert polygons. The map currently fetches the whole GeoJSON. MVT scales to thousands of polygons and lets MapLibre filter client-side.
- **NowcastNet** (deep-learning ensemble) replacing pySTEPS as the production forecaster.
- **Production deploy assets**: K8s manifests, GitHub Container Registry images, a real `docker-compose.prod.yml`.
- **A staged rollout / canary mechanism** for forecasters — route a fraction of traffic to a new algorithm, watch its calibration row trend before promoting.
- **Webhooks UI in `/console`**: subscription editor, rule builder, delivery log viewer. The CRUD routes work; the UI panels don't exist yet.
- **`@aeroza/sdk` reference docs** — per-method docs, codegen examples, version-pinning notes. The SDK itself is published-ready; the docs page that explains it is the next doc to land.

## Out of scope (probably)

These come up in conversation but are not on this team's plate today:

- Global coverage. Aeroza is CONUS-first because that's where MRMS / NWS coverage is dense; ICON / GFS / DWD-RV products exist but are someone else's good idea right now.
- Aviation-grade products (PIREPs, SIGMETs as first-class types). The API shape can carry them when needed; we're not chasing the FAA.
- A hosted multi-tenant SaaS. The architecture is multi-tenant-ready (every model has time + auth boundaries), but the operations of running it for paying customers is a separate company.

---

This file evolves with the code. If something here looks out of date, that's a bug — open a PR.
