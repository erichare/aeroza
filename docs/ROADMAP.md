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

### Phase 6a — pySTEPS nowcaster

`PystepsForecaster` runs Lucas–Kanade dense optical flow over the last `lookback` past observation grids (default 3) and semi-Lagrangian extrapolation forward at each requested horizon. Same `Forecaster` Protocol as `PersistenceForecaster`; opt in via `aeroza-nowcast-mrms --algorithm pysteps`.

The worker fetches the past observations from the catalog as part of each tick (`find_recent_mrms_grids`) and passes them on the existing `forecast()` call. When the catalog has fewer past frames than `lookback`, the forecaster gracefully falls back to persistence — cold-start protection so the worker doesn't crash on its first ticks after a cold start.

Install: `uv sync --extra nowcast` (Linux works out of the box; macOS needs `brew install libomp`, since pysteps' `setup.py` uses raw `-fopenmp`). Tests use `pytest.importorskip("pysteps")` so the rest of the suite stays green without the install.

### Phase 6b — categorical verification (POD / FAR / CSI)

The verifier now scores each forecast against a configurable dBZ threshold (default **35 dBZ** — operational meteorology's "convective cell" cutoff) and stores a four-cell contingency table per row (`hits`, `misses`, `false_alarms`, `correct_negatives`). `GET /v1/calibration` and `GET /v1/calibration/series` surface POD / FAR / CSI on the wire, computed from the **summed** contingency table (averaging ratios across rows would be wrong). The `/calibration` dashboard's metric switcher swaps between MAE (continuous) and POD/FAR/CSI (categorical skill).

This is the deterministic cousin of the probabilistic Brier / CRPS direction below: meaningful verification on existing deterministic forecasts (persistence + pySTEPS) without ensemble work.

## Up next (phase 6 continued)

Workstreams roughly equal in priority. Reordering to taste; nothing is locked in.

### Ensemble forecasting → Brier / CRPS

The next step on the *probabilistic* verification side is multi-member ensemble runs (pysteps' STEPS mode supports them out of the box). Ensemble output unlocks Brier scores, reliability diagrams, and CRPS — the proper probabilistic complement to the categorical POD/FAR/CSI scores phase 6b ships. Goes hand-in-hand with a "champion / challenger" mechanism that calibrates new algorithms against persistence before promoting them.

### Phase 6c — Auth + API keys

Bearer-token API keys now exist server-side. The format on the wire is `Authorization: Bearer aza_live_<random>`; the random portion is HMAC-SHA-256-hashed against `AEROZA_API_KEY_SALT` and only the digest is persisted. Operators mint keys with the `aeroza-api-keys` CLI (`create` / `list` / `revoke`); HTTP CRUD lands once we have an admin scope to gate it on.

Auth is **opt-in** via `AEROZA_AUTH_REQUIRED` (default `false`). When the flag is off, anonymous traffic still works — the dependency records who is calling for telemetry but doesn't enforce. Flipping the flag on makes `require_api_key` a precondition wherever it's wired.

Public surface this slice ships:

- `GET /v1/me` — introspect the calling key (name, owner, scopes, prefix, last-used, rate-limit class). Always requires a key.

What's still out of scope (intentionally) for v1:

- HTTP CRUD over `/v1/api-keys` — the CLI is the management plane until we have an admin scope.
- Per-key rate limiting (Redis token-bucket). The `rate_limit_class` column exists; the dependency that reads it lands once enforcement turns on.
- Scope-aware route gating. The shape is in place (keys carry a `scopes: text[]`); the per-route enforcement comes alongside the rate limiter.

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
