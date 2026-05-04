# Changelog

All notable changes to Aeroza are tracked here. The phase numbering matches
[`docs/ROADMAP.md`](docs/ROADMAP.md); this file is the release-style summary
that complements the roadmap's narrative.

The project does not yet follow strict [Semantic Versioning](https://semver.org/) —
the v1 wire surface is stable but everything is pre-1.0 in spirit. Breaking
changes will land behind a `/v2` prefix when they happen.

## Unreleased

### Changed
- `aeroza/query/v1.py` (1,317 lines) split into per-domain sub-routers under
  `aeroza/query/v1/`. The `/v1/*` wire surface is unchanged.
- Web `tsconfig.json` now matches the SDK's stricter flags
  (`noUncheckedIndexedAccess`, `exactOptionalPropertyTypes`).
- ESLint is wired up in both `web/` and `sdk-ts/` (was previously absent —
  `eslint-disable` comments referenced a rule plugin that wasn't installed).
  Web uses a hand-rolled flat config with `@typescript-eslint` +
  `react-hooks` rules; SDK uses a similar minimal flat config. Both run
  via `npm run lint`.
- CI gates the unit suite on a coverage floor (`--cov-fail-under=60`) so
  it can't regress below today's level.

### Fixed
- `aeroza/verify/store.py` used `datetime.now().astimezone()` (local-tz-aware)
  in three time-window queries. Now uses `datetime.now(UTC)` so behaviour
  doesn't depend on server locale.
- SDK class names are now `AerozaClient` / `AerozaApiError` /
  `AerozaClientOptions` (was missing the second `z`). The SDK is in-tree
  only — no consumers outside this repo to migrate.
- README architecture diagram was claiming METAR was roadmap; METAR shipped
  in Phase 6d.

## Phase 6f — Probabilistic verification (shipped)

Lagged-ensemble forecaster lands alongside Persistence / pySTEPS. Ensemble
runs add Brier score + CRPS + reliability bins to `nowcast_verifications`,
surfaced through `/v1/calibration*`. `/calibration` route in the web app
gets a metric switcher that swaps between continuous, categorical, and
probabilistic skill scores.

## Phase 6e — Theme + DX (shipped)

Theme refresh from warm-parchment to "Meridian". Site-wide PulseStrip
(system signs of life on every page). One-command `make start` boots
Postgres / Redis / NATS, applies migrations, and runs the API + web +
ingest workers + dispatcher under honcho. Backgrounded historical seed
on first boot (`scripts/seed-historical.sh`) so the dashboard isn't empty.

## Phase 6d — METAR (shipped)

METAR observation ingest from NOAA's Aviation Weather Center text feed.
Public surface: `GET /v1/metar` (filter by bbox, station, time window) and
`GET /v1/metar/{station_id}/latest`.

## Phase 6c — Auth (shipped)

Bearer-token API keys (`Authorization: Bearer aza_live_<random>`),
HMAC-SHA-256-hashed against `AEROZA_API_KEY_SALT`. CLI minting via
`aeroza-api-keys create|list|revoke`. `GET /v1/me` introspection route.
Auth is opt-in (`AEROZA_AUTH_REQUIRED`, default off).

## Phase 6b — Categorical verification (shipped)

POD / FAR / CSI computed from a four-cell contingency table per
`nowcast_verifications` row, with a configurable dBZ threshold (default
35 dBZ — operational meteorology's "convective cell" cutoff). Surfaced
through `/v1/calibration` and `/v1/calibration/series`; `/calibration`
dashboard gets a metric switcher.

## Phase 6a — pySTEPS forecaster (shipped)

`PystepsForecaster` runs Lucas–Kanade dense optical flow over the last
N past observation grids and semi-Lagrangian extrapolation forward at
each requested horizon. Opt-in via `aeroza-nowcast-mrms --algorithm pysteps`.
Falls back to persistence on cold start when the catalog has fewer past
frames than the configured lookback.

## Phase 5 — Polished UI (shipped)

Public landing page (`/`), interactive `/map` (MapLibre + alert polygons +
MRMS reflectivity overlay + scrubbable 6-hour timeline), `/calibration`
dashboard, refreshed `/docs` hub. `@aeroza/sdk` TypeScript client (the dev
console dogfoods it). Playwright E2E smoke for `/map`'s WebGL canvas.

## Phase 4 — Webhooks (shipped)

Subscription model with HMAC-SHA-256 signed payloads (Stripe-style
`Aeroza-Signature: v1=<hex>` and `Aeroza-Timestamp` headers, 5-minute
freshness window). Delivery worker with retry queue + circuit breaker.
Alert-rule DSL with discriminated pydantic unions for point and polygon
predicates. Subscriptions CRUD + alert-rules CRUD under `/v1/webhooks*`
and `/v1/alert-rules*`.

## Phase 3 — Nowcast + verify + calibration (shipped)

`Forecaster` Protocol with a `PersistenceForecaster` baseline. Verifier
scores each forecast against truth as soon as it lands; sample-weighted
MAE / bias / RMSE go to `nowcast_verifications`. `/v1/nowcasts`,
`/v1/calibration`, `/v1/calibration/series`.

## Phase 2 — MRMS (shipped)

Multi-Radar / Multi-Sensor reflectivity ingest from NOAA Open Data.
Two-stage pipeline: catalog (`mrms_files`) and Zarr materialisation
(`mrms_grids`). Public surface: `/v1/mrms/files`, `/v1/mrms/grids`,
`/v1/mrms/grids/sample`, `/v1/mrms/grids/polygon`, `/v1/mrms/tiles/{z}/{x}/{y}.png`.

## Phase 1 — Alerts (shipped)

NWS active-alerts ingest. `/v1/alerts` (GeoJSON FeatureCollection,
filterable by point / bbox / severity), `/v1/alerts/{id}`,
`/v1/alerts/stream` (SSE).

## Phase 0 — Scaffold (shipped)

FastAPI app skeleton, Postgres + PostGIS + Alembic, Redis, NATS JetStream,
Docker Compose, GitHub Actions CI. `/health` and the v1 API stub.
