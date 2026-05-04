# Aeroza roadmap

A history of what's shipped and a credible plan for what's next. Specific items are commitments; broader phases are aspirational and likely to reshape as we learn.

This file is the source of truth for "what state is the project in?" — the README links here, the dev console points users here, and PRs that close out a phase update this file alongside the code.

## Shipped (phases 0 – 6f)

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

Public-facing landing page (`/`), interactive `/map` (MapLibre raster basemap + alert GeoJSON polygons + MRMS reflectivity raster overlay + scrubbable 6-hour timeline), `/calibration` dashboard (matrix + per-row sparkline), refreshed `/docs` hub, `@aeroza/sdk` TypeScript client (the dev console dogfoods it), Playwright E2E smoke for `/map`'s WebGL canvas, `make ingest-*` targets so day-zero contributors are one command from live data. Theme: Fraunces display serif over Inter body, originally on a warm parchment palette (later replaced by Meridian — see Phase 6e).

### Phase 6a — pySTEPS nowcaster

`PystepsForecaster` runs Lucas–Kanade dense optical flow over the last `lookback` past observation grids (default 3) and semi-Lagrangian extrapolation forward at each requested horizon. Same `Forecaster` Protocol as `PersistenceForecaster`; opt in via `aeroza-nowcast-mrms --algorithm pysteps`.

The worker fetches the past observations from the catalog as part of each tick (`find_recent_mrms_grids`) and passes them on the existing `forecast()` call. When the catalog has fewer past frames than `lookback`, the forecaster gracefully falls back to persistence — cold-start protection so the worker doesn't crash on its first ticks after a cold start.

Install: `uv sync --extra nowcast` (Linux works out of the box; macOS needs `brew install libomp`, since pysteps' `setup.py` uses raw `-fopenmp`). Tests use `pytest.importorskip("pysteps")` so the rest of the suite stays green without the install.

### Phase 6b — categorical verification (POD / FAR / CSI)

The verifier now scores each forecast against a configurable dBZ threshold (default **35 dBZ** — operational meteorology's "convective cell" cutoff) and stores a four-cell contingency table per row (`hits`, `misses`, `false_alarms`, `correct_negatives`). `GET /v1/calibration` and `GET /v1/calibration/series` surface POD / FAR / CSI on the wire, computed from the **summed** contingency table (averaging ratios across rows would be wrong). The `/calibration` dashboard's metric switcher swaps between MAE (continuous) and POD/FAR/CSI (categorical skill).

This is the deterministic cousin of the probabilistic Brier / CRPS direction below: meaningful verification on existing deterministic forecasts (persistence + pySTEPS) without ensemble work.

### Phase 6c — Auth + API keys

Bearer-token API keys: `Authorization: Bearer aza_live_<random>`. The random portion is HMAC-SHA-256-hashed against `AEROZA_API_KEY_SALT` and only the digest is persisted. Operators mint keys with the `aeroza-api-keys` CLI (`create` / `list` / `revoke`); HTTP CRUD over `/v1/api-keys` lands once we have an admin scope to gate it on.

Auth is **opt-in** via `AEROZA_AUTH_REQUIRED` (default `false`). When the flag is off, anonymous traffic still works — the dependency records who is calling for telemetry but doesn't enforce. Flipping the flag on makes `require_api_key` a precondition wherever it's wired.

Public surface:

- `GET /v1/me` — introspect the calling key (name, owner, scopes, prefix, last-used, rate-limit class). Always requires a key.

Still out of scope (intentionally) for v1: per-key rate limiting (the `rate_limit_class` column exists; the Redis token-bucket dependency lands when enforcement turns on), scope-aware per-route gating (the `scopes: text[]` shape is in place), and HTTP CRUD over `/v1/api-keys`.

### Phase 6d — METAR ingest

Surface-station observations from the Aviation Weather Center JSON API. The `aeroza-ingest-metar` worker polls a configurable station list (default: a CONUS top-20 sample) every 5 minutes; AWC returns already-parsed records, so there is no in-tree METAR text parser. The raw text is preserved on each row for callers who want their own.

Public surface:

- `GET /v1/metar` — list with `station`, `since`, `until`, `bbox`, `limit` filters; newest first.
- `GET /v1/metar/{station_id}/latest` — most-recent observation for one station.

Useful as ground-truth point observations next to the MRMS gridded products, especially for sanity-checking nowcasts or computing station-resolved verification.

### Phase 6e — Presentation, theme, and DX

A run of post-Phase-5 work that turned a great-but-local product into something pitchable. Bundled here as one phase rather than itemised because it's all "make it land in five seconds when someone opens the link":

- **Meridian theme** (replaces warm parchment). Cool glacier base + prussian-ink text + aged-brass single-warm-spark accent. Reads like a marine chronometer / aviation chart instead of warm-SaaS — distinctive in the dev-tools register.
- **Hook-echo logo system**. Stylised supercell silhouette as a single closed SVG path. One source of truth (`web/components/AerozaLogo.tsx`) powers the React component, the favicon (`web/app/icon.svg`, auto-discovered by Next), and the Apple touch icon. 16px in the nav, 44px on the landing pitch.
- **Split-hero landing**. Pitch (headline + subhead + CTAs) above the fold, then a 60/40 live-map + "Were we right?" verification card below. The map proves *real, live data*; the card proves *we score every forecast publicly* — both halves of the pitch land in seconds without the visitor reading a single API path.
- **Site-wide PulseStrip**. A three-pill status row in the centre nav (active alerts · grid age · last-hour MAE) so every page-load reaffirms the system is alive. Suppressed on `/map` and `/demo` since those have richer page-level headers.
- **/demo Storm Replay**. Autoplay through the local archive *or* through one of four hand-curated historical events (Houston Derecho, Rolling Fork Outbreak, Mayfield Quad-State, April 2011 Super Outbreak — the last commentary-only since it predates MRMS). Smoothness tuned via 700 ms tile cross-fade in pinned mode + 1×/2×/4×/8× speed switcher.
- **Smoother radar + state borders**. Bilinear sampling at zoom ≥ 4 in the server-side tile renderer; client-side `raster-resampling: linear`; bundled `us-atlas` state-border GeoJSON layered between radar and alerts; in-map dBZ legend that appears alongside the severity legend when radar is on.
- **One-command setup (`make start`)**. New `make doctor` / `make bootstrap` / `make start` / `make stop` chain plus a `Procfile.dev` driven by [honcho](https://github.com/nickstenning/honcho). Goes from cold checkout to API + web + ingest workers running under one terminal in one command. `make extras-grib` and `make extras-nowcast` handle the additive-extra dance that bare `uv sync --extra X` gets wrong.
- **Historical event seeding**. `aeroza-ingest-mrms --at-time` pulls any past 24 h slice from NOAA's bucket. Combined with /demo's "Copy seed command" UX and the materialiser's "N more files waiting; re-run with `--batch-size N`" hint, populating the radar replay for any catalogued event is a two-paste workflow.
- **Friendlier errors**. Materialiser fast-fails at startup with a precise install hint when cfgrib is missing instead of grinding through every queued grid with a cryptic xarray error.

### Phase 6f — ensemble plumbing + Brier / CRPS

Probabilistic verification's ground floor. The `Forecaster` Protocol grew an `ensemble_size` field on `NowcastPrediction` and a per-forecaster `history_depth`; `mrms_nowcasts.ensemble_size` records the M each row's Zarr stores along its leading `member` dim, defaulting to 1 so the deterministic forecasters round-trip unchanged. A new `LaggedEnsembleForecaster` (`--algorithm lagged-ensemble`) is the simplest probabilistic baseline — members are the last K observations stacked along `member`, persisted forward — and pulls in zero extra deps so it runs on the default install.

The verifier detects ensembles off the catalog row's `ensemble_size`, computes Brier and fair-CRPS over the member dim, and falls back to the member-mean for the existing MAE/POD path. `nowcast_verifications` gained nullable `brier_score` / `crps` / `ensemble_size` columns; the calibration aggregator surfaces sample-weighted `brierMean` / `crpsMean` / `ensembleSize` (null when no ensemble row contributed) on `/v1/calibration` and `/v1/calibration/series`. Both fields land in `@aeroza/sdk` types so the dev console can chart them next.

This is the probabilistic complement to Phase 6b's categorical POD/FAR/CSI: the full skill triangle (continuous + categorical + probabilistic) now runs on every observation tick, with a baseline ensemble forecaster the upcoming STEPS / NowcastNet runs must beat on Brier skill.

## Up next (phase 6 continued)

Workstreams roughly equal in priority. Reordering to taste; nothing is locked in.

### Ensemble forecasting follow-ons

With the data plane in place, the remaining probabilistic-verification work is on the algorithm side and the UI:

- **STEPS ensemble mode** (pysteps native): higher-quality members from real stochastic perturbations of the optical-flow extrapolation, instead of "the recent past" the lagged baseline uses. Drop-in once the forecaster is written — the verifier already handles any (M, y, x) Zarr.
- **Reliability diagrams** on `/calibration`: bin the ensemble probabilities, plot observed frequency per bin. Needs a per-row JSONB column on `nowcast_verifications` (or a separate table) to retain bins for the aggregator.
- **`/calibration` UI**: surface `brierMean`/`crpsMean` next to MAE in the metric switcher, plus a champion-vs-challenger view that highlights when an ensemble forecaster pulls ahead of the persistence baseline on Brier skill.

### Auth follow-ons

The Phase-6c scaffolding is in place; turning it on for real traffic needs three more things: per-key rate limiting (Redis token-bucket reading the existing `rate_limit_class` column), scope-aware per-route gating (the existing `scopes: text[]` enforced via FastAPI dependencies), and HTTP CRUD over `/v1/api-keys` once an admin scope exists to gate it on.

### More ingest sources

- **NEXRAD Level II** (`s3://noaa-nexrad-level2`). Single-radar Cartesian products via `pyart`. Higher resolution than MRMS, narrower coverage. Useful for hyperlocal reflectivity / velocity.
- **HRRR / NBM model data** (`s3://noaa-hrrr-bdp-pds`). 3 km / 2.5 km gridded forecasts. The input for any "real" nowcaster beyond extrapolation; also the input for hours-out forecast data the API doesn't yet expose.

## Later (phase 7+)

Things we know we want, but not next:

- **Vector tiles (MVT)** for alert polygons. The map currently fetches the whole GeoJSON. MVT scales to thousands of polygons and lets MapLibre filter client-side.
- **NowcastNet** (deep-learning ensemble) replacing pySTEPS as the production forecaster.
- **Production deploy assets**: K8s manifests, GitHub Container Registry images, a real `docker-compose.prod.yml`.
- **A staged rollout / canary mechanism** for forecasters — route a fraction of traffic to a new algorithm, watch its calibration row trend before promoting.
- **Webhooks UI in `/console`**: ~~subscription editor~~ (PR #69), ~~rule builder~~ (PR #70), ~~delivery log viewer~~ (this PR). The console now drives every CRUD path the server exposes for webhooks + rules, and renders the audit trail inline.
- **`@aeroza/sdk` reference docs** — per-method docs, codegen examples, version-pinning notes. The SDK itself is published-ready; the docs page that explains it is the next doc to land.

## Out of scope (probably)

These come up in conversation but are not on this team's plate today:

- Global coverage. Aeroza is CONUS-first because that's where MRMS / NWS coverage is dense; ICON / GFS / DWD-RV products exist but are someone else's good idea right now.
- Aviation-grade products (PIREPs, SIGMETs as first-class types). The API shape can carry them when needed; we're not chasing the FAA.
- A hosted multi-tenant SaaS. The architecture is multi-tenant-ready (every model has time + auth boundaries), but the operations of running it for paying customers is a separate company.

---

This file evolves with the code. If something here looks out of date, that's a bug — open a PR.
