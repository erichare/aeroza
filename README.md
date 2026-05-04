# Aeroza

> Programmable weather intelligence: streaming APIs, geospatial queries, and probabilistic nowcasting for modern applications.

Aeroza turns weather into a queryable, streaming API. Real-time radar, predictive nowcasting with calibrated confidence, and geospatial queries — for applications that need to understand and react to weather in real time.

## Status

**Phases 0–6f shipped.** Live ingest (NWS alerts, MRMS reflectivity, METAR), queryable point/polygon/tile reads, three nowcasting algorithms (persistence baseline, pySTEPS Lucas–Kanade, lagged-ensemble), continuous verification with continuous (MAE/bias/RMSE), categorical (POD/FAR/CSI), and probabilistic (Brier / CRPS over the ensemble) skill scores, signed webhook delivery with a point/polygon alert-rule DSL, opt-in bearer-token auth (`aza_live_*` keys) with a `GET /v1/me` introspection route, and a polished web surface across six routes — landing (`/`), interactive `/map`, `/demo` Storm Replay (autoplay through the live archive or four hand-curated historical events with commentary), `/calibration` matrix + sparklines, dev `/console`, and `/docs`. The `@aeroza/sdk` TypeScript client is what the dev console drives every panel through.

What's next: a STEPS ensemble forecaster (real stochastic perturbations) on top of the Phase-6f data plane, reliability diagrams, per-key rate limiting, and additional ingest sources (NEXRAD L2, HRRR / NBM model data). See [docs/ROADMAP.md](docs/ROADMAP.md).

## Quickstart (development)

Requires Docker, Node 20+, and [uv](https://docs.astral.sh/uv/).

```bash
make start
```

That single command runs `make doctor` (preflight check), `make bootstrap` (creates `.env` with a real signing salt, syncs Python deps, starts Postgres / Redis / NATS, applies migrations), then launches the full stack via [honcho](https://github.com/nickstenning/honcho) under [`scripts/start-stack.sh`](scripts/start-stack.sh): FastAPI on :8000, the Next.js console on :3000, plus the alerts / MRMS / METAR ingest workers and the webhook dispatcher. When the `[grib]` extra is installed (`make extras-grib`), the launcher also runs the GRIB → Zarr materialiser, the **lagged-ensemble** nowcaster, and the verifier so the radar replay and `/calibration` light up out of the box. Ctrl+C tears everything down in one shot.

- API: <http://localhost:8000> — health at `/health`, Swagger at `/docs`.
- Console: <http://localhost:3000> — landing, `/map`, `/calibration`, `/console`, `/docs`.

**A backgrounded historical seed** (`scripts/seed-historical.sh`) kicks off automatically on first `make start`: it backfills ~3 hours of historical MRMS data via `aeroza-ingest-mrms --at-time` and drains the materialiser in `--once` batches if cfgrib is available, so the dashboard fills in over the first couple of minutes instead of staring at empty panels until live ingest catches up. Idempotent — re-runs short-circuit when the catalog already has data. Tail the log with `tail -f .seed.log`, or run it explicitly with `make seed`.

Run `make stop` to bring down the docker layer when you're done. Re-running `make start` is idempotent.

### By hand

If you'd rather drive the pieces individually:

```bash
make bootstrap            # one-time setup (idempotent)
make dev                  # FastAPI on :8000 — terminal 1
make web-dev              # Next console on :3000 — terminal 2
make ingest-alerts        # NWS active alerts → /v1/alerts* — terminal 3
make ingest-mrms          # MRMS file catalog → /v1/mrms/files — terminal 4
make ingest-metar         # METAR observations → /v1/metar — terminal 5
make materialise-mrms     # MRMS GRIB2 → Zarr → /v1/mrms/grids* (needs `[grib]`)
make nowcast-persistence  # Persistence-baseline nowcaster
make nowcast-lagged-ensemble  # Probabilistic baseline (Brier/CRPS) — no extras needed
make seed                 # Backfill ~3h of historical data so the dashboard isn't empty (idempotent)
uv run pytest             # Run unit tests
```

Every entry above is also a top-level `aeroza-*` script; `make help` lists all targets.

## Architecture

Modular monolith (FastAPI) with extracted ingest workers:

```
aeroza/
  cli/           Long-running workers + one-shot CLIs (aeroza-* scripts).
  ingest/        NWS alerts + MRMS reflectivity + METAR observations ingest.
                 NEXRAD L2 / HRRR / NBM are roadmap, not built.
  query/         REST read-side over the ingested data — alerts, MRMS
                 files / grids / tiles, METAR, nowcasts, calibration, stats.
                 Routes are split per domain under aeroza/query/v1/.
  nowcast/       Forecaster Protocol + persistence, pySTEPS Lucas–Kanade,
                 and lagged-ensemble forecasters.
  verify/        Continuous verification + sample-weighted aggregates —
                 continuous (MAE / bias / RMSE), categorical (POD / FAR / CSI),
                 and probabilistic (Brier / CRPS, reliability bins).
  webhooks/      Signed delivery, subscriptions CRUD, alert-rule DSL
                 (point + polygon predicates), dispatcher worker.
  tiles/         XYZ raster tiles for MRMS reflectivity (vector MVT
                 for alerts is roadmap).
  auth/          Bearer-token API keys (aza_live_*) — opt-in via
                 AEROZA_AUTH_REQUIRED.
  admin/         Dev-console-only seed-event endpoints (gated by
                 AEROZA_DEV_ADMIN_ENABLED).
  stream/        NATS publishers/subscribers + SSE gateway.
  shared/        DB session helpers, common schemas, HTTP client.
```

Storage: PostgreSQL 16 + PostGIS, Redis, S3 (Zarr). Streaming: NATS JetStream.

Public surface: 19 routes versioned under `/v1`. The TypeScript SDK in [`sdk-ts/`](sdk-ts/) (`@aeroza/sdk`) pins every wire shape and is what the dev console drives every panel through.

## Optional extras

`make start` installs only the core extras the live stack needs. Two heavy / system-dep-bearing ones stay opt-in.

> **Why `make extras-*` and not plain `uv sync --extra X`?** `uv sync --extra X` *replaces* the installed extra-set rather than adding to it — running `uv sync --extra grib` on a working stack will silently uninstall db / cache / stream / ingest / verify and break the rest of the platform. The `make extras-*` targets re-list the bootstrap extras so adding one stays additive in effect.

**`[grib]`** — `cfgrib` for decoding MRMS GRIB2 files. Required by `aeroza-materialise-mrms` (the worker fast-fails at startup with an install hint if missing).

```bash
# macOS
brew install eccodes
make extras-grib

# Debian/Ubuntu
sudo apt-get install -y libeccodes-dev
make extras-grib
```

**`[nowcast]`** — `pysteps` for the optical-flow forecaster. `PystepsForecaster` is opt-in via `aeroza-nowcast-mrms --algorithm pysteps`; without the extra installed the worker keeps running the persistence baseline.

```bash
# Linux: pysteps' setup.py just works.
make extras-nowcast

# macOS: pysteps' setup.py uses raw -fopenmp, which Apple clang doesn't accept.
brew install libomp
CFLAGS="-Xpreprocessor -fopenmp -I$(brew --prefix libomp)/include" \
LDFLAGS="-L$(brew --prefix libomp)/lib -lomp" \
make extras-nowcast
```

For both extras together (or to install everything including the heavy bits), `make install` runs `uv sync --all-extras` — fine when you have eccodes + libomp already in place.

## Project meta

- **Roadmap:** [docs/ROADMAP.md](docs/ROADMAP.md) — phase plan and what shipped vs. what's next.
- **Changelog:** [CHANGELOG.md](CHANGELOG.md) — release-style notes per phase.
- **Contributing:** [CONTRIBUTING.md](CONTRIBUTING.md) — branch / PR / merge flow.
- **Deploy:** [docs/DEPLOY-RAILWAY.md](docs/DEPLOY-RAILWAY.md) — Railway + Supabase + Vercel.

## License

MIT — see [LICENSE](LICENSE).
