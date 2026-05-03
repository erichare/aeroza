# Aeroza

> Programmable weather intelligence: streaming APIs, geospatial queries, and probabilistic nowcasting for modern applications.

Aeroza turns weather into a queryable, streaming API. Real-time radar, predictive nowcasting with calibrated confidence, and geospatial queries — for applications that need to understand and react to weather in real time.

## Status

**Phases 0–6d shipped.** Live ingest (NWS alerts, MRMS reflectivity, METAR), queryable point/polygon/tile reads, two nowcasting algorithms (persistence baseline + pySTEPS Lucas–Kanade), continuous verification with both continuous (MAE/bias/RMSE) and categorical (POD/FAR/CSI) skill scores, signed webhook delivery with a point/polygon alert-rule DSL, opt-in bearer-token auth (`aza_live_*` keys) with a `GET /v1/me` introspection route, and a polished web surface across six routes — landing (`/`), interactive `/map`, `/demo` Storm Replay (autoplay through the live archive or four hand-curated historical events with commentary), `/calibration` matrix + sparklines, dev `/console`, and `/docs`. The `@aeroza/sdk` TypeScript client (16 typed methods) is what the dev console drives every panel through.

What's next: ensemble forecasting (Brier / CRPS), per-key rate limiting, and additional ingest sources (NEXRAD L2, HRRR / NBM model data). See [docs/ROADMAP.md](docs/ROADMAP.md).

## Quickstart (development)

Requires Docker, Node 20+, and [uv](https://docs.astral.sh/uv/).

```bash
make start
```

That single command runs `make doctor` (preflight check), `make bootstrap` (creates `.env` with a real signing salt, syncs Python deps, starts Postgres / Redis / NATS, applies migrations), then launches the full stack via [honcho](https://github.com/nickstenning/honcho) and [`Procfile.dev`](Procfile.dev): FastAPI on :8000, the Next.js console on :3000, plus the alerts / MRMS / METAR ingest workers and the webhook dispatcher. Ctrl+C tears everything down in one shot.

- API: <http://localhost:8000> — health at `/health`, Swagger at `/docs`.
- Console: <http://localhost:3000> — landing, `/map`, `/calibration`, `/console`, `/docs`.

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
uv run pytest             # Run unit tests
```

Every entry above is also a top-level `aeroza-*` script; `make help` lists all targets.

## Architecture

Modular monolith (FastAPI) with extracted ingest workers:

```
aeroza/
  cli/           Long-running workers + one-shot CLIs (aeroza-* scripts).
  ingest/        NWS alerts + MRMS reflectivity ingest. NEXRAD L2 / HRRR
                 / NBM / METAR are roadmap, not built.
  query/         REST read-side over the ingested data
                 (alerts, MRMS files / grids, nowcasts, calibration, stats).
  nowcast/       Forecaster Protocol + persistence baseline.
                 pySTEPS / NowcastNet land here next.
  verify/        Continuous verification + sample-weighted aggregates
                 (MAE / bias / RMSE today; Brier / CRPS once we have an
                 ensemble forecaster).
  webhooks/      Signed delivery, subscriptions CRUD, alert-rule DSL
                 (point + polygon predicates), dispatcher worker.
  tiles/         XYZ raster tiles for MRMS reflectivity (vector MVT
                 for alerts is roadmap).
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

## License

MIT — see [LICENSE](LICENSE).
