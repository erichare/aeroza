# Aeroza

> Programmable weather intelligence: streaming APIs, geospatial queries, and probabilistic nowcasting for modern applications.

Aeroza turns weather into a queryable, streaming API. Real-time radar, predictive nowcasting with calibrated confidence, and geospatial queries — for applications that need to understand and react to weather in real time.

## Status

**Phases 0–5 shipped.** The platform has live ingest (NWS alerts, MRMS reflectivity), queryable point/polygon/tile reads, persistence-baseline nowcasting, continuous verification with a public calibration dashboard, signed webhook delivery with an alert-rule DSL, a `@aeroza/sdk` TypeScript client, and a polished web surface (`/`, `/map`, `/calibration`, `/console`, `/docs`).

What's next: pySTEPS nowcasting (so the calibration moat has more than one row), auth + API keys, and additional ingest sources (NEXRAD L2, HRRR / NBM model data, METAR). See [docs/ROADMAP.md](docs/ROADMAP.md).

## Quickstart (development)

Requires Docker and [uv](https://docs.astral.sh/uv/).

```bash
# Install Python deps
uv sync

# Start dev infrastructure (Postgres+PostGIS, Redis, NATS JetStream)
docker compose up -d

# Apply migrations
make migrate

# Run the API
make dev

# In separate terminals, start the long-running workers so the API
# surface has live data to serve. The `aeroza-*` CLIs are also fine
# if you'd rather not use make.
make ingest-alerts        # NWS active alerts → /v1/alerts*
make ingest-mrms          # MRMS file catalog → /v1/mrms/files
make materialise-mrms     # MRMS GRIB2 → Zarr → /v1/mrms/grids*

# Run tests
uv run pytest
```

The API listens on `http://localhost:8000`. Health check: `GET /health`. Interactive Swagger: <http://localhost:8000/docs>.

### Web (landing + map + calibration + dev console + docs)

The Next.js app in [`web/`](web/) is the public face. It serves the marketing landing page, an interactive `/map` (MapLibre + alert polygons + MRMS radar tiles + scrubbable timeline), the `/calibration` dashboard (algorithm × horizon MAE matrix + sparkline), a `/console` for live API testing, and a `/docs` hub.

```bash
make web-install   # one-time
make web-dev       # http://localhost:3000
```

Needs `make dev` (FastAPI on :8000) running in another terminal.

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

`uv sync --all-extras` skips heavy native dependencies. To decode MRMS GRIB2 files via [`cfgrib`](aeroza/ingest/mrms_decode.py), install the `[grib]` extra and the system `eccodes` library:

```bash
# macOS
brew install eccodes
uv sync --extra grib

# Debian/Ubuntu
sudo apt-get install -y libeccodes-dev
uv sync --extra grib
```

The decode path lazy-loads `cfgrib` so unit tests stay green without `eccodes` installed; only the actual end-to-end materialisation needs the system library.

## License

MIT — see [LICENSE](LICENSE).
