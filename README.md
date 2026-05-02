# Aeroza

> Programmable weather intelligence: streaming APIs, geospatial queries, and probabilistic nowcasting for modern applications.

Aeroza turns weather into a queryable, streaming API. Real-time radar, predictive nowcasting with calibrated confidence, and geospatial queries — for applications that need to understand and react to weather in real time.

## Status

Pre-alpha. Phase 0 scaffolding in progress. See [docs/ROADMAP.md](docs/ROADMAP.md) when published.

## Quickstart (development)

Requires Docker and [uv](https://docs.astral.sh/uv/).

```bash
# Install Python deps
uv sync

# Start dev infrastructure (Postgres+PostGIS, Redis, NATS JetStream)
docker compose up -d

# Run the API
make dev

# In separate terminals, start the long-running ingesters so the API
# surface has live data to serve. The `aeroza-*` CLIs are also fine if
# you'd rather not use make.
make ingest-alerts        # NWS active alerts → /v1/alerts*
make ingest-mrms          # MRMS file catalog → /v1/mrms/files
make materialise-mrms     # MRMS GRIB2 → Zarr → /v1/mrms/grids*

# Run tests
uv run pytest
```

The API listens on `http://localhost:8000`. Health check: `GET /health`.

### Dev console (web UI)

A small Next.js dev console lives in [`web/`](web/) — it visualises the
live `/v1/alerts/stream` SSE feed, the MRMS file catalog, and the system
health endpoint. Useful for testing and demoing while the polished Phase 5
client is still ahead. From the repo root:

```bash
make web-install   # one-time
make web-dev       # http://localhost:3000
```

Requires `make dev` (FastAPI on :8000) running in another terminal.

## Architecture

Modular monolith (FastAPI) with extracted ingest workers:

```
aeroza/
  ingest/    NEXRAD L2, MRMS, HRRR/NBM, METAR, NWS alerts
  query/     GraphQL + REST query layer
  stream/    SSE / WebSocket gateway, webhook dispatcher
  tiles/     Raster (WebP) + vector (MVT) tile servers
  nowcast/   pySTEPS pipeline (NowcastNet planned for v2)
  verify/    Continuous calibration: Brier, reliability, CRPS
  shared/    Schemas, geospatial utils, auth
```

Storage: PostgreSQL 16 + PostGIS, Redis, S3 (Zarr).
Streaming: NATS JetStream.

## Optional extras

The default `uv sync --all-extras` skips heavy native dependencies. To
decode MRMS GRIB2 files via [`cfgrib`](aeroza/ingest/mrms_decode.py),
install the `[grib]` extra and the system `eccodes` library:

```bash
# macOS
brew install eccodes
uv sync --extra grib

# Debian/Ubuntu
sudo apt-get install -y libeccodes-dev
uv sync --extra grib
```

The decode path lazy-loads `cfgrib` so unit tests stay green without
`eccodes` installed; only the actual end-to-end materialisation needs
the system library.

## License

MIT — see [LICENSE](LICENSE).
