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
uv run uvicorn aeroza.main:app --reload

# Run tests
uv run pytest
```

The API listens on `http://localhost:8000`. Health check: `GET /health`.

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

## License

MIT — see [LICENSE](LICENSE).
