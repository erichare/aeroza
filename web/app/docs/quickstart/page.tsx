import type { Metadata } from "next";
import Link from "next/link";

import { DocsLayout } from "@/components/DocsLayout";

export const metadata: Metadata = {
  title: "Quickstart",
  description:
    "Run Aeroza locally in five minutes: Postgres + Redis + NATS infra, FastAPI " +
    "on :8000, and your first geospatial query.",
};

const API_BASE = process.env.NEXT_PUBLIC_AEROZA_API_URL ?? "http://localhost:8000";

export default function QuickstartPage() {
  return (
    <DocsLayout>
      <h1>Quickstart</h1>
      <p>
        Five minutes from clean clone to a working API + dev console + first
        query. The dev infrastructure (Postgres+PostGIS, Redis, NATS JetStream)
        is dockerised; the FastAPI app and the Next.js console run on your
        host.
      </p>

      <h2>1 · Prerequisites</h2>
      <ul>
        <li>
          <strong>Python 3.13+</strong> via <code>uv</code> (the
          {" "}<a href="https://docs.astral.sh/uv/" target="_blank" rel="noreferrer">
            astral.sh installer
          </a>{" "}
          handles it)
        </li>
        <li>
          <strong>Docker</strong> or <strong>Podman</strong> (Postgres + Redis
          + NATS run as containers)
        </li>
        <li>
          <strong>Node 20+</strong> with <code>npm</code> (just for the dev
          console)
        </li>
        <li>
          Optional: <code>eccodes</code> on your host if you want the
          materialise worker to decode real GRIB2 (
          <code>brew install eccodes</code> on macOS)
        </li>
      </ul>

      <h2>2 · Bring up the dev infrastructure</h2>
      <p>From the repo root:</p>
      <pre>
        <code>{`make up         # Postgres :5432, Redis :6379, NATS :4222
make migrate    # apply Alembic migrations to the dev DB`}</code>
      </pre>
      <p>
        <code>make up</code> uses <code>docker-compose.yml</code> with the
        official PostGIS image so geospatial alert queries work out of the
        box. <code>make logs</code> tails container logs if anything looks
        wrong.
      </p>

      <h2>3 · Run the API</h2>
      <pre>
        <code>{`make dev        # uvicorn aeroza.main:app --reload :8000`}</code>
      </pre>
      <p>
        Confirm it answers:
      </p>
      <pre>
        <code>{`$ curl ${API_BASE}/health
{"status":"ok","version":"0.1.0"}`}</code>
      </pre>
      <p>
        Open the auto-generated Swagger UI at{" "}
        <a href={`${API_BASE}/docs`} target="_blank" rel="noreferrer">
          {API_BASE}/docs
        </a>{" "}
        for an interactive request builder over every public route.
      </p>

      <h2>4 · Run the dev console</h2>
      <pre>
        <code>{`cd web && npm install && npm run dev`}</code>
      </pre>
      <p>
        The console serves at{" "}
        <Link href="/console">localhost:3000/console</Link>. It exercises every
        endpoint live — alerts SSE stream, MRMS file catalog, materialised
        grids, point sample, and polygon reduction.
      </p>

      <h2>5 · Populate some data</h2>
      <p>
        On a fresh database the catalogs are empty. Two background workers
        fill them; either runs as a one-shot for cron-friendly testing.
      </p>
      <pre>
        <code>{`# Pull the most recent NWS alerts (US-wide, ~hundreds of rows).
uv run aeroza-ingest-alerts --once

# Discover MRMS files on the AWS Open Data bucket and persist the catalog.
uv run aeroza-ingest-mrms --once

# Materialise the latest few files into Zarr grids.
# Requires eccodes on your host; skip if you only want metadata.
uv run aeroza-materialise-mrms --once`}</code>
      </pre>

      <h2>6 · Your first query</h2>
      <p>
        Sample the latest reflectivity grid at a (lat, lng):
      </p>
      <pre>
        <code>{`$ curl '${API_BASE}/v1/mrms/grids/sample?lat=29.76&lng=-95.37'
{
  "type": "MrmsGridSample",
  "value": 39.19,
  "matchedLatitude": 29.8,
  "matchedLongitude": -95.4,
  ...
}`}</code>
      </pre>
      <p>
        Or reduce a polygon (max value over a region):
      </p>
      <pre>
        <code>{`$ curl '${API_BASE}/v1/mrms/grids/polygon?\
polygon=-95.7,29.5,-95.0,29.5,-95.0,30.0,-95.7,30.0&reducer=max'`}</code>
      </pre>
      <p>
        From here, <Link href="/docs/concepts">Concepts</Link> explains the
        data model; <Link href="/docs/api">API reference</Link> lists every
        route.
      </p>
    </DocsLayout>
  );
}
