import type { Metadata } from "next";
import Link from "next/link";

import { DocsLayout } from "@/components/DocsLayout";

export const metadata: Metadata = {
  title: "Docs",
  description:
    "Aeroza developer documentation: quickstart, concepts, and the full API reference.",
};

export default function DocsIndexPage() {
  return (
    <DocsLayout>
      <h1>Aeroza docs</h1>
      <p>
        Aeroza is an API-first weather-intelligence platform: streaming NWS
        alerts, materialised MRMS radar grids, surface METAR observations,
        queryable predictive nowcasts, and categorical verification metrics.
        This is the developer documentation — narrow on purpose, focused on
        the things you actually need to build against the running API.
      </p>

      <h2>Where to go next</h2>
      <ul>
        <li>
          <Link href="/docs/quickstart">Quickstart</Link> — bring up Postgres,
          Redis, NATS, and FastAPI locally, then make your first query.
        </li>
        <li>
          <Link href="/docs/concepts">Concepts</Link> — the data model behind
          alerts, MRMS files / grids, METAR observations, point sample, and
          polygon reduction.
        </li>
        <li>
          <Link href="/docs/api">API reference</Link> — every public route,
          plus a link to the auto-generated Swagger UI for interactive
          requests.
        </li>
        <li>
          <Link href="/demo">Storm Replay</Link> — narrated walkthroughs of
          curated historical events (April 27 2011 outbreak, etc.) showing
          alerts and radar evolving together. Best way to see the data model
          in motion before wiring up your own client.
        </li>
      </ul>

      <h2>What's not here yet</h2>
      <p>
        A dedicated SDK reference for <code>@aeroza/sdk</code> (with
        per-method docs and tree-shake hints) is the next doc to land. Until
        then, the SDK source is small enough to read end-to-end and the
        running FastAPI's Swagger UI at{" "}
        <a href="http://localhost:8000/docs" target="_blank" rel="noreferrer">
          /docs
        </a>{" "}
        is the source of truth for every wire shape.
      </p>
    </DocsLayout>
  );
}
