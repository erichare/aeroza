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
        Aeroza is an API-first weather-intelligence platform: streaming alerts,
        materialised radar grids, and queryable predictive nowcasts. This is
        the developer documentation — narrow on purpose, focused on the things
        you actually need to build against the running API.
      </p>

      <h2>Where to go next</h2>
      <ul>
        <li>
          <Link href="/docs/quickstart">Quickstart</Link> — bring up Postgres,
          Redis, NATS, and FastAPI locally, then make your first query.
        </li>
        <li>
          <Link href="/docs/concepts">Concepts</Link> — the data model behind
          alerts, MRMS files / grids, point sample, and polygon reduction.
        </li>
        <li>
          <Link href="/docs/api">API reference</Link> — every public route,
          plus a link to the auto-generated Swagger UI for interactive
          requests.
        </li>
      </ul>

      <h2>What's not here yet</h2>
      <p>
        Docs for the TypeScript SDK (<code>@aeroza/sdk</code>) and probabilistic
        nowcasting (Phase 3) are coming. Anything you don't see covered here
        is still being built; the running FastAPI is the source of truth in
        the meantime.
      </p>
    </DocsLayout>
  );
}
