import type { Metadata } from "next";

import { AlertsStreamPanel } from "@/components/AlertsStreamPanel";
import { HealthPanel } from "@/components/HealthPanel";
import { MrmsFilesPanel } from "@/components/MrmsFilesPanel";
import { MrmsGridsPanel } from "@/components/MrmsGridsPanel";
import { SamplePanel } from "@/components/SamplePanel";

export const metadata: Metadata = {
  title: "Dev Console",
  description:
    "Live test harness for the Aeroza weather-intelligence API: alerts SSE stream, " +
    "MRMS catalog, and system health.",
};

export default function ConsolePage() {
  return (
    <main className="mx-auto flex min-h-[calc(100vh-3rem)] w-full max-w-[1400px] flex-col gap-6 px-6 py-8">
      <header className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight text-text">
            Live test harness
          </h1>
          <p className="mt-1 max-w-2xl text-sm text-muted">
            Real-time view of the FastAPI surface — alerts SSE stream, MRMS
            catalog, and health. Designed for development and demo, not for
            production traffic.
          </p>
        </div>
        <nav className="flex flex-wrap gap-2 text-xs">
          <ApiLink href="/docs" label="OpenAPI / Swagger" />
          <ApiLink href="/v1/alerts" label="GET /v1/alerts" />
          <ApiLink href="/v1/mrms/files" label="GET /v1/mrms/files" />
          <ApiLink href="/v1/mrms/grids" label="GET /v1/mrms/grids" />
          <ApiLink
            href="/v1/mrms/grids/sample?lat=29.76&lng=-95.37"
            label="GET /v1/mrms/grids/sample"
          />
          <ApiLink href="/v1/stats" label="GET /v1/stats" />
        </nav>
      </header>

      <div className="grid gap-5 lg:grid-cols-3">
        <div className="lg:col-span-2">
          <AlertsStreamPanel />
        </div>
        <HealthPanel />
        <div className="lg:col-span-3">
          <MrmsFilesPanel />
        </div>
        <div className="lg:col-span-3">
          <MrmsGridsPanel />
        </div>
        <div className="lg:col-span-3">
          <SamplePanel />
        </div>
      </div>

      <footer className="mt-auto pt-6 text-center text-[11px] text-muted/60">
        Console v0.1 · {new Date().getFullYear()} · github.com/erichare/aeroza
      </footer>
    </main>
  );
}

/**
 * Link to a FastAPI route — opens in a new tab pointing at the configured
 * API base URL. Named ``ApiLink`` (not ``Link``) so it doesn't collide with
 * Next's own ``Link`` if we later add internal nav back to this file.
 */
function ApiLink({ href, label }: { href: string; label: string }) {
  const apiBase = process.env.NEXT_PUBLIC_AEROZA_API_URL ?? "http://localhost:8000";
  return (
    <a
      href={`${apiBase}${href}`}
      target="_blank"
      rel="noreferrer"
      className="rounded-md border border-border/70 px-2 py-1 font-mono text-muted hover:border-accent/60 hover:text-accent"
    >
      {label}
    </a>
  );
}
