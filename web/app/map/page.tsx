"use client";

import dynamic from "next/dynamic";
import Link from "next/link";
import { useCallback, useState } from "react";

import type { AlertFeatureCollection, Severity } from "@/lib/api";
import { StatusDot } from "@/components/StatusDot";

// MapLibre touches `window` at import time, so the map component must be
// client-only. We avoid `ssr: false` from a server component by isolating
// the dynamic import to this client page.
const AlertsMap = dynamic(
  () => import("@/components/AlertsMap").then((m) => m.AlertsMap),
  {
    ssr: false,
    loading: () => (
      <div className="flex h-full w-full items-center justify-center text-xs text-muted">
        Loading map…
      </div>
    ),
  },
);

const SEVERITY_LABEL_ORDER: Severity[] = [
  "Extreme",
  "Severe",
  "Moderate",
  "Minor",
  "Unknown",
];

export default function MapPage() {
  const [collection, setCollection] = useState<AlertFeatureCollection | null>(null);
  const [lastLoaded, setLastLoaded] = useState<Date | null>(null);

  const handleLoaded = useCallback((data: AlertFeatureCollection) => {
    setCollection(data);
    setLastLoaded(new Date());
  }, []);

  const counts = countBySeverity(collection?.features ?? []);
  const total = collection?.features.length ?? 0;
  const withGeometry =
    collection?.features.filter((f) => f.geometry !== null).length ?? 0;

  return (
    <div className="flex h-[calc(100vh-3rem)] flex-col">
      <header className="flex flex-wrap items-center justify-between gap-3 border-b border-border/60 bg-bg/60 px-6 py-2.5 backdrop-blur">
        <div className="flex items-center gap-3">
          <h1 className="font-mono text-[11px] uppercase tracking-[0.2em] text-text">
            Live Alerts
          </h1>
          <StatusDot
            tone={lastLoaded ? "success" : "warning"}
            label={lastLoaded ? `Updated ${formatRelative(lastLoaded)}` : "Loading…"}
            pulse={Boolean(lastLoaded)}
          />
        </div>

        <div className="flex flex-wrap items-center gap-1.5 text-[11px]">
          <span className="text-muted">
            {total} active · {withGeometry} mappable
          </span>
          <span className="text-muted/40">·</span>
          {SEVERITY_LABEL_ORDER.map((s) => (
            <SeverityCount key={s} severity={s} count={counts[s] ?? 0} />
          ))}
        </div>

        <div className="flex items-center gap-2 text-[11px]">
          <Link
            href="/console"
            className="rounded-md border border-border/60 px-2 py-1 text-muted hover:border-accent/60 hover:text-accent"
          >
            Console →
          </Link>
          <Link
            href="/docs"
            className="rounded-md border border-border/60 px-2 py-1 text-muted hover:border-accent/60 hover:text-accent"
          >
            Docs
          </Link>
        </div>
      </header>

      <div className="relative flex-1">
        <AlertsMap onLoaded={handleLoaded} />
      </div>
    </div>
  );
}

function SeverityCount({ severity, count }: { severity: Severity; count: number }) {
  const color = SEVERITY_HEX[severity];
  return (
    <span
      className="inline-flex items-center gap-1 rounded-md border border-border/60 px-1.5 py-0.5 font-mono"
      title={`${severity}: ${count}`}
    >
      <span
        className="inline-block h-1.5 w-1.5 rounded-full"
        style={{ background: color }}
      />
      <span className="text-text">{count}</span>
    </span>
  );
}

const SEVERITY_HEX: Record<Severity, string> = {
  Extreme: "#f87171",
  Severe: "#fbbf24",
  Moderate: "#38bdfa",
  Minor: "#34d399",
  Unknown: "#94a3b8",
};

function countBySeverity(
  features: ReadonlyArray<{ properties: { severity: Severity } }>,
): Record<Severity, number> {
  const out: Record<Severity, number> = {
    Extreme: 0,
    Severe: 0,
    Moderate: 0,
    Minor: 0,
    Unknown: 0,
  };
  for (const f of features) {
    out[f.properties.severity] = (out[f.properties.severity] ?? 0) + 1;
  }
  return out;
}

function formatRelative(then: Date): string {
  const seconds = Math.floor((Date.now() - then.getTime()) / 1000);
  if (seconds < 5) return "just now";
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  return then.toLocaleTimeString();
}
