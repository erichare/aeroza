"use client";

import dynamic from "next/dynamic";
import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";

import {
  type AlertFeatureCollection,
  type Severity,
  fetchMrmsGrids,
} from "@/lib/api";
import { StatusDot } from "@/components/StatusDot";
import { TimelineScrubber, type TimelineTick } from "@/components/TimelineScrubber";

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

const TIMELINE_WINDOW_HOURS = 6;
// How fresh the "live" pin should stay — re-anchoring once a minute keeps the
// scrubber's right edge near `now` without churning state every animation
// frame.
const LIVE_TICK_MS = 60_000;

export default function MapPage() {
  const [collection, setCollection] = useState<AlertFeatureCollection | null>(null);
  const [lastLoaded, setLastLoaded] = useState<Date | null>(null);

  // Track "now" coarsely so the timeline window slides forward over time.
  const [liveAt, setLiveAt] = useState(() => new Date());
  // The scrubbed "as-of" time. When equal to `liveAt`, the cursor is pinned
  // to live and the AlertsMap renders without a time filter (current truth).
  const [displayedAt, setDisplayedAt] = useState<Date>(() => new Date());
  const [isLive, setIsLive] = useState(true);

  const [ticks, setTicks] = useState<ReadonlyArray<TimelineTick>>([]);
  const [showRadar, setShowRadar] = useState(true);

  // Keep the timeline window's right edge sliding forward so 'now' stays
  // visible. Doesn't touch `displayedAt` unless the user is in live mode.
  useEffect(() => {
    const tick = () => {
      const now = new Date();
      setLiveAt(now);
      if (isLive) setDisplayedAt(now);
    };
    const id = setInterval(tick, LIVE_TICK_MS);
    return () => clearInterval(id);
  }, [isLive]);

  // Fetch the last few hours of MRMS grids → render as tick marks on the
  // scrubber so the user can see when the radar refreshed.
  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      try {
        const data = await fetchMrmsGrids({ limit: 200 });
        if (cancelled) return;
        setTicks(
          data.items.map((item) => ({
            at: item.validAt,
            label: `${item.product} @ ${new Date(item.validAt).toLocaleTimeString()}`,
          })),
        );
      } catch {
        // Tick marks are decorative — silently absorb a failed grid fetch
        // rather than blocking the page.
      }
    };
    void load();
    const id = setInterval(load, 60_000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, []);

  const handleLoaded = useCallback((data: AlertFeatureCollection) => {
    setCollection(data);
    setLastLoaded(new Date());
  }, []);

  const handleScrub = useCallback((next: Date) => {
    setDisplayedAt(next);
    setIsLive(false);
  }, []);

  const handleSnapLive = useCallback(() => {
    const now = new Date();
    setLiveAt(now);
    setDisplayedAt(now);
    setIsLive(true);
  }, []);

  const counts = useMemo(
    () => countActiveBySeverity(collection?.features ?? [], displayedAt),
    [collection, displayedAt],
  );
  const activeCount = Object.values(counts).reduce((a, b) => a + b, 0);
  const mappableCount =
    collection?.features.filter(
      (f) => f.geometry !== null && wasActive(f, displayedAt),
    ).length ?? 0;

  const timelineStart = useMemo(
    () => new Date(liveAt.getTime() - TIMELINE_WINDOW_HOURS * 3_600_000),
    [liveAt],
  );

  return (
    <div className="flex h-[calc(100vh-3rem)] flex-col">
      <header className="flex flex-wrap items-center justify-between gap-3 border-b border-border/60 bg-bg/60 px-6 py-2.5 backdrop-blur">
        <div className="flex items-center gap-3">
          <h1 className="font-mono text-[11px] uppercase tracking-[0.2em] text-text">
            Live Alerts
          </h1>
          <StatusDot
            tone={lastLoaded ? "success" : "warning"}
            label={
              lastLoaded
                ? isLive
                  ? `Updated ${formatRelative(lastLoaded)}`
                  : `Showing ${displayedAt.toLocaleTimeString()}`
                : "Loading…"
            }
            pulse={Boolean(lastLoaded) && isLive}
          />
        </div>

        <div className="flex flex-wrap items-center gap-1.5 text-[11px]">
          <span className="text-muted">
            {activeCount} active · {mappableCount} mappable
          </span>
          <span className="text-muted/40">·</span>
          {SEVERITY_LABEL_ORDER.map((s) => (
            <SeverityCount key={s} severity={s} count={counts[s] ?? 0} />
          ))}
        </div>

        <div className="flex items-center gap-2 text-[11px]">
          <button
            type="button"
            onClick={() => setShowRadar((v) => !v)}
            aria-pressed={showRadar}
            className={[
              "rounded-md border px-2 py-1 font-mono uppercase tracking-wide",
              showRadar
                ? "border-accent bg-accent/15 text-accent"
                : "border-border/60 text-muted hover:border-accent/60 hover:text-accent",
            ].join(" ")}
            title="Toggle MRMS reflectivity radar overlay"
          >
            {showRadar ? "● Radar" : "Radar"}
          </button>
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
        <AlertsMap
          displayedAt={isLive ? null : displayedAt}
          showRadar={showRadar}
          onLoaded={handleLoaded}
        />
      </div>

      <TimelineScrubber
        start={timelineStart}
        end={liveAt}
        value={displayedAt}
        onChange={handleScrub}
        liveAt={liveAt}
        isLive={isLive}
        onSnapLive={handleSnapLive}
        ticks={ticks}
        windowHours={TIMELINE_WINDOW_HOURS}
      />
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

interface AlertLike {
  geometry: unknown;
  properties: {
    severity: Severity;
    onset: string | null;
    effective: string | null;
    expires: string | null;
    ends: string | null;
  };
}

function countActiveBySeverity(
  features: ReadonlyArray<AlertLike>,
  asOf: Date,
): Record<Severity, number> {
  const out: Record<Severity, number> = {
    Extreme: 0,
    Severe: 0,
    Moderate: 0,
    Minor: 0,
    Unknown: 0,
  };
  for (const f of features) {
    if (!wasActive(f, asOf)) continue;
    out[f.properties.severity] = (out[f.properties.severity] ?? 0) + 1;
  }
  return out;
}

function wasActive(feature: AlertLike, asOf: Date): boolean {
  const p = feature.properties;
  const startStr = p.onset ?? p.effective;
  const endStr = p.ends ?? p.expires;
  const startMs = startStr ? Date.parse(startStr) : Number.NEGATIVE_INFINITY;
  const endMs = endStr ? Date.parse(endStr) : Number.POSITIVE_INFINITY;
  const t = asOf.getTime();
  return t >= startMs && t <= endMs;
}

function formatRelative(then: Date): string {
  const seconds = Math.floor((Date.now() - then.getTime()) / 1000);
  if (seconds < 5) return "just now";
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  return then.toLocaleTimeString();
}
