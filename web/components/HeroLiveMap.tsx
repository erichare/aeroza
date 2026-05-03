"use client";

import dynamic from "next/dynamic";
import Link from "next/link";
import { useCallback, useEffect, useState } from "react";

import {
  type AlertFeatureCollection,
  type Severity,
  fetchMrmsGrids,
} from "@/lib/api";

// AlertsMap touches `window` at import time (MapLibre); same dynamic-import
// pattern used by /map. The loading fallback inherits the parent's height
// so the layout doesn't jump when the map mounts.
const AlertsMap = dynamic(
  () => import("@/components/AlertsMap").then((m) => m.AlertsMap),
  {
    ssr: false,
    loading: () => (
      <div className="flex h-full w-full items-center justify-center bg-bg/60 text-xs text-muted">
        Booting map…
      </div>
    ),
  },
);

// Smaller-than-fullscreen viewport: zooms slightly tighter than the full
// CONUS bounds used on /map, so the hero feels framed rather than empty.
const HERO_BOUNDS: [number, number, number, number] = [-122, 26, -68, 49];

const FRESHNESS_TICK_MS = 30_000;

/**
 * Aeroza's landing-page hero: an embedded, live AlertsMap with a subtle
 * overlay strip showing system freshness + a single click-through to the
 * full /map view. Clicking anywhere on the map (including empty area)
 * navigates — the whole tile is a CTA.
 *
 * Why this lives on `/`: the previous hero was three text cards with API
 * paths, which doesn't prove anything to non-developers. The map shows
 * real CONUS radar + live alert polygons within a few seconds of page
 * load. The story tells itself.
 */
export function HeroLiveMap() {
  const [alertCollection, setAlertCollection] =
    useState<AlertFeatureCollection | null>(null);
  const [latestGridAt, setLatestGridAt] = useState<Date | null>(null);
  const [now, setNow] = useState<Date>(() => new Date());

  // Re-anchor "now" once per `FRESHNESS_TICK_MS` so the "X ago" badge
  // stays accurate without churning state on every render.
  useEffect(() => {
    const id = setInterval(() => setNow(new Date()), FRESHNESS_TICK_MS);
    return () => clearInterval(id);
  }, []);

  // Pull the freshest MRMS grid so the overlay can quote a real
  // "last grid landed Ns ago" — the load-bearing freshness number.
  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      try {
        const data = await fetchMrmsGrids({ limit: 1 });
        if (cancelled) return;
        if (data.items.length > 0) {
          setLatestGridAt(new Date(data.items[0].validAt));
        }
      } catch {
        // Freshness is decorative; a failed fetch leaves the badge in
        // its "—" state without breaking the map underneath.
      }
    };
    void load();
    const id = setInterval(load, FRESHNESS_TICK_MS);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, []);

  const handleAlertsLoaded = useCallback((data: AlertFeatureCollection) => {
    setAlertCollection(data);
  }, []);

  const activeCount = countActiveNow(alertCollection?.features ?? [], now);
  const freshness = formatFreshness(latestGridAt, now);

  return (
    <div className="group relative h-[480px] w-full overflow-hidden rounded-2xl border border-border/70 bg-surface/40 shadow-[0_1px_0_0_rgba(255,255,255,0.04)_inset]">
      <AlertsMap
        initialBounds={HERO_BOUNDS}
        showRadar
        hideLegend
        onLoaded={handleAlertsLoaded}
      />

      {/* Top overlay: live indicator + freshness + active-warning count.
          Pointer-events-none so the user can still drag the map underneath;
          the CTA below has its own click target. */}
      <div className="pointer-events-none absolute inset-x-3 top-3 z-10 flex items-center justify-between gap-3">
        <LivePill />
        <FreshnessBadges freshness={freshness} activeCount={activeCount} />
      </div>

      {/* Bottom-right CTA — the explicit click-through to /map. The whole
          card is also clickable via the absolute-positioned anchor below. */}
      <Link
        href="/map"
        className="pointer-events-auto absolute bottom-3 right-3 z-10 inline-flex items-center gap-1 rounded-md border border-accent/60 bg-bg/85 px-3 py-1.5 font-mono text-[11px] uppercase tracking-wide text-accent shadow-md backdrop-blur transition hover:bg-accent/10"
      >
        Open full map →
      </Link>

      {/* Whole-tile click target — sits under the explicit CTA + map
          interactions. We use a Link with absolute fill so anywhere the
          user wasn't dragging or clicking a control is also a CTA. */}
      <Link
        href="/map"
        aria-label="Open the live map"
        className="absolute inset-0 z-0"
      />
    </div>
  );
}

function LivePill() {
  return (
    <span className="inline-flex items-center gap-1.5 rounded-full border border-success/50 bg-bg/85 px-2.5 py-1 font-mono text-[10px] uppercase tracking-[0.18em] text-success shadow-md backdrop-blur">
      <span className="pulse-dot inline-block h-1.5 w-1.5 rounded-full bg-success" />
      Live · CONUS
    </span>
  );
}

function FreshnessBadges({
  freshness,
  activeCount,
}: {
  freshness: string;
  activeCount: number;
}) {
  return (
    <div className="flex items-center gap-1.5 font-mono text-[10px]">
      <span className="rounded-md border border-border/70 bg-bg/85 px-2 py-1 text-muted shadow-md backdrop-blur">
        Last grid <span className="text-text">{freshness}</span>
      </span>
      <span className="rounded-md border border-border/70 bg-bg/85 px-2 py-1 text-muted shadow-md backdrop-blur">
        <span className="text-text">{activeCount}</span> active
      </span>
    </div>
  );
}

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

function countActiveNow(
  features: ReadonlyArray<AlertLike>,
  asOf: Date,
): number {
  const t = asOf.getTime();
  let count = 0;
  for (const f of features) {
    const p = f.properties;
    const startStr = p.onset ?? p.effective;
    const endStr = p.ends ?? p.expires;
    const startMs = startStr ? Date.parse(startStr) : Number.NEGATIVE_INFINITY;
    const endMs = endStr ? Date.parse(endStr) : Number.POSITIVE_INFINITY;
    if (t >= startMs && t <= endMs) count += 1;
  }
  return count;
}

function formatFreshness(then: Date | null, now: Date): string {
  if (then === null) return "—";
  const seconds = Math.max(0, Math.floor((now.getTime() - then.getTime()) / 1000));
  if (seconds < 5) return "just now";
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  return `${hours}h ago`;
}
