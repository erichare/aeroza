"use client";

import dynamic from "next/dynamic";
import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";

import {
  type AlertFeatureCollection,
  type MrmsGridItem,
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

// Radar auto-loop window. The "feel alive" affordance: the page boots
// playing through every MRMS grid in the last hour, looping. Long
// enough to capture a developing storm cell; short enough that a fresh
// stack with only ~10 grids in the catalog still has something to play.
const LOOP_WINDOW_MS = 60 * 60 * 1000;

type LoopSpeed = 1 | 2 | 4 | 8;
const LOOP_SPEEDS: ReadonlyArray<LoopSpeed> = [1, 2, 4, 8];

// Frame cadence per speed multiplier — same numbers /demo's Storm
// Replay uses, so muscle memory transfers between the two pages.
const LOOP_FRAME_DURATION_MS: Record<LoopSpeed, number> = {
  1: 1500,
  2: 800,
  4: 400,
  8: 200,
};

const DEFAULT_LOOP_SPEED: LoopSpeed = 2;

export default function MapPage() {
  const [collection, setCollection] = useState<AlertFeatureCollection | null>(null);
  const [lastLoaded, setLastLoaded] = useState<Date | null>(null);

  // Track "now" coarsely so the timeline window slides forward over time.
  const [liveAt, setLiveAt] = useState(() => new Date());
  // The scrubbed "as-of" time. When equal to `liveAt`, the cursor is pinned
  // to live and the AlertsMap renders without a time filter (current truth).
  const [displayedAt, setDisplayedAt] = useState<Date>(() => new Date());
  const [isLive, setIsLive] = useState(true);

  // Recent MRMS grids — used both as scrubber tick marks (for the full
  // 6h window) and as the per-frame pin source for the 1h auto-loop.
  // Keeping the full grid items (not just timestamps) so the loop has
  // ``fileKey`` to pin AlertsMap's radar layer at each step.
  const [recentGrids, setRecentGrids] = useState<ReadonlyArray<MrmsGridItem>>(
    [],
  );
  const [showRadar, setShowRadar] = useState(true);

  // 1h radar auto-loop. Default-on so the page feels alive on first
  // load: every grid in the last hour is played in sequence at 2×.
  // Pauses automatically when the user scrubs the timeline; resumes
  // via the explicit Play button. Loop is hidden entirely when there
  // are fewer than 2 grids in the window.
  const [loopPlaying, setLoopPlaying] = useState(true);
  const [loopFrame, setLoopFrame] = useState(0);
  const [loopSpeed, setLoopSpeed] = useState<LoopSpeed>(DEFAULT_LOOP_SPEED);

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
  // scrubber so the user can see when the radar refreshed, and feed the
  // 1h auto-loop with per-frame fileKeys.
  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      try {
        const data = await fetchMrmsGrids({ limit: 200 });
        if (cancelled) return;
        // API returns newest-first; reverse so the loop plays oldest →
        // newest, which is what every weather replay does.
        const sorted = [...data.items].sort(
          (a, b) =>
            new Date(a.validAt).getTime() - new Date(b.validAt).getTime(),
        );
        setRecentGrids(sorted);
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

  // Last-hour subset of recent grids — drives the auto-loop. Memoised
  // off ``liveAt`` so the cutoff slides forward at the same cadence as
  // the rest of the page; that way newly-arrived grids slot into the
  // loop without a manual refresh.
  const loopGrids = useMemo<ReadonlyArray<MrmsGridItem>>(() => {
    const cutoff = liveAt.getTime() - LOOP_WINDOW_MS;
    return recentGrids.filter(
      (g) => new Date(g.validAt).getTime() >= cutoff,
    );
  }, [recentGrids, liveAt]);

  // Drive the loop frame. We deliberately depend on ``loopGrids.length``
  // instead of the array identity so a new grid sliding into the
  // window doesn't reset the cursor — the modulo lets it advance
  // through the new total cleanly.
  useEffect(() => {
    if (!loopPlaying) return;
    if (loopGrids.length < 2) return;
    const ms = LOOP_FRAME_DURATION_MS[loopSpeed];
    const id = setInterval(() => {
      setLoopFrame((f) => (loopGrids.length === 0 ? 0 : (f + 1) % loopGrids.length));
    }, ms);
    return () => clearInterval(id);
  }, [loopPlaying, loopSpeed, loopGrids.length]);

  // Clamp frame index when the underlying array shrinks (e.g. window
  // slid past an old grid). Without this the loop can briefly point
  // at undefined while the next interval tick wraps it around.
  useEffect(() => {
    if (loopGrids.length === 0) {
      setLoopFrame(0);
      return;
    }
    setLoopFrame((f) => f % loopGrids.length);
  }, [loopGrids.length]);

  const loopActive = loopPlaying && loopGrids.length >= 2;
  const loopGrid = loopActive ? (loopGrids[loopFrame] ?? null) : null;

  // Build scrubber tick marks from the recent grids — same shape the
  // TimelineScrubber consumed before this refactor, so the visible
  // tick density stays unchanged.
  const ticks = useMemo<ReadonlyArray<TimelineTick>>(
    () =>
      recentGrids.map((item) => ({
        at: item.validAt,
        label: `${item.product} @ ${new Date(item.validAt).toLocaleTimeString()}`,
      })),
    [recentGrids],
  );

  const handleLoaded = useCallback((data: AlertFeatureCollection) => {
    setCollection(data);
    setLastLoaded(new Date());
  }, []);

  const handleScrub = useCallback((next: Date) => {
    setDisplayedAt(next);
    setIsLive(false);
    // Manually scrubbing the timeline implies "I want to look at a
    // specific moment", which conflicts with the auto-loop's "play
    // through the last hour". Pause so the user's chosen frame sticks.
    setLoopPlaying(false);
  }, []);

  const handleSnapLive = useCallback(() => {
    const now = new Date();
    setLiveAt(now);
    setDisplayedAt(now);
    setIsLive(true);
    // Snapping to live also exits the loop — they're competing
    // metaphors and the user's last action wins.
    setLoopPlaying(false);
  }, []);

  const handleToggleLoop = useCallback(() => {
    setLoopPlaying((playing) => {
      const next = !playing;
      if (next) {
        // Resuming the loop pulls the cursor away from "live" so the
        // loopGrid takes over displayedAt below. We don't clear isLive
        // here — the loop's effective time is what matters; isLive is
        // recomputed implicitly by `effectiveDisplayedAt`.
        setIsLive(false);
      }
      return next;
    });
  }, []);

  // Effective "as-of" time for everything downstream. Loop wins over
  // the manual scrubber when active, which itself wins over isLive.
  // The map, the alert counts, and the scrubber's cursor all read the
  // same effective time — keeps the badges and the radar in sync.
  const effectiveDisplayedAt = useMemo<Date>(() => {
    if (loopGrid) return new Date(loopGrid.validAt);
    return displayedAt;
  }, [loopGrid, displayedAt]);

  const counts = useMemo(
    () => countActiveBySeverity(collection?.features ?? [], effectiveDisplayedAt),
    [collection, effectiveDisplayedAt],
  );
  const activeCount = Object.values(counts).reduce((a, b) => a + b, 0);
  const mappableCount =
    collection?.features.filter(
      (f) => f.geometry !== null && wasActive(f, effectiveDisplayedAt),
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
                ? loopActive
                  ? `Looping ${effectiveDisplayedAt.toLocaleTimeString()} · ${loopFrame + 1}/${loopGrids.length}`
                  : isLive
                    ? `Updated ${formatRelative(lastLoaded)}`
                    : `Showing ${displayedAt.toLocaleTimeString()}`
                : "Loading…"
            }
            pulse={Boolean(lastLoaded) && (isLive || loopActive)}
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
          <LoopControls
            playing={loopPlaying}
            available={loopGrids.length >= 2}
            speed={loopSpeed}
            onToggle={handleToggleLoop}
            onSpeedChange={setLoopSpeed}
          />
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
          displayedAt={loopActive ? effectiveDisplayedAt : isLive ? null : displayedAt}
          radarFileKey={loopGrid?.fileKey ?? null}
          showRadar={showRadar}
          onLoaded={handleLoaded}
        />
      </div>

      <TimelineScrubber
        start={timelineStart}
        end={liveAt}
        value={effectiveDisplayedAt}
        onChange={handleScrub}
        liveAt={liveAt}
        isLive={isLive && !loopActive}
        onSnapLive={handleSnapLive}
        ticks={ticks}
        windowHours={TIMELINE_WINDOW_HOURS}
      />
    </div>
  );
}

interface LoopControlsProps {
  playing: boolean;
  available: boolean;
  speed: LoopSpeed;
  onToggle: () => void;
  onSpeedChange: (next: LoopSpeed) => void;
}

/**
 * Compact play/pause + speed control for the radar auto-loop. Lives in
 * the header next to the Radar toggle so the two display affordances
 * cluster together. Speed selector only renders when the loop is
 * actively playing — saves header real-estate for the common case.
 *
 * The disabled state ("Loop 1h" with reduced opacity) is shown when
 * the catalog has fewer than two grids in the last hour. We surface
 * the control rather than hiding it so the user knows the affordance
 * exists; tooltips explain why it can't run yet.
 */
function LoopControls({
  playing,
  available,
  speed,
  onToggle,
  onSpeedChange,
}: LoopControlsProps) {
  const disabled = !available;
  const title = !available
    ? "Need at least 2 MRMS grids in the last hour to animate. Run the materialiser, then come back."
    : playing
      ? "Pause the 1-hour radar loop"
      : "Resume the 1-hour radar loop";
  return (
    <div className="flex items-center gap-1">
      <button
        type="button"
        onClick={onToggle}
        disabled={disabled}
        aria-pressed={playing && !disabled}
        title={title}
        className={[
          "rounded-md border px-2 py-1 font-mono uppercase tracking-wide",
          disabled
            ? "cursor-not-allowed border-border/40 text-muted/50"
            : playing
              ? "border-accent bg-accent/15 text-accent"
              : "border-border/60 text-muted hover:border-accent/60 hover:text-accent",
        ].join(" ")}
      >
        {playing && !disabled ? "❚❚ Loop 1h" : "▶ Loop 1h"}
      </button>
      {playing && !disabled ? (
        <div
          className="flex items-center gap-0.5 rounded-md border border-border/60 bg-bg/40 p-0.5"
          role="tablist"
          aria-label="Loop speed"
        >
          {LOOP_SPEEDS.map((s) => (
            <button
              key={s}
              type="button"
              role="tab"
              aria-selected={speed === s}
              onClick={() => onSpeedChange(s)}
              className={[
                "rounded-sm px-1.5 py-0.5 font-mono uppercase tracking-wide",
                speed === s ? "bg-accent/15 text-accent" : "text-muted hover:text-text",
              ].join(" ")}
            >
              {s}×
            </button>
          ))}
        </div>
      ) : null}
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

// Header severity dots — a slightly lighter / more luminous take on the
// map fill palette so they read against the glacier background in the
// nav strip. Same semantic ordering as `SEVERITY_FILL_COLOR` in
// `AlertsMap`.
const SEVERITY_HEX: Record<Severity, string> = {
  Extreme: "#c45a5a",
  Severe: "#d6a14a",
  Moderate: "#5993ad",
  Minor: "#5aa090",
  Unknown: "#8694a3",
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
