"use client";

import dynamic from "next/dynamic";
import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";

import { type MrmsGridItem, fetchMrmsGrids } from "@/lib/api";
import {
  type FeaturedEvent,
  FEATURED_EVENTS,
  findFeaturedEvent,
} from "@/lib/featuredEvents";

// AlertsMap touches `window` at import time; same dynamic-import pattern
// /map and the home-page hero use.
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

// Default: fetch enough recent grids to give live mode ~6h of replay
// material on a healthy 5-minute-cycle stack.
const LIVE_GRID_FETCH_LIMIT = 100;
// Featured events are bracketed to 4–8 hour windows; cap at 200 to
// safely fit the densest events without paging.
const EVENT_GRID_FETCH_LIMIT = 200;

// Frame cadence per speed multiplier. The numbers were re-tuned to play
// well with the AlertsMap radar layer's 700ms cross-fade in pinned
// mode: even the fastest speed (8×) holds long enough for the fade to
// substantially complete, so the eye doesn't see hard transitions
// between frames.
type PlaybackSpeed = 1 | 2 | 4 | 8;
const FRAME_DURATION_MS: Record<PlaybackSpeed, number> = {
  1: 1500,
  2: 800,
  4: 400,
  8: 200,
};
const DEFAULT_SPEED: PlaybackSpeed = 2;

// Default CONUS framing for live-archive mode (events override with
// their own bbox).
const LIVE_BOUNDS: [number, number, number, number] = [-122, 26, -68, 49];

type Mode =
  | { kind: "live" }
  | { kind: "event"; event: FeaturedEvent };

/**
 * `/demo` — "Storm Replay". Two modes, one chrome:
 *
 * 1. **Live archive** (default). Replays the most recent N grids from
 *    your local /v1/mrms/grids catalog. Always works on any healthy
 *    stack with a few hours of ingest.
 *
 * 2. **Featured event**. A small hand-curated catalog of major US
 *    weather events (Houston Derecho, Mayfield outbreak, etc.) with
 *    written commentary. Selecting one zooms the camera, fetches the
 *    grids in that bbox + time window, and plays them through.
 *
 * Pre-MRMS events (anything before ~2014) are tagged
 * `replayable: false` and render commentary only — radar suppressed
 * with a clear "pre-MRMS" notice instead of inventing graphics.
 */
export default function DemoPage() {
  const [mode, setMode] = useState<Mode>({ kind: "live" });
  const [grids, setGrids] = useState<ReadonlyArray<MrmsGridItem> | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [frameIndex, setFrameIndex] = useState(0);
  const [isPlaying, setIsPlaying] = useState(true);
  const [speed, setSpeed] = useState<PlaybackSpeed>(DEFAULT_SPEED);

  // Reload the grids whenever mode changes. Mode changes are the only
  // thing that meaningfully alters the time window we want to fetch.
  const loadGrids = useCallback(async () => {
    setLoadError(null);
    setFrameIndex(0);
    if (mode.kind === "event" && !mode.event.replayable) {
      // Non-replayable event: skip the fetch entirely; the page renders
      // commentary-only.
      setGrids([]);
      return;
    }
    try {
      const query =
        mode.kind === "event"
          ? {
              limit: EVENT_GRID_FETCH_LIMIT,
              since: mode.event.startUtc,
              until: mode.event.endUtc,
            }
          : { limit: LIVE_GRID_FETCH_LIMIT };
      const data = await fetchMrmsGrids(query);
      // Sort oldest → newest so frame 0 is the start of the replay.
      const sorted = [...data.items].sort(
        (a, b) =>
          new Date(a.validAt).getTime() - new Date(b.validAt).getTime(),
      );
      setGrids(sorted);
    } catch (err) {
      setLoadError(
        err instanceof Error ? err.message : "Failed to load grid catalog",
      );
      setGrids(null);
    }
  }, [mode]);

  useEffect(() => {
    void loadGrids();
  }, [loadGrids]);

  // Drive the autoplay clock. Each tick advances the frame; we loop
  // back to the start when we hit the end so the demo runs forever.
  useEffect(() => {
    if (!isPlaying) return;
    if (grids === null || grids.length < 2) return;
    const interval = FRAME_DURATION_MS[speed];
    const id = setInterval(() => {
      setFrameIndex((i) => (grids.length === 0 ? 0 : (i + 1) % grids.length));
    }, interval);
    return () => clearInterval(id);
  }, [isPlaying, speed, grids]);

  const currentGrid = grids?.[frameIndex] ?? null;
  const totalFrames = grids?.length ?? 0;
  const window = useMemo(() => computeWindow(grids, mode), [grids, mode]);
  const bounds: [number, number, number, number] =
    mode.kind === "event" ? mode.event.bbox : LIVE_BOUNDS;
  const isCommentaryOnly =
    mode.kind === "event" && !mode.event.replayable;

  const handleSelectMode = useCallback((next: Mode) => {
    setMode(next);
    setIsPlaying(true);
  }, []);

  return (
    <main className="flex h-[calc(100vh-3rem)] flex-col">
      <EventPicker
        currentId={mode.kind === "event" ? mode.event.id : null}
        onSelect={(id) => {
          if (id === null) {
            handleSelectMode({ kind: "live" });
            return;
          }
          const event = findFeaturedEvent(id);
          if (event) handleSelectMode({ kind: "event", event });
        }}
      />

      <Header
        mode={mode}
        currentGrid={currentGrid}
        totalFrames={totalFrames}
        frameIndex={frameIndex}
        window={window}
      />

      <div className="relative flex-1">
        {isCommentaryOnly ? (
          <CommentaryOnlyState event={(mode as { event: FeaturedEvent }).event} />
        ) : grids === null && loadError === null ? (
          <BootingState />
        ) : loadError !== null ? (
          <ErrorState message={loadError} onRetry={loadGrids} />
        ) : (grids?.length ?? 0) < 2 ? (
          <EmptyState mode={mode} gridCount={grids?.length ?? 0} />
        ) : (
          <>
            <AlertsMap
              initialBounds={bounds}
              showRadar
              radarFileKey={currentGrid?.fileKey ?? null}
              hideLegend
            />
            {mode.kind === "event" ? (
              <CommentaryOverlay event={mode.event} />
            ) : null}
          </>
        )}
      </div>

      <PlaybackBar
        frameIndex={frameIndex}
        totalFrames={totalFrames}
        currentGrid={currentGrid}
        isPlaying={isPlaying}
        speed={speed}
        disabled={isCommentaryOnly || (grids?.length ?? 0) < 2}
        onTogglePlay={() => setIsPlaying((v) => !v)}
        onSeek={setFrameIndex}
        onSpeedChange={setSpeed}
        onRefresh={loadGrids}
      />
    </main>
  );
}

function EventPicker({
  currentId,
  onSelect,
}: {
  currentId: string | null;
  onSelect: (id: string | null) => void;
}) {
  return (
    <nav
      aria-label="Replay source"
      className="flex items-center gap-1.5 overflow-x-auto border-b border-border/60 bg-bg/40 px-6 py-2 text-[11px] backdrop-blur"
    >
      <PickerTab
        active={currentId === null}
        onClick={() => onSelect(null)}
        label="Live archive"
        sub="Your local stack"
      />
      {FEATURED_EVENTS.map((event) => (
        <PickerTab
          key={event.id}
          active={currentId === event.id}
          onClick={() => onSelect(event.id)}
          label={event.name}
          sub={event.date}
          dim={!event.replayable}
        />
      ))}
    </nav>
  );
}

function PickerTab({
  active,
  onClick,
  label,
  sub,
  dim,
}: {
  active: boolean;
  onClick: () => void;
  label: string;
  sub: string;
  dim?: boolean;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      aria-pressed={active}
      className={[
        "flex shrink-0 flex-col items-start rounded-md border px-3 py-1.5 text-left transition-colors",
        active
          ? "border-accent bg-accent/15 text-accent"
          : "border-border/60 text-muted hover:border-accent/60 hover:text-text",
      ].join(" ")}
    >
      <span className="font-mono uppercase tracking-wide">
        {label}
        {dim ? (
          <span className="ml-1.5 rounded-sm border border-warning/40 bg-warning/10 px-1 py-0.5 text-[8px] font-medium uppercase tracking-wider text-warning">
            pre-MRMS
          </span>
        ) : null}
      </span>
      <span className="font-mono text-[9px] text-muted/80">{sub}</span>
    </button>
  );
}

function Header({
  mode,
  currentGrid,
  totalFrames,
  frameIndex,
  window,
}: {
  mode: Mode;
  currentGrid: MrmsGridItem | null;
  totalFrames: number;
  frameIndex: number;
  window: { start: Date; end: Date } | null;
}) {
  const title =
    mode.kind === "event" ? mode.event.name : "Storm Replay · Live archive";
  return (
    <header className="flex flex-wrap items-center justify-between gap-3 border-b border-border/60 bg-bg/60 px-6 py-2.5 backdrop-blur">
      <div className="flex flex-wrap items-baseline gap-3">
        <span className="font-mono text-[11px] uppercase tracking-[0.2em] text-accent">
          {mode.kind === "event" ? "Featured event" : "Storm Replay"}
        </span>
        <span className="font-display text-base font-semibold text-text">
          {title}
        </span>
        <span className="font-mono text-[11px] text-muted">
          {window
            ? `${formatTime(window.start)} → ${formatTime(window.end)}`
            : "—"}
        </span>
        {totalFrames > 0 ? (
          <span className="font-mono text-[11px] text-muted">
            {totalFrames} frames · {gapFromWindow(window, totalFrames)}/frame
          </span>
        ) : null}
      </div>
      <div className="flex items-center gap-3 text-[11px]">
        {currentGrid ? (
          <span className="font-mono text-muted">
            now showing{" "}
            <span className="text-text">
              {formatTime(new Date(currentGrid.validAt))}
            </span>{" "}
            <span className="text-muted/70">
              ({frameIndex + 1}/{totalFrames})
            </span>
          </span>
        ) : null}
        <Link
          href="/map"
          className="rounded-md border border-border/60 px-2.5 py-1 font-mono text-muted hover:border-accent/60 hover:text-accent"
        >
          Try it live →
        </Link>
      </div>
    </header>
  );
}

function CommentaryOverlay({ event }: { event: FeaturedEvent }) {
  // Floating commentary card on the left side of the map for replayable
  // events. Pointer-events-auto so the user can still scroll the long
  // commentary while the replay continues underneath.
  return (
    <aside className="pointer-events-auto absolute left-3 top-3 z-10 max-h-[calc(100%-1.5rem)] w-80 max-w-[calc(100%-1.5rem)] overflow-y-auto rounded-xl border border-border/60 bg-bg/90 p-4 shadow-xl backdrop-blur">
      <CommentaryBody event={event} />
    </aside>
  );
}

function CommentaryOnlyState({ event }: { event: FeaturedEvent }) {
  // For pre-MRMS events: no radar, just the commentary as the focal
  // content. We make the layout intentionally generous because the
  // commentary IS the value.
  return (
    <div className="mx-auto flex h-full max-w-2xl flex-col gap-4 px-6 py-8">
      <div className="rounded-md border border-warning/40 bg-warning/10 px-3 py-2 text-xs leading-relaxed text-warning">
        <strong className="font-semibold">Radar replay not available.</strong>{" "}
        This event predates MRMS (operational ~2014). The commentary is
        included for context — see "what we can't show" in the body.
      </div>
      <article className="rounded-2xl border border-border/70 bg-surface/40 p-6 backdrop-blur">
        <CommentaryBody event={event} headline />
      </article>
    </div>
  );
}

function CommentaryBody({
  event,
  headline,
}: {
  event: FeaturedEvent;
  headline?: boolean;
}) {
  return (
    <>
      <header className="mb-3">
        <p className="font-mono text-[10px] uppercase tracking-[0.2em] text-accent">
          {event.location} · {event.date}
        </p>
        {headline ? (
          <h2 className="mt-1 font-display text-2xl font-semibold leading-tight text-text">
            {event.name}
          </h2>
        ) : (
          <h3 className="mt-1 font-display text-base font-semibold leading-tight text-text">
            {event.name}
          </h3>
        )}
        <p className="mt-1.5 text-[11px] leading-relaxed text-muted">
          {event.summary}
        </p>
      </header>
      <div className="flex flex-col gap-2.5 text-[12px] leading-relaxed text-text/90">
        {event.commentary.map((para, i) => (
          <p key={i}>{para}</p>
        ))}
      </div>
      {event.reference ? (
        <a
          href={event.reference.url}
          target="_blank"
          rel="noreferrer"
          className="mt-4 inline-flex items-center gap-1 font-mono text-[10px] uppercase tracking-wide text-accent hover:underline"
        >
          {event.reference.label} ↗
        </a>
      ) : null}
    </>
  );
}

function PlaybackBar({
  frameIndex,
  totalFrames,
  currentGrid,
  isPlaying,
  speed,
  disabled,
  onTogglePlay,
  onSeek,
  onSpeedChange,
  onRefresh,
}: {
  frameIndex: number;
  totalFrames: number;
  currentGrid: MrmsGridItem | null;
  isPlaying: boolean;
  speed: PlaybackSpeed;
  disabled: boolean;
  onTogglePlay: () => void;
  onSeek: (frame: number) => void;
  onSpeedChange: (speed: PlaybackSpeed) => void;
  onRefresh: () => void;
}) {
  return (
    <footer className="border-t border-border/60 bg-bg/60 px-6 py-3 backdrop-blur">
      <div className="flex items-center gap-3">
        <button
          type="button"
          onClick={onTogglePlay}
          disabled={disabled}
          aria-pressed={isPlaying}
          aria-label={isPlaying ? "Pause replay" : "Play replay"}
          className={[
            "flex h-9 w-9 items-center justify-center rounded-md border font-mono text-sm transition-colors",
            disabled
              ? "cursor-not-allowed border-border/40 text-muted/40"
              : isPlaying
                ? "border-accent bg-accent/15 text-accent"
                : "border-border/60 text-muted hover:border-accent/60 hover:text-accent",
          ].join(" ")}
        >
          {isPlaying ? "❚❚" : "▶"}
        </button>

        <input
          type="range"
          min={0}
          max={Math.max(0, totalFrames - 1)}
          value={frameIndex}
          onChange={(e) => onSeek(Number(e.target.value))}
          disabled={disabled}
          aria-label="Frame seek"
          className="h-1.5 flex-1 cursor-pointer accent-accent disabled:cursor-not-allowed disabled:opacity-40"
        />

        <SpeedSwitcher value={speed} onChange={onSpeedChange} disabled={disabled} />

        <button
          type="button"
          onClick={onRefresh}
          className="rounded-md border border-border/60 px-2.5 py-1 font-mono text-[11px] text-muted hover:border-accent/60 hover:text-accent"
          title="Re-fetch the grid catalog"
        >
          Refresh
        </button>
      </div>

      {currentGrid ? (
        <div className="mt-2 flex flex-wrap items-center gap-x-4 gap-y-1 font-mono text-[10px] text-muted">
          <span>
            <span className="uppercase tracking-wide">grid</span>{" "}
            <span className="text-text">{currentGrid.product}</span> · level{" "}
            <span className="text-text">{currentGrid.level}</span>
          </span>
          <span className="truncate">
            <span className="uppercase tracking-wide">key</span>{" "}
            <span className="text-text/80">{currentGrid.fileKey}</span>
          </span>
        </div>
      ) : null}
    </footer>
  );
}

function SpeedSwitcher({
  value,
  onChange,
  disabled,
}: {
  value: PlaybackSpeed;
  onChange: (next: PlaybackSpeed) => void;
  disabled: boolean;
}) {
  const speeds: PlaybackSpeed[] = [1, 2, 4, 8];
  return (
    <div
      className="flex items-center gap-0.5 rounded-md border border-border/60 bg-bg/40 p-0.5 text-[11px]"
      role="tablist"
      aria-label="Playback speed"
    >
      {speeds.map((s) => (
        <button
          key={s}
          type="button"
          role="tab"
          aria-selected={value === s}
          disabled={disabled}
          onClick={() => onChange(s)}
          className={[
            "rounded-sm px-2 py-0.5 font-mono uppercase tracking-wide",
            value === s && !disabled
              ? "bg-accent/15 text-accent"
              : "text-muted hover:text-text disabled:text-muted/40",
          ].join(" ")}
        >
          {s}×
        </button>
      ))}
    </div>
  );
}

function BootingState() {
  return (
    <div className="flex h-full w-full items-center justify-center text-xs text-muted">
      Loading grid catalog…
    </div>
  );
}

function ErrorState({
  message,
  onRetry,
}: {
  message: string;
  onRetry: () => void;
}) {
  return (
    <div className="mx-auto flex h-full max-w-md flex-col items-center justify-center gap-3 px-6 text-center">
      <div className="rounded-md border border-warning/40 bg-warning/10 px-3 py-2 text-xs text-warning">
        {message}
      </div>
      <p className="text-xs text-muted">
        Is the API up? <code className="font-mono text-text">make start</code>{" "}
        boots the whole stack including ingest workers.
      </p>
      <button
        type="button"
        onClick={onRetry}
        className="rounded-md border border-accent bg-accent/15 px-3 py-1.5 font-mono text-[11px] uppercase tracking-wide text-accent hover:bg-accent/25"
      >
        Retry
      </button>
    </div>
  );
}

function EmptyState({
  mode,
  gridCount,
}: {
  mode: Mode;
  gridCount: number;
}) {
  if (mode.kind === "event") {
    const event = mode.event;
    return (
      <div className="mx-auto flex h-full max-w-xl flex-col items-center justify-center gap-3 px-6 text-center">
        <h2 className="font-display text-lg font-semibold text-text">
          No grids ingested for this event yet.
        </h2>
        <p className="text-sm leading-relaxed text-muted">
          Your local archive has{" "}
          <span className="text-text">{gridCount}</span> grid
          {gridCount === 1 ? "" : "s"} in the {event.name} window
          ({formatDateTime(new Date(event.startUtc))} →{" "}
          {formatDateTime(new Date(event.endUtc))}).
        </p>
        <p className="text-sm leading-relaxed text-muted">
          MRMS keeps roughly 24h of grids in NOAA's real-time bucket;
          historical events need to be pulled from the NCEI archive. A{" "}
          <code className="font-mono text-text">aeroza-ingest-mrms</code>{" "}
          extension that reads from the archive isn't shipped yet.
        </p>
        <Link
          href="/map"
          className="rounded-md border border-accent bg-accent/15 px-3 py-1.5 font-mono text-[11px] uppercase tracking-wide text-accent hover:bg-accent/25"
        >
          Try the live map →
        </Link>
      </div>
    );
  }
  return (
    <div className="mx-auto flex h-full max-w-lg flex-col items-center justify-center gap-3 px-6 text-center">
      <h2 className="font-display text-lg font-semibold text-text">
        Not enough grids to replay yet.
      </h2>
      <p className="text-sm leading-relaxed text-muted">
        {gridCount === 0
          ? "Your local archive has no MRMS grids."
          : `Your local archive has ${gridCount} grid${gridCount === 1 ? "" : "s"} — replay needs at least 2.`}{" "}
        Run <code className="font-mono text-text">make start</code> to launch
        the ingest workers, then come back in 10–20 minutes for a meaningful
        replay window. The MRMS publish cycle is ~5 minutes per grid.
      </p>
      <Link
        href="/map"
        className="rounded-md border border-accent bg-accent/15 px-3 py-1.5 font-mono text-[11px] uppercase tracking-wide text-accent hover:bg-accent/25"
      >
        Open live map →
      </Link>
    </div>
  );
}

function computeWindow(
  grids: ReadonlyArray<MrmsGridItem> | null,
  mode: Mode,
): { start: Date; end: Date } | null {
  // For events, we always want to display the curated window even if
  // grids haven't loaded yet — that's part of the event's identity.
  if (mode.kind === "event") {
    return {
      start: new Date(mode.event.startUtc),
      end: new Date(mode.event.endUtc),
    };
  }
  if (grids === null || grids.length === 0) return null;
  return {
    start: new Date(grids[0].validAt),
    end: new Date(grids[grids.length - 1].validAt),
  };
}

function gapFromWindow(
  window: { start: Date; end: Date } | null,
  totalFrames: number,
): string {
  if (window === null || totalFrames < 2) return "—";
  const totalMs = window.end.getTime() - window.start.getTime();
  const gapMs = Math.round(totalMs / Math.max(1, totalFrames - 1));
  const gapMinutes = Math.round(gapMs / 60_000);
  if (gapMinutes < 1) return "<1 min";
  return `~${gapMinutes} min`;
}

function formatTime(d: Date): string {
  return d.toLocaleTimeString(undefined, {
    hour: "2-digit",
    minute: "2-digit",
  });
}

function formatDateTime(d: Date): string {
  return d.toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}
