"use client";

import dynamic from "next/dynamic";
import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";

import { type MrmsGridItem, fetchMrmsGrids } from "@/lib/api";

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

// How many recent grids to pull. ~5-min cycle × 100 = ~8h of MRMS, plenty
// of replay material on a healthy stack.
const GRID_FETCH_LIMIT = 100;

// Frame cadence at each speed. The values are deliberately spaced so the
// jump from 1× → 4× is felt (each frame holds for a quarter as long).
type PlaybackSpeed = 1 | 4 | 16;
const FRAME_DURATION_MS: Record<PlaybackSpeed, number> = {
  1: 800,
  4: 200,
  16: 50,
};

const HERO_BOUNDS: [number, number, number, number] = [-122, 26, -68, 49];

/**
 * `/demo` — "Storm Replay". Auto-finds the most recent radar grids in the
 * user's local archive and plays through them at adjustable speed against
 * the same `AlertsMap` /map uses. The page proves two things at once: the
 * product handles real historical data, and we can deliver a dramatic
 * weather sequence on demand for screen-shares without depending on
 * whatever happens to be over CONUS right now.
 *
 * The page is *fully introspective*: nothing about the curated event is
 * hardcoded. We fetch /v1/mrms/grids?limit=N, sort by validAt, and play
 * through whatever's there. On a fresh install with little data, the
 * empty state walks the user through `make start` + waiting a bit.
 */
export default function DemoPage() {
  const [grids, setGrids] = useState<ReadonlyArray<MrmsGridItem> | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [frameIndex, setFrameIndex] = useState(0);
  const [isPlaying, setIsPlaying] = useState(true);
  const [speed, setSpeed] = useState<PlaybackSpeed>(4);

  // Fetch once on mount. We don't poll — the user can click "Refresh"
  // explicitly if they want newer data; the autoplay shouldn't get
  // disrupted mid-storm.
  const loadGrids = useCallback(async () => {
    try {
      const data = await fetchMrmsGrids({ limit: GRID_FETCH_LIMIT });
      // Sort oldest → newest so frame 0 is the start of the replay.
      const sorted = [...data.items].sort(
        (a, b) =>
          new Date(a.validAt).getTime() - new Date(b.validAt).getTime(),
      );
      setGrids(sorted);
      setFrameIndex(0);
      setLoadError(null);
    } catch (err) {
      setLoadError(
        err instanceof Error ? err.message : "Failed to load grid catalog",
      );
    }
  }, []);

  useEffect(() => {
    void loadGrids();
  }, [loadGrids]);

  // Drive the autoplay clock. Each tick advances the frame; we loop back
  // to the start when we hit the end so the demo runs forever.
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

  // Compute the replay's overall window so the chrome can describe
  // *what* this is showing — "Replay · 06:12 → 14:08 UTC" reads like a
  // forecaster's note rather than "100 frames of stuff".
  const window = useMemo(() => computeWindow(grids), [grids]);

  return (
    <main className="flex h-[calc(100vh-3rem)] flex-col">
      <Header
        currentGrid={currentGrid}
        totalFrames={totalFrames}
        frameIndex={frameIndex}
        window={window}
      />

      <div className="relative flex-1">
        {grids === null && loadError === null ? (
          <BootingState />
        ) : loadError !== null ? (
          <ErrorState message={loadError} onRetry={loadGrids} />
        ) : (grids?.length ?? 0) < 2 ? (
          <EmptyState gridCount={grids?.length ?? 0} />
        ) : (
          <AlertsMap
            initialBounds={HERO_BOUNDS}
            showRadar
            radarFileKey={currentGrid?.fileKey ?? null}
            hideLegend
          />
        )}
      </div>

      <PlaybackBar
        frameIndex={frameIndex}
        totalFrames={totalFrames}
        currentGrid={currentGrid}
        isPlaying={isPlaying}
        speed={speed}
        onTogglePlay={() => setIsPlaying((v) => !v)}
        onSeek={setFrameIndex}
        onSpeedChange={setSpeed}
        onRefresh={loadGrids}
      />
    </main>
  );
}

function Header({
  currentGrid,
  totalFrames,
  frameIndex,
  window,
}: {
  currentGrid: MrmsGridItem | null;
  totalFrames: number;
  frameIndex: number;
  window: { start: Date; end: Date } | null;
}) {
  return (
    <header className="flex flex-wrap items-center justify-between gap-3 border-b border-border/60 bg-bg/60 px-6 py-2.5 backdrop-blur">
      <div className="flex flex-wrap items-baseline gap-3">
        <span className="font-mono text-[11px] uppercase tracking-[0.2em] text-accent">
          Storm Replay
        </span>
        <span className="font-display text-base font-semibold text-text">
          {window
            ? `${formatTime(window.start)} → ${formatTime(window.end)}`
            : "—"}
        </span>
        <span className="font-mono text-[11px] text-muted">
          {totalFrames > 0 ? (
            <>
              {totalFrames} frames · {speedFromGap(window, totalFrames)}/frame
            </>
          ) : (
            "no frames yet"
          )}
        </span>
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

function PlaybackBar({
  frameIndex,
  totalFrames,
  currentGrid,
  isPlaying,
  speed,
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
  onTogglePlay: () => void;
  onSeek: (frame: number) => void;
  onSpeedChange: (speed: PlaybackSpeed) => void;
  onRefresh: () => void;
}) {
  const disabled = totalFrames < 2;
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
  const speeds: PlaybackSpeed[] = [1, 4, 16];
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

function EmptyState({ gridCount }: { gridCount: number }) {
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
): { start: Date; end: Date } | null {
  if (grids === null || grids.length === 0) return null;
  return {
    start: new Date(grids[0].validAt),
    end: new Date(grids[grids.length - 1].validAt),
  };
}

function speedFromGap(
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
