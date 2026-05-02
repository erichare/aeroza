"use client";

import { useEffect, useMemo, useRef, useState } from "react";

const WINDOW_HOURS_DEFAULT = 6;

export interface TimelineTick {
  /** ISO timestamp; rendered as a vertical hairline on the track. */
  at: string;
  /** Optional label for screen-reader / hover affordance. */
  label?: string;
}

interface TimelineScrubberProps {
  /** Earliest moment shown on the track. Defaults to `now - windowHours`. */
  start?: Date;
  /** Latest moment shown on the track. Defaults to `now`. */
  end?: Date;
  /** Currently-selected "as-of" time. */
  value: Date;
  /** Fires while dragging — debouncing is the caller's call. */
  onChange: (next: Date) => void;
  /** "Live" snap target — when set, the Live button restores it. */
  liveAt: Date;
  /** Whether the cursor is currently pinned to live. */
  isLive: boolean;
  /** Called when the user explicitly snaps to live. */
  onSnapLive: () => void;
  /** Tick marks (e.g. radar frame valid_at). Rendered behind the cursor. */
  ticks?: ReadonlyArray<TimelineTick>;
  /** Width of window in hours when start/end omitted. */
  windowHours?: number;
}

/**
 * Horizontal scrub bar — drag the cursor to set an "as-of" time. All time
 * math is in absolute ms-since-epoch; the visible track is just `(t - start)
 * / (end - start)` mapped to pixel width. The component is purely visual /
 * controlled — the parent owns the time state.
 */
export function TimelineScrubber({
  start,
  end,
  value,
  onChange,
  liveAt,
  isLive,
  onSnapLive,
  ticks = [],
  windowHours = WINDOW_HOURS_DEFAULT,
}: TimelineScrubberProps) {
  const trackRef = useRef<HTMLDivElement | null>(null);
  const [isDragging, setIsDragging] = useState(false);

  const { startMs, endMs } = useMemo(() => {
    const e = (end ?? new Date()).getTime();
    const s = (start ?? new Date(e - windowHours * 3_600_000)).getTime();
    return { startMs: s, endMs: e };
  }, [start, end, windowHours]);

  const valueMs = clamp(value.getTime(), startMs, endMs);
  const cursorPct = ((valueMs - startMs) / Math.max(1, endMs - startMs)) * 100;
  const liveMs = clamp(liveAt.getTime(), startMs, endMs);
  const livePct = ((liveMs - startMs) / Math.max(1, endMs - startMs)) * 100;

  // Drag handlers. Pointer events handle mouse + touch with one set of code.
  useEffect(() => {
    if (!isDragging) return;

    const handleMove = (e: PointerEvent) => {
      const track = trackRef.current;
      if (!track) return;
      const rect = track.getBoundingClientRect();
      const ratio = clamp((e.clientX - rect.left) / rect.width, 0, 1);
      onChange(new Date(startMs + ratio * (endMs - startMs)));
    };
    const handleUp = () => setIsDragging(false);

    window.addEventListener("pointermove", handleMove);
    window.addEventListener("pointerup", handleUp);
    window.addEventListener("pointercancel", handleUp);
    return () => {
      window.removeEventListener("pointermove", handleMove);
      window.removeEventListener("pointerup", handleUp);
      window.removeEventListener("pointercancel", handleUp);
    };
  }, [isDragging, onChange, startMs, endMs]);

  const handleTrackPointerDown = (e: React.PointerEvent<HTMLDivElement>) => {
    const track = trackRef.current;
    if (!track) return;
    const rect = track.getBoundingClientRect();
    const ratio = clamp((e.clientX - rect.left) / rect.width, 0, 1);
    onChange(new Date(startMs + ratio * (endMs - startMs)));
    setIsDragging(true);
  };

  return (
    <div className="flex items-center gap-3 border-t border-border/60 bg-bg/85 px-4 py-2.5 backdrop-blur">
      <button
        type="button"
        onClick={onSnapLive}
        disabled={isLive}
        aria-label="Snap to live"
        className={[
          "shrink-0 rounded-md border px-2 py-1 font-mono text-[10px] uppercase tracking-wide",
          isLive
            ? "border-success/50 bg-success/15 text-success"
            : "border-border/70 text-muted hover:border-accent/60 hover:text-accent",
        ].join(" ")}
      >
        {isLive ? "● Live" : "Snap to live"}
      </button>

      <div
        ref={trackRef}
        role="slider"
        aria-label="Timeline scrubber"
        aria-valuemin={startMs}
        aria-valuemax={endMs}
        aria-valuenow={valueMs}
        aria-valuetext={new Date(valueMs).toLocaleString()}
        tabIndex={0}
        onPointerDown={handleTrackPointerDown}
        onKeyDown={(e) => {
          const step = 60_000; // 1-min nudge
          if (e.key === "ArrowLeft") {
            onChange(new Date(clamp(valueMs - step, startMs, endMs)));
            e.preventDefault();
          } else if (e.key === "ArrowRight") {
            onChange(new Date(clamp(valueMs + step, startMs, endMs)));
            e.preventDefault();
          }
        }}
        className="relative h-7 flex-1 cursor-pointer rounded-full border border-border/60 bg-surface/40"
      >
        {/* Tick marks — radar frames, alert effective times, etc. */}
        {ticks.map((tick) => {
          const t = new Date(tick.at).getTime();
          if (t < startMs || t > endMs) return null;
          const pct = ((t - startMs) / Math.max(1, endMs - startMs)) * 100;
          return (
            <span
              key={tick.at}
              title={tick.label ?? new Date(t).toLocaleTimeString()}
              className="pointer-events-none absolute top-1.5 bottom-1.5 w-px bg-accent/35"
              style={{ left: `${pct}%` }}
            />
          );
        })}

        {/* Live marker — distinct color so it's still visible behind the cursor. */}
        <span
          className="pointer-events-none absolute top-1 bottom-1 w-[2px] bg-success/60"
          style={{ left: `${livePct}%` }}
          aria-hidden
        />

        {/* Cursor handle. */}
        <span
          className="pointer-events-none absolute top-1/2 z-10 h-5 w-[3px] -translate-x-1/2 -translate-y-1/2 rounded-sm bg-accent shadow-[0_0_8px_rgba(56,189,248,0.7)]"
          style={{ left: `${cursorPct}%` }}
          aria-hidden
        />
      </div>

      <div className="shrink-0 text-right font-mono text-[11px] tabular-nums text-text">
        <div>{new Date(valueMs).toLocaleTimeString()}</div>
        <div className="text-[9px] uppercase tracking-wide text-muted">
          {formatRelative(new Date(valueMs), new Date(liveMs))}
        </div>
      </div>
    </div>
  );
}

function clamp(n: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, n));
}

function formatRelative(at: Date, ref: Date): string {
  const seconds = Math.round((ref.getTime() - at.getTime()) / 1000);
  if (seconds <= 1) return "live";
  if (seconds < 90) return `${seconds}s ago`;
  const minutes = Math.round(seconds / 60);
  if (minutes < 90) return `${minutes}m ago`;
  const hours = (seconds / 3600).toFixed(1);
  return `${hours}h ago`;
}
