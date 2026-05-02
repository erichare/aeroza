"use client";

import { useMemo } from "react";

export interface SparklineProps {
  /** Data points, oldest → newest. */
  values: ReadonlyArray<number | null>;
  /** Optional explicit min/max — useful when comparing series with the
   * same scale (so a row of sparklines uses one Y axis). */
  yMin?: number;
  yMax?: number;
  /** Width / height in pixels. */
  width?: number;
  height?: number;
  /** Stroke + fill color. Defaults to the palette accent. */
  color?: string;
  /** Optional aria-label override. */
  label?: string;
}

const DEFAULT_WIDTH = 80;
const DEFAULT_HEIGHT = 22;

/**
 * Inline SVG sparkline — one polyline + a faint area fill.
 *
 * No charting library: the cells are tiny, the data shape is fixed,
 * and we'd rather pay 60 lines of code than ship a 30 KB chart engine
 * to render a 80×22 line.
 */
export function Sparkline({
  values,
  yMin,
  yMax,
  width = DEFAULT_WIDTH,
  height = DEFAULT_HEIGHT,
  color = "rgb(var(--accent))",
  label,
}: SparklineProps) {
  const path = useMemo(
    () => buildPath(values, { width, height, yMin, yMax }),
    [values, width, height, yMin, yMax],
  );

  if (path.linePoints === null) {
    return (
      <span
        className="inline-block text-[10px] font-mono text-muted/60"
        aria-label={label ?? "no data"}
        style={{ width, height, lineHeight: `${height}px` }}
      >
        —
      </span>
    );
  }

  const ariaLabel =
    label ??
    `Trend: ${values.length} points, latest ${(values.at(-1) ?? 0).toFixed(2)}`;

  return (
    <svg
      role="img"
      aria-label={ariaLabel}
      width={width}
      height={height}
      viewBox={`0 0 ${width} ${height}`}
      style={{ display: "block" }}
    >
      <path
        d={path.areaPath}
        fill={color}
        fillOpacity={0.18}
        stroke="none"
      />
      <polyline
        fill="none"
        stroke={color}
        strokeWidth={1.4}
        strokeLinejoin="round"
        strokeLinecap="round"
        points={path.linePoints}
      />
      {/* Final point dot — small visual anchor for "what's the latest?". */}
      {path.lastPoint ? (
        <circle
          cx={path.lastPoint.x}
          cy={path.lastPoint.y}
          r={1.6}
          fill={color}
        />
      ) : null}
    </svg>
  );
}

interface PathResult {
  linePoints: string | null;
  areaPath: string;
  lastPoint: { x: number; y: number } | null;
}

function buildPath(
  values: ReadonlyArray<number | null>,
  opts: { width: number; height: number; yMin?: number; yMax?: number },
): PathResult {
  const { width, height } = opts;
  const padY = 1.5;
  const usableH = Math.max(1, height - 2 * padY);

  const finite = values.filter((v): v is number => v !== null && Number.isFinite(v));
  if (finite.length === 0) {
    return { linePoints: null, areaPath: "", lastPoint: null };
  }

  const minV = opts.yMin ?? Math.min(...finite);
  const maxV = opts.yMax ?? Math.max(...finite);
  const span = Math.max(1e-9, maxV - minV);

  const stepX = values.length === 1 ? 0 : width / (values.length - 1);

  // Build line through finite points; missing points break the line.
  const segments: Array<Array<{ x: number; y: number }>> = [];
  let current: Array<{ x: number; y: number }> = [];
  values.forEach((v, i) => {
    const x = values.length === 1 ? width / 2 : i * stepX;
    if (v === null || !Number.isFinite(v)) {
      if (current.length > 0) {
        segments.push(current);
        current = [];
      }
      return;
    }
    const y = padY + (1 - (v - minV) / span) * usableH;
    current.push({ x, y });
  });
  if (current.length > 0) segments.push(current);

  // Pick the longest segment for the polyline (sparkline; we don't need
  // discontinuous polyline rendering, just a representative trend).
  const longest = segments.reduce(
    (a, b) => (b.length > a.length ? b : a),
    [] as Array<{ x: number; y: number }>,
  );
  if (longest.length === 0) {
    return { linePoints: null, areaPath: "", lastPoint: null };
  }
  const linePoints = longest.map((p) => `${p.x},${p.y}`).join(" ");

  // Area = polyline + drop down to baseline at first/last.
  const baseline = padY + usableH;
  const first = longest[0];
  const last = longest.at(-1)!;
  const areaPath = [
    `M ${first.x} ${baseline}`,
    `L ${first.x} ${first.y}`,
    ...longest.slice(1).map((p) => `L ${p.x} ${p.y}`),
    `L ${last.x} ${baseline}`,
    "Z",
  ].join(" ");

  return { linePoints, areaPath, lastPoint: last };
}
