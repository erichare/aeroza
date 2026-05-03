"use client";

import { useMemo } from "react";

import type { ReliabilityRow } from "@/lib/api";

const SIZE = 220;
const PAD = 28; // axis labels + ticks

/**
 * Reliability diagram — forecast probability vs observed frequency.
 *
 * The canonical "is this ensemble's uncertainty honest?" picture:
 * group cells by the probability the forecaster assigned to the
 * threshold-crossing event, plot the observed frequency in each
 * bucket, compare against the diagonal. A perfectly-calibrated
 * forecaster lies on `y = x`; under-confident forecasts curve above
 * the diagonal, over-confident below.
 *
 * One panel per (algorithm × horizon) row. Empty bins (count=0) are
 * skipped rather than plotted at zero. Bin sizes are encoded in dot
 * radius so a viewer can tell which buckets are well-supported
 * versus marginally-supported.
 */
export function ReliabilityDiagram({
  row,
}: {
  row: ReliabilityRow;
}) {
  const totalCount = useMemo(
    () => row.bins.reduce((sum, b) => sum + b.count, 0),
    [row],
  );
  // Skip the panel entirely when no cells contributed — UX is
  // cleaner than rendering an empty axis with the diagonal line.
  if (totalCount === 0) return null;

  // Project bin centers to chart coords. The chart frame is
  // PAD..(SIZE - PAD) on both axes, with x = forecast probability
  // and y = observed frequency. Pre-compute once so the SVG is
  // declarative.
  const project = (x: number, y: number) => ({
    cx: PAD + x * (SIZE - 2 * PAD),
    cy: SIZE - PAD - y * (SIZE - 2 * PAD),
  });
  const dots = row.bins
    .filter((b) => b.count > 0 && b.observedFrequency !== null)
    .map((b) => {
      const { cx, cy } = project(b.meanProb, b.observedFrequency ?? 0);
      // Dot area scales with the bin's share of total cells —
      // sqrt so it stays readable when one bin dominates. Floor at
      // 2 px so even tiny bins are clickable / hoverable.
      const share = b.count / totalCount;
      const r = Math.max(2, 2 + Math.sqrt(share) * 6);
      return {
        cx,
        cy,
        r,
        title: `prob ${b.meanProb.toFixed(2)} · observed ${(b.observedFrequency ?? 0).toFixed(2)} · n=${b.count}`,
      };
    });
  const polylinePoints = dots.map((d) => `${d.cx},${d.cy}`).join(" ");

  // Gridlines at 0.25, 0.5, 0.75 — readable orientation without the
  // chart turning into graph paper. The diagonal is the calibration
  // reference; tinted slightly warmer so it visually anchors.
  const ticks = [0.25, 0.5, 0.75];
  const diagonalStart = project(0, 0);
  const diagonalEnd = project(1, 1);

  return (
    <figure className="flex flex-col items-start gap-1.5">
      <svg
        viewBox={`0 0 ${SIZE} ${SIZE}`}
        className="h-[180px] w-[180px] sm:h-[220px] sm:w-[220px]"
        role="img"
        aria-label={`Reliability diagram for ${row.algorithm} at ${row.forecastHorizonMinutes} min`}
      >
        <rect
          x={PAD}
          y={PAD}
          width={SIZE - 2 * PAD}
          height={SIZE - 2 * PAD}
          fill="transparent"
          stroke="currentColor"
          strokeOpacity={0.15}
          strokeWidth={1}
        />
        {ticks.map((t) => {
          const { cx } = project(t, 0);
          const { cy } = project(0, t);
          return (
            <g key={t} stroke="currentColor" strokeOpacity={0.07}>
              <line x1={cx} y1={PAD} x2={cx} y2={SIZE - PAD} />
              <line x1={PAD} y1={cy} x2={SIZE - PAD} y2={cy} />
            </g>
          );
        })}
        <line
          x1={diagonalStart.cx}
          y1={diagonalStart.cy}
          x2={diagonalEnd.cx}
          y2={diagonalEnd.cy}
          stroke="currentColor"
          strokeOpacity={0.35}
          strokeDasharray="3 3"
          strokeWidth={1}
        />
        {dots.length > 1 ? (
          <polyline
            points={polylinePoints}
            fill="none"
            stroke="var(--color-accent, #c4892f)"
            strokeWidth={1.5}
            strokeOpacity={0.7}
          />
        ) : null}
        {dots.map((d, i) => (
          <circle
            key={i}
            cx={d.cx}
            cy={d.cy}
            r={d.r}
            fill="var(--color-accent, #c4892f)"
            stroke="var(--color-bg, #0f1620)"
            strokeWidth={1}
          >
            <title>{d.title}</title>
          </circle>
        ))}
        {/* Axis labels — small, in monospace to match the rest of
            the calibration chrome. */}
        <text
          x={SIZE / 2}
          y={SIZE - 6}
          textAnchor="middle"
          fontFamily="ui-monospace, monospace"
          fontSize="9"
          fill="currentColor"
          fillOpacity={0.6}
        >
          forecast probability
        </text>
        <text
          x={6}
          y={SIZE / 2}
          textAnchor="middle"
          fontFamily="ui-monospace, monospace"
          fontSize="9"
          fill="currentColor"
          fillOpacity={0.6}
          transform={`rotate(-90 6 ${SIZE / 2})`}
        >
          observed frequency
        </text>
      </svg>
      <figcaption className="font-mono text-[10px] uppercase tracking-wider text-muted">
        Reliability · {row.algorithm} · {row.forecastHorizonMinutes}m
      </figcaption>
    </figure>
  );
}
