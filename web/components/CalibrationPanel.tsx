"use client";

import { useEffect, useMemo, useState } from "react";

import {
  type CalibrationItem,
  type CalibrationResponse,
  fetchCalibration,
} from "@/lib/api";

import { Panel } from "./Panel";
import { StatusDot } from "./StatusDot";

const WINDOW_OPTIONS: ReadonlyArray<{ label: string; hours: number }> = [
  { label: "1h", hours: 1 },
  { label: "6h", hours: 6 },
  { label: "24h", hours: 24 },
  { label: "7d", hours: 168 },
  { label: "30d", hours: 720 },
];

const REFRESH_INTERVAL_MS = 60_000;

/**
 * Calibration dashboard — the public face of the §3.3 moat.
 *
 * Reads `/v1/calibration` and lays out the sample-weighted MAE / bias /
 * RMSE per `(algorithm, forecastHorizonMinutes)`. The shape is a small
 * matrix: algorithms across rows, horizons across columns. A sparkline
 * mini-bar per cell shows MAE relative to the worst row in the window —
 * good enough to spot a real algorithm pulling ahead of persistence
 * without dragging in a charting lib.
 */
export function CalibrationPanel() {
  const [windowHours, setWindowHours] = useState<number>(24);
  const [data, setData] = useState<CalibrationResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    setIsLoading(true);
    const load = async () => {
      try {
        const next = await fetchCalibration({ windowHours });
        if (cancelled) return;
        setData(next);
        setError(null);
      } catch (err) {
        if (cancelled) return;
        setError(err instanceof Error ? err.message : "Failed to load calibration");
      } finally {
        if (!cancelled) setIsLoading(false);
      }
    };
    void load();
    const interval = setInterval(load, REFRESH_INTERVAL_MS);
    return () => {
      cancelled = true;
      clearInterval(interval);
    };
  }, [windowHours]);

  const matrix = useMemo(() => buildMatrix(data?.items ?? []), [data]);

  return (
    <Panel
      title="Calibration · MAE / bias / RMSE"
      subtitle="GET /v1/calibration · sample-weighted, grouped by algorithm × horizon"
      actions={
        <div className="flex items-center gap-3">
          <WindowSwitcher value={windowHours} onChange={setWindowHours} />
          <FreshnessBadge
            generatedAt={data?.generatedAt ?? null}
            isLoading={isLoading}
            error={error}
          />
        </div>
      }
    >
      <div className="px-5 py-5">
        {error ? (
          <div className="rounded-md border border-warning/40 bg-warning/10 px-3 py-2 text-xs text-warning">
            {error}
          </div>
        ) : null}

        {!error && data && data.items.length === 0 ? (
          <EmptyState />
        ) : null}

        {!error && matrix.algorithms.length > 0 ? (
          <CalibrationMatrix matrix={matrix} />
        ) : null}

        <Footnote />
      </div>
    </Panel>
  );
}

interface CalibrationMatrixData {
  algorithms: string[];
  horizons: number[];
  cells: Map<string, CalibrationItem>;
  worstMae: number;
}

function buildMatrix(items: ReadonlyArray<CalibrationItem>): CalibrationMatrixData {
  const algorithms = uniqueSorted(items.map((i) => i.algorithm));
  const horizons = uniqueSorted(items.map((i) => i.forecastHorizonMinutes), (a, b) => a - b);
  const cells = new Map<string, CalibrationItem>();
  let worstMae = 0;
  for (const item of items) {
    cells.set(`${item.algorithm}:${item.forecastHorizonMinutes}`, item);
    if (item.maeMean > worstMae) worstMae = item.maeMean;
  }
  return { algorithms, horizons, cells, worstMae };
}

function CalibrationMatrix({ matrix }: { matrix: CalibrationMatrixData }) {
  const { algorithms, horizons, cells, worstMae } = matrix;
  return (
    <div className="overflow-x-auto">
      <table className="w-full min-w-[40rem] border-separate border-spacing-y-1 text-xs">
        <thead>
          <tr className="text-[10px] uppercase tracking-wide text-muted">
            <th className="pb-2 pr-4 text-left font-medium">Algorithm</th>
            {horizons.map((h) => (
              <th key={h} className="pb-2 px-3 text-left font-medium">
                {h} min
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {algorithms.map((algo) => (
            <tr key={algo} className="rounded-md">
              <td className="rounded-l-md bg-surface/30 py-2 pl-3 pr-4 font-mono text-text">
                {algo}
              </td>
              {horizons.map((h, i) => {
                const cell = cells.get(`${algo}:${h}`);
                const isLast = i === horizons.length - 1;
                return (
                  <td
                    key={h}
                    className={[
                      "bg-surface/30 px-3 py-2 align-top",
                      isLast ? "rounded-r-md" : "",
                    ].join(" ")}
                  >
                    {cell ? <CalibrationCell cell={cell} worstMae={worstMae} /> : <Dash />}
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function CalibrationCell({
  cell,
  worstMae,
}: {
  cell: CalibrationItem;
  worstMae: number;
}) {
  // Bar fills proportional to (mae / worstMae). The worst cell maxes out;
  // a more accurate algorithm shows a shorter bar — visual moat in one
  // glance.
  const ratio = worstMae > 0 ? cell.maeMean / worstMae : 0;
  const barPct = Math.min(100, Math.max(0, ratio * 100));
  return (
    <div className="flex flex-col gap-1.5">
      <div className="flex items-baseline justify-between gap-3">
        <span className="font-mono text-sm tabular-nums text-text">
          {cell.maeMean.toFixed(2)}
        </span>
        <span className="font-mono text-[10px] tabular-nums text-muted">
          n={formatCount(cell.sampleCount)}
        </span>
      </div>
      <div className="relative h-1.5 overflow-hidden rounded-full bg-bg/60">
        <span
          className="absolute inset-y-0 left-0 bg-accent/60"
          style={{ width: `${barPct}%` }}
          aria-hidden
        />
      </div>
      <div className="flex justify-between font-mono text-[10px] tabular-nums text-muted">
        <span title="Bias mean">
          bias {cell.biasMean >= 0 ? "+" : ""}
          {cell.biasMean.toFixed(2)}
        </span>
        <span title="RMSE mean">rmse {cell.rmseMean.toFixed(2)}</span>
      </div>
    </div>
  );
}

function WindowSwitcher({
  value,
  onChange,
}: {
  value: number;
  onChange: (next: number) => void;
}) {
  return (
    <div className="flex items-center gap-1 rounded-md border border-border/70 bg-bg/40 p-0.5 text-[11px]">
      {WINDOW_OPTIONS.map((opt) => {
        const active = opt.hours === value;
        return (
          <button
            key={opt.hours}
            type="button"
            onClick={() => onChange(opt.hours)}
            className={[
              "rounded-sm px-2 py-0.5 font-mono uppercase tracking-wide",
              active
                ? "bg-accent/15 text-accent"
                : "text-muted hover:text-text",
            ].join(" ")}
          >
            {opt.label}
          </button>
        );
      })}
    </div>
  );
}

function FreshnessBadge({
  generatedAt,
  isLoading,
  error,
}: {
  generatedAt: string | null;
  isLoading: boolean;
  error: string | null;
}) {
  if (error) return <StatusDot tone="warning" label="Stale" />;
  if (isLoading && !generatedAt) {
    return <StatusDot tone="warning" label="Loading…" />;
  }
  if (!generatedAt) return <StatusDot tone="muted" label="—" />;
  return (
    <StatusDot
      tone="success"
      label={`as of ${new Date(generatedAt).toLocaleTimeString()}`}
      pulse
    />
  );
}

function EmptyState() {
  return (
    <div className="rounded-xl border border-dashed border-border/60 bg-bg/30 px-5 py-8 text-center">
      <p className="text-sm text-muted">
        No verifications scored in this window yet.
      </p>
      <p className="mt-1 text-xs text-muted/70">
        Run the verifier (<code className="font-mono">aeroza-verify-nowcasts</code>) to populate calibration metrics.
      </p>
    </div>
  );
}

function Footnote() {
  return (
    <p className="mt-5 text-[11px] text-muted">
      Sample-weighted: a verification with N=1M cells contributes N times to the
      averages. The bar inside each MAE cell scales relative to the worst
      algorithm in the window — shorter bars mean a more accurate forecaster.
    </p>
  );
}

function Dash() {
  return <span className="font-mono text-muted/40">—</span>;
}

function uniqueSorted<T extends string | number>(
  values: ReadonlyArray<T>,
  compare?: (a: T, b: T) => number,
): T[] {
  const out = Array.from(new Set(values));
  if (compare) out.sort(compare);
  else out.sort();
  return out;
}

function formatCount(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}k`;
  return String(n);
}
