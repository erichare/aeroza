"use client";

import { useEffect, useMemo, useState } from "react";

import {
  type CalibrationItem,
  type CalibrationResponse,
  type CalibrationSeriesItem,
  type CalibrationSeriesPoint,
  type CalibrationSeriesResponse,
  type ReliabilityRow,
  fetchCalibration,
  fetchCalibrationSeries,
} from "@/lib/api";

import { Panel } from "./Panel";
import { ReliabilityDiagram } from "./ReliabilityDiagram";
import { Sparkline } from "./Sparkline";
import { StatusDot } from "./StatusDot";

const WINDOW_OPTIONS: ReadonlyArray<{ label: string; hours: number }> = [
  { label: "1h", hours: 1 },
  { label: "6h", hours: 6 },
  { label: "24h", hours: 24 },
  { label: "7d", hours: 168 },
  { label: "30d", hours: 720 },
];

type MetricKey = "mae" | "pod" | "far" | "csi" | "brier" | "crps";

const METRIC_OPTIONS: ReadonlyArray<{ key: MetricKey; label: string; help: string }> = [
  { key: "mae", label: "MAE", help: "Mean absolute error — lower is better" },
  { key: "pod", label: "POD", help: "Probability of detection (hits / observed events) — higher is better" },
  { key: "far", label: "FAR", help: "False-alarm ratio (false alarms / forecast events) — lower is better" },
  { key: "csi", label: "CSI", help: "Critical success index (hits / (hits + misses + false alarms)) — higher is better" },
  {
    key: "brier",
    label: "Brier",
    help:
      "Brier score for the ensemble's event-probability forecast at the dBZ threshold. " +
      "Range [0,1]; lower is better. Only populated for ensemble forecasters.",
  },
  {
    key: "crps",
    label: "CRPS",
    help:
      "Continuous Ranked Probability Score (fair ensemble estimator). Same units as the variable (dBZ). " +
      "Lower is better; collapses to MAE for a single-member forecast.",
  },
];

// The metrics that require an ensemble forecaster to be meaningful.
// Cells from deterministic algorithms render an em-dash with a tooltip
// explaining "ensembles only" instead of a misleading 0.
const PROBABILISTIC_METRICS: ReadonlySet<MetricKey> = new Set(["brier", "crps"]);

const REFRESH_INTERVAL_MS = 60_000;

/**
 * Calibration dashboard — the public face of the §3.3 moat.
 *
 * Reads `/v1/calibration` and lays out the chosen metric per
 * `(algorithm, forecastHorizonMinutes)`. The shape is a small matrix:
 * algorithms across rows, horizons across columns. The metric switcher
 * toggles between MAE (continuous error) and POD/FAR/CSI (categorical
 * skill at the configured threshold, default 35 dBZ). Bars and sparklines
 * scale to the worst row in the window in the metric-specific direction —
 * lower-is-better for MAE/FAR, higher-is-better for POD/CSI.
 */
export function CalibrationPanel() {
  const [windowHours, setWindowHours] = useState<number>(24);
  const [metric, setMetric] = useState<MetricKey>("mae");
  const [data, setData] = useState<CalibrationResponse | null>(null);
  const [series, setSeries] = useState<CalibrationSeriesResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    setIsLoading(true);
    const load = async () => {
      try {
        const bucketSeconds = pickBucketSeconds(windowHours);
        // Aggregate + per-series fetched in parallel — they hit different
        // SQL paths but share the same window, so doing them together
        // keeps the dashboard one-shot.
        const [aggregate, ts] = await Promise.all([
          fetchCalibration({ windowHours }),
          fetchCalibrationSeries({ windowHours, bucketSeconds }),
        ]);
        if (cancelled) return;
        setData(aggregate);
        setSeries(ts);
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

  const matrix = useMemo(
    () => buildMatrix(data?.items ?? [], series?.items ?? [], metric),
    [data, series, metric],
  );

  return (
    <Panel
      title={`Calibration · ${metricTitle(metric)}`}
      subtitle={metricSubtitle(metric)}
      actions={
        <div className="flex items-center gap-3">
          <MetricSwitcher value={metric} onChange={setMetric} />
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
          <CalibrationMatrix matrix={matrix} metric={metric} />
        ) : null}

        {!error && metric === "brier" && (data?.reliability ?? []).length > 0 ? (
          <ReliabilityDiagrams rows={data!.reliability} />
        ) : null}

        <Footnote metric={metric} />
      </div>
    </Panel>
  );
}

function ReliabilityDiagrams({ rows }: { rows: ReadonlyArray<ReliabilityRow> }) {
  return (
    <section className="mt-5 border-t border-border/40 pt-4">
      <h3 className="mb-2 font-mono text-[10px] uppercase tracking-wider text-muted">
        Reliability — observed frequency vs forecast probability
      </h3>
      <p className="mb-3 max-w-2xl text-[11px] leading-relaxed text-muted">
        A perfectly-calibrated forecaster lies on the diagonal: when it
        says 30%, the event happens 30% of the time. Curves above the
        diagonal mean the forecaster is under-confident (events happen
        more often than predicted); below means over-confident. Dot
        size scales with the bin's share of total cells, so bins with
        thin support are visibly thinner. Empty bins are skipped.
      </p>
      <div className="flex flex-wrap gap-4">
        {rows.map((row) => (
          <ReliabilityDiagram
            key={`${row.algorithm}:${row.forecastHorizonMinutes}`}
            row={row}
          />
        ))}
      </div>
    </section>
  );
}

interface CalibrationMatrixData {
  algorithms: string[];
  horizons: number[];
  cells: Map<string, CalibrationItem>;
  serieses: Map<string, CalibrationSeriesItem>;
  /** Worst observed value for the active metric — anchors bar and sparkline scales. */
  worstValue: number;
  /** Persistence baseline value at each horizon for the active metric — anchors
   * the champion-vs-baseline delta ribbon. Null when no persistence row exists
   * at that horizon, or when persistence didn't have the active metric. */
  baselineByHorizon: Map<number, number>;
}

const BASELINE_ALGORITHM = "persistence";

function buildMatrix(
  items: ReadonlyArray<CalibrationItem>,
  seriesItems: ReadonlyArray<CalibrationSeriesItem>,
  metric: MetricKey,
): CalibrationMatrixData {
  const algorithms = uniqueSorted(items.map((i) => i.algorithm));
  const horizons = uniqueSorted(items.map((i) => i.forecastHorizonMinutes), (a, b) => a - b);
  const cells = new Map<string, CalibrationItem>();
  let worstValue = 0;
  for (const item of items) {
    cells.set(`${item.algorithm}:${item.forecastHorizonMinutes}`, item);
    const v = pickItemMetric(item, metric);
    if (v !== null && v > worstValue) worstValue = v;
  }
  const serieses = new Map<string, CalibrationSeriesItem>();
  for (const s of seriesItems) {
    serieses.set(`${s.algorithm}:${s.forecastHorizonMinutes}`, s);
  }
  const baselineByHorizon = new Map<number, number>();
  for (const horizon of horizons) {
    const baselineCell = cells.get(`${BASELINE_ALGORITHM}:${horizon}`);
    if (!baselineCell) continue;
    const v = pickItemMetric(baselineCell, metric);
    if (v !== null) baselineByHorizon.set(horizon, v);
  }
  return { algorithms, horizons, cells, serieses, worstValue, baselineByHorizon };
}

// For these metrics, lower values are better (forecast accuracy). For
// the rest (POD, CSI), higher values are better. The champion ribbon
// uses this to flip the sign on its "↑ X% vs persistence" calculation.
const LOWER_IS_BETTER: ReadonlySet<MetricKey> = new Set(["mae", "far", "brier", "crps"]);

interface BaselineDelta {
  /** Always positive: how much better/worse the algorithm is, in metric units. */
  magnitude: number;
  /** Percent improvement vs baseline (0–100). Capped at 999 for display. */
  percent: number;
  /** True when the algorithm beats the baseline on the active metric. */
  better: boolean;
}

function computeBaselineDelta(
  metric: MetricKey,
  cellValue: number,
  baselineValue: number,
): BaselineDelta | null {
  if (!Number.isFinite(cellValue) || !Number.isFinite(baselineValue)) return null;
  // Avoid divide-by-zero on a perfect baseline; we don't render the
  // ribbon in that degenerate case (the row already says everything).
  if (baselineValue === 0) return null;
  const lowerIsBetter = LOWER_IS_BETTER.has(metric);
  const delta = lowerIsBetter ? baselineValue - cellValue : cellValue - baselineValue;
  const better = delta > 0;
  const magnitude = Math.abs(delta);
  const percent = Math.min(999, (magnitude / Math.abs(baselineValue)) * 100);
  return { magnitude, percent, better };
}

/**
 * Pick a sparkline bucket width from the requested window.
 *
 * Aim for ~24-30 points so the sparkline reads as a trend, not a
 * histogram. Hourly for 24h or less, then linear scale up to a daily
 * bucket for the 30-day view.
 */
function pickBucketSeconds(windowHours: number): number {
  if (windowHours <= 6) return 900;       // 15 min → 24 points
  if (windowHours <= 24) return 3600;     // 1 h → 24 points
  if (windowHours <= 168) return 21_600;  // 6 h → 28 points
  return 86_400;                          // 1 day → up to 30
}

function CalibrationMatrix({
  matrix,
  metric,
}: {
  matrix: CalibrationMatrixData;
  metric: MetricKey;
}) {
  const { algorithms, horizons, cells, serieses, worstValue, baselineByHorizon } = matrix;
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
                {algo === BASELINE_ALGORITHM ? (
                  <span
                    className="ml-1.5 align-middle font-mono text-[9px] uppercase tracking-wider text-muted/70"
                    title="Persistence is the baseline every other algorithm is judged against"
                  >
                    baseline
                  </span>
                ) : null}
              </td>
              {horizons.map((h, i) => {
                const cell = cells.get(`${algo}:${h}`);
                const series = serieses.get(`${algo}:${h}`);
                const isLast = i === horizons.length - 1;
                const baseline = baselineByHorizon.get(h);
                // Suppress the delta on the baseline row — it would
                // always be 0 and clutter the eye away from the cells
                // it's anchoring.
                const showDelta = algo !== BASELINE_ALGORITHM && baseline !== undefined;
                return (
                  <td
                    key={h}
                    className={[
                      "bg-surface/30 px-3 py-2 align-top",
                      isLast ? "rounded-r-md" : "",
                    ].join(" ")}
                  >
                    {cell ? (
                      <CalibrationCell
                        cell={cell}
                        series={series}
                        worstValue={worstValue}
                        metric={metric}
                        baselineValue={showDelta ? baseline : undefined}
                      />
                    ) : (
                      <Dash />
                    )}
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
  series,
  worstValue,
  metric,
  baselineValue,
}: {
  cell: CalibrationItem;
  series: CalibrationSeriesItem | undefined;
  worstValue: number;
  metric: MetricKey;
  /** Persistence's value at the same horizon, or undefined when this row is the baseline. */
  baselineValue: number | undefined;
}) {
  const value = pickItemMetric(cell, metric);
  const delta =
    value !== null && baselineValue !== undefined
      ? computeBaselineDelta(metric, value, baselineValue)
      : null;

  // For continuous (MAE), bar grows toward worst. For ratios (POD/FAR/CSI),
  // bar fills proportional to the value itself in [0, 1] — so a perfect
  // POD shows full bar, a poor FAR shows nearly empty bar.
  const barPct = computeBarPct(metric, value, worstValue);

  // Per-cell sparkline: pull the chosen metric per bucket, scaled
  // against the same worst value as the matrix so the Y-axis is shared
  // across rows. Buckets with no samples (or no categorical data) render
  // as gaps (null), not zero.
  const sparkValues = series?.points.map((p) =>
    p.sampleCount > 0 ? pickPointMetric(p, metric) : null,
  );

  const sparkYMax = metric === "brier" || isRatioMetric(metric)
    ? 1
    : worstValue > 0
      ? worstValue
      : undefined;
  const isProbabilistic = PROBABILISTIC_METRICS.has(metric);

  // Probabilistic metrics use Brier-sample-count (ensemble-only cells)
  // as the denominator; deterministic metrics use the full sample count.
  const sampleCountForMetric = isProbabilistic ? cell.brierSampleCount : cell.sampleCount;
  // Hide the bar when there's no data for this metric (e.g. Brier on a
  // deterministic-only cell). Empty cells render the dash inline below.
  const hasValue = value !== null;

  return (
    <div className="flex flex-col gap-1.5">
      <div className="flex items-baseline justify-between gap-3">
        <span className="flex items-baseline gap-2">
          <span
            className="font-mono text-sm tabular-nums text-text"
            title={
              !hasValue && isProbabilistic
                ? "Brier/CRPS only score ensemble forecasters. Run aeroza-nowcast-mrms --algorithm lagged-ensemble alongside this row."
                : undefined
            }
          >
            {hasValue ? formatMetric(metric, value) : "—"}
          </span>
          {delta ? <BaselineDeltaPill delta={delta} /> : null}
        </span>
        <span className="font-mono text-[10px] tabular-nums text-muted">
          n={formatCount(sampleCountForMetric)}
        </span>
      </div>
      {hasValue ? (
        <div className="relative h-1.5 overflow-hidden rounded-full bg-bg/60">
          <span
            className="absolute inset-y-0 left-0 bg-accent/60"
            style={{ width: `${barPct}%` }}
            aria-hidden
          />
        </div>
      ) : (
        <div className="h-1.5" aria-hidden />
      )}
      {sparkValues && sparkValues.length > 1 ? (
        <Sparkline
          values={sparkValues}
          yMin={0}
          yMax={sparkYMax}
          width={120}
          height={20}
          label={`${metric.toUpperCase()} trend for ${cell.algorithm} at ${cell.forecastHorizonMinutes} min`}
        />
      ) : null}
      <div className="flex justify-between font-mono text-[10px] tabular-nums text-muted">
        {metric === "mae" ? (
          <>
            <span title="Bias mean">
              bias {cell.biasMean >= 0 ? "+" : ""}
              {cell.biasMean.toFixed(2)}
            </span>
            <span title="RMSE mean">rmse {cell.rmseMean.toFixed(2)}</span>
          </>
        ) : isProbabilistic ? (
          <>
            <span title="Ensemble size for this row (members per forecast)">
              {cell.ensembleSize === null ? "M —" : `M=${cell.ensembleSize}`}
            </span>
            <span title="MAE mean (member-mean forecast)">
              mae {cell.maeMean.toFixed(2)}
            </span>
          </>
        ) : (
          <>
            <span title="Threshold for POD/FAR/CSI (dBZ)">
              {cell.thresholdDbz === null ? "thr —" : `thr ${cell.thresholdDbz.toFixed(0)}`}
            </span>
            <span title="MAE mean">mae {cell.maeMean.toFixed(2)}</span>
          </>
        )}
      </div>
    </div>
  );
}

function BaselineDeltaPill({ delta }: { delta: BaselineDelta }) {
  // Tiny ribbon: arrow + percent. Green when this algorithm beats
  // persistence on the active metric; muted-warm when it trails. We
  // don't go red — losing to the baseline isn't a failure mode worth
  // shouting about, just a worth-knowing.
  const tone = delta.better
    ? "border-success/30 bg-success/10 text-success"
    : "border-warning/30 bg-warning/10 text-warning";
  const arrow = delta.better ? "↑" : "↓";
  // Percent gets <1 = "<1%", whole-number resolution otherwise. Tighter
  // than three decimals; the ribbon is supposed to be a glance.
  const percent =
    delta.percent < 1
      ? "<1%"
      : delta.percent >= 100
        ? `${Math.round(delta.percent)}%`
        : `${delta.percent.toFixed(0)}%`;
  return (
    <span
      className={[
        "rounded-sm border px-1.5 py-px font-mono text-[9px] uppercase tracking-wider",
        tone,
      ].join(" ")}
      title={`${
        delta.better ? "Beats" : "Trails"
      } persistence by ${delta.magnitude.toFixed(3)} (${delta.percent.toFixed(1)}%) on this metric`}
    >
      {arrow} {percent}
    </span>
  );
}

function MetricSwitcher({
  value,
  onChange,
}: {
  value: MetricKey;
  onChange: (next: MetricKey) => void;
}) {
  return (
    <div
      className="flex items-center gap-1 rounded-md border border-border/70 bg-bg/40 p-0.5 text-[11px]"
      role="tablist"
      aria-label="Calibration metric"
    >
      {METRIC_OPTIONS.map((opt) => {
        const active = opt.key === value;
        return (
          <button
            key={opt.key}
            type="button"
            role="tab"
            aria-selected={active}
            title={opt.help}
            onClick={() => onChange(opt.key)}
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

function Footnote({ metric }: { metric: MetricKey }) {
  if (metric === "mae") {
    return (
      <p className="mt-5 text-[11px] text-muted">
        Sample-weighted: a verification with N=1M cells contributes N times to
        the averages. The bar inside each MAE cell scales relative to the worst
        algorithm in the window — shorter bars mean a more accurate forecaster.
        The sparkline shows MAE per time bucket so you can watch a real
        algorithm's accuracy trend down over time.
      </p>
    );
  }
  if (metric === "brier") {
    return (
      <p className="mt-5 text-[11px] text-muted">
        Brier score = mean squared error of the ensemble's event-probability
        forecast against the {`{0,1}`} observation at the verifier's threshold
        (default 35 dBZ). Range [0,1]; lower is better. Only ensemble
        forecasters (e.g.{" "}
        <code className="font-mono">--algorithm lagged-ensemble</code>) populate
        Brier — deterministic rows render <span className="font-mono">—</span>.
        The <span className="font-mono">M=</span> badge below each cell
        is the ensemble size; <span className="font-mono">n=</span> is the
        number of cells that contributed.
      </p>
    );
  }
  if (metric === "crps") {
    return (
      <p className="mt-5 text-[11px] text-muted">
        CRPS = Continuous Ranked Probability Score (fair ensemble estimator).
        Same units as the variable (dBZ); lower is better. Generalises MAE to
        the full ensemble distribution and collapses back to MAE for a
        single-member forecast — so you can read "how much spread of
        probability around the truth?" off this number. Only ensemble
        forecasters populate it.
      </p>
    );
  }
  return (
    <p className="mt-5 text-[11px] text-muted">
      POD / FAR / CSI are categorical skill scores at the verifier's
      threshold (default 35 dBZ — operational meteorology's "convective
      cell" cutoff). Scores aggregate from the summed contingency table —
      averaging ratios across rows would be wrong. Cells show <span className="font-mono">—</span> when
      no contributing row had categorical metrics, when rows scored at
      mismatched thresholds (POD/FAR/CSI still sum coherently, but the
      threshold field is left blank), or when the denominator is zero.
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

function isRatioMetric(metric: MetricKey): boolean {
  return metric !== "mae";
}

function pickItemMetric(item: CalibrationItem, metric: MetricKey): number | null {
  switch (metric) {
    case "mae":
      return item.maeMean;
    case "pod":
      return item.pod;
    case "far":
      return item.far;
    case "csi":
      return item.csi;
    case "brier":
      return item.brierMean;
    case "crps":
      return item.crpsMean;
  }
}

function pickPointMetric(point: CalibrationSeriesPoint, metric: MetricKey): number | null {
  switch (metric) {
    case "mae":
      return point.maeMean;
    case "pod":
      return point.pod;
    case "far":
      return point.far;
    case "csi":
      return point.csi;
    case "brier":
      return point.brierMean;
    case "crps":
      return point.crpsMean;
  }
}

function computeBarPct(metric: MetricKey, value: number | null, worstValue: number): number {
  if (value === null) return 0;
  if (metric === "brier") {
    // Brier ∈ [0, 1]; the bar fills proportionally to the score itself
    // (lower-is-better). Same convention the ratio metrics use, just
    // with the inverse interpretation — a tall bar means a poorly
    // calibrated probability.
    return Math.min(100, Math.max(0, value * 100));
  }
  if (isRatioMetric(metric)) {
    return Math.min(100, Math.max(0, value * 100));
  }
  if (worstValue <= 0) return 0;
  return Math.min(100, Math.max(0, (value / worstValue) * 100));
}

function formatMetric(metric: MetricKey, value: number): string {
  if (metric === "brier") return value.toFixed(3);
  if (isRatioMetric(metric)) return value.toFixed(3);
  return value.toFixed(2);
}

function metricTitle(metric: MetricKey): string {
  switch (metric) {
    case "mae":
      return "MAE / bias / RMSE";
    case "pod":
      return "POD (probability of detection)";
    case "far":
      return "FAR (false-alarm ratio)";
    case "csi":
      return "CSI (critical success index)";
    case "brier":
      return "Brier score (probabilistic)";
    case "crps":
      return "CRPS (continuous ranked probability score)";
  }
}

function metricSubtitle(metric: MetricKey): string {
  if (metric === "mae") {
    return "GET /v1/calibration · sample-weighted, grouped by algorithm × horizon";
  }
  if (PROBABILISTIC_METRICS.has(metric)) {
    return "GET /v1/calibration · ensemble rows only, grouped by algorithm × horizon";
  }
  return "GET /v1/calibration · summed contingency table @ 35 dBZ, grouped by algorithm × horizon";
}
