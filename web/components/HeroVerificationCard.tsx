"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";

import { type CalibrationItem, fetchCalibration } from "@/lib/api";

const REFRESH_INTERVAL_MS = 60_000;

// 24-hour window matches the default the /calibration dashboard reaches
// for. Aligning here keeps the headline number consistent between this
// hero card and the deep-link target — clicking through never surprises.
const WINDOW_HOURS = 24;

/**
 * "Were we right?" — the verification half of the landing-page hero.
 *
 * The pitch most weather APIs can't make: every forecast we issue is
 * scored against the matching observation as soon as the truth lands,
 * and the result is public. This card surfaces that mechanism in
 * three numbers a non-technical viewer can absorb in two seconds:
 *
 *   1. Sample-weighted MAE for the persistence baseline at 30 min.
 *   2. How many forecasts we scored in the last 24h.
 *   3. How many cells fed those means (proves N is large).
 *
 * Plus a mini per-horizon row so a forecaster can sanity-check at
 * 10 / 30 / 60 min. Everything links into /calibration for the full
 * algorithm × horizon matrix + sparklines.
 */
export function HeroVerificationCard() {
  const [items, setItems] = useState<ReadonlyArray<CalibrationItem> | null>(
    null,
  );
  const [error, setError] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      try {
        const data = await fetchCalibration({ windowHours: WINDOW_HOURS });
        if (cancelled) return;
        setItems(data.items);
        setError(null);
      } catch (err) {
        if (cancelled) return;
        setError(
          err instanceof Error ? err.message : "Failed to load calibration",
        );
      } finally {
        if (!cancelled) setIsLoading(false);
      }
    };
    void load();
    const id = setInterval(load, REFRESH_INTERVAL_MS);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, []);

  // Pick the row most viewers will read first. Probabilistic-skill
  // (Brier) on an ensemble row at 30 min is the strongest pitch when
  // available; otherwise we fall back to MAE on the persistence
  // baseline at 30 min. The selection is dynamic so the hero
  // auto-upgrades from "deterministic MAE" to "publicly verified
  // probabilistic skill" the moment the lagged-ensemble worker starts
  // running.
  const headline = useMemo(() => pickHeadline(items ?? []), [items]);

  const totalSamples = useMemo(
    () => (items ?? []).reduce((sum, row) => sum + row.sampleCount, 0),
    [items],
  );
  const totalVerifications = useMemo(
    () => (items ?? []).reduce((sum, row) => sum + row.verificationCount, 0),
    [items],
  );

  return (
    <article className="flex h-full flex-col gap-4 rounded-2xl border border-border/70 bg-surface/50 p-5 shadow-[0_1px_0_0_rgba(255,255,255,0.04)_inset] backdrop-blur">
      <header className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <p className="font-mono text-[10px] uppercase tracking-[0.2em] text-accent">
            The moat · §3.3
          </p>
          <h2 className="mt-1 font-display text-xl font-semibold leading-tight text-text">
            Were we right?
          </h2>
          <p className="mt-1.5 text-xs leading-relaxed text-muted">
            Every forecast scored against reality, in public. Nobody else in
            the dev-API weather space publishes this.
          </p>
        </div>
      </header>

      {error ? (
        <ErrorState message={error} />
      ) : isLoading && items === null ? (
        <LoadingState />
      ) : items !== null && items.length === 0 ? (
        <EmptyState />
      ) : (
        <>
          <HeadlineMetric
            headline={headline}
            totalSamples={totalSamples}
            totalVerifications={totalVerifications}
          />
          <HorizonMiniGrid items={items ?? []} />
        </>
      )}

      <Link
        href="/calibration"
        className="mt-auto inline-flex items-center gap-1 self-start rounded-md border border-border/70 px-3 py-1.5 text-xs text-muted hover:border-accent/60 hover:text-accent"
      >
        See the full matrix →
      </Link>
    </article>
  );
}

interface Headline {
  row: CalibrationItem;
  /** "brier" when an ensemble row is leading; "mae" when we fell back to deterministic. */
  kind: "brier" | "mae";
  value: number;
}

function HeadlineMetric({
  headline,
  totalSamples,
  totalVerifications,
}: {
  headline: Headline | null;
  totalSamples: number;
  totalVerifications: number;
}) {
  if (!headline) return null;
  const { row, kind, value } = headline;
  const valueLabel = kind === "brier" ? "Brier score" : "dBZ MAE";
  const formatted = kind === "brier" ? value.toFixed(3) : value.toFixed(2);
  // Auto-upgrade microcopy: "probabilistic" leads when an ensemble row
  // is in play, otherwise "deterministic". Keeps the visitor oriented
  // around what they're seeing without a legend.
  const flavour =
    kind === "brier"
      ? `Brier ∈ [0, 1] · ${labelFor(row.algorithm)}${
          row.ensembleSize ? ` (M=${row.ensembleSize})` : ""
        } · ${row.forecastHorizonMinutes}-min horizon · last ${WINDOW_HOURS}h`
      : `${labelFor(row.algorithm)} · ${row.forecastHorizonMinutes}-min horizon · last ${WINDOW_HOURS}h`;
  return (
    <div className="rounded-xl border border-border/60 bg-bg/40 p-4">
      <div className="flex items-baseline gap-2">
        <span className="font-display text-3xl font-semibold tabular-nums text-text">
          {formatted}
        </span>
        <span className="font-mono text-[11px] uppercase tracking-wide text-muted">
          {valueLabel}
        </span>
        {kind === "brier" ? (
          <span
            className="ml-1 rounded-sm border border-accent/40 bg-accent/10 px-1.5 py-px font-mono text-[9px] uppercase tracking-wider text-accent"
            title="Ensemble probabilistic skill score — the proper complement to deterministic MAE"
          >
            ensemble
          </span>
        ) : null}
      </div>
      <p className="mt-1 text-[11px] text-muted">{flavour}</p>
      <div className="mt-3 flex flex-wrap gap-x-4 gap-y-1 font-mono text-[10px] tabular-nums text-muted">
        <span>
          <span className="text-text">{formatCount(totalVerifications)}</span>{" "}
          forecasts scored
        </span>
        <span>
          <span className="text-text">{formatCount(totalSamples)}</span> cells
          contributed
        </span>
      </div>
    </div>
  );
}

function HorizonMiniGrid({ items }: { items: ReadonlyArray<CalibrationItem> }) {
  // One row per (algorithm, horizon) — sorted so persistence comes first
  // and horizons read low → high. Limit to 6 rows so the card stays
  // glanceable on small screens.
  const rows = useMemo(() => sortRows(items).slice(0, 6), [items]);
  if (rows.length === 0) return null;
  return (
    <div className="flex flex-col gap-1 text-[11px]">
      <div className="flex items-center justify-between font-mono text-[9px] uppercase tracking-wider text-muted/80">
        <span>Algo · horizon</span>
        <span>MAE</span>
      </div>
      {rows.map((row) => (
        <div
          key={`${row.algorithm}:${row.forecastHorizonMinutes}`}
          className="flex items-center justify-between rounded-md border border-border/40 bg-bg/30 px-2.5 py-1.5"
        >
          <span className="truncate font-mono text-text">
            {labelFor(row.algorithm)}{" "}
            <span className="text-muted">· {row.forecastHorizonMinutes}m</span>
          </span>
          <span className="font-mono tabular-nums text-text">
            {row.maeMean.toFixed(2)} <span className="text-muted">dBZ</span>
          </span>
        </div>
      ))}
    </div>
  );
}

function LoadingState() {
  // Mirrors the real layout (headline metric + small horizon rows) so the
  // column has the same visual weight before data lands. Skeletons avoid
  // the layout shift that "Loading…" → real-card produces.
  return (
    <>
      <div className="rounded-xl border border-border/60 bg-bg/40 p-4">
        <div className="flex items-baseline gap-2">
          <span className="block h-7 w-20 animate-pulse rounded bg-muted/20" />
          <span className="font-mono text-[11px] uppercase tracking-wide text-muted/60">
            dBZ MAE
          </span>
        </div>
        <span className="mt-2 block h-3 w-48 animate-pulse rounded bg-muted/15" />
      </div>
      <div className="flex flex-col gap-1.5">
        {[10, 30, 60].map((m) => (
          <div
            key={m}
            className="flex h-7 items-center justify-between rounded-md border border-border/40 bg-bg/30 px-2.5"
          >
            <span className="block h-2.5 w-24 animate-pulse rounded bg-muted/15" />
            <span className="block h-2.5 w-12 animate-pulse rounded bg-muted/15" />
          </div>
        ))}
      </div>
    </>
  );
}

function EmptyState() {
  return (
    <div className="rounded-xl border border-dashed border-border/60 bg-bg/30 p-4 text-xs leading-relaxed text-muted">
      <p className="text-text">No verifications scored yet.</p>
      <p className="mt-1.5">
        Run <code className="font-mono">aeroza-verify-nowcasts</code> alongside
        the nowcaster — the moat fills in within a few minutes of forecasts
        landing alongside real observations.
      </p>
    </div>
  );
}

function ErrorState({ message }: { message: string }) {
  // Even when the API is unreachable, the card still has to land the
  // pitch. Render a meaningful preview-shape: example numbers with an
  // explicit "preview" marker so nobody mistakes them for live data, plus
  // a small warning row at the bottom that doesn't shout.
  return (
    <>
      <div className="relative rounded-xl border border-border/60 bg-bg/40 p-4">
        <span className="absolute right-3 top-3 rounded-md border border-warning/40 bg-warning/10 px-1.5 py-0.5 font-mono text-[9px] uppercase tracking-wider text-warning">
          Preview
        </span>
        <div className="flex items-baseline gap-2">
          <span className="font-display text-3xl font-semibold tabular-nums text-text/60">
            2.34
          </span>
          <span className="font-mono text-[11px] uppercase tracking-wide text-muted/70">
            dBZ MAE
          </span>
        </div>
        <p className="mt-1 text-[11px] text-muted/70">
          Persistence · 30-min horizon · last 24h (illustrative)
        </p>
      </div>
      <div
        role="status"
        className="rounded-md border border-warning/30 bg-warning/5 px-3 py-2 text-[11px] text-warning"
      >
        Live numbers unavailable — {message.toLowerCase()}.
      </div>
    </>
  );
}

function pickHeadline(
  items: ReadonlyArray<CalibrationItem>,
): Headline | null {
  if (items.length === 0) return null;

  // Probabilistic skill at 30 min is the strongest pitch when present
  // — the moment an ensemble worker has scored anything, the hero
  // upgrades to "publicly verified probabilistic skill".
  const ensemble30 = items.find(
    (i) =>
      i.brierMean !== null &&
      i.forecastHorizonMinutes === 30 &&
      i.algorithm !== "persistence",
  );
  if (ensemble30 && ensemble30.brierMean !== null) {
    return { row: ensemble30, kind: "brier", value: ensemble30.brierMean };
  }
  // Any ensemble row, lowest horizon first — still better pitch than
  // deterministic on most days.
  const sortedEnsemble = [...items]
    .filter((i) => i.brierMean !== null)
    .sort((a, b) => a.forecastHorizonMinutes - b.forecastHorizonMinutes);
  if (sortedEnsemble.length > 0 && sortedEnsemble[0].brierMean !== null) {
    return {
      row: sortedEnsemble[0],
      kind: "brier",
      value: sortedEnsemble[0].brierMean,
    };
  }

  // Deterministic fall-back: persistence at 30 min, then any 30-min
  // row, then the lowest-horizon row.
  const persistence30 = items.find(
    (i) => i.algorithm === "persistence" && i.forecastHorizonMinutes === 30,
  );
  if (persistence30) {
    return { row: persistence30, kind: "mae", value: persistence30.maeMean };
  }
  const any30 = items.find((i) => i.forecastHorizonMinutes === 30);
  if (any30) return { row: any30, kind: "mae", value: any30.maeMean };
  const fallback = [...items].sort(
    (a, b) => a.forecastHorizonMinutes - b.forecastHorizonMinutes,
  )[0];
  return { row: fallback, kind: "mae", value: fallback.maeMean };
}

function sortRows(
  items: ReadonlyArray<CalibrationItem>,
): ReadonlyArray<CalibrationItem> {
  // persistence first, then alphabetical; within each algorithm, low
  // horizon → high horizon.
  return [...items].sort((a, b) => {
    if (a.algorithm !== b.algorithm) {
      if (a.algorithm === "persistence") return -1;
      if (b.algorithm === "persistence") return 1;
      return a.algorithm.localeCompare(b.algorithm);
    }
    return a.forecastHorizonMinutes - b.forecastHorizonMinutes;
  });
}

function labelFor(algorithm: string): string {
  // Capitalize for human-readable display without doing translation.
  if (algorithm.length === 0) return algorithm;
  return algorithm[0].toUpperCase() + algorithm.slice(1);
}

function formatCount(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}k`;
  return String(n);
}
