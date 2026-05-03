"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";

import {
  type CalibrationItem,
  type Stats,
  fetchCalibration,
  fetchStats,
} from "@/lib/api";

const STATS_REFRESH_MS = 30_000;
const CALIBRATION_REFRESH_MS = 60_000;
// 1h window keeps the headline MAE responsive to recent ingest activity —
// long enough that the number isn't twitchy, short enough that "system is
// scoring forecasts right now" is actually current.
const CALIBRATION_WINDOW_HOURS = 1;
// Anchor point for the "Ns ago" badge. Re-anchored on its own interval so
// the relative time stays accurate without re-fetching `/v1/stats`.
const CLOCK_TICK_MS = 15_000;

/**
 * Site-wide pulse strip — three compact pills in the top nav that prove
 * the system is alive on every page-load:
 *
 *   1. Active NWS alerts (links to /map)
 *   2. Freshest MRMS grid age (links to /map)
 *   3. Verifier's headline MAE for the last hour (links to /calibration)
 *
 * Hidden on small viewports so the nav doesn't wrap, and gracefully
 * degrades to "—" placeholders when either endpoint is unreachable. The
 * bar stays visible regardless — its job is "yes the lights are on", not
 * "everything is working perfectly".
 */
export function PulseStrip() {
  const [stats, setStats] = useState<Stats | null>(null);
  const [calibration, setCalibration] = useState<
    ReadonlyArray<CalibrationItem> | null
  >(null);
  const [now, setNow] = useState<Date>(() => new Date());

  // Fetch stats every 30s — small, cheap aggregate. The route returns
  // alerts.active + mrms.latestValidAt + a grid count which is enough for
  // the "active" and "grid age" pills.
  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      try {
        const data = await fetchStats();
        if (!cancelled) setStats(data);
      } catch {
        // Silent — pulse strip degrades to "—" rather than blocking the page.
      }
    };
    void load();
    const id = setInterval(load, STATS_REFRESH_MS);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, []);

  // Fetch calibration on a slower cadence than stats — verifications drip
  // in over minutes, not seconds, so 60s is plenty.
  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      try {
        const data = await fetchCalibration({
          windowHours: CALIBRATION_WINDOW_HOURS,
        });
        if (!cancelled) setCalibration(data.items);
      } catch {
        // Silent — strip falls back to "—" for the MAE pill.
      }
    };
    void load();
    const id = setInterval(load, CALIBRATION_REFRESH_MS);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, []);

  // Re-anchor "now" so the grid-age badge updates without re-fetching.
  useEffect(() => {
    const id = setInterval(() => setNow(new Date()), CLOCK_TICK_MS);
    return () => clearInterval(id);
  }, []);

  const isAlive = stats !== null;
  const activeAlerts = stats?.alerts.active ?? null;
  const gridAge = useMemo(
    () => formatAge(stats?.mrms.latestValidAt ?? null, now),
    [stats, now],
  );
  const headlineMae = useMemo(() => pickHeadlineMae(calibration), [calibration]);
  // Auto-shows the ensemble pill the moment a Brier-scored row appears
  // for the canonical 30-min horizon. When no ensemble row exists, the
  // strip stays on its three-pill default — no empty placeholder.
  const ensembleHeadline = useMemo(
    () => pickEnsembleHeadline(calibration),
    [calibration],
  );

  return (
    <div className="hidden items-center gap-1.5 md:flex">
      <Pill
        href="/map"
        live={isAlive}
        label="active"
        value={activeAlerts !== null ? String(activeAlerts) : "—"}
        title={
          activeAlerts !== null
            ? `${activeAlerts} active NWS alerts right now`
            : "Stats endpoint unreachable"
        }
      />
      <Pill
        href="/map"
        label="grid"
        value={gridAge ?? "—"}
        title={
          stats?.mrms.latestValidAt
            ? `Latest MRMS grid valid at ${new Date(stats.mrms.latestValidAt).toLocaleTimeString()}`
            : "No MRMS grids yet"
        }
      />
      <Pill
        href="/calibration"
        label="MAE 1h"
        value={headlineMae !== null ? headlineMae.toFixed(2) : "—"}
        title={
          headlineMae !== null
            ? `Persistence baseline · 30-min horizon · last ${CALIBRATION_WINDOW_HOURS}h`
            : "No verifications scored in the last hour"
        }
      />
      {ensembleHeadline !== null ? (
        <Pill
          href="/calibration"
          label={`Brier ${CALIBRATION_WINDOW_HOURS}h`}
          value={ensembleHeadline.brier.toFixed(3)}
          title={`${ensembleHeadline.algorithm} · M=${ensembleHeadline.size} ensemble · 30-min horizon · last ${CALIBRATION_WINDOW_HOURS}h`}
        />
      ) : null}
    </div>
  );
}

function Pill({
  href,
  value,
  label,
  live,
  title,
}: {
  href: string;
  value: string;
  label: string;
  live?: boolean;
  title: string;
}) {
  return (
    <Link
      href={href}
      title={title}
      className="group inline-flex items-center gap-1.5 rounded-md border border-border/60 bg-bg/40 px-2 py-1 font-mono text-[10px] text-muted transition-colors hover:border-accent/60 hover:text-text"
    >
      {live ? (
        <span className="pulse-dot inline-block h-1.5 w-1.5 rounded-full bg-success" />
      ) : null}
      <span className="tabular-nums text-text">{value}</span>
      <span className="uppercase tracking-wide text-muted/80 group-hover:text-muted">
        {label}
      </span>
    </Link>
  );
}

function formatAge(then: string | null, now: Date): string | null {
  if (then === null) return null;
  const ms = now.getTime() - new Date(then).getTime();
  if (Number.isNaN(ms) || ms < 0) return null;
  const seconds = Math.floor(ms / 1000);
  if (seconds < 5) return "now";
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h`;
  const days = Math.floor(hours / 24);
  return `${days}d`;
}

function pickHeadlineMae(
  items: ReadonlyArray<CalibrationItem> | null,
): number | null {
  if (items === null || items.length === 0) return null;
  // Prefer the same row /calibration's hero metric anchors on so the
  // numbers stay consistent across pages.
  const persistence30 = items.find(
    (i) => i.algorithm === "persistence" && i.forecastHorizonMinutes === 30,
  );
  if (persistence30) return persistence30.maeMean;
  const any30 = items.find((i) => i.forecastHorizonMinutes === 30);
  if (any30) return any30.maeMean;
  const sorted = [...items].sort(
    (a, b) => a.forecastHorizonMinutes - b.forecastHorizonMinutes,
  );
  return sorted[0]?.maeMean ?? null;
}

interface EnsembleHeadline {
  brier: number;
  size: number;
  algorithm: string;
}

function pickEnsembleHeadline(
  items: ReadonlyArray<CalibrationItem> | null,
): EnsembleHeadline | null {
  if (items === null || items.length === 0) return null;
  // Same priority as the hero card: ensemble row at 30 min first, then
  // the lowest-horizon ensemble row. Stays in sync if/when the
  // `HeroVerificationCard` heuristic evolves.
  const ensemble30 = items.find(
    (i) =>
      i.brierMean !== null &&
      i.ensembleSize !== null &&
      i.forecastHorizonMinutes === 30,
  );
  const target =
    ensemble30 ??
    [...items]
      .filter((i) => i.brierMean !== null && i.ensembleSize !== null)
      .sort((a, b) => a.forecastHorizonMinutes - b.forecastHorizonMinutes)[0];
  if (!target || target.brierMean === null || target.ensembleSize === null) {
    return null;
  }
  return {
    brier: target.brierMean,
    size: target.ensembleSize,
    algorithm: target.algorithm,
  };
}
