"use client";

import { useEffect, useState } from "react";

import {
  API_BASE,
  fetchHealth,
  fetchStats,
  type Health,
  type Stats,
} from "@/lib/api";

import { Panel } from "./Panel";
import { StatusDot } from "./StatusDot";

const POLL_MS = 10_000;

export function HealthPanel() {
  const [health, setHealth] = useState<Health | null>(null);
  const [stats, setStats] = useState<Stats | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [lastChecked, setLastChecked] = useState<number | null>(null);

  useEffect(() => {
    let cancelled = false;
    const ping = async () => {
      try {
        const [h, s] = await Promise.all([fetchHealth(), fetchStats()]);
        if (cancelled) return;
        setHealth(h);
        setStats(s);
        setError(null);
      } catch (err) {
        if (!cancelled) setError(err instanceof Error ? err.message : "Unknown error");
      } finally {
        if (!cancelled) setLastChecked(Date.now());
      }
    };
    void ping();
    const interval = setInterval(ping, POLL_MS);
    return () => {
      cancelled = true;
      clearInterval(interval);
    };
  }, []);

  return (
    <Panel
      title="System · health & stats"
      subtitle="GET /health + /v1/stats · polled every 10s"
      actions={
        error ? (
          <StatusDot tone="danger" label="DOWN" />
        ) : health ? (
          <StatusDot tone="success" label={health.status.toUpperCase()} pulse />
        ) : (
          <StatusDot tone="warning" label="…" />
        )
      }
    >
      <div className="flex flex-col gap-3 px-5 py-4 text-xs">
        {error ? (
          <div className="rounded-md border border-danger/40 bg-danger/10 px-3 py-2 text-danger">
            {error}
          </div>
        ) : null}

        <div className="grid grid-cols-2 gap-3 sm:grid-cols-3">
          <Stat label="Version" value={health?.version ?? "—"} />
          <Stat
            label="Last check"
            value={lastChecked ? new Date(lastChecked).toLocaleTimeString() : "—"}
          />
          <Stat label="API base" value={API_BASE} mono />
        </div>

        <div className="rounded-xl border border-border/60 bg-bg/40 p-3">
          <div className="mb-2 text-[10px] uppercase tracking-wide text-muted">
            Alerts
          </div>
          <div className="grid grid-cols-3 gap-3">
            <Stat label="active" value={String(stats?.alerts.active ?? "—")} accent />
            <Stat label="total" value={String(stats?.alerts.total ?? "—")} />
            <Stat
              label="latest expires"
              value={
                stats?.alerts.latestExpires
                  ? formatRelative(new Date(stats.alerts.latestExpires))
                  : "—"
              }
            />
          </div>
        </div>

        <div className="rounded-xl border border-border/60 bg-bg/40 p-3">
          <div className="mb-2 text-[10px] uppercase tracking-wide text-muted">
            MRMS
          </div>
          <div className="grid grid-cols-3 gap-3">
            <Stat label="files" value={String(stats?.mrms.files ?? "—")} />
            <Stat
              label="grids"
              value={String(stats?.mrms.gridsMaterialised ?? "—")}
              accent
            />
            <Stat
              label="pending"
              value={String(stats?.mrms.filesPending ?? "—")}
              tone={
                stats && stats.mrms.filesPending > 0 ? "warning" : undefined
              }
            />
          </div>
          <div className="mt-2 grid grid-cols-2 gap-3">
            <Stat
              label="latest valid_at"
              value={
                stats?.mrms.latestValidAt
                  ? formatRelative(new Date(stats.mrms.latestValidAt))
                  : "—"
              }
            />
            <Stat
              label="latest materialised"
              value={
                stats?.mrms.latestGridMaterialisedAt
                  ? formatRelative(new Date(stats.mrms.latestGridMaterialisedAt))
                  : "—"
              }
            />
          </div>
        </div>
      </div>
    </Panel>
  );
}

interface StatProps {
  label: string;
  value: string;
  mono?: boolean;
  accent?: boolean;
  tone?: "warning" | "danger";
}

function Stat({ label, value, mono = false, accent = false, tone }: StatProps) {
  const valueClass = tone === "danger"
    ? "text-danger"
    : tone === "warning"
      ? "text-warning"
      : accent
        ? "text-accent"
        : "text-text";
  return (
    <div className="flex flex-col gap-1">
      <span className="text-[10px] uppercase tracking-wide text-muted">{label}</span>
      <span
        className={[
          "truncate",
          mono ? "font-mono text-[11px]" : "text-sm font-medium",
          valueClass,
        ].join(" ")}
      >
        {value}
      </span>
    </div>
  );
}

function formatRelative(date: Date): string {
  const seconds = Math.floor((Date.now() - date.getTime()) / 1000);
  if (seconds < 0) return `in ${Math.abs(seconds)}s`;
  if (seconds < 60) return `${seconds}s ago`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86_400) return `${Math.floor(seconds / 3600)}h ago`;
  return `${Math.floor(seconds / 86_400)}d ago`;
}
