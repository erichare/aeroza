"use client";

import { useEffect, useState } from "react";

import { API_BASE, fetchHealth, type Health } from "@/lib/api";

import { Panel } from "./Panel";
import { StatusDot } from "./StatusDot";

const POLL_MS = 10_000;

export function HealthPanel() {
  const [health, setHealth] = useState<Health | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [lastChecked, setLastChecked] = useState<number | null>(null);

  useEffect(() => {
    let cancelled = false;
    const ping = async () => {
      try {
        const data = await fetchHealth();
        if (cancelled) return;
        setHealth(data);
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
      title="System · health"
      subtitle="GET /health · polled every 10s"
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
      <div className="grid grid-cols-1 gap-4 px-5 py-4 text-xs sm:grid-cols-2">
        <Stat label="Version" value={health?.version ?? "—"} />
        <Stat label="API base" value={API_BASE} mono />
        <Stat
          label="Last check"
          value={lastChecked ? new Date(lastChecked).toLocaleTimeString() : "—"}
        />
        <Stat label="Status" value={error ?? health?.status ?? "—"} tone={error ? "danger" : undefined} />
      </div>
    </Panel>
  );
}

function Stat({
  label,
  value,
  mono = false,
  tone,
}: {
  label: string;
  value: string;
  mono?: boolean;
  tone?: "danger";
}) {
  return (
    <div className="flex flex-col gap-1 rounded-lg border border-border/60 bg-bg/40 px-3 py-2">
      <span className="text-[10px] uppercase tracking-wide text-muted">{label}</span>
      <span
        className={[
          "truncate text-text",
          mono ? "font-mono text-[11px]" : "text-sm font-medium",
          tone === "danger" ? "text-danger" : "",
        ].join(" ")}
      >
        {value}
      </span>
    </div>
  );
}
