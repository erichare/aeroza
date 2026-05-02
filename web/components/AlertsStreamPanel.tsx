"use client";

import { useEffect, useState } from "react";

import {
  type AlertFeatureCollection,
  type Severity,
  fetchAlerts,
} from "@/lib/api";
import { useAlertStream } from "@/lib/useAlertStream";

import { Panel } from "./Panel";
import { SeverityBadge } from "./SeverityBadge";
import { StatusDot } from "./StatusDot";

const SEVERITY_FILTERS: Array<Severity | "All"> = [
  "All",
  "Extreme",
  "Severe",
  "Moderate",
  "Minor",
];

const RECENT_LIMIT = 25;

export function AlertsStreamPanel() {
  const stream = useAlertStream();
  const [filter, setFilter] = useState<(typeof SEVERITY_FILTERS)[number]>("All");
  const [recent, setRecent] = useState<AlertFeatureCollection | null>(null);
  const [recentError, setRecentError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      try {
        const data = await fetchAlerts({ limit: RECENT_LIMIT });
        if (!cancelled) {
          setRecent(data);
          setRecentError(null);
        }
      } catch (err) {
        if (!cancelled) {
          setRecentError(err instanceof Error ? err.message : "Unknown error");
        }
      }
    };
    void load();
    const interval = setInterval(load, 30_000);
    return () => {
      cancelled = true;
      clearInterval(interval);
    };
  }, []);

  const visibleEvents = stream.events.filter(
    (e) => filter === "All" || e.severity === filter,
  );

  return (
    <Panel
      title="Alerts · live stream"
      subtitle="GET /v1/alerts/stream · re-emits aeroza.alerts.nws.new"
      actions={
        <div className="flex items-center gap-3">
          <ConnectionIndicator state={stream.state} />
          <button
            type="button"
            onClick={stream.reconnect}
            className="rounded-md border border-border/70 px-2 py-1 text-xs text-muted hover:border-accent/60 hover:text-accent"
          >
            Reconnect
          </button>
          <button
            type="button"
            onClick={stream.clear}
            disabled={stream.events.length === 0}
            className="rounded-md border border-border/70 px-2 py-1 text-xs text-muted hover:border-accent/60 hover:text-accent disabled:opacity-50 disabled:hover:border-border/70 disabled:hover:text-muted"
          >
            Clear
          </button>
        </div>
      }
    >
      <div className="flex flex-col gap-3 p-5">
        <div className="flex flex-wrap items-center gap-1.5">
          {SEVERITY_FILTERS.map((s) => (
            <button
              key={s}
              type="button"
              onClick={() => setFilter(s)}
              className={[
                "rounded-md border px-2 py-0.5 text-[11px] uppercase tracking-wide",
                filter === s
                  ? "border-accent bg-accent/10 text-accent"
                  : "border-border/70 text-muted hover:border-accent/40 hover:text-text",
              ].join(" ")}
            >
              {s}
            </button>
          ))}
          <span className="ml-auto text-[11px] text-muted">
            {visibleEvents.length} live · {recent?.features.length ?? 0} recent
          </span>
        </div>

        {stream.error ? (
          <div className="rounded-md border border-warning/40 bg-warning/10 px-3 py-2 text-xs text-warning">
            {stream.error}
          </div>
        ) : null}

        <div className="grid gap-4 lg:grid-cols-2">
          <FeedColumn
            heading="Live (SSE)"
            empty="Waiting for events… try `aeroza-ingest-alerts --once` to populate."
            items={visibleEvents.map((e) => ({
              key: `${e.id}-${e.receivedAt}`,
              event: e.event,
              headline: e.headline,
              severity: e.severity,
              senderName: e.senderName,
              areaDesc: e.areaDesc,
              timestamp: new Date(e.receivedAt).toLocaleTimeString(),
              timeLabel: "received",
            }))}
          />
          <FeedColumn
            heading="Recent (REST · /v1/alerts)"
            empty={recentError ?? "No active alerts."}
            errored={recentError !== null}
            items={(recent?.features ?? []).map((f) => ({
              key: f.properties.id,
              event: f.properties.event,
              headline: f.properties.headline,
              severity: f.properties.severity,
              senderName: f.properties.senderName,
              areaDesc: f.properties.areaDesc,
              timestamp: f.properties.expires
                ? new Date(f.properties.expires).toLocaleString()
                : "—",
              timeLabel: "expires",
            }))}
          />
        </div>
      </div>
    </Panel>
  );
}

interface FeedItem {
  key: string;
  event: string;
  headline: string | null;
  severity: Severity;
  senderName: string | null;
  areaDesc: string | null;
  timestamp: string;
  timeLabel: string;
}

function FeedColumn({
  heading,
  items,
  empty,
  errored = false,
}: {
  heading: string;
  items: FeedItem[];
  empty: string;
  errored?: boolean;
}) {
  return (
    <div className="flex min-h-[20rem] flex-col rounded-xl border border-border/60 bg-bg/40">
      <div className="border-b border-border/60 px-3 py-2 text-[11px] font-medium uppercase tracking-wide text-muted">
        {heading}
      </div>
      <div className="flex-1 overflow-y-auto">
        {items.length === 0 ? (
          <div
            className={[
              "flex h-full items-center justify-center px-3 text-center text-xs",
              errored ? "text-danger" : "text-muted",
            ].join(" ")}
          >
            {empty}
          </div>
        ) : (
          <ul className="divide-y divide-border/40">
            {items.map((item) => (
              <li key={item.key} className="px-3 py-2.5">
                <div className="flex items-start justify-between gap-2">
                  <div className="min-w-0 flex-1">
                    <div className="flex items-center gap-2">
                      <SeverityBadge severity={item.severity} />
                      <span className="truncate text-xs font-semibold text-text">
                        {item.event}
                      </span>
                    </div>
                    {item.headline ? (
                      <p className="mt-1 truncate text-xs text-muted">{item.headline}</p>
                    ) : null}
                    {item.areaDesc ? (
                      <p className="mt-0.5 truncate text-[11px] text-muted/80">
                        {item.areaDesc}
                      </p>
                    ) : null}
                  </div>
                  <div className="flex flex-col items-end gap-0.5 whitespace-nowrap text-right">
                    <span className="font-mono text-[10px] text-muted">
                      {item.timestamp}
                    </span>
                    <span className="text-[9px] uppercase tracking-wide text-muted/60">
                      {item.timeLabel}
                    </span>
                  </div>
                </div>
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  );
}

function ConnectionIndicator({
  state,
}: {
  state: ReturnType<typeof useAlertStream>["state"];
}) {
  if (state === "open") return <StatusDot tone="success" label="Live" pulse />;
  if (state === "connecting") return <StatusDot tone="warning" label="Connecting…" />;
  if (state === "error") return <StatusDot tone="warning" label="Retrying" />;
  return <StatusDot tone="danger" label="Disconnected" />;
}
