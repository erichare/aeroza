"use client";

import { useEffect, useState } from "react";

import {
  type AlertRule,
  type AlertRuleStatus,
  fetchAlertRules,
} from "@/lib/api";

import { Panel } from "./Panel";
import { StatusDot } from "./StatusDot";

const REFRESH_INTERVAL_MS = 30_000;

const STATUS_TONE: Record<AlertRuleStatus, "success" | "warning" | "danger"> = {
  active: "success",
  paused: "warning",
  disabled: "danger",
};

/**
 * Alert rules panel — read-only view of `/v1/alert-rules`.
 *
 * Renders the discriminated config (point vs polygon) inline so the
 * predicate's actual shape is visible. Same posture as
 * `WebhooksPanel`: enough to confirm "yes my rule landed and the
 * dispatcher is filtering on it" without reaching for Postgres.
 */
export function AlertRulesPanel() {
  const [items, setItems] = useState<AlertRule[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      try {
        const data = await fetchAlertRules({ limit: 50 });
        if (cancelled) return;
        setItems(data.items);
        setError(null);
      } catch (err) {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : "Failed to load alert rules");
        }
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

  return (
    <Panel
      title="Alert rules · webhook predicate DSL"
      subtitle="GET /v1/alert-rules · refreshed every 30 s"
      actions={
        <span className="font-mono text-[11px] text-muted">{items.length}</span>
      }
    >
      <div className="px-5 py-4">
        {error ? (
          <div className="rounded-md border border-warning/40 bg-warning/10 px-3 py-2 text-xs text-warning">
            {error}
          </div>
        ) : null}

        {!error && !isLoading && items.length === 0 ? (
          <EmptyState />
        ) : null}

        {items.length > 0 ? (
          <ul className="divide-y divide-border/40">
            {items.map((rule) => (
              <RuleRow key={rule.id} rule={rule} />
            ))}
          </ul>
        ) : null}
      </div>
    </Panel>
  );
}

function RuleRow({ rule }: { rule: AlertRule }) {
  return (
    <li className="grid grid-cols-[8rem_1fr_auto] items-start gap-3 py-2.5">
      <StatusDot tone={STATUS_TONE[rule.status]} label={rule.status} />
      <div className="min-w-0">
        <div className="flex items-center gap-2">
          <span className="text-xs font-semibold text-text">{rule.name}</span>
          <KindBadge kind={rule.config.type} />
        </div>
        {rule.description ? (
          <p className="mt-0.5 truncate text-[11px] text-muted">{rule.description}</p>
        ) : null}
        <div className="mt-1 font-mono text-[10px] text-muted">
          <RulePreview rule={rule} />
        </div>
      </div>
      <div className="text-right font-mono text-[10px] text-muted/70">
        <div>{new Date(rule.createdAt).toLocaleDateString()}</div>
        <div className="break-all">{rule.id.slice(0, 8)}…</div>
      </div>
    </li>
  );
}

function KindBadge({ kind }: { kind: "point" | "polygon" }) {
  const cls =
    kind === "point"
      ? "border-accent/60 bg-accent/10 text-accent"
      : "border-success/60 bg-success/10 text-success";
  return (
    <span
      className={[
        "rounded-md border px-1.5 py-0.5 font-mono text-[10px] uppercase tracking-wide",
        cls,
      ].join(" ")}
    >
      {kind}
    </span>
  );
}

function RulePreview({ rule }: { rule: AlertRule }) {
  const { config } = rule;
  const pred = `value ${config.predicate.op} ${config.predicate.threshold}`;
  if (config.type === "point") {
    return (
      <span>
        sample at ({config.lat.toFixed(3)}, {config.lng.toFixed(3)}) where {pred}
      </span>
    );
  }
  // polygon
  const reducerLabel =
    config.reducer === "count_ge" && config.countThreshold != null
      ? `count_ge[${config.countThreshold}]`
      : config.reducer;
  // Truncate long polygons so the row stays one-line readable.
  const polyShort =
    config.polygon.length > 64 ? `${config.polygon.slice(0, 60)}…` : config.polygon;
  return (
    <span>
      {reducerLabel}([{polyShort}]) {pred}
    </span>
  );
}

function EmptyState() {
  return (
    <div className="rounded-xl border border-dashed border-border/60 bg-bg/30 px-5 py-6 text-center">
      <p className="text-sm text-text">No alert rules yet.</p>
      <p className="mt-1 text-xs text-muted">
        Alert rules filter the webhook firehose by geography. Two predicate
        kinds: <code className="font-mono">point</code> (alert polygon
        intersects a circle of radius R around (lat, lng)) and{" "}
        <code className="font-mono">polygon</code> (alert polygon intersects a
        caller-supplied GeoJSON polygon).
      </p>
      <p className="mt-3 text-xs text-muted/80">
        Create one via <code className="font-mono">POST /v1/alert-rules</code>{" "}
        or the{" "}
        <a
          href="http://localhost:8000/docs#/alert-rules"
          target="_blank"
          rel="noreferrer"
          className="text-accent underline"
        >
          Swagger UI
        </a>
        .
      </p>
    </div>
  );
}
