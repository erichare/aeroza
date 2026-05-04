"use client";

import { useCallback, useEffect, useMemo, useState } from "react";

import {
  type AlertRule,
  type AlertRuleStatus,
  type PredicateOp,
  type RuleConfigPolygonReducer,
  type WebhookSubscriptionRedacted,
  createAlertRule,
  deleteAlertRule,
  fetchAlertRules,
  fetchWebhooks,
  updateAlertRule,
} from "@/lib/api";

import { Panel } from "./Panel";
import { StatusDot } from "./StatusDot";

const REFRESH_INTERVAL_MS = 30_000;

const STATUS_TONE: Record<AlertRuleStatus, "success" | "warning" | "danger"> = {
  active: "success",
  paused: "warning",
  disabled: "danger",
};

const PREDICATE_OPS: ReadonlyArray<PredicateOp> = [">", ">=", "<", "<=", "==", "!="];
const REDUCERS: ReadonlyArray<RuleConfigPolygonReducer> = ["max", "mean", "min", "count_ge"];

const DEFAULT_PRODUCT = "MergedReflectivityComposite";
const DEFAULT_LEVEL = "00.50";

type RuleKind = "point" | "polygon";

/**
 * Alert rules panel — full CRUD over `/v1/alert-rules`.
 *
 * Mirrors the posture of `WebhooksPanel`: read on a 30 s tick, refresh
 * on demand after mutations, single inline create form (no modal).
 * Pause/resume goes through `PATCH ... { status }` since there's no
 * dedicated route. Delete uses the platform `confirm()` dialog.
 *
 * Surfaces the new runtime fields the dispatcher writes back —
 * `currentlyFiring` and `lastValue` make the panel actually
 * diagnostic ("yes my rule is firing right now, here's the value
 * the dispatcher saw") instead of just a list of registered shapes.
 */
export function AlertRulesPanel() {
  const [items, setItems] = useState<AlertRule[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [showCreate, setShowCreate] = useState(false);
  const [pendingId, setPendingId] = useState<string | null>(null);

  const load = useCallback(async () => {
    try {
      const data = await fetchAlertRules({ limit: 50 });
      setItems(data.items);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load alert rules");
    } finally {
      setIsLoading(false);
    }
  }, []);

  useEffect(() => {
    let cancelled = false;
    const tick = async () => {
      if (cancelled) return;
      await load();
    };
    void tick();
    const id = setInterval(tick, REFRESH_INTERVAL_MS);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [load]);

  const handleCreated = useCallback(
    (created: AlertRule) => {
      // Optimistically prepend so the list shows the new row before the
      // next 30 s tick lands. Newest-first matches the server ordering.
      setItems((prev) => [created, ...prev]);
      setShowCreate(false);
      void load();
    },
    [load],
  );

  const handleToggleStatus = useCallback(
    async (rule: AlertRule) => {
      const next: AlertRuleStatus = rule.status === "active" ? "paused" : "active";
      setPendingId(rule.id);
      try {
        const updated = await updateAlertRule(rule.id, { status: next });
        setItems((prev) => prev.map((r) => (r.id === rule.id ? updated : r)));
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to update rule");
      } finally {
        setPendingId(null);
      }
    },
    [],
  );

  const handleDelete = useCallback(
    async (id: string) => {
      if (!window.confirm(`Delete alert rule ${id.slice(0, 8)}…? This cannot be undone.`)) {
        return;
      }
      setPendingId(id);
      try {
        await deleteAlertRule(id);
        setItems((prev) => prev.filter((r) => r.id !== id));
        void load();
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to delete rule");
      } finally {
        setPendingId(null);
      }
    },
    [load],
  );

  return (
    <Panel
      title="Alert rules · webhook predicate DSL"
      subtitle="GET / POST / PATCH / DELETE /v1/alert-rules · refreshed every 30 s"
      actions={
        <div className="flex items-center gap-2">
          <span className="font-mono text-[11px] text-muted">{items.length}</span>
          <button
            type="button"
            onClick={() => setShowCreate((v) => !v)}
            className={[
              "rounded-md border px-2 py-1 font-mono text-[11px] uppercase tracking-wide",
              showCreate
                ? "border-border/60 text-muted hover:border-accent/60 hover:text-accent"
                : "border-accent bg-accent/15 text-accent hover:bg-accent/25",
            ].join(" ")}
          >
            {showCreate ? "Cancel" : "+ New"}
          </button>
        </div>
      }
    >
      <div className="px-5 py-4">
        {error ? (
          <div className="mb-3 rounded-md border border-warning/40 bg-warning/10 px-3 py-2 text-xs text-warning">
            {error}
          </div>
        ) : null}

        {showCreate ? (
          <CreateForm onCreated={handleCreated} onError={setError} />
        ) : null}

        {!error && !isLoading && items.length === 0 && !showCreate ? (
          <EmptyState onCreateClick={() => setShowCreate(true)} />
        ) : null}

        {items.length > 0 ? (
          <ul className="divide-y divide-border/40">
            {items.map((rule) => (
              <RuleRow
                key={rule.id}
                rule={rule}
                pending={pendingId === rule.id}
                onToggleStatus={handleToggleStatus}
                onDelete={handleDelete}
              />
            ))}
          </ul>
        ) : null}
      </div>
    </Panel>
  );
}

interface CreateFormProps {
  onCreated: (rule: AlertRule) => void;
  onError: (message: string) => void;
}

/**
 * Inline create form. Predicate kind (point vs polygon) toggles which
 * group of fields renders; we keep both kinds in one component so the
 * affordance reads "one place to register a rule" rather than a
 * choose-your-own-adventure.
 *
 * Subscription target is required: the server FK-validates it and 404s
 * if missing, but we'd rather catch it client-side. We fetch the
 * available subscriptions once and present them as a dropdown — a
 * raw UUID input is footgun-territory.
 */
function CreateForm({ onCreated, onError }: CreateFormProps) {
  const [kind, setKind] = useState<RuleKind>("point");
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [subscriptionId, setSubscriptionId] = useState<string>("");
  const [subscriptions, setSubscriptions] = useState<WebhookSubscriptionRedacted[]>([]);
  const [subsLoading, setSubsLoading] = useState(true);
  const [submitting, setSubmitting] = useState(false);

  // Predicate
  const [op, setOp] = useState<PredicateOp>(">=");
  const [threshold, setThreshold] = useState<string>("35");

  // Point-kind fields
  const [lat, setLat] = useState<string>("29.76");
  const [lng, setLng] = useState<string>("-95.37");

  // Polygon-kind fields
  const [polygon, setPolygon] = useState<string>("");
  const [reducer, setReducer] = useState<RuleConfigPolygonReducer>("max");
  const [countThreshold, setCountThreshold] = useState<string>("");

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      try {
        const data = await fetchWebhooks({ limit: 50 });
        if (cancelled) return;
        setSubscriptions(data.items);
        // Pre-select the first one — most rules are bound to whichever
        // subscription you just created.
        if (data.items.length > 0) {
          setSubscriptionId(data.items[0].id);
        }
      } catch (err) {
        if (!cancelled) {
          onError(err instanceof Error ? err.message : "Failed to load subscriptions");
        }
      } finally {
        if (!cancelled) setSubsLoading(false);
      }
    };
    void load();
    return () => {
      cancelled = true;
    };
  }, [onError]);

  const noSubscriptions = !subsLoading && subscriptions.length === 0;

  const handleSubmit = useCallback(
    async (e: React.FormEvent<HTMLFormElement>) => {
      e.preventDefault();
      const trimmedName = name.trim();
      if (!trimmedName) {
        onError("Name is required.");
        return;
      }
      if (!subscriptionId) {
        onError("Pick a webhook subscription to bind this rule to.");
        return;
      }
      const thresholdNum = Number(threshold);
      if (!Number.isFinite(thresholdNum)) {
        onError("Threshold must be a number.");
        return;
      }
      const predicate = { op, threshold: thresholdNum };

      let body;
      if (kind === "point") {
        const latNum = Number(lat);
        const lngNum = Number(lng);
        if (!Number.isFinite(latNum) || !Number.isFinite(lngNum)) {
          onError("Latitude and longitude must be numbers.");
          return;
        }
        if (latNum < -90 || latNum > 90 || lngNum < -180 || lngNum > 180) {
          onError("Latitude must be in [-90, 90] and longitude in [-180, 180].");
          return;
        }
        body = {
          subscriptionId,
          name: trimmedName,
          description: description.trim() || null,
          config: {
            type: "point" as const,
            product: DEFAULT_PRODUCT,
            level: DEFAULT_LEVEL,
            predicate,
            lat: latNum,
            lng: lngNum,
          },
        };
      } else {
        const trimmedPoly = polygon.trim();
        if (!trimmedPoly) {
          onError("Polygon is required (lng,lat,lng,lat,…).");
          return;
        }
        const parts = trimmedPoly.split(",");
        if (parts.length < 6 || parts.length % 2 !== 0) {
          onError("Polygon needs ≥ 3 vertices (≥ 6 numbers, even count).");
          return;
        }
        let countThresholdNum: number | null = null;
        if (reducer === "count_ge") {
          countThresholdNum = Number(countThreshold);
          if (!Number.isFinite(countThresholdNum)) {
            onError("count_ge requires a numeric countThreshold.");
            return;
          }
        }
        body = {
          subscriptionId,
          name: trimmedName,
          description: description.trim() || null,
          config: {
            type: "polygon" as const,
            product: DEFAULT_PRODUCT,
            level: DEFAULT_LEVEL,
            predicate,
            polygon: trimmedPoly,
            reducer,
            countThreshold: countThresholdNum,
          },
        };
      }

      setSubmitting(true);
      try {
        const created = await createAlertRule(body);
        onCreated(created);
      } catch (err) {
        onError(err instanceof Error ? err.message : "Create failed");
      } finally {
        setSubmitting(false);
      }
    },
    [
      kind,
      name,
      description,
      subscriptionId,
      op,
      threshold,
      lat,
      lng,
      polygon,
      reducer,
      countThreshold,
      onCreated,
      onError,
    ],
  );

  return (
    <form
      onSubmit={handleSubmit}
      className="mb-4 flex flex-col gap-3 rounded-xl border border-border/60 bg-bg/40 p-4"
    >
      {noSubscriptions ? (
        <div className="rounded-md border border-warning/40 bg-warning/10 px-3 py-2 text-[11px] text-warning">
          No webhook subscriptions yet — alert rules need one to bind to.
          Create a subscription in the panel to the left first.
        </div>
      ) : null}

      <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
        <Field label="Name" required>
          <input
            type="text"
            required
            value={name}
            onChange={(e) => setName(e.target.value)}
            placeholder="Houston TX 35dBZ"
            maxLength={128}
            className="rounded-md border border-border/60 bg-surface/60 px-3 py-1.5 text-[12px] text-text outline-none focus:border-accent"
          />
        </Field>
        <Field label="Subscription" required>
          <select
            required
            disabled={subsLoading || noSubscriptions}
            value={subscriptionId}
            onChange={(e) => setSubscriptionId(e.target.value)}
            className="rounded-md border border-border/60 bg-surface/60 px-3 py-1.5 font-mono text-[12px] text-text outline-none focus:border-accent disabled:opacity-50"
          >
            {subsLoading ? (
              <option value="">Loading subscriptions…</option>
            ) : noSubscriptions ? (
              <option value="">No subscriptions</option>
            ) : (
              subscriptions.map((s) => (
                <option key={s.id} value={s.id}>
                  {s.id.slice(0, 8)}… — {s.url}
                </option>
              ))
            )}
          </select>
        </Field>
      </div>

      <Field label="Description (optional)">
        <input
          type="text"
          value={description}
          onChange={(e) => setDescription(e.target.value)}
          placeholder="Page on-call when MRMS reflectivity at the depot ≥ 35 dBZ"
          maxLength={512}
          className="rounded-md border border-border/60 bg-surface/60 px-3 py-1.5 text-[12px] text-text outline-none focus:border-accent"
        />
      </Field>

      <div className="flex items-center gap-2">
        <span className="font-mono text-[10px] uppercase tracking-wider text-muted">Kind</span>
        <RadioPill checked={kind === "point"} onClick={() => setKind("point")}>
          point
        </RadioPill>
        <RadioPill checked={kind === "polygon"} onClick={() => setKind("polygon")}>
          polygon
        </RadioPill>
      </div>

      {kind === "point" ? (
        <div className="grid grid-cols-2 gap-3">
          <Field label="Latitude">
            <input
              type="number"
              step="any"
              required
              value={lat}
              onChange={(e) => setLat(e.target.value)}
              className="rounded-md border border-border/60 bg-surface/60 px-3 py-1.5 font-mono text-[12px] text-text outline-none focus:border-accent"
            />
          </Field>
          <Field label="Longitude">
            <input
              type="number"
              step="any"
              required
              value={lng}
              onChange={(e) => setLng(e.target.value)}
              className="rounded-md border border-border/60 bg-surface/60 px-3 py-1.5 font-mono text-[12px] text-text outline-none focus:border-accent"
            />
          </Field>
        </div>
      ) : (
        <div className="grid grid-cols-1 gap-3">
          <Field label="Polygon (lng,lat,lng,lat,…)">
            <input
              type="text"
              required
              value={polygon}
              onChange={(e) => setPolygon(e.target.value)}
              placeholder="-95.7,29.5,-95.0,29.5,-95.0,30.0,-95.7,30.0"
              className="rounded-md border border-border/60 bg-surface/60 px-3 py-1.5 font-mono text-[12px] text-text outline-none focus:border-accent"
            />
          </Field>
          <div className="grid grid-cols-2 gap-3">
            <Field label="Reducer">
              <select
                value={reducer}
                onChange={(e) => setReducer(e.target.value as RuleConfigPolygonReducer)}
                className="rounded-md border border-border/60 bg-surface/60 px-3 py-1.5 font-mono text-[12px] text-text outline-none focus:border-accent"
              >
                {REDUCERS.map((r) => (
                  <option key={r} value={r}>
                    {r}
                  </option>
                ))}
              </select>
            </Field>
            {reducer === "count_ge" ? (
              <Field label="countThreshold">
                <input
                  type="number"
                  step="any"
                  required
                  value={countThreshold}
                  onChange={(e) => setCountThreshold(e.target.value)}
                  placeholder="40"
                  className="rounded-md border border-border/60 bg-surface/60 px-3 py-1.5 font-mono text-[12px] text-text outline-none focus:border-accent"
                />
              </Field>
            ) : null}
          </div>
        </div>
      )}

      <div className="grid grid-cols-2 gap-3">
        <Field label="Operator">
          <select
            value={op}
            onChange={(e) => setOp(e.target.value as PredicateOp)}
            className="rounded-md border border-border/60 bg-surface/60 px-3 py-1.5 font-mono text-[12px] text-text outline-none focus:border-accent"
          >
            {PREDICATE_OPS.map((o) => (
              <option key={o} value={o}>
                {o}
              </option>
            ))}
          </select>
        </Field>
        <Field label="Threshold (dBZ)">
          <input
            type="number"
            step="any"
            required
            value={threshold}
            onChange={(e) => setThreshold(e.target.value)}
            className="rounded-md border border-border/60 bg-surface/60 px-3 py-1.5 font-mono text-[12px] text-text outline-none focus:border-accent"
          />
        </Field>
      </div>

      <div className="flex items-center justify-end gap-2 pt-1">
        <button
          type="submit"
          disabled={submitting || noSubscriptions}
          className={[
            "rounded-md border px-3 py-1.5 font-mono text-[11px] uppercase tracking-wide",
            submitting || noSubscriptions
              ? "cursor-not-allowed border-border/40 text-muted/50"
              : "border-accent bg-accent/15 text-accent hover:bg-accent/25",
          ].join(" ")}
        >
          {submitting ? "Creating…" : "Create rule"}
        </button>
      </div>
    </form>
  );
}

function Field({
  label,
  children,
  required,
}: {
  label: string;
  children: React.ReactNode;
  required?: boolean;
}) {
  return (
    <label className="flex flex-col gap-1">
      <span className="font-mono text-[10px] uppercase tracking-wider text-muted">
        {label}
        {required ? <span className="ml-1 text-warning">required</span> : null}
      </span>
      {children}
    </label>
  );
}

function RadioPill({
  checked,
  onClick,
  children,
}: {
  checked: boolean;
  onClick: () => void;
  children: React.ReactNode;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={[
        "rounded-md border px-2 py-0.5 font-mono text-[11px] uppercase tracking-wide",
        checked
          ? "border-accent bg-accent/15 text-accent"
          : "border-border/60 text-muted hover:border-accent/60 hover:text-accent",
      ].join(" ")}
    >
      {children}
    </button>
  );
}

interface RuleRowProps {
  rule: AlertRule;
  pending: boolean;
  onToggleStatus: (rule: AlertRule) => Promise<void>;
  onDelete: (id: string) => Promise<void>;
}

function RuleRow({ rule, pending, onToggleStatus, onDelete }: RuleRowProps) {
  const lastSeen = useMemo(() => {
    if (rule.lastEvaluatedAt === null) return null;
    return new Date(rule.lastEvaluatedAt);
  }, [rule.lastEvaluatedAt]);

  const toggleLabel = rule.status === "active" ? "Pause" : "Resume";
  const toggleEnabled = rule.status !== "disabled";

  return (
    <li className="grid grid-cols-[8rem_1fr_auto] items-start gap-3 py-2.5">
      <div className="flex flex-col gap-1">
        <StatusDot tone={STATUS_TONE[rule.status]} label={rule.status} />
        {rule.currentlyFiring ? (
          <span
            className="inline-flex items-center gap-1 rounded-md border border-danger/40 bg-danger/10 px-1.5 py-0.5 font-mono text-[9px] uppercase tracking-wider text-danger"
            title="Predicate is currently true on the latest observation"
          >
            <span className="h-1.5 w-1.5 rounded-full bg-danger pulse-dot" />
            firing
          </span>
        ) : null}
      </div>
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
        {rule.lastValue !== null || lastSeen !== null ? (
          <div className="mt-1 flex flex-wrap gap-3 font-mono text-[10px] text-muted/80">
            {rule.lastValue !== null ? (
              <span>last value · <span className="text-text">{rule.lastValue.toFixed(2)}</span></span>
            ) : null}
            {lastSeen !== null ? (
              <span title={lastSeen.toISOString()}>
                last evaluated · <span className="text-text">{relativeTime(lastSeen)}</span>
              </span>
            ) : null}
          </div>
        ) : null}
      </div>
      <div className="flex flex-col items-end gap-1.5 text-right font-mono text-[10px] text-muted/70">
        <div>{new Date(rule.createdAt).toLocaleDateString()}</div>
        <div className="break-all">{rule.id.slice(0, 8)}…</div>
        <div className="flex gap-1">
          <button
            type="button"
            disabled={pending || !toggleEnabled}
            onClick={() => void onToggleStatus(rule)}
            aria-label={`${toggleLabel} alert rule ${rule.id}`}
            className={[
              "rounded-md border px-1.5 py-0.5 text-[10px] uppercase tracking-wider",
              pending || !toggleEnabled
                ? "cursor-not-allowed border-border/40 text-muted/50"
                : "border-border/50 text-muted hover:border-accent/60 hover:text-accent",
            ].join(" ")}
          >
            {toggleLabel}
          </button>
          <button
            type="button"
            disabled={pending}
            onClick={() => void onDelete(rule.id)}
            aria-label={`Delete alert rule ${rule.id}`}
            className={[
              "rounded-md border px-1.5 py-0.5 text-[10px] uppercase tracking-wider",
              pending
                ? "cursor-not-allowed border-border/40 text-muted/50"
                : "border-border/50 text-muted hover:border-warning/60 hover:text-warning",
            ].join(" ")}
          >
            Delete
          </button>
        </div>
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

/**
 * "5m ago", "2h ago" — short form for the row meta line. Falls back
 * to the absolute date for ages over a day so the line stays honest.
 */
function relativeTime(when: Date): string {
  const diffMs = Date.now() - when.getTime();
  const diffSec = Math.max(0, Math.round(diffMs / 1000));
  if (diffSec < 60) return `${diffSec}s ago`;
  const diffMin = Math.round(diffSec / 60);
  if (diffMin < 60) return `${diffMin}m ago`;
  const diffHr = Math.round(diffMin / 60);
  if (diffHr < 24) return `${diffHr}h ago`;
  return when.toLocaleDateString();
}

function EmptyState({ onCreateClick }: { onCreateClick: () => void }) {
  return (
    <div className="rounded-xl border border-dashed border-border/60 bg-bg/30 px-5 py-6 text-center">
      <p className="text-sm text-text">No alert rules yet.</p>
      <p className="mt-1 text-xs text-muted">
        Alert rules filter the webhook firehose by geography. Two predicate
        kinds: <code className="font-mono">point</code> (predicate over the
        value sampled at one (lat, lng)) and{" "}
        <code className="font-mono">polygon</code> (predicate over a reducer's
        output — max / mean / min / count_ge — over the cells inside a
        polygon).
      </p>
      <button
        type="button"
        onClick={onCreateClick}
        className="mt-3 inline-flex rounded-md border border-accent bg-accent/15 px-3 py-1.5 font-mono text-[11px] uppercase tracking-wide text-accent hover:bg-accent/25"
      >
        + New alert rule
      </button>
    </div>
  );
}
