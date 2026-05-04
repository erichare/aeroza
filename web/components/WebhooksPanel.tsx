"use client";

import { useCallback, useEffect, useState } from "react";

import {
  type WebhookStatus,
  type WebhookSubscription,
  type WebhookSubscriptionRedacted,
  createWebhook,
  deleteWebhook,
  fetchWebhooks,
} from "@/lib/api";

import { Panel } from "./Panel";
import { StatusDot } from "./StatusDot";

const REFRESH_INTERVAL_MS = 30_000;

const STATUS_TONE: Record<WebhookStatus, "success" | "warning" | "danger"> = {
  active: "success",
  paused: "warning",
  disabled: "danger",
};

// The two NATS subjects the dispatcher fans out today. Mirrored from
// `aeroza.webhooks.schemas.WEBHOOK_EVENT_TYPES` — keeping a hand-rolled
// list here means we don't import server schemas into the browser bundle.
const SUPPORTED_EVENTS: ReadonlyArray<{ value: string; label: string }> = [
  { value: "aeroza.alerts.nws.new", label: "Newly-observed NWS alerts" },
  { value: "aeroza.nowcast.grids.new", label: "Newly-issued nowcasts" },
];

/**
 * Webhooks panel — list + create + delete against `/v1/webhooks`.
 *
 * Reads the current subscription list every 30 s, plus on demand
 * after a successful create or delete so the row count is honest.
 * The create form is inline (no modal) — the dev console favours
 * "everything you might do is on this page" over progressive
 * disclosure. Deletes are confirmed via the platform `confirm()`
 * dialog; we trust the user has a recent-enough browser.
 *
 * On create, the server returns a one-shot signing ``secret``. We
 * surface it once with a copy button and drop it from state on
 * dismissal — there's no second chance to read it.
 */
export function WebhooksPanel() {
  const [items, setItems] = useState<WebhookSubscriptionRedacted[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [showCreate, setShowCreate] = useState(false);
  // Last-created subscription's full record (with `secret`). Held in
  // state only — never persisted, never re-fetched. Cleared by the
  // user dismissing the confirmation banner.
  const [lastCreated, setLastCreated] = useState<WebhookSubscription | null>(
    null,
  );

  const load = useCallback(async () => {
    try {
      const data = await fetchWebhooks({ limit: 50 });
      setItems(data.items);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load webhooks");
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
    (created: WebhookSubscription) => {
      setLastCreated(created);
      setShowCreate(false);
      void load();
    },
    [load],
  );

  const handleDelete = useCallback(
    async (id: string) => {
      if (!window.confirm(`Delete webhook ${id.slice(0, 8)}…? This cannot be undone.`)) {
        return;
      }
      try {
        await deleteWebhook(id);
        // Optimistic refresh — we already know the row's gone, so
        // drop it locally to avoid the perceptible lag of waiting
        // on the next ``load()``.
        setItems((prev) => prev.filter((s) => s.id !== id));
        void load();
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to delete webhook");
      }
    },
    [load],
  );

  return (
    <Panel
      title="Webhooks · subscriptions"
      subtitle="GET / POST / DELETE /v1/webhooks · refreshed every 30 s"
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

        {lastCreated ? (
          <CreatedSecretBanner
            sub={lastCreated}
            onDismiss={() => setLastCreated(null)}
          />
        ) : null}

        {showCreate ? (
          <CreateForm onCreated={handleCreated} onError={setError} />
        ) : null}

        {!error && !isLoading && items.length === 0 && !showCreate ? (
          <EmptyState onCreateClick={() => setShowCreate(true)} />
        ) : null}

        {items.length > 0 ? (
          <ul className="divide-y divide-border/40">
            {items.map((sub) => (
              <SubscriptionRow key={sub.id} sub={sub} onDelete={handleDelete} />
            ))}
          </ul>
        ) : null}
      </div>
    </Panel>
  );
}

interface CreateFormProps {
  onCreated: (sub: WebhookSubscription) => void;
  onError: (message: string) => void;
}

function CreateForm({ onCreated, onError }: CreateFormProps) {
  const [url, setUrl] = useState("");
  const [description, setDescription] = useState("");
  const [events, setEvents] = useState<ReadonlySet<string>>(
    new Set([SUPPORTED_EVENTS[0].value]),
  );
  const [submitting, setSubmitting] = useState(false);

  const toggleEvent = (value: string): void => {
    setEvents((prev) => {
      const next = new Set(prev);
      if (next.has(value)) next.delete(value);
      else next.add(value);
      return next;
    });
  };

  const handleSubmit = useCallback(
    async (e: React.FormEvent<HTMLFormElement>) => {
      e.preventDefault();
      if (events.size === 0) {
        onError("Pick at least one event type to subscribe to.");
        return;
      }
      setSubmitting(true);
      try {
        const created = await createWebhook({
          url: url.trim(),
          events: Array.from(events),
          description: description.trim() || null,
        });
        onCreated(created);
      } catch (err) {
        onError(err instanceof Error ? err.message : "Create failed");
      } finally {
        setSubmitting(false);
      }
    },
    [url, description, events, onCreated, onError],
  );

  return (
    <form
      onSubmit={handleSubmit}
      className="mb-4 flex flex-col gap-3 rounded-xl border border-border/60 bg-bg/40 p-4"
    >
      <div className="flex flex-col gap-1">
        <label className="font-mono text-[10px] uppercase tracking-wider text-muted">
          URL <span className="text-warning">required</span>
        </label>
        <input
          type="url"
          required
          placeholder="https://example.com/aeroza-webhook"
          value={url}
          onChange={(e) => setUrl(e.target.value)}
          className="rounded-md border border-border/60 bg-surface/60 px-3 py-1.5 font-mono text-[12px] text-text outline-none focus:border-accent"
        />
      </div>
      <div className="flex flex-col gap-1">
        <label className="font-mono text-[10px] uppercase tracking-wider text-muted">
          Description (optional)
        </label>
        <input
          type="text"
          placeholder="Production alerts → PagerDuty"
          value={description}
          onChange={(e) => setDescription(e.target.value)}
          className="rounded-md border border-border/60 bg-surface/60 px-3 py-1.5 text-[12px] text-text outline-none focus:border-accent"
          maxLength={512}
        />
      </div>
      <fieldset className="flex flex-col gap-2">
        <legend className="font-mono text-[10px] uppercase tracking-wider text-muted">
          Events
        </legend>
        {SUPPORTED_EVENTS.map((opt) => {
          const checked = events.has(opt.value);
          return (
            <label
              key={opt.value}
              className="flex items-center gap-2 text-[12px] text-text"
            >
              <input
                type="checkbox"
                checked={checked}
                onChange={() => toggleEvent(opt.value)}
                className="accent-accent"
              />
              <code className="font-mono text-[11px] text-muted">
                {opt.value}
              </code>
              <span className="text-[11px] text-muted/80">— {opt.label}</span>
            </label>
          );
        })}
      </fieldset>
      <div className="flex items-center justify-end gap-2 pt-1">
        <button
          type="submit"
          disabled={submitting || !url.trim() || events.size === 0}
          className={[
            "rounded-md border px-3 py-1.5 font-mono text-[11px] uppercase tracking-wide",
            submitting || !url.trim() || events.size === 0
              ? "cursor-not-allowed border-border/40 text-muted/50"
              : "border-accent bg-accent/15 text-accent hover:bg-accent/25",
          ].join(" ")}
        >
          {submitting ? "Creating…" : "Create webhook"}
        </button>
      </div>
    </form>
  );
}

interface CreatedSecretBannerProps {
  sub: WebhookSubscription;
  onDismiss: () => void;
}

function CreatedSecretBanner({ sub, onDismiss }: CreatedSecretBannerProps) {
  const [copied, setCopied] = useState(false);
  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(sub.secret);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch {
      // Clipboard can be denied (insecure context, sandboxed iframe).
      // Fall through; the user can still triple-click the <code>.
    }
  };
  return (
    <div
      role="status"
      className="mb-3 flex flex-col gap-2 rounded-md border border-success/40 bg-success/10 px-3 py-2 text-[12px] text-success"
    >
      <div className="flex items-center justify-between">
        <strong className="font-semibold">Webhook created.</strong>
        <button
          type="button"
          onClick={onDismiss}
          className="font-mono text-[10px] uppercase tracking-wider text-success/80 hover:text-success"
        >
          Dismiss
        </button>
      </div>
      <p className="text-[11px] leading-relaxed text-success/90">
        Copy the signing secret now — it's shown <em>once</em> and never
        readable again. Use it to verify the{" "}
        <code className="font-mono">Aeroza-Signature</code> HMAC on
        incoming requests.
      </p>
      <div className="flex items-center gap-2">
        <code className="select-all flex-1 overflow-x-auto rounded-md border border-success/30 bg-bg/50 px-2 py-1 font-mono text-[11px] text-text">
          {sub.secret}
        </code>
        <button
          type="button"
          onClick={handleCopy}
          className="rounded-md border border-success/40 bg-success/15 px-2 py-1 font-mono text-[10px] uppercase tracking-wider text-success hover:bg-success/25"
        >
          {copied ? "Copied" : "Copy"}
        </button>
      </div>
    </div>
  );
}

interface SubscriptionRowProps {
  sub: WebhookSubscriptionRedacted;
  onDelete: (id: string) => Promise<void>;
}

function SubscriptionRow({ sub, onDelete }: SubscriptionRowProps) {
  return (
    <li className="grid grid-cols-[8rem_1fr_auto] items-start gap-3 py-2.5">
      <StatusDot tone={STATUS_TONE[sub.status]} label={sub.status} />
      <div className="min-w-0">
        <div className="truncate font-mono text-[12px] text-text">{sub.url}</div>
        {sub.description ? (
          <p className="mt-0.5 truncate text-[11px] text-muted">{sub.description}</p>
        ) : null}
        <div className="mt-1 flex flex-wrap gap-1.5">
          {sub.events.map((e) => (
            <span
              key={e}
              className="rounded-md border border-border/50 bg-bg/40 px-1.5 py-0.5 font-mono text-[10px] text-muted"
            >
              {e}
            </span>
          ))}
        </div>
      </div>
      <div className="flex flex-col items-end gap-1.5 text-right font-mono text-[10px] text-muted/70">
        <div>{new Date(sub.createdAt).toLocaleDateString()}</div>
        <div className="break-all">{sub.id.slice(0, 8)}…</div>
        <button
          type="button"
          onClick={() => void onDelete(sub.id)}
          aria-label={`Delete webhook ${sub.id}`}
          className="rounded-md border border-border/50 px-1.5 py-0.5 text-[10px] uppercase tracking-wider text-muted hover:border-warning/60 hover:text-warning"
        >
          Delete
        </button>
      </div>
    </li>
  );
}

function EmptyState({ onCreateClick }: { onCreateClick: () => void }) {
  return (
    <div className="rounded-xl border border-dashed border-border/60 bg-bg/30 px-5 py-6 text-center">
      <p className="text-sm text-text">No webhook subscriptions yet.</p>
      <p className="mt-1 text-xs text-muted">
        Webhooks fire on every <code className="font-mono">aeroza.alerts.nws.new</code>{" "}
        and <code className="font-mono">aeroza.nowcast.grids.new</code> event,
        signed with HMAC-SHA256 in the <code className="font-mono">Aeroza-Signature</code> header.
      </p>
      <button
        type="button"
        onClick={onCreateClick}
        className="mt-3 inline-flex rounded-md border border-accent bg-accent/15 px-3 py-1.5 font-mono text-[11px] uppercase tracking-wide text-accent hover:bg-accent/25"
      >
        + New webhook
      </button>
    </div>
  );
}
