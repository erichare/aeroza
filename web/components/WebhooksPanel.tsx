"use client";

import { useEffect, useState } from "react";

import {
  type WebhookStatus,
  type WebhookSubscriptionRedacted,
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

/**
 * Webhooks panel — read-only view of `/v1/webhooks`.
 *
 * Lists every subscription registered with the dispatcher. The
 * full CRUD surface lives on the server; the editor is roadmap.
 * For now this is enough to confirm "yes, my POST landed" and to
 * eyeball status transitions when the dispatcher's circuit
 * breaker flips a sub to `disabled`.
 */
export function WebhooksPanel() {
  const [items, setItems] = useState<WebhookSubscriptionRedacted[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      try {
        const data = await fetchWebhooks({ limit: 50 });
        if (cancelled) return;
        setItems(data.items);
        setError(null);
      } catch (err) {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : "Failed to load webhooks");
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
      title="Webhooks · subscriptions"
      subtitle="GET /v1/webhooks · refreshed every 30 s"
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
            {items.map((sub) => (
              <SubscriptionRow key={sub.id} sub={sub} />
            ))}
          </ul>
        ) : null}
      </div>
    </Panel>
  );
}

function SubscriptionRow({ sub }: { sub: WebhookSubscriptionRedacted }) {
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
      <div className="text-right font-mono text-[10px] text-muted/70">
        <div>{new Date(sub.createdAt).toLocaleDateString()}</div>
        <div className="break-all">{sub.id.slice(0, 8)}…</div>
      </div>
    </li>
  );
}

function EmptyState() {
  return (
    <div className="rounded-xl border border-dashed border-border/60 bg-bg/30 px-5 py-6 text-center">
      <p className="text-sm text-muted">No webhook subscriptions yet.</p>
      <p className="mt-1 text-xs text-muted/70">
        Create one with <code className="font-mono">POST /v1/webhooks</code> or
        the <a
          href="http://localhost:8000/docs#/webhooks"
          target="_blank"
          rel="noreferrer"
          className="text-accent underline"
        >Swagger UI</a>.
      </p>
    </div>
  );
}
