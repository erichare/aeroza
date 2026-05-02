"use client";

import { useEffect, useMemo, useState } from "react";

import { fetchMrmsFiles, type MrmsFileItem } from "@/lib/api";

import { Panel } from "./Panel";
import { StatusDot } from "./StatusDot";

const POLL_MS = 30_000;
const DEFAULT_LIMIT = 60;

export function MrmsFilesPanel() {
  const [product, setProduct] = useState("MergedReflectivityComposite");
  const [level, setLevel] = useState("00.50");
  const [items, setItems] = useState<MrmsFileItem[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [lastFetched, setLastFetched] = useState<number | null>(null);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      try {
        setLoading(true);
        const data = await fetchMrmsFiles({
          product: product || undefined,
          level: level || undefined,
          limit: DEFAULT_LIMIT,
        });
        if (cancelled) return;
        setItems(data.items);
        setError(null);
        setLastFetched(Date.now());
      } catch (err) {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : "Unknown error");
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    };
    void load();
    const interval = setInterval(load, POLL_MS);
    return () => {
      cancelled = true;
      clearInterval(interval);
    };
  }, [product, level]);

  const stats = useMemo(() => buildStats(items), [items]);

  return (
    <Panel
      title="MRMS · file catalog"
      subtitle="GET /v1/mrms/files · populated by aeroza-ingest-mrms"
      actions={
        <span className="text-[11px] text-muted">
          {loading ? (
            <StatusDot tone="warning" label="Loading…" />
          ) : error ? (
            <StatusDot tone="danger" label="Error" />
          ) : (
            <StatusDot tone="success" label={`${items.length} files`} />
          )}
        </span>
      }
    >
      <div className="flex flex-col gap-3 p-5">
        <div className="flex flex-wrap items-center gap-2">
          <Field label="Product">
            <input
              value={product}
              onChange={(e) => setProduct(e.target.value)}
              className="w-56 rounded-md border border-border/70 bg-bg/60 px-2 py-1 text-xs text-text outline-none focus:border-accent/60"
              placeholder="MergedReflectivityComposite"
            />
          </Field>
          <Field label="Level">
            <input
              value={level}
              onChange={(e) => setLevel(e.target.value)}
              className="w-24 rounded-md border border-border/70 bg-bg/60 px-2 py-1 text-xs text-text outline-none focus:border-accent/60"
              placeholder="00.50"
            />
          </Field>
          <span className="ml-auto font-mono text-[11px] text-muted">
            {lastFetched
              ? `last fetched ${new Date(lastFetched).toLocaleTimeString()}`
              : "—"}
          </span>
        </div>

        {error ? (
          <div className="rounded-md border border-danger/40 bg-danger/10 px-3 py-2 text-xs text-danger">
            {error}
          </div>
        ) : null}

        <Timeline items={items} />

        <FilesTable items={items} stats={stats} />
      </div>
    </Panel>
  );
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <label className="flex flex-col gap-1">
      <span className="text-[10px] font-medium uppercase tracking-wide text-muted">
        {label}
      </span>
      {children}
    </label>
  );
}

interface CatalogStats {
  totalBytes: number;
  earliest: Date | null;
  latest: Date | null;
  cadenceSeconds: number | null;
}

function buildStats(items: MrmsFileItem[]): CatalogStats {
  if (items.length === 0) {
    return { totalBytes: 0, earliest: null, latest: null, cadenceSeconds: null };
  }
  // `items` is ordered newest-first by the API.
  const totalBytes = items.reduce((sum, i) => sum + i.sizeBytes, 0);
  const latest = new Date(items[0].validAt);
  const earliest = new Date(items[items.length - 1].validAt);
  const span = (latest.getTime() - earliest.getTime()) / 1000;
  const cadenceSeconds = items.length > 1 ? span / (items.length - 1) : null;
  return { totalBytes, earliest, latest, cadenceSeconds };
}

function Timeline({ items }: { items: MrmsFileItem[] }) {
  if (items.length === 0) {
    return (
      <div className="rounded-xl border border-border/60 bg-bg/40 px-3 py-6 text-center text-xs text-muted">
        No catalog rows. Try `aeroza-ingest-mrms --once`.
      </div>
    );
  }
  // Compute relative offsets so the most recent file pins to the right edge.
  const latest = new Date(items[0].validAt).getTime();
  const earliest = new Date(items[items.length - 1].validAt).getTime();
  const span = Math.max(latest - earliest, 1);
  return (
    <div className="rounded-xl border border-border/60 bg-bg/40 px-3 py-3">
      <div className="mb-2 flex items-center justify-between text-[10px] uppercase tracking-wide text-muted">
        <span>{new Date(earliest).toLocaleString()}</span>
        <span>timeline · valid_at</span>
        <span>{new Date(latest).toLocaleString()}</span>
      </div>
      <div className="relative h-10 w-full overflow-hidden rounded-md bg-border/30">
        {items.map((item) => {
          const t = new Date(item.validAt).getTime();
          const offset = ((t - earliest) / span) * 100;
          return (
            <span
              key={item.key}
              className="absolute top-1 h-8 w-[2px] bg-accent/70"
              style={{ left: `${offset}%` }}
              title={`${item.product} ${item.level} · ${new Date(item.validAt).toISOString()}`}
            />
          );
        })}
      </div>
    </div>
  );
}

function FilesTable({ items, stats }: { items: MrmsFileItem[]; stats: CatalogStats }) {
  return (
    <div className="rounded-xl border border-border/60 bg-bg/40">
      <div className="grid grid-cols-3 gap-3 border-b border-border/60 px-3 py-2 text-[11px] text-muted">
        <span>
          <span className="text-muted/70">cadence:</span>{" "}
          <span className="font-mono text-text">
            {stats.cadenceSeconds != null ? `${stats.cadenceSeconds.toFixed(1)}s` : "—"}
          </span>
        </span>
        <span>
          <span className="text-muted/70">total bytes:</span>{" "}
          <span className="font-mono text-text">{formatBytes(stats.totalBytes)}</span>
        </span>
        <span className="text-right">
          <span className="text-muted/70">latest:</span>{" "}
          <span className="font-mono text-text">
            {stats.latest ? formatRelative(stats.latest) : "—"}
          </span>
        </span>
      </div>
      <div className="max-h-80 overflow-y-auto">
        {items.length === 0 ? (
          <div className="px-3 py-6 text-center text-xs text-muted">No rows.</div>
        ) : (
          <table className="w-full text-left text-xs">
            <thead className="sticky top-0 bg-surface/90 backdrop-blur">
              <tr className="text-[10px] uppercase tracking-wide text-muted">
                <th className="px-3 py-2 font-medium">Valid at</th>
                <th className="px-3 py-2 font-medium">Product / level</th>
                <th className="px-3 py-2 font-medium">Size</th>
                <th className="px-3 py-2 font-medium">Key</th>
              </tr>
            </thead>
            <tbody>
              {items.map((item) => (
                <tr
                  key={item.key}
                  className="border-t border-border/40 hover:bg-bg/60"
                >
                  <td className="whitespace-nowrap px-3 py-1.5 font-mono text-[11px] text-text">
                    {new Date(item.validAt).toLocaleTimeString()}
                  </td>
                  <td className="px-3 py-1.5">
                    <span className="text-text">{item.product}</span>{" "}
                    <span className="text-muted">· {item.level}</span>
                  </td>
                  <td className="whitespace-nowrap px-3 py-1.5 font-mono text-[11px] text-muted">
                    {formatBytes(item.sizeBytes)}
                  </td>
                  <td className="max-w-[280px] truncate px-3 py-1.5 font-mono text-[10px] text-muted/80">
                    {item.key.split("/").pop()}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB"];
  const i = Math.min(
    Math.floor(Math.log(bytes) / Math.log(1024)),
    units.length - 1,
  );
  return `${(bytes / 1024 ** i).toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
}

function formatRelative(date: Date): string {
  const seconds = Math.floor((Date.now() - date.getTime()) / 1000);
  if (seconds < 0) return "future";
  if (seconds < 60) return `${seconds}s ago`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86_400) return `${Math.floor(seconds / 3600)}h ago`;
  return `${Math.floor(seconds / 86_400)}d ago`;
}
