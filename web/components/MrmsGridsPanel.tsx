"use client";

import { useEffect, useMemo, useState } from "react";

import { fetchMrmsGrids, type MrmsGridItem } from "@/lib/api";
import { formatBytes, formatRelative } from "@/lib/format";

import { Panel } from "./Panel";
import { StatusDot } from "./StatusDot";

const POLL_MS = 30_000;
const DEFAULT_LIMIT = 60;

export function MrmsGridsPanel() {
  const [product, setProduct] = useState("MergedReflectivityComposite");
  const [level, setLevel] = useState("00.50");
  const [items, setItems] = useState<MrmsGridItem[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [lastFetched, setLastFetched] = useState<number | null>(null);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      try {
        setLoading(true);
        const data = await fetchMrmsGrids({
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
      title="MRMS · materialised grids"
      subtitle="GET /v1/mrms/grids · populated by aeroza-materialise-mrms"
      actions={
        <span className="text-[11px] text-muted">
          {loading ? (
            <StatusDot tone="warning" label="Loading…" />
          ) : error ? (
            <StatusDot tone="danger" label="Error" />
          ) : (
            <StatusDot tone="success" label={`${items.length} grids`} />
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

        <StatsRow stats={stats} count={items.length} />

        <GridsTable items={items} />
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
  totalNbytes: number;
  latestMaterialisedAt: Date | null;
  uniqueShapes: number;
  uniqueDtypes: number;
}

function buildStats(items: MrmsGridItem[]): CatalogStats {
  if (items.length === 0) {
    return {
      totalNbytes: 0,
      latestMaterialisedAt: null,
      uniqueShapes: 0,
      uniqueDtypes: 0,
    };
  }
  const totalNbytes = items.reduce((sum, i) => sum + i.nbytes, 0);
  const latestMaterialisedAt = items
    .map((i) => new Date(i.materialisedAt))
    .reduce((a, b) => (a.getTime() > b.getTime() ? a : b));
  const uniqueShapes = new Set(items.map((i) => i.shape.join("x"))).size;
  const uniqueDtypes = new Set(items.map((i) => i.dtype)).size;
  return { totalNbytes, latestMaterialisedAt, uniqueShapes, uniqueDtypes };
}

function StatsRow({ stats, count }: { stats: CatalogStats; count: number }) {
  return (
    <div className="grid grid-cols-2 gap-2 sm:grid-cols-4">
      <Stat label="grids" value={String(count)} />
      <Stat label="total bytes" value={formatBytes(stats.totalNbytes)} mono />
      <Stat
        label="last materialised"
        value={
          stats.latestMaterialisedAt ? formatRelative(stats.latestMaterialisedAt) : "—"
        }
      />
      <Stat
        label="unique shape · dtype"
        value={`${stats.uniqueShapes} · ${stats.uniqueDtypes}`}
      />
    </div>
  );
}

function Stat({
  label,
  value,
  mono = false,
}: {
  label: string;
  value: string;
  mono?: boolean;
}) {
  return (
    <div className="flex flex-col gap-1 rounded-lg border border-border/60 bg-bg/40 px-3 py-2">
      <span className="text-[10px] uppercase tracking-wide text-muted">{label}</span>
      <span
        className={[
          "truncate text-text",
          mono ? "font-mono text-[12px]" : "text-sm font-medium",
        ].join(" ")}
      >
        {value}
      </span>
    </div>
  );
}

function GridsTable({ items }: { items: MrmsGridItem[] }) {
  if (items.length === 0) {
    return (
      <div className="rounded-xl border border-border/60 bg-bg/40 px-3 py-6 text-center text-xs text-muted">
        No materialised grids. Try{" "}
        <span className="font-mono text-text">aeroza-materialise-mrms --once</span>.
      </div>
    );
  }
  return (
    <div className="rounded-xl border border-border/60 bg-bg/40">
      <div className="max-h-80 overflow-y-auto">
        <table className="w-full text-left text-xs">
          <thead className="sticky top-0 bg-surface/90 backdrop-blur">
            <tr className="text-[10px] uppercase tracking-wide text-muted">
              <th className="px-3 py-2 font-medium">Valid at</th>
              <th className="px-3 py-2 font-medium">Variable</th>
              <th className="px-3 py-2 font-medium">Shape · dtype</th>
              <th className="px-3 py-2 font-medium">Bytes</th>
              <th className="px-3 py-2 font-medium">Zarr URI</th>
            </tr>
          </thead>
          <tbody>
            {items.map((item) => (
              <tr
                key={item.fileKey}
                className="border-t border-border/40 hover:bg-bg/60"
              >
                <td className="whitespace-nowrap px-3 py-1.5 font-mono text-[11px] text-text">
                  {new Date(item.validAt).toLocaleTimeString()}
                </td>
                <td className="whitespace-nowrap px-3 py-1.5 text-text">
                  {item.variable}
                </td>
                <td className="whitespace-nowrap px-3 py-1.5 font-mono text-[11px] text-muted">
                  {item.shape.join("×")}{" "}
                  <span className="text-muted/70">· {item.dtype}</span>
                </td>
                <td className="whitespace-nowrap px-3 py-1.5 font-mono text-[11px] text-muted">
                  {formatBytes(item.nbytes)}
                </td>
                <td className="max-w-[280px] truncate px-3 py-1.5 font-mono text-[10px] text-muted/80">
                  {item.zarrUri}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

