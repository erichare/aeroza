"use client";

import { useState } from "react";

import { ApiError, fetchMrmsSample, type MrmsGridSample } from "@/lib/api";
import { formatRelative } from "@/lib/format";

import { Panel } from "./Panel";
import { StatusDot } from "./StatusDot";

const DEFAULT_LAT = 29.76;
const DEFAULT_LNG = -95.37;
const DEFAULT_PRODUCT = "MergedReflectivityComposite";
const DEFAULT_LEVEL = "00.50";
const DEFAULT_TOLERANCE = 0.05;

type SampleStatus =
  | { kind: "idle" }
  | { kind: "loading" }
  | { kind: "ok"; sample: MrmsGridSample; queriedAt: number }
  | { kind: "error"; status: number | null; message: string };

export function SamplePanel() {
  const [lat, setLat] = useState<string>(String(DEFAULT_LAT));
  const [lng, setLng] = useState<string>(String(DEFAULT_LNG));
  const [product, setProduct] = useState<string>(DEFAULT_PRODUCT);
  const [level, setLevel] = useState<string>(DEFAULT_LEVEL);
  const [toleranceDeg, setToleranceDeg] = useState<string>(String(DEFAULT_TOLERANCE));
  const [status, setStatus] = useState<SampleStatus>({ kind: "idle" });

  const handleSubmit = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    const latNum = Number.parseFloat(lat);
    const lngNum = Number.parseFloat(lng);
    const tol = Number.parseFloat(toleranceDeg);
    if (!Number.isFinite(latNum) || !Number.isFinite(lngNum) || !Number.isFinite(tol)) {
      setStatus({ kind: "error", status: null, message: "lat, lng, and tolerance must be numbers" });
      return;
    }
    setStatus({ kind: "loading" });
    try {
      const sample = await fetchMrmsSample({
        lat: latNum,
        lng: lngNum,
        product: product || undefined,
        level: level || undefined,
        toleranceDeg: tol,
      });
      setStatus({ kind: "ok", sample, queriedAt: Date.now() });
    } catch (err) {
      const apiStatus = err instanceof ApiError ? err.status : null;
      const message = err instanceof Error ? err.message : "Unknown error";
      setStatus({ kind: "error", status: apiStatus, message });
    }
  };

  return (
    <Panel
      title="MRMS · point sample"
      subtitle="GET /v1/mrms/grids/sample · nearest-cell value at (lat, lng)"
      actions={<HeaderStatus status={status} />}
    >
      <form
        onSubmit={handleSubmit}
        className="flex flex-col gap-4 p-5"
      >
        <div className="grid grid-cols-2 gap-3 sm:grid-cols-5">
          <Field label="Latitude">
            <NumberInput value={lat} onChange={setLat} placeholder={String(DEFAULT_LAT)} />
          </Field>
          <Field label="Longitude">
            <NumberInput value={lng} onChange={setLng} placeholder={String(DEFAULT_LNG)} />
          </Field>
          <Field label="Product">
            <TextInput value={product} onChange={setProduct} placeholder={DEFAULT_PRODUCT} />
          </Field>
          <Field label="Level">
            <TextInput value={level} onChange={setLevel} placeholder={DEFAULT_LEVEL} />
          </Field>
          <Field label="Tolerance °">
            <NumberInput
              value={toleranceDeg}
              onChange={setToleranceDeg}
              placeholder={String(DEFAULT_TOLERANCE)}
              step="0.01"
            />
          </Field>
        </div>

        <div className="flex items-center gap-2">
          <button
            type="submit"
            disabled={status.kind === "loading"}
            className="rounded-md border border-accent/60 bg-accent/10 px-3 py-1.5 text-xs font-medium text-accent hover:bg-accent/20 disabled:opacity-50"
          >
            {status.kind === "loading" ? "Sampling…" : "Sample"}
          </button>
          <span className="text-[11px] text-muted">
            Default to Houston, TX. Try lat <span className="font-mono">29.76</span>, lng{" "}
            <span className="font-mono">-95.37</span>.
          </span>
        </div>

        <ResultArea status={status} />
      </form>
    </Panel>
  );
}

function HeaderStatus({ status }: { status: SampleStatus }) {
  if (status.kind === "loading") return <StatusDot tone="warning" label="Sampling…" />;
  if (status.kind === "ok") return <StatusDot tone="success" label="OK" pulse />;
  if (status.kind === "error") {
    if (status.status === 404) return <StatusDot tone="warning" label="No data" />;
    return <StatusDot tone="danger" label={`HTTP ${status.status ?? "?"}`} />;
  }
  return <StatusDot tone="muted" label="Idle" />;
}

function ResultArea({ status }: { status: SampleStatus }) {
  if (status.kind === "idle") {
    return (
      <div className="rounded-xl border border-border/60 bg-bg/40 px-3 py-6 text-center text-xs text-muted">
        Pick a point and click <span className="text-text">Sample</span>. The query reads from the
        latest materialised grid for the product · level you picked.
      </div>
    );
  }
  if (status.kind === "loading") {
    return (
      <div className="rounded-xl border border-border/60 bg-bg/40 px-3 py-6 text-center text-xs text-muted">
        Loading…
      </div>
    );
  }
  if (status.kind === "error") {
    const tone = status.status === 404 ? "warning" : "danger";
    const ringClass = tone === "warning"
      ? "border-warning/40 bg-warning/10 text-warning"
      : "border-danger/40 bg-danger/10 text-danger";
    return (
      <div className={`rounded-md border px-3 py-2 text-xs ${ringClass}`}>
        {status.message}
      </div>
    );
  }
  return <SampleResult sample={status.sample} queriedAt={status.queriedAt} />;
}

function SampleResult({ sample, queriedAt }: { sample: MrmsGridSample; queriedAt: number }) {
  const validAge = formatRelative(new Date(sample.validAt), queriedAt);
  return (
    <div className="rounded-xl border border-border/60 bg-bg/40 p-4">
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <div>
          <div className="text-[10px] uppercase tracking-wide text-muted">value</div>
          <div className="font-mono text-2xl font-semibold text-accent">
            {formatValue(sample.value)}
          </div>
          <div className="text-[11px] text-muted">{sample.variable}</div>
        </div>
        <div className="text-right">
          <div className="text-[10px] uppercase tracking-wide text-muted">valid_at</div>
          <div className="font-mono text-xs text-text">
            {new Date(sample.validAt).toLocaleString()}
          </div>
          <div className="text-[11px] text-muted">{validAge}</div>
        </div>
      </div>

      <dl className="mt-4 grid grid-cols-2 gap-3 text-xs sm:grid-cols-3">
        <DefItem label="requested" value={`${formatCoord(sample.requestedLatitude)}, ${formatCoord(sample.requestedLongitude)}`} />
        <DefItem label="matched cell" value={`${formatCoord(sample.matchedLatitude)}, ${formatCoord(sample.matchedLongitude)}`} accent />
        <DefItem label="tolerance" value={`${sample.toleranceDeg}°`} />
        <DefItem label="product" value={sample.product} />
        <DefItem label="level" value={sample.level} />
        <DefItem label="file_key" value={shortenKey(sample.fileKey)} mono small />
      </dl>
    </div>
  );
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <label className="flex flex-col gap-1">
      <span className="text-[10px] font-medium uppercase tracking-wide text-muted">{label}</span>
      {children}
    </label>
  );
}

interface NumberInputProps {
  value: string;
  onChange: (next: string) => void;
  placeholder?: string;
  step?: string;
}

function NumberInput({ value, onChange, placeholder, step }: NumberInputProps) {
  return (
    <input
      type="number"
      inputMode="decimal"
      step={step ?? "any"}
      value={value}
      onChange={(e) => onChange(e.target.value)}
      placeholder={placeholder}
      className="rounded-md border border-border/70 bg-bg/60 px-2 py-1 text-xs text-text outline-none focus:border-accent/60"
    />
  );
}

interface TextInputProps {
  value: string;
  onChange: (next: string) => void;
  placeholder?: string;
}

function TextInput({ value, onChange, placeholder }: TextInputProps) {
  return (
    <input
      type="text"
      value={value}
      onChange={(e) => onChange(e.target.value)}
      placeholder={placeholder}
      className="rounded-md border border-border/70 bg-bg/60 px-2 py-1 text-xs text-text outline-none focus:border-accent/60"
    />
  );
}

interface DefItemProps {
  label: string;
  value: string;
  accent?: boolean;
  mono?: boolean;
  small?: boolean;
}

function DefItem({ label, value, accent = false, mono = false, small = false }: DefItemProps) {
  return (
    <div className="flex min-w-0 flex-col gap-0.5">
      <dt className="text-[10px] uppercase tracking-wide text-muted">{label}</dt>
      <dd
        className={[
          "truncate",
          mono ? "font-mono" : "font-medium",
          small ? "text-[11px]" : "text-sm",
          accent ? "text-accent" : "text-text",
        ].join(" ")}
        title={value}
      >
        {value}
      </dd>
    </div>
  );
}

function formatValue(v: number): string {
  if (!Number.isFinite(v)) return "—";
  const abs = Math.abs(v);
  if (abs >= 100) return v.toFixed(1);
  if (abs >= 1) return v.toFixed(2);
  return v.toFixed(3);
}

function formatCoord(deg: number): string {
  return deg.toFixed(4);
}

function shortenKey(key: string): string {
  // Keys look like 'CONUS/Product_Level/YYYYMMDD/MRMS_..._HHMMSS.grib2.gz'.
  // The basename is what's recognisable; full key is preserved in the title attr.
  const base = key.split("/").pop();
  return base ?? key;
}
