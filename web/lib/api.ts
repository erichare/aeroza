/**
 * Tiny FastAPI client. No SWR, no axios — just fetch + types.
 *
 * The console runs at `:3000` and talks to FastAPI at `:8000` by default.
 * `NEXT_PUBLIC_AEROZA_API_URL` overrides the base for staging / remote testing.
 */

export const API_BASE: string =
  process.env.NEXT_PUBLIC_AEROZA_API_URL ?? "http://localhost:8000";

// ---------------------------------------------------------------------------
// Wire types — mirror the FastAPI/pydantic schemas. Kept hand-rolled (no
// codegen) because the surface is small and codegen would dwarf the diff.
// ---------------------------------------------------------------------------

export type Severity = "Unknown" | "Minor" | "Moderate" | "Severe" | "Extreme";

export interface AlertProperties {
  id: string;
  event: string;
  headline: string | null;
  severity: Severity;
  urgency: string;
  certainty: string;
  senderName: string | null;
  areaDesc: string | null;
  effective: string | null;
  onset: string | null;
  expires: string | null;
  ends: string | null;
}

export interface AlertDetailProperties extends AlertProperties {
  description: string | null;
  instruction: string | null;
}

/**
 * Minimal GeoJSON Geometry shape — `coordinates` is intentionally `unknown`
 * (the real schema is a recursive union of arrays of numbers). The console
 * only renders metadata; if/when we add a map, switch to `@types/geojson`.
 */
export interface GeoJsonGeometry {
  type: string;
  coordinates: unknown;
}

export interface AlertFeature<P = AlertProperties> {
  type: "Feature";
  geometry: GeoJsonGeometry | null;
  properties: P;
}

export interface AlertFeatureCollection {
  type: "FeatureCollection";
  features: AlertFeature[];
}

export interface MrmsFileItem {
  key: string;
  product: string;
  level: string;
  validAt: string;
  sizeBytes: number;
  etag: string | null;
}

export interface MrmsFileList {
  items: MrmsFileItem[];
}

export interface MrmsGridItem {
  fileKey: string;
  product: string;
  level: string;
  validAt: string;
  zarrUri: string;
  variable: string;
  dims: string[];
  shape: number[];
  dtype: string;
  nbytes: number;
  materialisedAt: string;
}

export interface MrmsGridList {
  items: MrmsGridItem[];
}

export interface Health {
  status: string;
  version: string;
}

// ---------------------------------------------------------------------------
// Fetchers
// ---------------------------------------------------------------------------

class ApiError extends Error {
  constructor(
    public readonly status: number,
    message: string,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

async function getJson<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    ...init,
    cache: "no-store",
    headers: { Accept: "application/json", ...init?.headers },
  });
  if (!res.ok) {
    throw new ApiError(res.status, `${res.status} ${res.statusText} — ${path}`);
  }
  return (await res.json()) as T;
}

export async function fetchHealth(): Promise<Health> {
  return getJson<Health>("/health");
}

export interface AlertQuery {
  severity?: Severity;
  bbox?: string;
  point?: string;
  limit?: number;
}

export async function fetchAlerts(q: AlertQuery = {}): Promise<AlertFeatureCollection> {
  const params = new URLSearchParams();
  if (q.severity) params.set("severity", q.severity);
  if (q.bbox) params.set("bbox", q.bbox);
  if (q.point) params.set("point", q.point);
  if (q.limit) params.set("limit", String(q.limit));
  const qs = params.toString();
  return getJson<AlertFeatureCollection>(`/v1/alerts${qs ? `?${qs}` : ""}`);
}

export interface MrmsQuery {
  product?: string;
  level?: string;
  since?: string;
  until?: string;
  limit?: number;
}

export async function fetchMrmsFiles(q: MrmsQuery = {}): Promise<MrmsFileList> {
  const params = new URLSearchParams();
  if (q.product) params.set("product", q.product);
  if (q.level) params.set("level", q.level);
  if (q.since) params.set("since", q.since);
  if (q.until) params.set("until", q.until);
  if (q.limit) params.set("limit", String(q.limit));
  const qs = params.toString();
  return getJson<MrmsFileList>(`/v1/mrms/files${qs ? `?${qs}` : ""}`);
}

export async function fetchMrmsGrids(q: MrmsQuery = {}): Promise<MrmsGridList> {
  const params = new URLSearchParams();
  if (q.product) params.set("product", q.product);
  if (q.level) params.set("level", q.level);
  if (q.since) params.set("since", q.since);
  if (q.until) params.set("until", q.until);
  if (q.limit) params.set("limit", String(q.limit));
  const qs = params.toString();
  return getJson<MrmsGridList>(`/v1/mrms/grids${qs ? `?${qs}` : ""}`);
}

export { ApiError };
