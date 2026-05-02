/**
 * Wire shapes for the Aeroza v1 API.
 *
 * Mirrors the FastAPI / pydantic schemas. Kept hand-rolled (no codegen)
 * because the surface is small and codegen output would dwarf the
 * interesting code. When the API surface gets bigger or we ship a
 * non-TS SDK, switch to OpenAPI codegen and delete this module.
 */

export type Severity =
  | "Unknown"
  | "Minor"
  | "Moderate"
  | "Severe"
  | "Extreme";

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
 * Minimal GeoJSON Geometry shape — `coordinates` is intentionally
 * `unknown` (the real schema is a recursive union of arrays of numbers).
 * Switch to `@types/geojson` once a consumer needs structural geometry
 * access.
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

export interface AlertDetailFeature
  extends AlertFeature<AlertDetailProperties> {}

export interface MrmsFileItem {
  key: string;
  product: string;
  level: string;
  validAt: string;
  sizeBytes: number;
  etag: string | null;
}

export interface MrmsFileList {
  type: "MrmsFileList";
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
  type: "MrmsGridList";
  items: MrmsGridItem[];
}

export interface MrmsGridSample {
  type: "MrmsGridSample";
  fileKey: string;
  product: string;
  level: string;
  validAt: string;
  variable: string;
  value: number;
  requestedLatitude: number;
  requestedLongitude: number;
  matchedLatitude: number;
  matchedLongitude: number;
  toleranceDeg: number;
}

export type PolygonReducer = "max" | "mean" | "min" | "count_ge";

export interface MrmsGridPolygonSample {
  type: "MrmsGridPolygonSample";
  fileKey: string;
  product: string;
  level: string;
  validAt: string;
  variable: string;
  reducer: PolygonReducer;
  threshold: number | null;
  value: number;
  cellCount: number;
  vertexCount: number;
  bboxMinLatitude: number;
  bboxMinLongitude: number;
  bboxMaxLatitude: number;
  bboxMaxLongitude: number;
}

export interface Health {
  status: string;
  version: string;
}

export interface AlertsStats {
  total: number;
  active: number;
  latestExpires: string | null;
}

export interface MrmsStats {
  files: number;
  gridsMaterialised: number;
  filesPending: number;
  latestValidAt: string | null;
  latestGridMaterialisedAt: string | null;
}

export interface Stats {
  type: "Stats";
  generatedAt: string;
  alerts: AlertsStats;
  mrms: MrmsStats;
}

// ---------------------------------------------------------------------------
// Calibration

/**
 * One row of the calibration aggregate. Per (algorithm × forecast horizon)
 * over the requested window. Sample-weighted means: a verification with
 * N=1M cells contributes N times to maeMean / biasMean / rmseMean.
 */
export interface CalibrationItem {
  algorithm: string;
  forecastHorizonMinutes: number;
  verificationCount: number;
  sampleCount: number;
  maeMean: number;
  biasMean: number;
  rmseMean: number;
}

export interface CalibrationResponse {
  type: "Calibration";
  generatedAt: string;
  windowHours: number;
  items: CalibrationItem[];
}

export interface CalibrationQuery {
  /** Default: 24. Server clamps to ≤ 720 (30 days). */
  windowHours?: number;
  algorithm?: string;
  product?: string;
  level?: string;
}

/**
 * One time-bucket of metrics on a calibration sparkline.
 * `bucketStart` is the inclusive lower edge (ISO timestamp).
 */
export interface CalibrationSeriesPoint {
  bucketStart: string;
  verificationCount: number;
  sampleCount: number;
  maeMean: number;
  biasMean: number;
  rmseMean: number;
}

/** Per-(algorithm × horizon) sparkline. Points are oldest → newest. */
export interface CalibrationSeriesItem {
  algorithm: string;
  forecastHorizonMinutes: number;
  points: CalibrationSeriesPoint[];
}

export interface CalibrationSeriesResponse {
  type: "CalibrationSeries";
  generatedAt: string;
  windowHours: number;
  bucketSeconds: number;
  items: CalibrationSeriesItem[];
}

export interface CalibrationSeriesQuery {
  /** Default: 24h. */
  windowHours?: number;
  /** Default: 3600 (1 hour). Server allows [300, 86400]. */
  bucketSeconds?: number;
  algorithm?: string;
  product?: string;
  level?: string;
}

// ---------------------------------------------------------------------------
// Query parameters

export interface AlertQuery {
  severity?: Severity;
  bbox?: string;
  point?: string;
  limit?: number;
}

export interface MrmsQuery {
  product?: string;
  level?: string;
  since?: string;
  until?: string;
  limit?: number;
}

export interface SampleQuery {
  lat: number;
  lng: number;
  product?: string;
  level?: string;
  atTime?: string;
  toleranceDeg?: number;
}

export interface PolygonQuery {
  /** Flat `lng,lat,lng,lat,…` — ≥3 vertices, ring implicitly closed. */
  polygon: string;
  reducer?: PolygonReducer;
  /** Required when `reducer === "count_ge"`. */
  threshold?: number;
  product?: string;
  level?: string;
  atTime?: string;
}
