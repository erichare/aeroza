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

// ---------------------------------------------------------------------------
// Auth
//
// The bearer-token format on the wire is `Authorization: Bearer aza_live_*`.
// `Me` is what `GET /v1/me` returns — the calling key's metadata, redacted
// to what the caller already knows.

export interface Me {
  type: "Me";
  name: string;
  prefix: string;
  owner: string;
  scopes: string[];
  rateLimitClass: string;
  lastUsedAt: string | null;
}

// ---------------------------------------------------------------------------
// METAR — surface observations from the Aviation Weather Center JSON API.
//
// Measurement fields are all nullable: AWC drops sensor readings that
// weren't reporting, but the row itself (with stationId + observationTime)
// is persisted so "last seen" queries are accurate.

export interface MetarObservation {
  type: "MetarObservation";
  stationId: string;
  observationTime: string;
  latitude: number;
  longitude: number;
  rawText: string;
  tempC: number | null;
  dewpointC: number | null;
  windSpeedKt: number | null;
  windDirectionDeg: number | null;
  windGustKt: number | null;
  visibilitySm: number | null;
  altimeterHpa: number | null;
  flightCategory: string | null;
}

export interface MetarObservationList {
  type: "MetarObservationList";
  items: MetarObservation[];
}

export interface MetarQuery {
  /** ICAO 4-letter id (case-insensitive). */
  station?: string;
  /** ISO-8601 lower bound (inclusive) on observationTime. */
  since?: string;
  /** ISO-8601 upper bound (exclusive) on observationTime. */
  until?: string;
  /** `min_lng,min_lat,max_lng,max_lat` — same convention as alerts. */
  bbox?: string;
  /** Default 100, max 500. */
  limit?: number;
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
 *
 * Categorical fields (`pod`, `far`, `csi`) are computed from the summed
 * contingency table and are nullable: `null` means the contributing rows
 * had no categorical metrics, or scored at mismatched thresholds, or the
 * denominator was zero.
 *
 * Probabilistic fields (`brierMean`, `crpsMean`, `ensembleSize`) are
 * populated only when at least one ensemble nowcast contributed to the
 * bucket. `brierMean` and `crpsMean` are sample-weighted across only
 * those ensemble rows; `brierSampleCount` is the cell count behind the
 * means. `ensembleSize` is `null` when rows in the bucket used different
 * ensemble sizes.
 */
export interface CalibrationItem {
  algorithm: string;
  forecastHorizonMinutes: number;
  verificationCount: number;
  sampleCount: number;
  maeMean: number;
  biasMean: number;
  rmseMean: number;
  thresholdDbz: number | null;
  pod: number | null;
  far: number | null;
  csi: number | null;
  ensembleSize: number | null;
  brierSampleCount: number;
  brierMean: number | null;
  crpsMean: number | null;
}

/**
 * One bin of a reliability diagram. `lower` is the inclusive lower
 * edge of the forecast-probability bucket (`upper = lower + 1/N`).
 * `observedFrequency` is the chart's y-coordinate — `null` when the
 * bin is empty so the UI can skip the dot.
 */
export interface ReliabilityBin {
  lower: number;
  count: number;
  observed: number;
  meanProb: number;
  observedFrequency: number | null;
}

/**
 * Per-(algorithm × horizon) reliability diagram data. Surfaced as
 * part of the `/v1/calibration` response so the UI can render the
 * diagram alongside the matrix without a second fetch.
 */
export interface ReliabilityRow {
  algorithm: string;
  forecastHorizonMinutes: number;
  bins: ReliabilityBin[];
}

export interface CalibrationResponse {
  type: "Calibration";
  generatedAt: string;
  windowHours: number;
  items: CalibrationItem[];
  /** Empty when no ensemble row contributed to the window. */
  reliability: ReliabilityRow[];
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
  thresholdDbz: number | null;
  pod: number | null;
  far: number | null;
  csi: number | null;
  ensembleSize: number | null;
  brierSampleCount: number;
  brierMean: number | null;
  crpsMean: number | null;
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
// Webhooks
//
// The list endpoint returns secret-redacted shapes; the create response
// returns the full shape with `secret` set once. Patches are partial.

export type WebhookStatus = "active" | "paused" | "disabled";

/** List/get response — no `secret`. */
export interface WebhookSubscriptionRedacted {
  type: "WebhookSubscriptionRedacted";
  id: string;
  url: string;
  events: string[];
  description: string | null;
  status: WebhookStatus;
  createdAt: string;
  updatedAt: string;
}

/** Create response — includes the freshly-minted signing secret. */
export interface WebhookSubscription {
  type: "WebhookSubscription";
  id: string;
  url: string;
  events: string[];
  description: string | null;
  status: WebhookStatus;
  /** Shown once at creation; subsequent reads omit this. */
  secret: string;
  createdAt: string;
  updatedAt: string;
}

export interface WebhookSubscriptionList {
  type: "WebhookSubscriptionList";
  items: WebhookSubscriptionRedacted[];
}

export interface WebhookSubscriptionQuery {
  status?: WebhookStatus;
  limit?: number;
}

// ---------------------------------------------------------------------------
// Alert rules (webhook predicate DSL)

export type AlertRuleStatus = "active" | "paused" | "disabled";
export type PredicateOp = ">" | ">=" | "<" | "<=" | "==" | "!=";
export type RuleConfigPolygonReducer = "max" | "mean" | "min" | "count_ge";

export interface Predicate {
  op: PredicateOp;
  threshold: number;
}

export interface PointRuleConfig {
  type: "point";
  product: string;
  level: string;
  predicate: Predicate;
  lat: number;
  lng: number;
}

export interface PolygonRuleConfig {
  type: "polygon";
  product: string;
  level: string;
  predicate: Predicate;
  /** Flat `lng,lat,lng,lat,…` (≥3 vertices, ring implicitly closed). */
  polygon: string;
  reducer: RuleConfigPolygonReducer;
  countThreshold?: number | null;
}

export type RuleConfig = PointRuleConfig | PolygonRuleConfig;

export interface AlertRule {
  type: "AlertRule";
  id: string;
  subscriptionId: string;
  name: string;
  description: string | null;
  status: AlertRuleStatus;
  config: RuleConfig;
  createdAt: string;
  updatedAt: string;
}

export interface AlertRuleList {
  type: "AlertRuleList";
  items: AlertRule[];
}

export interface AlertRuleQuery {
  status?: AlertRuleStatus;
  /** Filter to rules bound to this webhook subscription. */
  subscriptionId?: string;
  limit?: number;
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

// ---------------------------------------------------------------------------
// Admin — seed historical events (gated by AEROZA_DEV_ADMIN_ENABLED)

export type AdminSeedEventState = "running" | "succeeded" | "failed";

/**
 * Snapshot of a seed-event task as the admin endpoints expose it.
 * `state` is derived server-side from `finishedAt`/`error` so callers
 * don't have to compute it. The /demo "Seed this event" button polls
 * `getSeedEventStatus` until `state` flips to a terminal value.
 */
export interface AdminSeedEventTask {
  type: "AdminSeedEventTask";
  since: string;
  until: string;
  product: string;
  level: string;
  startedAt: string;
  finishedAt: string | null;
  cfgribAvailable: boolean;
  filesInserted: number;
  filesUpdated: number;
  gridsMaterialised: number;
  error: string | null;
  state: AdminSeedEventState;
}

export interface AdminSeedEventRequest {
  /** ISO-8601 tz-aware lower bound (inclusive). */
  since: string;
  /** ISO-8601 tz-aware upper bound (exclusive). */
  until: string;
  /** Defaults to MergedReflectivityComposite. */
  product?: string;
  /** Defaults to "00.50". */
  level?: string;
}

export interface AdminSeedEventStatusQuery {
  since: string;
  until: string;
  product?: string;
  level?: string;
}
