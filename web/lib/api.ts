/**
 * Thin façade over `@aeroza/sdk` — the TypeScript SDK is the source of
 * truth for wire types and HTTP. This module:
 *
 * 1. Constructs a singleton `AerozaClient` configured for the dev console.
 * 2. Re-exports the SDK's wire types under the names the components
 *    have been using since before the SDK existed.
 * 3. Re-exports legacy `fetchX` named exports as small wrappers around
 *    client methods so existing `useEffect` call sites work unchanged.
 *
 * This is the "dogfood the SDK" piece of PR #3: every fetch in the dev
 * console flows through `@aeroza/sdk`, so any awkwardness shows up as
 * we use it, not in a hypothetical future audit.
 */

import {
  AerozaApiError as SdkApiError,
  AerozaClient,
  type AlertFeatureCollection,
  type AlertQuery,
  type AlertRule,
  type AlertRuleCreate,
  type AlertRuleList,
  type AlertRulePatch,
  type AlertRuleQuery,
  type CalibrationQuery,
  type CalibrationResponse,
  type CalibrationSeriesQuery,
  type CalibrationSeriesResponse,
  type WebhookDeliveryList,
  type WebhookDeliveryQuery,
  type WebhookSubscription,
  type WebhookSubscriptionCreate,
  type WebhookSubscriptionList,
  type WebhookSubscriptionQuery,
  type Health,
  type MrmsFileList,
  type MrmsGridList,
  type MrmsGridPolygonSample as SdkMrmsGridPolygonSample,
  type MrmsGridSample as SdkMrmsGridSample,
  type MrmsQuery,
  type PolygonQuery as SdkPolygonQuery,
  type SampleQuery as SdkSampleQuery,
  type Stats,
} from "@aeroza/sdk";

export const API_BASE: string =
  process.env.NEXT_PUBLIC_AEROZA_API_URL ?? "http://localhost:8000";

/**
 * Static-origin URL for pre-rendered radar tiles (Cloudflare R2 fronted
 * by ``tiles.aeroza.app``). When ``NEXT_PUBLIC_AEROZA_TILES_URL`` is
 * unset (local dev), this falls back to ``null`` and the URL builder
 * routes through the on-demand FastAPI tile route instead. The build
 * step inlines the env value, so production deployments must set this
 * in Vercel for the static-tile path to be active.
 */
export const TILES_BASE: string | null = (() => {
  const raw = process.env.NEXT_PUBLIC_AEROZA_TILES_URL?.trim();
  return raw && raw.length > 0 ? raw.replace(/\/$/, "") : null;
})();

const client = new AerozaClient({ apiBase: API_BASE });

/**
 * Build the MapLibre raster-source tile template for a pinned grid.
 *
 * Production path (``TILES_BASE`` set): returns the R2 static origin
 * URL. The path mirrors the bucket's object key 1:1 —
 * ``{fileKey}/{z}/{x}/{y}.webp`` — and MapLibre substitutes
 * ``{z}/{x}/{y}`` per tile. Cache-Control on the underlying objects
 * is ``public, max-age=31536000, immutable``, so once an edge POP
 * fetches a tile, every subsequent request is a free cache hit.
 *
 * Fallback path (``TILES_BASE`` null): returns the legacy on-demand
 * FastAPI route. Same shape today's deployed code uses; lets local
 * dev (no R2) and the materialisation-lag window (first ~30s after
 * a grid lands) still work, just slower.
 *
 * ``fileKey`` may contain path separators (real MRMS keys are
 * ``CONUS/MergedReflectivityComposite_00.50/.../...grib2.gz``). We
 * encode segment-by-segment for the R2 path so the slashes stay as
 * path separators, and as a single component for the legacy
 * ``?fileKey=`` query param.
 */
export function buildRadarTileUrlTemplate(
  fileKey: string,
  format: "webp" | "png" = "webp",
): string {
  if (TILES_BASE !== null) {
    const encodedKey = fileKey.split("/").map(encodeURIComponent).join("/");
    return `${TILES_BASE}/${encodedKey}/{z}/{x}/{y}.${format}`;
  }
  // Legacy fallback. PNG extension is the URL-shape hangover from
  // MapLibre's tile-template grammar — the FastAPI route serves WebP
  // when ``Accept: image/webp`` is present, which every browser sends.
  return `${API_BASE}/v1/mrms/tiles/{z}/{x}/{y}.png?fileKey=${encodeURIComponent(fileKey)}`;
}

/**
 * Live-mode tile template, when the caller doesn't yet have a pinned
 * ``file_key``. Only relevant on the legacy fallback path — the
 * static R2 origin has no "live" URL, every tile is keyed on a
 * concrete fileKey. Callers using ``TILES_BASE`` should poll
 * :func:`fetchMrmsLatest` to resolve the latest fileKey first.
 */
export function buildLiveRadarTileUrlTemplate(): string {
  // Cache-bust so MapLibre re-fetches when the materialiser produces a
  // newer grid in the ~30s window before the next /v1/mrms/latest poll
  // catches up. This only fires when TILES_BASE is unset.
  return `${API_BASE}/v1/mrms/tiles/{z}/{x}/{y}.png?_=${Date.now()}`;
}

export interface MrmsLatestResponse {
  fileKey: string;
  validAt: string;
  product: string;
  level: string;
}

/**
 * Poll the API for the most recent materialised grid's
 * ``{fileKey, validAt}``. The radar dashboard uses this every 30s as
 * its live-mode pin — tile bytes come from the static origin, so this
 * is the only API round-trip a "live" map needs.
 */
export async function fetchMrmsLatest(opts: {
  product?: string;
  level?: string;
} = {}): Promise<MrmsLatestResponse> {
  const params = new URLSearchParams();
  if (opts.product) params.set("product", opts.product);
  if (opts.level) params.set("level", opts.level);
  const url = `${API_BASE}/v1/mrms/latest${params.toString() ? `?${params.toString()}` : ""}`;
  const response = await fetch(url, {
    headers: { Accept: "application/json" },
  });
  if (!response.ok) {
    throw new Error(
      `fetchMrmsLatest failed: HTTP ${response.status} ${response.statusText}`,
    );
  }
  return (await response.json()) as MrmsLatestResponse;
}

/**
 * The newest grid whose *full tile pyramid* the prewarm worker has
 * published to R2, read from the static origin's ``latest.json`` pointer.
 *
 * Why this exists: ``/v1/mrms/latest`` (and the ``/v1/mrms/grids`` catalog)
 * return a grid the instant it materialises, but its tiles take a render
 * cycle to land in R2. Pinning the live radar to that grid 404s every tile
 * until prewarm catches up. This pointer only advances once the tiles are
 * actually uploaded, so a map pinned to it is a 100% CDN hit — no flicker.
 *
 * Returns ``null`` when the static origin isn't configured (local dev → the
 * on-demand API tile route already handles freshness) or the pointer is
 * missing/unreadable (cold bucket, pre-deploy). Callers fall back to the
 * newest catalog grid in that case — same behaviour as before this existed.
 */
export interface LatestPrewarmedGrid {
  fileKey: string;
  validAt: string;
  product: string;
  level: string;
}

export async function fetchLatestPrewarmedGrid(): Promise<LatestPrewarmedGrid | null> {
  if (TILES_BASE === null) return null;
  try {
    const response = await fetch(`${TILES_BASE}/latest.json`, {
      headers: { Accept: "application/json" },
      // The object carries a short max-age; skip the browser's HTTP cache so
      // polling always revalidates against the (edge-cached) origin.
      cache: "no-store",
    });
    if (!response.ok) return null;
    const data = (await response.json()) as Partial<LatestPrewarmedGrid>;
    if (typeof data?.fileKey !== "string" || data.fileKey.length === 0) {
      return null;
    }
    return {
      fileKey: data.fileKey,
      validAt: typeof data.validAt === "string" ? data.validAt : "",
      product: typeof data.product === "string" ? data.product : "",
      level: typeof data.level === "string" ? data.level : "",
    };
  } catch {
    // Network error / bad JSON → fall back to the catalog path.
    return null;
  }
}

// ---------------------------------------------------------------------------
// Re-exported wire types — components import these as before.
// ---------------------------------------------------------------------------

export type {
  AdminSeedEventRequest,
  AdminSeedEventState,
  AdminSeedEventStatusQuery,
  AdminSeedEventTask,
  AlertDetailFeature,
  AlertDetailProperties,
  AlertFeature,
  AlertFeatureCollection,
  AlertProperties,
  AlertRule,
  AlertRuleCreate,
  AlertRuleList,
  AlertRulePatch,
  AlertRuleQuery,
  AlertRuleStatus,
  CalibrationItem,
  CalibrationQuery,
  CalibrationResponse,
  CalibrationSeriesItem,
  CalibrationSeriesPoint,
  CalibrationSeriesQuery,
  CalibrationSeriesResponse,
  PointRuleConfig,
  PolygonRuleConfig,
  Predicate,
  PredicateOp,
  RuleConfig,
  RuleConfigPolygonReducer,
  WebhookDelivery,
  WebhookDeliveryList,
  WebhookDeliveryQuery,
  WebhookDeliveryStatus,
  WebhookStatus,
  WebhookSubscription,
  WebhookSubscriptionCreate,
  WebhookSubscriptionList,
  WebhookSubscriptionQuery,
  WebhookSubscriptionRedacted,
  GeoJsonGeometry,
  Health,
  MrmsFileItem,
  MrmsFileList,
  MrmsGridItem,
  MrmsGridList,
  MrmsStats,
  PolygonReducer,
  ReliabilityBin,
  ReliabilityRow,
  Severity,
  Stats,
} from "@aeroza/sdk";

// Local aliases — kept stable for callers that imported these specific
// names from `web/lib/api.ts` historically.
export type MrmsGridSample = SdkMrmsGridSample;
export type MrmsSampleQuery = SdkSampleQuery;
export type MrmsPolygonSample = SdkMrmsGridPolygonSample;
export type MrmsPolygonQuery = SdkPolygonQuery;
export const ApiError = SdkApiError;

// ---------------------------------------------------------------------------
// Legacy fetcher functions — small wrappers over client methods so the
// existing components don't need to know an SDK exists.
// ---------------------------------------------------------------------------

export function fetchHealth(): Promise<Health> {
  return client.getHealth();
}

export function fetchStats(): Promise<Stats> {
  return client.getStats();
}

export function fetchAlerts(
  query: AlertQuery = {},
): Promise<AlertFeatureCollection> {
  return client.listAlerts(query);
}

export function fetchHistoricalAlerts(
  query: import("@aeroza/sdk").HistoricalAlertQuery,
): Promise<AlertFeatureCollection> {
  return client.listHistoricalAlerts(query);
}

export function fetchMrmsFiles(query: MrmsQuery = {}): Promise<MrmsFileList> {
  return client.listMrmsFiles(query);
}

export function fetchMrmsGrids(query: MrmsQuery = {}): Promise<MrmsGridList> {
  return client.listMrmsGrids(query);
}

export function fetchMrmsSample(
  query: SdkSampleQuery,
): Promise<SdkMrmsGridSample> {
  return client.sampleGrid(query);
}

export function fetchMrmsPolygon(
  query: SdkPolygonQuery,
): Promise<SdkMrmsGridPolygonSample> {
  return client.reduceGridOverPolygon(query);
}

export function fetchCalibration(
  query: CalibrationQuery = {},
): Promise<CalibrationResponse> {
  return client.getCalibration(query);
}

export function fetchCalibrationSeries(
  query: CalibrationSeriesQuery = {},
): Promise<CalibrationSeriesResponse> {
  return client.getCalibrationSeries(query);
}

export function fetchWebhooks(
  query: WebhookSubscriptionQuery = {},
): Promise<WebhookSubscriptionList> {
  return client.listWebhooks(query);
}

export function createWebhook(
  body: WebhookSubscriptionCreate,
): Promise<WebhookSubscription> {
  return client.createWebhook(body);
}

export function deleteWebhook(id: string): Promise<void> {
  return client.deleteWebhook(id);
}

export function fetchWebhookDeliveries(
  subscriptionId: string,
  query: WebhookDeliveryQuery = {},
): Promise<WebhookDeliveryList> {
  return client.listWebhookDeliveries(subscriptionId, query);
}

export function fetchAlertRules(query: AlertRuleQuery = {}): Promise<AlertRuleList> {
  return client.listAlertRules(query);
}

export function createAlertRule(body: AlertRuleCreate): Promise<AlertRule> {
  return client.createAlertRule(body);
}

export function updateAlertRule(id: string, body: AlertRulePatch): Promise<AlertRule> {
  return client.updateAlertRule(id, body);
}

export function deleteAlertRule(id: string): Promise<void> {
  return client.deleteAlertRule(id);
}

/**
 * URL of the alerts SSE stream. The console wires this into a native
 * `EventSource` (see `lib/useAlertStream.ts`); the SDK doesn't wrap
 * `EventSource` directly because connection lifecycle is best owned by
 * the consumer.
 */
export function alertsStreamUrl(): string {
  return client.alertsStreamUrl();
}

// ---------------------------------------------------------------------------
// Admin — seed historical events. Both routes 404 when the server's
// `AEROZA_DEV_ADMIN_ENABLED` flag is off; callers handle that as
// "feature not available" rather than retrying.

export function startSeedEvent(
  body: import("@aeroza/sdk").AdminSeedEventRequest,
): Promise<import("@aeroza/sdk").AdminSeedEventTask> {
  return client.startSeedEvent(body);
}

export function fetchSeedEventStatus(
  query: import("@aeroza/sdk").AdminSeedEventStatusQuery,
): Promise<import("@aeroza/sdk").AdminSeedEventTask> {
  return client.getSeedEventStatus(query);
}
