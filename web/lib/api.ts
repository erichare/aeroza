/**
 * Thin façade over `@aeroza/sdk` — the TypeScript SDK is the source of
 * truth for wire types and HTTP. This module:
 *
 * 1. Constructs a singleton `AeroaClient` configured for the dev console.
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
  AeroaApiError as SdkApiError,
  AeroaClient,
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

const client = new AeroaClient({ apiBase: API_BASE });

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
