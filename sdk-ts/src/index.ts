/**
 * @aeroza/sdk — TypeScript client for the Aeroza weather-intelligence API.
 *
 * Single entry point. Construct an `AerozaClient` with an `apiBase` URL
 * and call typed methods that mirror the v1 REST surface. SSE for
 * `/v1/alerts/stream` is exposed via `alertsStreamUrl()` for use with
 * the platform `EventSource`.
 *
 * @example
 * ```ts
 * import { AerozaClient } from "@aeroza/sdk";
 *
 * const client = new AerozaClient({ apiBase: "http://localhost:8000" });
 * const sample = await client.sampleGrid({ lat: 29.76, lng: -95.37 });
 * console.log(sample.value, sample.matchedLatitude, sample.matchedLongitude);
 * ```
 */

export { AerozaClient, AerozaApiError } from "./client";
export type { AerozaClientOptions } from "./client";
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
  AlertQuery,
  AlertsStats,
  HistoricalAlertQuery,
  CalibrationItem,
  CalibrationQuery,
  CalibrationResponse,
  AlertRule,
  AlertRuleCreate,
  AlertRuleList,
  AlertRulePatch,
  AlertRuleQuery,
  AlertRuleStatus,
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
  WebhookStatus,
  WebhookSubscription,
  WebhookSubscriptionCreate,
  WebhookSubscriptionList,
  WebhookSubscriptionQuery,
  WebhookSubscriptionRedacted,
  WebhookDelivery,
  WebhookDeliveryList,
  WebhookDeliveryQuery,
  WebhookDeliveryStatus,
  GeoJsonGeometry,
  Health,
  Me,
  MetarObservation,
  MetarObservationList,
  MetarQuery,
  MrmsFileItem,
  MrmsFileList,
  MrmsGridItem,
  MrmsGridList,
  MrmsGridPolygonSample,
  MrmsGridSample,
  MrmsQuery,
  MrmsStats,
  PolygonQuery,
  PolygonReducer,
  ReliabilityBin,
  ReliabilityRow,
  SampleQuery,
  Severity,
  Stats,
} from "./types";
