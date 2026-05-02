/**
 * @aeroza/sdk — TypeScript client for the Aeroza weather-intelligence API.
 *
 * Single entry point. Construct an `AeroaClient` with an `apiBase` URL
 * and call typed methods that mirror the v1 REST surface. SSE for
 * `/v1/alerts/stream` is exposed via `alertsStreamUrl()` for use with
 * the platform `EventSource`.
 *
 * @example
 * ```ts
 * import { AeroaClient } from "@aeroza/sdk";
 *
 * const client = new AeroaClient({ apiBase: "http://localhost:8000" });
 * const sample = await client.sampleGrid({ lat: 29.76, lng: -95.37 });
 * console.log(sample.value, sample.matchedLatitude, sample.matchedLongitude);
 * ```
 */

export { AeroaClient, AeroaApiError } from "./client";
export type { AeroaClientOptions } from "./client";
export type {
  AlertDetailFeature,
  AlertDetailProperties,
  AlertFeature,
  AlertFeatureCollection,
  AlertProperties,
  AlertQuery,
  AlertsStats,
  CalibrationItem,
  CalibrationQuery,
  CalibrationResponse,
  AlertRule,
  AlertRuleList,
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
  WebhookSubscriptionList,
  WebhookSubscriptionQuery,
  WebhookSubscriptionRedacted,
  GeoJsonGeometry,
  Health,
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
  SampleQuery,
  Severity,
  Stats,
} from "./types";
