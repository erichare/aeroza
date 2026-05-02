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
  type CalibrationQuery,
  type CalibrationResponse,
  type CalibrationSeriesQuery,
  type CalibrationSeriesResponse,
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
  AlertDetailFeature,
  AlertDetailProperties,
  AlertFeature,
  AlertFeatureCollection,
  AlertProperties,
  CalibrationItem,
  CalibrationQuery,
  CalibrationResponse,
  CalibrationSeriesItem,
  CalibrationSeriesPoint,
  CalibrationSeriesQuery,
  CalibrationSeriesResponse,
  GeoJsonGeometry,
  Health,
  MrmsFileItem,
  MrmsFileList,
  MrmsGridItem,
  MrmsGridList,
  MrmsStats,
  PolygonReducer,
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

/**
 * URL of the alerts SSE stream. The console wires this into a native
 * `EventSource` (see `lib/useAlertStream.ts`); the SDK doesn't wrap
 * `EventSource` directly because connection lifecycle is best owned by
 * the consumer.
 */
export function alertsStreamUrl(): string {
  return client.alertsStreamUrl();
}
