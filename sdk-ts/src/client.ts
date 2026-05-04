/**
 * AerozaClient — typed wrapper over the Aeroza v1 REST API.
 *
 * One class, one method per route, no generics. The whole point of the
 * SDK at this stage is to (a) pin the wire types so consumers get a
 * compile-time signal when an endpoint shape changes, and (b) be the
 * forcing function for API contract design — anything awkward in the
 * SDK is a thing to fix in the API.
 *
 * SSE for `/v1/alerts/stream` is intentionally *not* wrapped behind a
 * method. Browser code uses the platform `EventSource`; this class
 * exposes a `streamUrl()` builder so consumers can wire it up themselves
 * with their own lifecycle management. We may add a higher-level helper
 * later, but the contract should grow upward, not down.
 */

import type {
  AdminSeedEventRequest,
  AdminSeedEventStatusQuery,
  AdminSeedEventTask,
  AlertDetailFeature,
  AlertFeatureCollection,
  AlertQuery,
  HistoricalAlertQuery,
  AlertRule,
  AlertRuleCreate,
  AlertRuleList,
  AlertRulePatch,
  AlertRuleQuery,
  CalibrationQuery,
  CalibrationResponse,
  CalibrationSeriesQuery,
  CalibrationSeriesResponse,
  Me,
  MetarObservation,
  MetarObservationList,
  MetarQuery,
  WebhookDeliveryList,
  WebhookDeliveryQuery,
  WebhookSubscription,
  WebhookSubscriptionCreate,
  WebhookSubscriptionList,
  WebhookSubscriptionQuery,
  Health,
  MrmsFileList,
  MrmsGridItem,
  MrmsGridList,
  MrmsGridPolygonSample,
  MrmsGridSample,
  MrmsQuery,
  PolygonQuery,
  SampleQuery,
  Stats,
} from "./types";

export interface AerozaClientOptions {
  /** Base URL for the API, e.g. `http://localhost:8000`. No trailing slash. */
  apiBase: string;
  /**
   * Optional `fetch` implementation. Defaults to `globalThis.fetch`.
   * Inject a stub in tests; in node 18+ and modern browsers the global
   * works as-is.
   */
  fetch?: typeof globalThis.fetch;
  /**
   * Optional bearer-token API key (an `aza_live_*` string from
   * `aeroza-api-keys create`). When set, the client adds
   * `Authorization: Bearer <token>` to every request. Anonymous traffic
   * is still allowed by default on the server; set this once routes
   * require auth.
   */
  apiKey?: string;
  /**
   * Optional headers merged into every request. Use for telemetry,
   * `Accept-Language`, or any custom header that isn't covered by
   * `apiKey`.
   */
  defaultHeaders?: Record<string, string>;
}

/**
 * FastAPI returns `{"detail": "..."}` on errors. The SDK surfaces the
 * detail as the message and keeps the status for callers that want to
 * branch on 404 vs 422 vs 5xx without parsing strings.
 */
export class AerozaApiError extends Error {
  public readonly status: number;
  public readonly detail: string | null;

  constructor(status: number, detail: string | null, fallback: string) {
    super(detail ?? fallback);
    this.name = "AerozaApiError";
    this.status = status;
    this.detail = detail;
  }
}

export class AerozaClient {
  private readonly apiBase: string;
  private readonly fetchImpl: typeof globalThis.fetch;
  private readonly defaultHeaders: Record<string, string>;

  constructor(options: AerozaClientOptions) {
    this.apiBase = stripTrailingSlashes(options.apiBase);
    this.fetchImpl = options.fetch ?? globalThis.fetch.bind(globalThis);
    const headers: Record<string, string> = { ...(options.defaultHeaders ?? {}) };
    if (options.apiKey) {
      headers.Authorization = `Bearer ${options.apiKey}`;
    }
    this.defaultHeaders = headers;
  }

  // -------------------------------------------------------------------------
  // Health & meta

  async getHealth(): Promise<Health> {
    return this.getJson<Health>("/health");
  }

  async getStats(): Promise<Stats> {
    return this.getJson<Stats>("/v1/stats");
  }

  /**
   * Introspect the calling API key. Requires `apiKey` to be set on
   * the client; raises 401 otherwise.
   */
  async getMe(): Promise<Me> {
    return this.getJson<Me>("/v1/me");
  }

  // -------------------------------------------------------------------------
  // Alerts

  async listAlerts(query: AlertQuery = {}): Promise<AlertFeatureCollection> {
    const params = new URLSearchParams();
    if (query.severity) params.set("severity", query.severity);
    if (query.bbox) params.set("bbox", query.bbox);
    if (query.point) params.set("point", query.point);
    if (query.limit !== undefined) params.set("limit", String(query.limit));
    return this.getJson<AlertFeatureCollection>(
      this.withQuery("/v1/alerts", params),
    );
  }

  async getAlert(alertId: string): Promise<AlertDetailFeature> {
    return this.getJson<AlertDetailFeature>(
      `/v1/alerts/${encodeURIComponent(alertId)}`,
    );
  }

  /**
   * List historical NWS Storm-Based Warnings issued during a UTC window
   * for one or more forecast offices, sourced from the IEM archive.
   * Used by the /demo Storm Replay to overlay the warnings that were
   * actually in force during a featured event — NWS's own /alerts
   * endpoint only retains the last ~30 days, which doesn't cover the
   * 2021–2024 events in the catalog.
   */
  async listHistoricalAlerts(
    query: HistoricalAlertQuery,
  ): Promise<AlertFeatureCollection> {
    const params = new URLSearchParams({
      since: query.since,
      until: query.until,
      wfos: query.wfos.join(","),
    });
    return this.getJson<AlertFeatureCollection>(
      this.withQuery("/v1/alerts/historical", params),
    );
  }

  /**
   * URL of the SSE stream for newly-observed alerts. Use with the
   * platform `EventSource` (browser) or any SSE client; the SDK does
   * not wrap connection lifecycle.
   */
  alertsStreamUrl(): string {
    return `${this.apiBase}/v1/alerts/stream`;
  }

  // -------------------------------------------------------------------------
  // MRMS — catalog

  async listMrmsFiles(query: MrmsQuery = {}): Promise<MrmsFileList> {
    return this.getJson<MrmsFileList>(
      this.withQuery("/v1/mrms/files", buildMrmsParams(query)),
    );
  }

  async listMrmsGrids(query: MrmsQuery = {}): Promise<MrmsGridList> {
    return this.getJson<MrmsGridList>(
      this.withQuery("/v1/mrms/grids", buildMrmsParams(query)),
    );
  }

  async getMrmsGrid(fileKey: string): Promise<MrmsGridItem> {
    // The path converter accepts the slash-bearing CONUS-prefixed key as a
    // single parameter; we still escape it so a stray `?` or `#` can't break
    // out of the path.
    const safe = fileKey
      .split("/")
      .map((segment) => encodeURIComponent(segment))
      .join("/");
    return this.getJson<MrmsGridItem>(`/v1/mrms/grids/${safe}`);
  }

  // -------------------------------------------------------------------------
  // MRMS — queries

  async sampleGrid(query: SampleQuery): Promise<MrmsGridSample> {
    const params = new URLSearchParams({
      lat: String(query.lat),
      lng: String(query.lng),
    });
    if (query.product) params.set("product", query.product);
    if (query.level) params.set("level", query.level);
    if (query.atTime) params.set("at_time", query.atTime);
    if (query.toleranceDeg !== undefined) {
      params.set("tolerance_deg", String(query.toleranceDeg));
    }
    return this.getJson<MrmsGridSample>(
      this.withQuery("/v1/mrms/grids/sample", params),
    );
  }

  async reduceGridOverPolygon(
    query: PolygonQuery,
  ): Promise<MrmsGridPolygonSample> {
    const params = new URLSearchParams({
      polygon: query.polygon,
    });
    if (query.reducer) params.set("reducer", query.reducer);
    if (query.threshold !== undefined) {
      params.set("threshold", String(query.threshold));
    }
    if (query.product) params.set("product", query.product);
    if (query.level) params.set("level", query.level);
    if (query.atTime) params.set("at_time", query.atTime);
    return this.getJson<MrmsGridPolygonSample>(
      this.withQuery("/v1/mrms/grids/polygon", params),
    );
  }

  // -------------------------------------------------------------------------
  // Calibration

  async getCalibration(
    query: CalibrationQuery = {},
  ): Promise<CalibrationResponse> {
    const params = new URLSearchParams();
    if (query.windowHours !== undefined) {
      params.set("windowHours", String(query.windowHours));
    }
    if (query.algorithm) params.set("algorithm", query.algorithm);
    if (query.product) params.set("product", query.product);
    if (query.level) params.set("level", query.level);
    return this.getJson<CalibrationResponse>(
      this.withQuery("/v1/calibration", params),
    );
  }

  async getCalibrationSeries(
    query: CalibrationSeriesQuery = {},
  ): Promise<CalibrationSeriesResponse> {
    const params = new URLSearchParams();
    if (query.windowHours !== undefined) {
      params.set("windowHours", String(query.windowHours));
    }
    if (query.bucketSeconds !== undefined) {
      params.set("bucketSeconds", String(query.bucketSeconds));
    }
    if (query.algorithm) params.set("algorithm", query.algorithm);
    if (query.product) params.set("product", query.product);
    if (query.level) params.set("level", query.level);
    return this.getJson<CalibrationSeriesResponse>(
      this.withQuery("/v1/calibration/series", params),
    );
  }

  // -------------------------------------------------------------------------
  // METAR

  async listMetar(query: MetarQuery = {}): Promise<MetarObservationList> {
    const params = new URLSearchParams();
    if (query.station) params.set("station", query.station);
    if (query.since) params.set("since", query.since);
    if (query.until) params.set("until", query.until);
    if (query.bbox) params.set("bbox", query.bbox);
    if (query.limit !== undefined) params.set("limit", String(query.limit));
    return this.getJson<MetarObservationList>(
      this.withQuery("/v1/metar", params),
    );
  }

  async getLatestMetar(stationId: string): Promise<MetarObservation> {
    return this.getJson<MetarObservation>(
      `/v1/metar/${encodeURIComponent(stationId)}/latest`,
    );
  }

  // -------------------------------------------------------------------------
  // Webhooks. Server has full CRUD; the SDK now exposes list, create,
  // and delete. ``createWebhook`` returns the signing ``secret`` — a
  // one-shot, never-readable-again field, so callers should hand it
  // to the user immediately.

  async listWebhooks(
    query: WebhookSubscriptionQuery = {},
  ): Promise<WebhookSubscriptionList> {
    const params = new URLSearchParams();
    if (query.status) params.set("status", query.status);
    if (query.limit !== undefined) params.set("limit", String(query.limit));
    return this.getJson<WebhookSubscriptionList>(
      this.withQuery("/v1/webhooks", params),
    );
  }

  /**
   * Register a new webhook subscription. The response's ``secret`` is
   * shown only on this call; it isn't readable from later list/get
   * responses, so keep it (we surface it once in the dashboard with
   * a copy button, then drop it from state).
   */
  async createWebhook(
    body: WebhookSubscriptionCreate,
  ): Promise<WebhookSubscription> {
    return this.postJson<WebhookSubscription>("/v1/webhooks", body);
  }

  /**
   * Delete a webhook subscription by id. Server returns 204 on
   * success; we throw on any other status. Idempotent — re-running
   * after a previous delete returns 404 (caller catches as needed).
   */
  async deleteWebhook(id: string): Promise<void> {
    await this.deleteRequest(`/v1/webhooks/${encodeURIComponent(id)}`);
  }

  /**
   * Recent delivery attempts for one subscription, newest first.
   * Each row is one attempt the dispatcher made (initial + retries),
   * which is what you actually want when answering "did my webhook
   * fire / why is it broken?". The signed payload itself is omitted
   * from the wire (read the DB directly if you need it).
   */
  async listWebhookDeliveries(
    subscriptionId: string,
    query: WebhookDeliveryQuery = {},
  ): Promise<WebhookDeliveryList> {
    const params = new URLSearchParams();
    if (query.status) params.set("status", query.status);
    if (query.limit !== undefined) params.set("limit", String(query.limit));
    return this.getJson<WebhookDeliveryList>(
      this.withQuery(
        `/v1/webhooks/${encodeURIComponent(subscriptionId)}/deliveries`,
        params,
      ),
    );
  }

  // -------------------------------------------------------------------------
  // Alert rules. Server has full CRUD; the SDK exposes list, create,
  // update, and delete. Each rule is bound to a webhook subscription
  // (the FK target). Status transitions go through ``updateAlertRule``
  // — there's no dedicated pause/resume route.

  async listAlertRules(query: AlertRuleQuery = {}): Promise<AlertRuleList> {
    const params = new URLSearchParams();
    if (query.status) params.set("status", query.status);
    if (query.subscriptionId) params.set("subscriptionId", query.subscriptionId);
    if (query.limit !== undefined) params.set("limit", String(query.limit));
    return this.getJson<AlertRuleList>(this.withQuery("/v1/alert-rules", params));
  }

  /**
   * Create a new alert rule bound to an existing webhook subscription.
   * The server validates ``subscriptionId`` exists and 404s if not.
   */
  async createAlertRule(body: AlertRuleCreate): Promise<AlertRule> {
    return this.postJson<AlertRule>("/v1/alert-rules", body);
  }

  /**
   * Patch an existing rule. Every field on ``body`` is optional;
   * ``config`` (when present) is replaced wholesale. Used for
   * pause/resume by patching ``status`` alone.
   */
  async updateAlertRule(id: string, body: AlertRulePatch): Promise<AlertRule> {
    return this.patchJson<AlertRule>(
      `/v1/alert-rules/${encodeURIComponent(id)}`,
      body,
    );
  }

  /**
   * Delete an alert rule by id. Server returns 204 on success;
   * idempotent — a second delete returns 404 (caller catches as needed).
   */
  async deleteAlertRule(id: string): Promise<void> {
    await this.deleteRequest(`/v1/alert-rules/${encodeURIComponent(id)}`);
  }

  // -------------------------------------------------------------------------
  // Admin — seed historical events
  //
  // Both endpoints sit under `/v1/admin/seed-event` and are gated by
  // the `AEROZA_DEV_ADMIN_ENABLED` env flag on the server. When the
  // flag is off, the routes 404 — the SDK surfaces that as
  // `AerozaApiError` with status 404, and the /demo button hides the
  // "Seed this event" affordance.

  /**
   * Kick off (or rejoin) a background seed for the given window.
   * Returns immediately with the task snapshot; idempotent under
   * double-clicks (a second call for the same window returns the
   * in-flight task).
   */
  async startSeedEvent(
    body: AdminSeedEventRequest,
  ): Promise<AdminSeedEventTask> {
    return this.postJson<AdminSeedEventTask>("/v1/admin/seed-event", body);
  }

  /**
   * Read-only snapshot of the seed task for the given window. 404s
   * (raised as `AerozaApiError`) when no task exists yet — the caller
   * is expected to treat that as "not started" and decide whether to
   * POST or simply not show progress.
   */
  async getSeedEventStatus(
    query: AdminSeedEventStatusQuery,
  ): Promise<AdminSeedEventTask> {
    const params = new URLSearchParams({
      since: query.since,
      until: query.until,
    });
    if (query.product) params.set("product", query.product);
    if (query.level) params.set("level", query.level);
    return this.getJson<AdminSeedEventTask>(
      this.withQuery("/v1/admin/seed-event/status", params),
    );
  }

  // -------------------------------------------------------------------------
  // Internal

  private withQuery(path: string, params: URLSearchParams): string {
    const qs = params.toString();
    return qs ? `${path}?${qs}` : path;
  }

  private async getJson<T>(path: string): Promise<T> {
    const url = `${this.apiBase}${path}`;
    const response = await this.fetchImpl(url, {
      method: "GET",
      headers: { Accept: "application/json", ...this.defaultHeaders },
      cache: "no-store",
    });
    return this.parseResponse<T>(response, path);
  }

  private async deleteRequest(path: string): Promise<void> {
    const url = `${this.apiBase}${path}`;
    const response = await this.fetchImpl(url, {
      method: "DELETE",
      headers: { Accept: "application/json", ...this.defaultHeaders },
      cache: "no-store",
    });
    if (!response.ok) {
      let detail: string | null = null;
      try {
        const body: unknown = await response.json();
        if (
          body !== null &&
          typeof body === "object" &&
          "detail" in body &&
          typeof (body as { detail: unknown }).detail === "string"
        ) {
          detail = (body as { detail: string }).detail;
        }
      } catch {
        // Non-JSON body (e.g. an empty 204 also lands here harmlessly).
      }
      throw new AerozaApiError(
        response.status,
        detail,
        `${response.status} ${response.statusText} — ${path}`,
      );
    }
  }

  private async postJson<T>(path: string, body: unknown): Promise<T> {
    const url = `${this.apiBase}${path}`;
    const response = await this.fetchImpl(url, {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
        ...this.defaultHeaders,
      },
      body: JSON.stringify(body),
      cache: "no-store",
    });
    return this.parseResponse<T>(response, path);
  }

  private async patchJson<T>(path: string, body: unknown): Promise<T> {
    const url = `${this.apiBase}${path}`;
    const response = await this.fetchImpl(url, {
      method: "PATCH",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
        ...this.defaultHeaders,
      },
      body: JSON.stringify(body),
      cache: "no-store",
    });
    return this.parseResponse<T>(response, path);
  }

  private async parseResponse<T>(response: Response, path: string): Promise<T> {
    if (!response.ok) {
      let detail: string | null = null;
      try {
        const body: unknown = await response.json();
        if (
          body !== null &&
          typeof body === "object" &&
          "detail" in body &&
          typeof (body as { detail: unknown }).detail === "string"
        ) {
          detail = (body as { detail: string }).detail;
        }
      } catch {
        // Non-JSON body — fall through to status-line fallback.
      }
      throw new AerozaApiError(
        response.status,
        detail,
        `${response.status} ${response.statusText} — ${path}`,
      );
    }
    return (await response.json()) as T;
  }
}

/**
 * Trim every trailing slash from ``input``. A linear-time scan that
 * avoids regex-based polynomial-ReDoS surface for callers passing a
 * pathological apiBase (e.g. ``"http://x" + "/".repeat(1e6)``).
 */
function stripTrailingSlashes(input: string): string {
  let end = input.length;
  while (end > 0 && input.charCodeAt(end - 1) === 47 /* '/' */) {
    end -= 1;
  }
  return end === input.length ? input : input.slice(0, end);
}

function buildMrmsParams(query: MrmsQuery): URLSearchParams {
  const params = new URLSearchParams();
  if (query.product) params.set("product", query.product);
  if (query.level) params.set("level", query.level);
  if (query.since) params.set("since", query.since);
  if (query.until) params.set("until", query.until);
  if (query.limit !== undefined) params.set("limit", String(query.limit));
  return params;
}
