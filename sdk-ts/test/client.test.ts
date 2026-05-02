/**
 * Unit tests for AeroaClient.
 *
 * Stubs `fetch` with an in-process function that asserts on the request
 * URL / headers and returns canned JSON. No live API, no MSW — the
 * surface is small enough that a 60-line stub is clearer than a mock
 * server for this package.
 */

import { describe, expect, it } from "vitest";

import {
  AeroaApiError,
  AeroaClient,
  type MrmsGridPolygonSample,
  type MrmsGridSample,
  type Stats,
} from "../src/index";

interface FakeRequest {
  url: string;
  init: RequestInit;
}

function createFakeFetch(
  responder: (req: FakeRequest) => {
    status?: number;
    body: unknown;
  },
): {
  fetch: typeof globalThis.fetch;
  calls: FakeRequest[];
} {
  const calls: FakeRequest[] = [];
  const fakeFetch: typeof globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === "string" ? input : input.toString();
    const request = { url, init };
    calls.push(request);
    const { status = 200, body } = responder(request);
    return new Response(body === undefined ? null : JSON.stringify(body), {
      status,
      headers: { "content-type": "application/json" },
    });
  };
  return { fetch: fakeFetch, calls };
}

const API_BASE = "http://localhost:8000";

describe("AeroaClient construction", () => {
  it("strips trailing slashes from apiBase", async () => {
    const { fetch, calls } = createFakeFetch(() => ({
      body: { status: "ok", version: "0.1.0" },
    }));
    const client = new AeroaClient({ apiBase: `${API_BASE}/`, fetch });
    await client.getHealth();
    expect(calls[0]?.url).toBe(`${API_BASE}/health`);
  });

  it("merges defaultHeaders into every request", async () => {
    const { fetch, calls } = createFakeFetch(() => ({
      body: { status: "ok", version: "0.1.0" },
    }));
    const client = new AeroaClient({
      apiBase: API_BASE,
      fetch,
      defaultHeaders: { "x-aeroza-trace": "abc-123" },
    });
    await client.getHealth();
    const headers = calls[0]?.init.headers as Record<string, string>;
    expect(headers["x-aeroza-trace"]).toBe("abc-123");
    expect(headers["Accept"]).toBe("application/json");
  });
});

describe("AeroaClient.getStats", () => {
  it("decodes the Stats envelope", async () => {
    const stats: Stats = {
      type: "Stats",
      generatedAt: "2026-05-01T12:00:00Z",
      alerts: { total: 5, active: 2, latestExpires: null },
      mrms: {
        files: 1,
        gridsMaterialised: 1,
        filesPending: 0,
        latestValidAt: null,
        latestGridMaterialisedAt: null,
      },
    };
    const { fetch } = createFakeFetch(() => ({ body: stats }));
    const client = new AeroaClient({ apiBase: API_BASE, fetch });
    await expect(client.getStats()).resolves.toEqual(stats);
  });
});

describe("AeroaClient.listAlerts", () => {
  it("includes severity, bbox, point, and limit when set", async () => {
    const { fetch, calls } = createFakeFetch(() => ({
      body: { type: "FeatureCollection", features: [] },
    }));
    const client = new AeroaClient({ apiBase: API_BASE, fetch });
    await client.listAlerts({
      severity: "Severe",
      bbox: "-100,30,-99,31",
      point: "29.76,-95.37",
      limit: 50,
    });
    const url = new URL(calls[0]!.url);
    expect(url.pathname).toBe("/v1/alerts");
    expect(url.searchParams.get("severity")).toBe("Severe");
    expect(url.searchParams.get("bbox")).toBe("-100,30,-99,31");
    expect(url.searchParams.get("point")).toBe("29.76,-95.37");
    expect(url.searchParams.get("limit")).toBe("50");
  });

  it("omits absent params", async () => {
    const { fetch, calls } = createFakeFetch(() => ({
      body: { type: "FeatureCollection", features: [] },
    }));
    const client = new AeroaClient({ apiBase: API_BASE, fetch });
    await client.listAlerts();
    expect(calls[0]?.url).toBe(`${API_BASE}/v1/alerts`);
  });
});

describe("AeroaClient.getAlert", () => {
  it("URL-encodes alert ids that contain reserved chars", async () => {
    const { fetch, calls } = createFakeFetch(() => ({
      body: {
        type: "Feature",
        geometry: null,
        properties: {
          id: "urn:oid:2.49.0.1.840.0.abc",
          event: "Severe Thunderstorm Warning",
          headline: null,
          severity: "Severe",
          urgency: "Expected",
          certainty: "Likely",
          senderName: null,
          areaDesc: null,
          effective: null,
          onset: null,
          expires: null,
          ends: null,
          description: null,
          instruction: null,
        },
      },
    }));
    const client = new AeroaClient({ apiBase: API_BASE, fetch });
    await client.getAlert("urn:oid:2.49.0.1.840.0.abc");
    expect(calls[0]?.url).toBe(
      `${API_BASE}/v1/alerts/urn%3Aoid%3A2.49.0.1.840.0.abc`,
    );
  });
});

describe("AeroaClient.alertsStreamUrl", () => {
  it("builds a fully qualified SSE URL", () => {
    const client = new AeroaClient({ apiBase: API_BASE });
    expect(client.alertsStreamUrl()).toBe(`${API_BASE}/v1/alerts/stream`);
  });
});

describe("AeroaClient.getMrmsGrid", () => {
  it("preserves slashes in the file_key path segment", async () => {
    const { fetch, calls } = createFakeFetch(() => ({
      body: {
        fileKey: "CONUS/X_00.50/20260501/MRMS_X.grib2.gz",
        product: "X",
        level: "00.50",
        validAt: "2026-05-01T12:00:00Z",
        zarrUri: "/var/zarr/x.zarr",
        variable: "reflectivity",
        dims: ["latitude", "longitude"],
        shape: [3, 3],
        dtype: "float32",
        nbytes: 36,
        materialisedAt: "2026-05-01T12:01:00Z",
      },
    }));
    const client = new AeroaClient({ apiBase: API_BASE, fetch });
    await client.getMrmsGrid("CONUS/X_00.50/20260501/MRMS_X.grib2.gz");
    expect(calls[0]?.url).toBe(
      `${API_BASE}/v1/mrms/grids/CONUS/X_00.50/20260501/MRMS_X.grib2.gz`,
    );
  });
});

describe("AeroaClient.sampleGrid", () => {
  it("camelCase atTime maps to snake_case at_time on the wire", async () => {
    const { fetch, calls } = createFakeFetch(() => ({
      body: {
        type: "MrmsGridSample",
        fileKey: "k1",
        product: "P",
        level: "00.50",
        validAt: "2026-05-01T12:00:00Z",
        variable: "reflectivity",
        value: 5,
        requestedLatitude: 20.5,
        requestedLongitude: -99.5,
        matchedLatitude: 20.5,
        matchedLongitude: -99.5,
        toleranceDeg: 0.05,
      } satisfies MrmsGridSample,
    }));
    const client = new AeroaClient({ apiBase: API_BASE, fetch });
    await client.sampleGrid({
      lat: 20.5,
      lng: -99.5,
      atTime: "2026-05-01T12:00:00Z",
      toleranceDeg: 0.1,
    });
    const url = new URL(calls[0]!.url);
    expect(url.searchParams.get("at_time")).toBe("2026-05-01T12:00:00Z");
    expect(url.searchParams.get("tolerance_deg")).toBe("0.1");
  });
});

describe("AeroaClient.reduceGridOverPolygon", () => {
  it("forwards reducer + threshold + product/level", async () => {
    const polygonResponse: MrmsGridPolygonSample = {
      type: "MrmsGridPolygonSample",
      fileKey: "k1",
      product: "P",
      level: "00.50",
      validAt: "2026-05-01T12:00:00Z",
      variable: "reflectivity",
      reducer: "count_ge",
      threshold: 40,
      value: 17,
      cellCount: 35,
      vertexCount: 4,
      bboxMinLatitude: 29.5,
      bboxMinLongitude: -95.7,
      bboxMaxLatitude: 30.0,
      bboxMaxLongitude: -95.0,
    };
    const { fetch, calls } = createFakeFetch(() => ({ body: polygonResponse }));
    const client = new AeroaClient({ apiBase: API_BASE, fetch });
    const result = await client.reduceGridOverPolygon({
      polygon: "-95.7,29.5,-95.0,29.5,-95.0,30.0,-95.7,30.0",
      reducer: "count_ge",
      threshold: 40,
      product: "MergedReflectivityComposite",
      level: "00.50",
    });
    expect(result).toEqual(polygonResponse);
    const url = new URL(calls[0]!.url);
    expect(url.pathname).toBe("/v1/mrms/grids/polygon");
    expect(url.searchParams.get("reducer")).toBe("count_ge");
    expect(url.searchParams.get("threshold")).toBe("40");
    expect(url.searchParams.get("product")).toBe("MergedReflectivityComposite");
    expect(url.searchParams.get("level")).toBe("00.50");
  });
});

describe("AeroaApiError", () => {
  it("uses the FastAPI detail field as the error message", async () => {
    const { fetch } = createFakeFetch(() => ({
      status: 404,
      body: { detail: "no cell within 0.05° of (lat=50, lng=-50)" },
    }));
    const client = new AeroaClient({ apiBase: API_BASE, fetch });
    try {
      await client.sampleGrid({ lat: 50, lng: -50 });
      expect.unreachable("expected AeroaApiError");
    } catch (err) {
      expect(err).toBeInstanceOf(AeroaApiError);
      const apiErr = err as AeroaApiError;
      expect(apiErr.status).toBe(404);
      expect(apiErr.detail).toContain("no cell within");
      expect(apiErr.message).toContain("no cell within");
    }
  });

  it("falls back to status line when the body is not JSON", async () => {
    const fakeFetch: typeof globalThis.fetch = async () =>
      new Response("oops", {
        status: 500,
        statusText: "Internal Server Error",
        headers: { "content-type": "text/plain" },
      });
    const client = new AeroaClient({ apiBase: API_BASE, fetch: fakeFetch });
    try {
      await client.getStats();
      expect.unreachable("expected AeroaApiError");
    } catch (err) {
      const apiErr = err as AeroaApiError;
      expect(apiErr.status).toBe(500);
      expect(apiErr.detail).toBeNull();
      expect(apiErr.message).toContain("500");
      expect(apiErr.message).toContain("/v1/stats");
    }
  });
});
