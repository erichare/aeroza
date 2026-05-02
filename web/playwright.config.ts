import { defineConfig, devices } from "@playwright/test";

/**
 * Playwright config for the web console smoke suite.
 *
 * Scope: a thin set of "does the page render?" checks — most importantly
 * that the MapLibre WebGL canvas actually paints something. The Chrome
 * MCP can't verify this (it controls a hidden tab whose rAF loop is
 * paused); a real browser session under Playwright can.
 *
 * The webServer config starts the Next.js prod build automatically so
 * `npm run test:e2e` is one-shot. CI overrides via the env so we can
 * point at an already-running server (faster turnaround on flaky-test
 * iteration, and lets the workflow share one server with multiple
 * test shards if we ever add them).
 */

const PORT = Number(process.env.AEROZA_WEB_E2E_PORT ?? 3100);
const BASE_URL = process.env.AEROZA_WEB_E2E_BASE_URL ?? `http://localhost:${PORT}`;
const REUSE_SERVER = process.env.AEROZA_WEB_E2E_REUSE_SERVER === "1";

export default defineConfig({
  testDir: "./e2e",
  // Single worker — these tests share the dev API on :8000 and we want
  // determinism over wall-clock speed.
  workers: 1,
  forbidOnly: Boolean(process.env.CI),
  retries: process.env.CI ? 2 : 0,
  reporter: process.env.CI ? [["github"], ["list"]] : "list",

  use: {
    baseURL: BASE_URL,
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
    video: "retain-on-failure",
  },

  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],

  webServer: REUSE_SERVER
    ? undefined
    : {
        // Build + start once at the start of the run; Playwright tears it
        // down at the end. Using `next start` rather than `next dev`
        // because the dev server's HMR overlay sometimes fights the test.
        command: `npm run build && npm run start -- --port ${PORT}`,
        url: BASE_URL,
        reuseExistingServer: !process.env.CI,
        timeout: 180_000,
        stdout: "ignore",
        stderr: "pipe",
      },
});
