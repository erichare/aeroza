import { expect, test } from "@playwright/test";

/**
 * `/docs/api/explorer` smoke test.
 *
 * Regression guard for the all-black render bug fixed in this PR. The
 * Scalar standalone bundle adds `dark-mode` to <body> and writes
 * `html { background: rgb(15,15,15) }` directly on the document root,
 * which previously turned the entire Aeroza page chrome dark-on-dark.
 * The fix re-asserts the Meridian palette via a scoped <style> block
 * with `body.dark-mode` selectors. We assert the page-level
 * background stays light (i.e. our reset wins over the bundle).
 */

test.describe("/docs/api/explorer", () => {
  test("page chrome stays on the Meridian palette under Scalar's dark-mode injection", async ({
    page,
  }) => {
    await page.goto("/docs/api/explorer");

    // Scalar mounts asynchronously via the CDN bundle; wait for the
    // shell so we know the scoped reset has loaded too.
    await expect(page.getByRole("heading", { name: "API explorer" })).toBeVisible();

    // The body picks up `dark-mode` from Scalar — that's expected.
    // The page chrome reset still wins because we use `!important`.
    const bg = await page.evaluate(
      () => getComputedStyle(document.documentElement).backgroundColor,
    );
    // The Meridian palette background is explicitly NOT rgb(15, 15, 15).
    // Loose check: it's not Scalar's dark default. Looser still — any
    // light-ish value is fine — but pinning the failure mode is the goal.
    expect(bg).not.toBe("rgb(15, 15, 15)");
  });
});
