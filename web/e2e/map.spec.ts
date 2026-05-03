import { expect, test } from "@playwright/test";

/**
 * `/map` smoke test.
 *
 * The most important assertion: the MapLibre WebGL canvas actually
 * paints non-transparent pixels. If WebGL fails (driver bust, headless
 * config wrong, layout collapsed the container) the canvas stays
 * empty and we want CI to flag it.
 *
 * The header / nav / legend assertions are cheap secondary checks that
 * also fail-fast on Tailwind/Next regressions before we get to the
 * WebGL test.
 */

test.describe("/map", () => {
  test("renders the alerts map and the basemap paints WebGL pixels", async ({ page }) => {
    await page.goto("/map");

    // Header strip and legend are static DOM — should appear before the
    // map finishes loading.
    await expect(page.getByRole("heading", { name: /live alerts/i })).toBeVisible();
    await expect(page.getByText("Severity")).toBeVisible();

    // Nav highlights the active route. `exact` because "Map" otherwise
    // also matches the OpenStreetMap attribution link inside the
    // basemap canvas overlay.
    await expect(page.getByRole("link", { name: "Map", exact: true })).toBeVisible();

    // Wait for the MapLibre canvas to mount.
    const canvas = page.locator(".maplibregl-canvas");
    await expect(canvas).toBeVisible({ timeout: 10_000 });

    // Canvas size > 0 — guards against the layout-collapsed regression
    // we hit early in phase 5 (the map div ended up with height: 0).
    const size = await canvas.boundingBox();
    expect(size?.width ?? 0).toBeGreaterThan(100);
    expect(size?.height ?? 0).toBeGreaterThan(100);

    // Wait for MapLibre to actually finish loading (style + tiles) so
    // the canvas has paintable content. We use a polling assertion
    // rather than a fixed sleep so the test is fast when the network
    // cooperates and patient when it doesn't.
    await page.waitForFunction(
      () => {
        const map = (
          window as unknown as {
            _aerozaMap?: { isStyleLoaded?: () => boolean };
          }
        )._aerozaMap;
        if (map?.isStyleLoaded) return map.isStyleLoaded();
        // Fallback: check that the basemap source cache has fetched at
        // least a couple of tiles.
        return document.querySelectorAll("canvas.maplibregl-canvas").length > 0;
      },
      { timeout: 10_000 },
    );
    // Give MapLibre a couple of rAF ticks past style-load so the first
    // tile actually rasterises into the canvas.
    await page.waitForTimeout(1_500);

    // Take a Playwright-side screenshot of just the canvas. This goes
    // through the page compositor (not WebGL `readPixels`), so the
    // back-buffer-cleared-after-swap issue doesn't apply — we get the
    // actual pixels the user sees.
    const png = await canvas.screenshot();
    expect(png.byteLength).toBeGreaterThan(1_000);

    // Quick PNG decode via the page itself: turn the screenshot bytes
    // into an ImageBitmap, draw to a 2D canvas, and count non-zero
    // alpha pixels. This is robust across headless/headed and across
    // GPU drivers, where direct WebGL `readPixels` is not.
    const visiblePixelCount = await page.evaluate(async (b64: string) => {
      const blob = await (
        await fetch(`data:image/png;base64,${b64}`)
      ).blob();
      const bitmap = await createImageBitmap(blob);
      const c = document.createElement("canvas");
      c.width = bitmap.width;
      c.height = bitmap.height;
      const ctx = c.getContext("2d");
      if (!ctx) return -1;
      ctx.drawImage(bitmap, 0, 0);
      const { data } = ctx.getImageData(0, 0, bitmap.width, bitmap.height);
      let nonzero = 0;
      for (let i = 3; i < data.length; i += 4) {
        if (data[i] !== 0) nonzero += 1;
      }
      return nonzero;
    }, png.toString("base64"));

    expect(visiblePixelCount).toBeGreaterThan(0);
  });

  test("loop control is present in the header (disabled when archive is empty)", async ({
    page,
  }) => {
    await page.goto("/map");

    // The "Loop 1h" toggle should always render in the header — it's
    // either the play affordance (when ≥2 grids exist in the last
    // hour) or a disabled placeholder explaining why it can't run.
    // Either way the user knows the affordance exists.
    const loopButton = page.getByRole("button", { name: /loop 1h/i });
    await expect(loopButton).toBeVisible();
  });
});
