import { expect, test } from "@playwright/test";

/**
 * `/calibration` smoke test.
 *
 * Verifies the metric switcher exposes all six tabs (MAE / POD / FAR /
 * CSI / Brier / CRPS) and that switching between them changes the
 * panel header. The matrix may be empty in CI (no verifications
 * scored against an ephemeral DB), so we don't assert specific values
 * — just that the chrome wires through correctly. Catches the
 * Tailwind/typecheck regressions that broke the metric switcher
 * during Phase 6f's UI rollout.
 */

test.describe("/calibration", () => {
  test("metric switcher exposes Brier and CRPS tabs", async ({ page }) => {
    await page.goto("/calibration");

    const switcher = page.getByRole("tablist", { name: "Calibration metric" });
    await expect(switcher).toBeVisible();

    for (const label of ["MAE", "POD", "FAR", "CSI", "Brier", "CRPS"]) {
      await expect(
        switcher.getByRole("tab", { name: label, exact: true }),
      ).toBeVisible();
    }
  });

  test("switching to Brier changes the panel header", async ({ page }) => {
    await page.goto("/calibration");

    // Default is MAE.
    await expect(page.getByText(/MAE \/ bias \/ RMSE/)).toBeVisible();

    await page
      .getByRole("tablist", { name: "Calibration metric" })
      .getByRole("tab", { name: "Brier", exact: true })
      .click();

    await expect(page.getByText(/Brier score \(probabilistic\)/)).toBeVisible();
  });
});
