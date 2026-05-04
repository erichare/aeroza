import { expect, test } from "@playwright/test";

/**
 * `/console` webhooks-panel smoke test.
 *
 * The panel was read-only until this PR; now it has a "+ New" button
 * that toggles a create form, plus per-row Delete buttons. We assert
 * the chrome is reachable — full create+delete round-trip would
 * require a real DB session, so the lighter check is "the affordances
 * the user clicks are visible and behave like buttons."
 */

test.describe("/console — webhooks", () => {
  test("New button toggles the create form", async ({ page }) => {
    await page.goto("/console");
    // The panel header carries "Webhooks · subscriptions".
    await expect(page.getByText(/Webhooks · subscriptions/)).toBeVisible();

    const newBtn = page
      .locator(":scope")
      .getByRole("button", { name: /^\+ New$/ })
      .first();
    await expect(newBtn).toBeVisible();
    await newBtn.click();

    // After click, the form's URL field should appear.
    await expect(
      page.getByPlaceholder("https://example.com/aeroza-webhook"),
    ).toBeVisible();
  });
});
