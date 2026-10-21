import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import BindingLeafView from "./BindingLeafView.svelte";
import { leaf, resetLeaf, SEED, state } from "./binding-leaf-view-test-helpers";

// Split from BindingLeafView.browser.test.ts by contract surface: inferred gap attribution.
// Siblings: BindingLeafView{,.graph,.rows,.technical-details,.provenance,.period}.browser.test.ts.
vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return {
    ...actual,
    getWarnings: vi.fn().mockResolvedValue([]),
    getStates: vi.fn(),
    getGraph: vi.fn(),
    getLineage: vi.fn(),
    getDocsForVariable: vi.fn(),
    getValues: vi.fn(),
  };
});

beforeEach(resetLeaf);

describe("BindingLeafView representation picker (#678)", () => {
  it("marks resolution-only gaps as inferred and unattributed", async () => {
    await render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      ...leaf([
        state({
          state_id: "2",
          variant: "individer",
          variant_label: "Individuals",
          delivery_column_name: "AliasA",
          valid_from: "2021-07-01",
          valid_to: "2021-12-31",
          period_token: "HT2021",
          provenance: "inferred:resolution-gap",
        }),
      ]),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    const disclosure = document.querySelector<HTMLDetailsElement>(
      "details.tech-details",
    );
    expect(disclosure?.open).toBe(false);
    await expect
      .element(page.getByText("Inferred intervals"))
      .not.toBeVisible();

    await page.getByText("Technical details", { exact: true }).click();

    await expect.element(page.getByText("Inferred intervals")).toBeVisible();
    expect(disclosure?.textContent).toContain(
      "Interval 2021-07-01 – 2021-12-31",
    );
    expect(disclosure?.textContent).toContain(
      "Attribution Unattributed; retained by catalog resolution",
    );
    expect(disclosure?.textContent).not.toContain("Corrected deliveries");
    expect(disclosure?.textContent).not.toContain("Provider-documented");
  });
});
