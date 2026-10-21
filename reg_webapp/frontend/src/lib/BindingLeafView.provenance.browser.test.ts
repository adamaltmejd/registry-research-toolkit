import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import { getRelatedDocuments } from "./api";
import BindingLeafView from "./BindingLeafView.svelte";
import { leaf, resetLeaf, SEED, state } from "./binding-leaf-view-test-helpers";

// Split from BindingLeafView.browser.test.ts by contract surface: inferred gap
// attribution and curated matrix-answer sources.
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
    getRelatedDocuments: vi.fn(),
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

  it("cites a curated matrix answer's source page and links the rehosted PDF", async () => {
    // Fails if the Source line disappears, or links the provider URL although the
    // catalog serves the cited PDF (matched by its SHA256).
    const sha256 =
      "90140a04b0e3e592a0f9b6814aa09005ed655441336747cec5f1cfbe3fc7b0d7";
    vi.mocked(getRelatedDocuments).mockResolvedValue([
      {
        title: "CIS 2016 questionnaire",
        filename: "cis2016.pdf",
        source_url: "https://www.scb.se/cis2016.pdf",
        license: "CC BY 4.0",
        fetched: "2026-06-01",
        sha256,
        byte_size: 48,
      },
    ]);
    await render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      ...leaf([
        state({
          state_id: "2",
          delivery_column_name: "SAMARB_B",
          provenance: [
            "curated:matrix-answer",
            "Document: https://www.scb.se/cis2016.pdf",
            `SHA256: ${sha256}`,
            "Pages: SAMARB_A=14, SAMARB_B=12",
          ].join("\n"),
        }),
      ]),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    await expect
      .element(page.getByText("CIS 2016 questionnaire"))
      .not.toBeVisible();

    await page.getByText("Technical details", { exact: true }).click();

    const link = page.getByRole("link", { name: "CIS 2016 questionnaire" });
    await expect.element(link).toBeVisible();
    expect(link.element().getAttribute("href")).toBe(
      "/api/docs/file/scb/lisa/cis2016.pdf",
    );
    expect(
      link.element().closest("p")?.textContent?.replace(/\s+/g, " ").trim(),
    ).toBe("CIS 2016 questionnaire, p. 12");
  });
});
