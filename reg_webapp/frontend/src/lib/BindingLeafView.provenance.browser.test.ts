import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import {
  getBindingGraph,
  getBindingLineageWarnings,
  getCatalogNode,
  getDocsForVariable,
  getValueSetCodes,
} from "./api";
import BindingLeafView from "./BindingLeafView.svelte";
import {
  node,
  pickerStates,
  SEED,
  state,
  statesResponse,
} from "./binding-leaf-view-test-helpers";
import { projectStore } from "./project_store.svelte";
import { router } from "./router.svelte";
import { windowStore } from "./window.svelte";

// Split from BindingLeafView.browser.test.ts by contract surface: curated matrix provenance and gap attribution.
// Siblings: BindingLeafView{,.graph,.rows,.technical-details,.provenance,.period}.browser.test.ts.
vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return {
    ...actual,
    getDataWarnings: vi.fn().mockResolvedValue([]),
    getCatalogNode: vi.fn(),
    getBindingGraph: vi.fn(),
    getBindingLineageWarnings: vi.fn(),
    getDocsForVariable: vi.fn(),
    getValueSetCodes: vi.fn(),
  };
});

function matrixProvenance(
  answerKey: string,
  column: string,
  pageNumber: number,
  wave: "cis2014" | "cis2016" = "cis2016",
): string {
  const isCis2014 = wave === "cis2014";
  return (
    `curated:scb-${wave}-matrix-answer\n` +
    JSON.stringify({
      answer_key: answerKey,
      columns: [column],
      evidence: {
        document: `SCB quality declaration, Appendix 2, historical concordance ${isCis2014 ? "CIS2014" : "CIS2016"} column`,
        noted: isCis2014 ? "2026-09-14" : "2026-09-13",
        pages: { [column]: pageNumber },
        question: isCis2014
          ? "VariabelRegister_Källa is Fråga 18 i enkäten Innovationsverksamhet 2012-2014; native edition 2012 - 2014 resolves the target while stale VariabelReferenstid says 2010–2012."
          : "Question 18 of the 2014–2016 questionnaire; mappings use the separately labelled CIS2016 concordance column on pages 23–27.",
        sha256:
          "68513ec189f2222986831f3042a069d66181df3fdb3619e705d0f2ef91ce8c5b",
        url: "https://www.scb.se/contentassets/9e6a00ac2fc7421cabab329528166232/uf0315_kd_2018_ah_191113.pdf#page=23",
      },
      source: {
        cvid: isCis2014 ? 400684 : 469456,
        edition: isCis2014 ? "2012 - 2014" : "2014 - 2016",
        register: "scb/innovation-foretag",
        register_id: 257,
        register_variant: "_default",
        register_variant_id: 553,
        regver_id: isCis2014 ? 7293 : 11529,
        var_id: 15662,
      },
    })
  );
}

beforeEach(() => {
  vi.mocked(getCatalogNode).mockReset();
  vi.mocked(getCatalogNode).mockImplementation(async (_fqid, params) => {
    const variant =
      typeof params?.variant === "string" ? params.variant : undefined;
    return statesResponse(
      pickerStates.filter(
        (s) => variant === undefined || s.variant === variant,
      ),
    );
  });
  // The graph fetch: an EMPTY graph by default (no nodes) → the picker uses the list
  // itself and the header derives no qualifier. Member-identity cases override it.
  vi.mocked(getBindingGraph).mockReset();
  vi.mocked(getBindingGraph).mockResolvedValue({
    nodes: [],
    edges: [],
    focus_id: null,
  } as never);
  vi.mocked(getBindingLineageWarnings).mockReset();
  vi.mocked(getBindingLineageWarnings).mockResolvedValue({
    binding: "scb/lisa/kon",
    lineage_warnings: [],
  } as never);
  vi.mocked(getValueSetCodes).mockReset();
  vi.mocked(getValueSetCodes).mockImplementation(
    async (valueSetId, { state = null, q = "", offset = 0, limit = 200 }) => {
      const codes =
        valueSetId === "814"
          ? [
              { code: "0", label: "Nej" },
              { code: "1", label: "Ja" },
            ]
          : [];
      return {
        value_set_id: String(valueSetId),
        state_id: state,
        period_scope: "intervals",
        q,
        total: codes.length,
        offset,
        limit,
        codes: codes.slice(offset, offset + limit),
      };
    },
  );
  vi.mocked(getDocsForVariable).mockReset();
  vi.mocked(getDocsForVariable).mockResolvedValue({
    results: [],
    total_count: 0,
  } as never);
  // No `?period` — the embedded states drive the plan.
  window.history.pushState({}, "", "/__reset__");
  router.navigate("/catalog/scb/lisa/kon");
  windowStore.set(null);
  projectStore.newProject({
    reg_meta_version: "reg_meta/v1.0.0",
    steward: "global",
  });
});

describe("BindingLeafView representation picker (#678)", () => {
  it("shows curated matrix evidence only after technical details expands while preserving errata", async () => {
    const correctionEvidence =
      "The steward holds CO11 for 2017; SCB's metadata omits it.";
    const question =
      "Question 18 of the 2014–2016 questionnaire; mappings use the separately labelled CIS2016 concordance column on pages 23–27.";
    await render(BindingLeafView, {
      fqidPath: "scb/innovation-foretag/co11",
      node: node(
        [
          state({
            state_id: "20",
            variant: "_default",
            variant_label: "All enterprises",
            delivery_column_name: "CO11",
            valid_from: "2014-01-01",
            valid_to: "2016-12-31",
            provenance: matrixProvenance(
              "group-enterprises-sweden",
              "CO11",
              23,
            ),
          }),
          state({
            state_id: "21",
            variant: "_default",
            variant_label: "All enterprises",
            delivery_column_name: "CO11",
            valid_from: "2017-01-01",
            valid_to: "2017-12-31",
            provenance: `errata:omitted-column-in-version\n${correctionEvidence}`,
          }),
        ],
        {
          fqid: "scb/innovation-foretag/co11",
          name: "Enterprises in Sweden",
        },
      ),
      ...SEED,
      vintageYear: 2024,
    });

    const disclosure = document.querySelector<HTMLDetailsElement>(
      "details.tech-details",
    );
    expect(disclosure?.open).toBe(false);
    await expect
      .element(page.getByText("Curated matrix evidence"))
      .not.toBeVisible();
    await expect.element(page.getByText(question)).not.toBeVisible();
    await expect.element(page.getByText(correctionEvidence)).not.toBeVisible();

    await page.getByText("Technical details", { exact: true }).click();

    await expect
      .element(page.getByText("Curated matrix evidence"))
      .toBeVisible();
    await expect.element(page.getByText(question)).toBeVisible();
    await expect.element(page.getByText(correctionEvidence)).toBeVisible();
    expect(disclosure?.textContent).toContain(
      "Interpretation Curated answer partition interpreted from provider metadata; not a provider assertion or availability correction",
    );
    expect(disclosure?.textContent).toContain(
      "Applies to variant All enterprises",
    );
    expect(disclosure?.textContent).toContain(
      "Applies to interval 2014 – 2016",
    );
    expect(disclosure?.textContent).toContain(
      "Answer key group-enterprises-sweden",
    );
    expect(disclosure?.textContent).toContain("Source column CO11");
    expect(disclosure?.textContent).toContain("Source edition 2014 - 2016");
    expect(disclosure?.textContent).toContain("Evidence page CO11: 23");
    expect(disclosure?.textContent).toContain(
      "Evidence document SCB quality declaration, Appendix 2, historical concordance CIS2016 column",
    );
    expect(disclosure?.textContent).toContain(
      "Original source identifiers cvid=469456; register_id=257; register_variant_id=553; regver_id=11529; var_id=15662",
    );
    expect(disclosure?.textContent).toContain(
      "Document SHA-256 68513ec189f2222986831f3042a069d66181df3fdb3619e705d0f2ef91ce8c5b",
    );
    const sourceLink = page.getByRole("link", { name: "Open source document" });
    await expect
      .element(sourceLink)
      .toHaveAttribute(
        "href",
        "https://www.scb.se/contentassets/9e6a00ac2fc7421cabab329528166232/uf0315_kd_2018_ah_191113.pdf#page=23",
      );
    await expect.element(sourceLink).toHaveAttribute("target", "_blank");
    await expect
      .element(sourceLink)
      .toHaveAttribute("rel", "noopener noreferrer");
    expect(disclosure?.textContent).toContain("Corrected deliveries");
    expect(disclosure?.textContent).toContain("omitted-column-in-version");
  });

  it("renders a CIS2014 matrix provenance with its own edition and identifiers", async () => {
    const question =
      "VariabelRegister_Källa is Fråga 18 i enkäten Innovationsverksamhet 2012-2014; native edition 2012 - 2014 resolves the target while stale VariabelReferenstid says 2010–2012.";
    await render(BindingLeafView, {
      fqidPath:
        "scb/innovation-foretag/cis2014-cooperation-group-enterprises-sweden",
      node: node(
        [
          state({
            state_id: "50",
            variant: "_default",
            variant_label: "All enterprises",
            delivery_column_name: "CO11",
            valid_from: "2012-01-01",
            valid_to: "2014-12-31",
            data_type: null,
            data_length: null,
            value_set_id: "814",
            value_set_version_label: "Ja eller nej",
            value_set_summary: { code_count: 2, integer_range: null },
            provenance: matrixProvenance(
              "group-enterprises-sweden",
              "CO11",
              23,
              "cis2014",
            ),
          }),
        ],
        {
          fqid: "scb/innovation-foretag/cis2014-cooperation-group-enterprises-sweden",
          name: "Other group enterprises — Sweden",
        },
      ),
      ...SEED,
      vintageYear: 2024,
    });

    await page.getByText("Technical details", { exact: true }).click();
    await expect.element(page.getByText(question)).toBeVisible();
    const disclosure = document.querySelector("details.tech-details");
    expect(disclosure?.textContent).toContain("Source edition 2012 - 2014");
    expect(disclosure?.textContent).toContain(
      "Original source identifiers cvid=400684; register_id=257; register_variant_id=553; regver_id=7293; var_id=15662",
    );
  });

  it("limits curated matrix evidence to the selected variant and period", async () => {
    const selected = state({
      state_id: "30",
      variant: "_default",
      variant_label: "All enterprises",
      delivery_column_name: "CO11",
      valid_from: "2014-01-01",
      valid_to: "2016-12-31",
      provenance: matrixProvenance("group-enterprises-sweden", "CO11", 23),
    });
    const selectedPeer = state({
      state_id: "31",
      variant: "_default",
      delivery_column_name: "CO10",
      valid_from: "2014-01-01",
      valid_to: "2016-12-31",
    });
    const otherPeriod = state({
      state_id: "32",
      variant: "_default",
      variant_label: "All enterprises",
      delivery_column_name: "CO11",
      valid_from: "2010-01-01",
      valid_to: "2012-12-31",
      provenance: matrixProvenance(
        "cis2014-out-of-period",
        "CO11",
        23,
        "cis2014",
      ),
    });
    const otherVariant = state({
      state_id: "33",
      variant: "groups",
      variant_label: "Enterprise groups",
      delivery_column_name: "CO12",
      valid_from: "2014-01-01",
      valid_to: "2016-12-31",
      provenance: matrixProvenance("group-foreign", "CO12", 25),
    });
    vi.mocked(getCatalogNode).mockImplementation(async (_fqid, params) => {
      const periodStates = [selected, selectedPeer, otherVariant];
      return statesResponse(
        params?.variant === "_default"
          ? periodStates.filter((item) => item.variant === "_default")
          : periodStates,
      );
    });
    router.navigate(
      "/catalog/scb/innovation-foretag/co11?period=2014..2016&variant=_default",
    );

    await render(BindingLeafView, {
      fqidPath: "scb/innovation-foretag/co11",
      node: node([selected, selectedPeer, otherPeriod, otherVariant]),
      ...SEED,
      vintageYear: 2024,
    });

    await vi.waitFor(() =>
      expect(getCatalogNode).toHaveBeenCalledWith(
        "scb/innovation-foretag/co11",
        expect.objectContaining({
          period: "2014..2016",
          variant: "_default",
        }),
      ),
    );
    await page.getByText("Technical details", { exact: true }).click();

    await expect
      .element(page.getByText("group-enterprises-sweden", { exact: true }))
      .toBeVisible();
    await expect
      .element(page.getByText("cis2014-out-of-period", { exact: true }))
      .not.toBeInTheDocument();
    await expect
      .element(page.getByText("group-foreign", { exact: true }))
      .not.toBeInTheDocument();
    expect(
      document.querySelector<HTMLDetailsElement>("details.tech-details")
        ?.textContent,
    ).not.toContain("cis2014-out-of-period");
    expect(
      document.querySelector<HTMLDetailsElement>("details.tech-details")
        ?.textContent,
    ).not.toContain("CO12");
  });

  it("omits unknown, malformed, or unsafe curated provenance without crashing", async () => {
    await render(BindingLeafView, {
      fqidPath: "scb/innovation-foretag/co11",
      node: node([
        state({
          state_id: "40",
          delivery_column_name: "CO11",
          provenance: "curated:future-format\n{}",
        }),
        state({
          state_id: "41",
          delivery_column_name: "CONA1",
          provenance: "curated:scb-cis2016-matrix-answer\n{not-json}",
        }),
        state({
          state_id: "43",
          delivery_column_name: "CO11",
          provenance: "curated:scb-cis2014-matrix-answer\n{not-json}",
        }),
        state({
          state_id: "44",
          delivery_column_name: "CO12",
          provenance: "curated:scb-cis2014-matrix-answer-extra\n{}",
        }),
        state({
          state_id: "42",
          delivery_column_name: "CO12",
          provenance: matrixProvenance("group-foreign", "CO12", 25).replace(
            "https://",
            "javascript:",
          ),
        }),
      ]),
      ...SEED,
      vintageYear: 2024,
    });

    await expect
      .element(page.getByRole("heading", { name: "Kön" }))
      .toBeVisible();
    await page.getByText("Technical details", { exact: true }).click();
    await expect
      .element(page.getByText("Curated matrix evidence"))
      .not.toBeInTheDocument();
    await expect
      .element(page.getByRole("link", { name: "Open source document" }))
      .not.toBeInTheDocument();
  });

  it("marks resolution-only gaps as inferred and unattributed", async () => {
    await render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node([
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
