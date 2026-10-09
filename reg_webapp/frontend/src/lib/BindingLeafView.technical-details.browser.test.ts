import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import {
  getDocsForVariable,
  getGraph,
  getLineage,
  getStates,
  getValues,
} from "./api";
import BindingLeafView from "./BindingLeafView.svelte";
import {
  leaf,
  pickerStates,
  SEED,
  single,
  state,
} from "./binding-leaf-view-test-helpers";
import { projectStore } from "./project_store.svelte";
import { router } from "./router.svelte";
import { windowStore } from "./window.svelte";

// Split from BindingLeafView.browser.test.ts by contract surface: tags, notes and the Technical details disclosure.
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

const singleWithStructural = [
  state({
    state_id: "1",
    variant: "individer",
    data_type: "char",
    data_length: "1",
    delivery_column_name: "Kon",
  }),
];

beforeEach(() => {
  vi.mocked(getStates).mockReset();
  vi.mocked(getStates).mockImplementation(async (_fqid, params) => {
    const variant =
      typeof params?.variant === "string" ? params.variant : undefined;
    return pickerStates.filter(
      (s) => variant === undefined || s.variant === variant,
    );
  });
  // The graph fetch: an EMPTY graph by default (no nodes) → the picker uses the list
  // itself and the header derives no qualifier. Member-identity cases override it.
  vi.mocked(getGraph).mockReset();
  vi.mocked(getGraph).mockResolvedValue({
    nodes: [],
    edges: [],
    focus_id: null,
  } as never);
  vi.mocked(getLineage).mockReset();
  vi.mocked(getLineage).mockResolvedValue({
    edges: [],
    warnings: [],
    registers: [],
  });
  vi.mocked(getValues).mockReset();
  vi.mocked(getValues).mockResolvedValue({
    items: [],
    next_cursor: null,
    total: 0,
  });
  vi.mocked(getDocsForVariable).mockReset();
  vi.mocked(getDocsForVariable).mockResolvedValue({
    items: [],
    next_cursor: null,
    total: 0,
    register_ingested: true,
  });
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
  it("renders thematic tag chips and recommendation notes", async () => {
    await render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      ...leaf(single, {
        tags: [
          {
            slug: "income",
            label: "Income & earnings",
            rank: 0,
            starred: true,
            note: "primary fixture measure",
          },
        ],
      }),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    await expect.element(page.getByText("Income & earnings")).toBeVisible();
    await expect
      .element(page.getByText("Recommended: primary fixture measure"))
      .toBeVisible();
  });

  it("demotes a single state's Data type / Delivery column into the bottom 'Technical details' disclosure (#1038)", async () => {
    await render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      ...leaf(singleWithStructural),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    await expect
      .element(page.getByText("Technical details").first())
      .toBeVisible();

    const disclosures = [
      ...document.querySelectorAll<HTMLDetailsElement>("details.tech-details"),
    ];
    expect(disclosures).toHaveLength(1);
    const tech = disclosures[0];
    expect(tech.open).toBe(false);
    expect(tech.textContent).toContain("Sensitive");
    expect(tech.textContent).toContain("Identifier");
    expect(tech.textContent).toContain("Data type");
    expect(tech.textContent).toContain("Delivery column");
    expect(tech.closest(".state-detail")).toBeNull();
    const promptMeta = [
      ...document.querySelectorAll(".state-detail dl.meta"),
    ].find((dl) => !dl.closest("details.tech-details"));
    expect(promptMeta).toBeDefined();
    const promptText = promptMeta?.textContent ?? "";
    expect(promptText).toContain("Variant");
    expect(promptText).toContain("Valid");
    expect(tech.textContent).not.toContain("Variant");
    expect(tech.textContent).not.toContain("Value-set version");
  });

  it("shows corrected intervals and evidence only after technical details expands", async () => {
    const evidence = "The steward holds Kon for 2020; SCB's metadata omits it.";
    await render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      ...leaf([
        state({
          state_id: "1",
          variant: "individer",
          variant_label: "Individuals",
          delivery_column_name: "Kon",
          valid_from: "2019-01-01",
          valid_to: "2019-12-31",
        }),
        state({
          state_id: "2",
          variant: "individer",
          variant_label: "Individuals",
          delivery_column_name: "Kon",
          valid_from: "2020-01-01",
          valid_to: "2020-12-31",
          provenance: `errata:omitted-column-in-version\n${evidence}`,
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
    expect(disclosure?.textContent).toContain("Corrected deliveries");
    await expect.element(page.getByText(evidence)).not.toBeVisible();

    await page.getByText("Technical details", { exact: true }).click();

    await expect.element(page.getByText(evidence)).toBeVisible();
    await expect
      .element(page.getByText("omitted-column-in-version", { exact: true }))
      .toBeVisible();
    const interval = document.querySelector<HTMLDivElement>(
      '.correction-list div[title="2020-01-01 – 2020-12-31"]',
    );
    expect(interval?.textContent).toContain("Interval 2020");
    expect(disclosure?.textContent).not.toContain("2019-01-01");
  });

  it("scopes an overlapping correction to its source edition", async () => {
    const evidence = "The steward holds the autumn delivery SCB omitted.";
    await render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      ...leaf([
        state({
          state_id: "2",
          variant: "individer",
          variant_label: "Individuals",
          delivery_column_name: "Kon",
          valid_from: "2022-01-01",
          valid_to: "2022-12-31",
          period_token: "2022",
          provenance:
            "errata:scoped-attributions\n" +
            JSON.stringify([
              {
                class: "omitted-column-in-version",
                evidence,
                source_editions: ["Höstterminen 2022"],
              },
            ]),
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
    await expect.element(page.getByText(evidence)).not.toBeVisible();

    await page.getByText("Technical details", { exact: true }).click();

    await expect.element(page.getByText(evidence)).toBeVisible();
    await expect
      .element(page.getByText("Höstterminen 2022", { exact: true }))
      .toBeVisible();
    expect(disclosure?.textContent).toContain("Catalog interval 2022");
    expect(disclosure?.textContent).toContain(
      "Attribution Provider-documented with a scoped correction",
    );
    expect(disclosure?.textContent).not.toContain("Corrected interval 2022");
  });

  it("keeps evidence paired with each scoped source edition", async () => {
    const springEvidence = "The steward holds the spring AliasA delivery.";
    const autumnEvidence = "The steward holds the autumn AliasB delivery.";
    await render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      ...leaf([
        state({
          state_id: "2",
          variant: "individer",
          variant_label: "Individuals",
          delivery_column_name: "AliasB",
          valid_from: "2021-01-01",
          valid_to: "2021-12-31",
          period_token: "2021",
          provenance:
            "errata:scoped-attributions\n" +
            JSON.stringify([
              {
                class: "omitted-column-in-version",
                evidence: springEvidence,
                source_editions: ["VT2021"],
              },
              {
                class: "omitted-column-in-version",
                evidence: autumnEvidence,
                source_editions: ["HT2021"],
              },
            ]),
        }),
      ]),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    await page.getByText("Technical details", { exact: true }).click();

    const items = Array.from(
      document.querySelectorAll<HTMLLIElement>(".correction-list > li"),
    );
    expect(items).toHaveLength(2);
    expect(items[0]?.textContent).toContain("VT2021");
    expect(items[0]?.textContent).toContain(springEvidence);
    expect(items[0]?.textContent).not.toContain("HT2021");
    expect(items[0]?.textContent).not.toContain(autumnEvidence);
    expect(items[1]?.textContent).toContain("HT2021");
    expect(items[1]?.textContent).toContain(autumnEvidence);
    expect(items[1]?.textContent).not.toContain("VT2021");
    expect(items[1]?.textContent).not.toContain(springEvidence);
  });

  it("keeps correction-only evidence paired without provider attribution", async () => {
    const springEvidence = "The steward holds the spring AliasA delivery.";
    const autumnEvidence = "The steward holds the autumn AliasB delivery.";
    await render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      ...leaf([
        state({
          state_id: "2",
          variant: "individer",
          variant_label: "Individuals",
          delivery_column_name: "AliasB",
          valid_from: "2021-01-01",
          valid_to: "2021-12-31",
          period_token: "2021",
          provenance:
            "errata:overlapping-attributions\n" +
            JSON.stringify([
              {
                class: "omitted-column-in-version",
                evidence: springEvidence,
                source_editions: ["VT2021"],
              },
              {
                class: "omitted-column-in-version",
                evidence: autumnEvidence,
                source_editions: ["HT2021"],
              },
            ]),
        }),
      ]),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    await page.getByText("Technical details", { exact: true }).click();

    const items = Array.from(
      document.querySelectorAll<HTMLLIElement>(".correction-list > li"),
    );
    expect(items).toHaveLength(2);
    expect(items[0]?.textContent).toContain("VT2021");
    expect(items[0]?.textContent).toContain(springEvidence);
    expect(items[0]?.textContent).not.toContain("HT2021");
    expect(items[0]?.textContent).not.toContain(autumnEvidence);
    expect(items[1]?.textContent).toContain("HT2021");
    expect(items[1]?.textContent).toContain(autumnEvidence);
    expect(items[1]?.textContent).not.toContain("VT2021");
    expect(items[1]?.textContent).not.toContain(springEvidence);
    const disclosure = document.querySelector<HTMLDetailsElement>(
      "details.tech-details",
    );
    expect(disclosure?.textContent).toContain("Interval 2021");
    expect(disclosure?.textContent).not.toContain("Catalog interval");
    expect(disclosure?.textContent).not.toContain("Provider-documented");
  });
});
