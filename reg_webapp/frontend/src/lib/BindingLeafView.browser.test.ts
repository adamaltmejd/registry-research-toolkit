import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type {
  BindingNodeData,
  GraphState,
  RelationshipGraph,
  StatesResponse,
  VariableGraphNode,
  VariableStateModel,
} from "./api";
import {
  getBindingGraph,
  getBindingLineageWarnings,
  getCatalogNode,
  getDocsForVariable,
  getRelatedDocuments,
} from "./api";
import BindingLeafView from "./BindingLeafView.svelte";
import { projectStore } from "./project_store.svelte";
import { router } from "./router.svelte";
import { windowStore } from "./window.svelte";

// Two surfaces under test:
//   1. the direct representation picker (#678) — the variable's representations
//      list as selectable rows; selecting rows + Add commits the right
//      `addFromCatalog` payloads; out-of-window rows dim; empty selection / no
//      seed disables Add.
//   2. the #670 member identity, now derived from the relationship-graph FOCUS node
//      (#678/#904) — the leaf's single `/graph` fetch feeds both the picker graph
//      renderer AND the header qualifier + "member of ⟨group⟩" link.
//
// The leaf + sibling-panel GETs (graph / lineage warnings / parsed docs / the
// ?period resolve) are stubbed so nothing hits a real fetch;
// the panels are independent failure domains, so an empty/rejecting stub never
// blanks the picker under test.
vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return {
    ...actual,
    getCatalogNode: vi.fn(),
    getBindingGraph: vi.fn(),
    getBindingLineageWarnings: vi.fn(),
    getDocsForVariable: vi.fn(),
    getRelatedDocuments: vi.fn(),
  };
});

/** A minimal VariableStateModel — only the fields the add planner reads. */
function state(over: Partial<VariableStateModel>): VariableStateModel {
  return {
    state_id: 1,
    variant: "v",
    variant_label: null,
    register_variant_id: 1,
    valid_from: "1992-01-01",
    valid_to: "9999-12-31",
    data_type: null,
    data_length: null,
    delivery_column_name: null,
    source_register_text: null,
    value_set_version_label: "",
    value_set_id: null,
    value_set: null,
    is_identifier: false,
    classification_slug: null,
    ...over,
  };
}

function gstate(over: Partial<GraphState>): GraphState {
  return {
    state_id: 1,
    variant: "v",
    variant_label: null,
    representation_run_id: 1,
    valid_from: "2000-01-01",
    valid_to: "2020-12-31",
    value_set_id: null,
    value_set_version_label: "",
    classification_slug: null,
    delivery_column_name: null,
    ...over,
  };
}

/** A minimal BindingNode leaf carrying `states`; the embedded edge arms are empty
 * so only the picker under test renders. `over` lets a case add fields the #670
 * member-identity path reads (`fqid`, `name`, `group`). */
function node(
  states: VariableStateModel[],
  over: Partial<BindingNodeData> = {},
): BindingNodeData {
  return {
    kind: "binding",
    fqid: "scb/lisa/kon",
    name: "Kön",
    definition: null,
    description: null,
    measurement_unit: null,
    is_identifier: false,
    is_sensitive: false,
    register_id: 1,
    variable_id: 1,
    source_register_id: null,
    source_register_text: null,
    states,
    same_as: [],
    lineage: [],
    succession_chain: [],
    via_same_as: null,
    ...over,
  } as unknown as BindingNodeData;
}

function statesResponse(states: VariableStateModel[]): StatesResponse {
  return { states } as unknown as StatesResponse;
}

/** A relationship graph whose focus node is a variable carrying the given facets +
 * group label — the #670 header-identity source. `focusFqid` is the focus node's
 * own fqid (may differ from the leaf's under a same_as alias). */
function graph(
  over: Partial<VariableGraphNode> = {},
  focusId = "v1",
): RelationshipGraph {
  const focus: VariableGraphNode = {
    kind: "variable",
    id: focusId,
    fqid: "scb/lisa/kon",
    label: "Kön",
    group_key: null,
    group_label: null,
    definition: null,
    description: null,
    operational_definition: null,
    facets: [],
    states: [],
    same_as: [],
    ...over,
  };
  return { nodes: [focus], edges: [], focus_id: focusId };
}

/** One variant, no delivery column → the picker enumerates zero rows. */
const single = [state({ state_id: 1, variant: "individer" })];

/** Picker rows: two distinct (variant, delivery column) representations, each
 * with a finite window. `Kon` (individer, 2010–2015) and `Sni` (arbetsstallen,
 * 2018–2020) — two selectable rows over the full history. */
const pickerStates = [
  state({
    state_id: 1,
    variant: "individer",
    delivery_column_name: "Kon",
    valid_from: "2010-01-01",
    valid_to: "2015-12-31",
    value_set_version_label: "1-siffrig",
  }),
  state({
    state_id: 2,
    variant: "arbetsstallen",
    delivery_column_name: "Sni",
    valid_from: "2018-01-01",
    valid_to: "2020-12-31",
    value_set_version_label: "SNI 2007",
  }),
];

const singleWithStructural = [
  state({
    state_id: 1,
    variant: "individer",
    data_type: "char",
    data_length: "1",
    delivery_column_name: "Kon",
  }),
];

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
  vi.mocked(getDocsForVariable).mockReset();
  vi.mocked(getDocsForVariable).mockResolvedValue({
    results: [],
    total_count: 0,
  } as never);
  vi.mocked(getRelatedDocuments).mockReset();
  vi.mocked(getRelatedDocuments).mockResolvedValue({
    kind: "related-documents",
    ingested: true,
    register: "lisa",
    documents: [],
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

const SEED = {
  regMetaVersion: "reg_meta/v1.0.0",
  steward: "global",
  windowMinYear: 1960,
} as const;

describe("BindingLeafView representation picker (#678)", () => {
  it("does not fetch or render register source documents on variable pages (#967)", async () => {
    vi.mocked(getRelatedDocuments).mockResolvedValue({
      kind: "related-documents",
      ingested: true,
      register: "lisa",
      documents: [
        {
          title: "LISA source PDF",
          filename: "lisa.pdf",
          source_url: "https://www.scb.se/lisa",
          license: "CC BY 4.0",
          fetched: "2026-06-01",
          sha256: "a".repeat(64),
          byte_size: 1024,
        },
      ],
    });

    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(single),
      ...SEED,
      vintageYear: 2024,
    });

    await expect
      .element(page.getByRole("heading", { name: "Kön" }))
      .toBeVisible();
    await expect
      .element(page.getByRole("heading", { name: "Source documents" }))
      .not.toBeInTheDocument();
    expect(getRelatedDocuments).not.toHaveBeenCalled();
  });

  it("mounts the picker graph when no delivery-column rows are selectable", async () => {
    vi.mocked(getBindingGraph).mockResolvedValue({
      nodes: [
        {
          kind: "variable",
          id: "v1",
          fqid: "scb/lisa/kon",
          label: "Kön",
          group_key: "g",
          group_label: "Kön concept",
          definition: null,
          description: null,
          operational_definition: null,
          facets: [],
          states: [
            gstate({
              delivery_column_name: null,
              value_set_version_label: "uncolumned coding",
            }),
          ],
          same_as: [],
        },
        {
          kind: "variable",
          id: "v2",
          fqid: "scb/lisa/kon2",
          label: "Kön successor",
          group_key: "g",
          group_label: "Kön concept",
          definition: null,
          description: null,
          operational_definition: null,
          facets: [],
          states: [
            gstate({
              state_id: 2,
              representation_run_id: 2,
              delivery_column_name: null,
              valid_from: "2021-01-01",
              valid_to: null,
            }),
          ],
          same_as: [],
        },
      ],
      edges: [
        {
          id: "v1-v2",
          kind: "succession",
          source: "v1",
          target: "v2",
          label: null,
          effective_year: 2021,
        },
      ],
      focus_id: "v1",
    } as RelationshipGraph as never);

    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(single),
      ...SEED,
      vintageYear: 2024,
    });

    await vi.waitFor(() => {
      if (!document.querySelector(".rep-picker .graph-picker")) {
        throw new Error("picker graph not rendered");
      }
    });
    expect(document.querySelector(".col-list")).toBeNull();
    expect(document.body.textContent).toContain("uncolumned coding");
    await expect
      .element(page.getByRole("link", { name: "kon2" }))
      .toBeVisible();
  });

  it("does not leave an empty picker when a zero-row graph is rejected", async () => {
    const nodes: VariableGraphNode[] = Array.from({ length: 19 }, (_, i) => ({
      kind: "variable",
      id: `v${i}`,
      fqid: i === 0 ? "scb/lisa/kon" : `scb/lisa/kon${i}`,
      label: i === 0 ? "Kön" : `Kön ${i}`,
      group_key: "huge",
      group_label: "Huge concept",
      definition: null,
      description: null,
      operational_definition: null,
      facets: [],
      states: [
        gstate({
          state_id: i + 1,
          representation_run_id: i + 1,
          delivery_column_name: null,
          value_set_version_label: `coding ${i}`,
        }),
      ],
      same_as: [],
    }));
    vi.mocked(getBindingGraph).mockResolvedValue({
      nodes,
      edges: [
        {
          id: "v0-v1",
          kind: "succession",
          source: "v0",
          target: "v1",
          label: null,
          effective_year: 2021,
        },
      ],
      focus_id: "v0",
    } as RelationshipGraph as never);

    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(single),
      ...SEED,
      vintageYear: 2024,
    });

    await vi.waitFor(() => {
      expect(getBindingGraph).toHaveBeenCalledTimes(1);
      if (!document.querySelector(".member-identity .qualifier")) {
        throw new Error("graph-derived member identity not rendered");
      }
      expect(document.querySelector(".graph-picker")).toBeNull();
      expect(document.querySelector(".rep-picker")).toBeNull();
      expect(document.querySelector(".col-list")).toBeNull();
    });
  });

  it("does not clamp open-ended graph timelines to the steward period ceiling", async () => {
    const openEnded = graph({
      states: [
        gstate({
          valid_from: "2000-01-01",
          valid_to: null,
          delivery_column_name: "Kon",
        }),
      ],
    });
    openEnded.nodes.push({
      kind: "variable",
      id: "v2",
      fqid: "scb/lisa/kon2",
      label: "Kön successor",
      group_key: null,
      group_label: null,
      definition: null,
      description: null,
      operational_definition: null,
      facets: [],
      states: [
        gstate({
          state_id: 2,
          representation_run_id: 2,
          delivery_column_name: "Kon2",
          valid_from: "2005-01-01",
          valid_to: "2008-12-31",
        }),
      ],
      same_as: [],
    });
    openEnded.edges.push({
      id: "v1-v2",
      kind: "succession",
      source: "v1",
      target: "v2",
      label: null,
      effective_year: 2005,
    });
    vi.mocked(getBindingGraph).mockResolvedValue(openEnded as never);

    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node([
        state({
          valid_from: "2000-01-01",
          valid_to: "9999-12-31",
          delivery_column_name: "Kon",
        }),
      ]),
      ...SEED,
      windowMinYear: 2000,
      windowMaxYear: 2010,
      vintageYear: 2026,
    });

    await expect.element(page.getByText("coverage through 2010")).toBeVisible();

    const graphTicks = await vi.waitFor(() => {
      const labels = [
        ...document.querySelectorAll(".graph-picker .graph-tick"),
      ].map((el) => el.textContent?.trim() ?? "");
      if (!labels.includes("2026")) {
        throw new Error(`picker graph ticks not ready: ${labels.join(", ")}`);
      }
      return labels;
    });
    expect(graphTicks).toContain("2026");
  });

  it("lists each representation row with its delivery column + period span", async () => {
    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(pickerStates),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    // One row per (variant, delivery column), keyed checkbox per column.
    await expect
      .element(page.getByRole("checkbox", { name: /Kon/ }))
      .toBeVisible();
    await expect
      .element(page.getByRole("checkbox", { name: /Sni/ }))
      .toBeVisible();
    // The column's full-history period span is shown (scoped to the picker — the
    // same span text also appears in the value-set viewer's usage list).
    const spans = await vi.waitFor(() => {
      const els = [...document.querySelectorAll(".rep-picker .period")].map(
        (el) => el.textContent?.trim(),
      );
      if (els.length < 2) {
        throw new Error("picker period spans not yet rendered");
      }
      return els;
    });
    expect(spans).toEqual(["2010 – 2015", "2018 – 2020"]);
  });

  it("renders the add footer only once a row is staged", async () => {
    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(pickerStates),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    await expect
      .element(page.getByRole("button", { name: "Add to project" }))
      .not.toBeInTheDocument();

    const konRow = page.getByRole("checkbox", { name: /Kon/ });
    await konRow.click();
    await expect.element(konRow).toBeChecked();
    const add = page.getByRole("button", { name: "Add to project" });
    await expect.element(add).toBeEnabled();
    await expect.element(page.getByText("+1 column")).toBeVisible();
  });

  it("a partially-selected variable's select-all is INDETERMINATE with no accent fill (#678)", async () => {
    // pickerStates is a 2-column variable (Kon/Sni → a subheading). Selecting ONE
    // column makes the variable's select-all indeterminate: native :indeterminate
    // (the dash), NOT :checked (so the accent-fill rule never applies).
    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(pickerStates),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    await page.getByRole("checkbox", { name: /Kon/ }).click();

    const selectAll = await vi.waitFor(() => {
      const el = document.querySelector<HTMLInputElement>(
        'input[aria-label^="Select all columns of"]',
      );
      if (!el) {
        throw new Error("variable select-all not yet rendered");
      }
      return el;
    });
    // Partial → indeterminate, NOT checked (the accent fill is :checked-only, so the
    // box keeps its surface bg + border with only the visible dash).
    expect(selectAll.indeterminate).toBe(true);
    expect(selectAll.checked).toBe(false);

    // Selecting the OTHER column flips it to fully checked (accent fill returns).
    await page.getByRole("checkbox", { name: /Sni/ }).click();
    await vi.waitFor(() => {
      if (!selectAll.checked || selectAll.indeterminate) {
        throw new Error("select-all not yet fully checked");
      }
    });
  });

  it("selecting rows + Apply commits the right staged diff", async () => {
    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(pickerStates),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    await page.getByRole("checkbox", { name: /Kon/ }).click();
    await page.getByRole("checkbox", { name: /Sni/ }).click();
    await expect.element(page.getByText("+2 columns")).toBeVisible();
    await page
      .getByRole("button", {
        name: /Add to project|Remove from project|Apply changes/,
      })
      .click();

    await expect.element(page.getByText(/\+2 columns/)).toBeVisible();
    expect(projectStore.draft?.sources).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          register_variant: "scb/lisa/individer",
          period: { from: 2010, to: 2015 },
          bindings: [
            expect.objectContaining({
              variable: "scb/lisa/kon",
              representation: null,
              display_name: "Kon",
            }),
          ],
        }),
        expect.objectContaining({
          register_variant: "scb/lisa/arbetsstallen",
          period: { from: 2018, to: 2020 },
          bindings: [
            expect.objectContaining({
              variable: "scb/lisa/kon",
              representation: null,
              display_name: "Sni",
            }),
          ],
        }),
      ]),
    );
  });

  it("resolves a staged add against the final merged source period", async () => {
    projectStore.applyStagedDiff({
      adds: [
        {
          registerVariant: "scb/lisa/individer",
          period: 2000,
          binding: {
            variable: "scb/lisa/other",
            type: "opaque",
          },
        },
      ],
    });
    const periods: (string | undefined)[] = [];
    vi.mocked(getCatalogNode).mockImplementation(async (_fqid, params) => {
      periods.push(params?.period);
      return statesResponse([
        state({
          variant: "individer",
          delivery_column_name: "Kon",
          data_type: "int",
        }),
      ]);
    });

    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(pickerStates),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    await page.getByRole("checkbox", { name: /Kon/ }).click();
    await page
      .getByRole("button", {
        name: /Add to project|Remove from project|Apply changes/,
      })
      .click();

    await expect.element(page.getByText(/\+1 column/)).toBeVisible();
    expect(periods).toContain("2000,2010..2015");
    expect(periods).not.toContain("2010..2015");
    expect(projectStore.draft?.sources[0]).toEqual(
      expect.objectContaining({
        period: [2000, { from: 2010, to: 2015 }],
        bindings: expect.arrayContaining([
          expect.objectContaining({ variable: "scb/lisa/other" }),
          expect.objectContaining({
            variable: "scb/lisa/kon",
            type: "numeric",
          }),
        ]),
      }),
    );
  });

  it("clears the previous applied confirmation when a new staged diff starts", async () => {
    vi.mocked(getCatalogNode).mockResolvedValue(
      statesResponse([
        state({
          variant: "individer",
          delivery_column_name: "Kon",
          data_type: "int",
        }),
      ]),
    );
    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(pickerStates),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    await page.getByRole("checkbox", { name: /Kon/ }).click();
    await page
      .getByRole("button", {
        name: /Add to project|Remove from project|Apply changes/,
      })
      .click();
    await vi.waitFor(() => {
      expect(document.querySelector(".add-confirm")?.textContent).toContain(
        "+1 column",
      );
    });

    await page.getByRole("checkbox", { name: /Kon/ }).click();

    await expect.element(page.getByText("-1 column")).toBeVisible();
    await vi.waitFor(() => {
      expect(document.querySelector(".add-confirm")).toBeNull();
    });
  });

  it("keeps staged keys visible when a draft edit makes async apply stale", async () => {
    let resolveFetch: () => void = () => {
      throw new Error("resolve fetch not started");
    };
    const resolveStarted = new Promise<void>((resolve) => {
      vi.mocked(getCatalogNode).mockImplementation(async (_fqid, params) => {
        if (params?.period === "2010..2015") {
          resolve();
          await new Promise<void>((done) => {
            resolveFetch = done;
          });
          return statesResponse([
            state({
              variant: "individer",
              delivery_column_name: "Kon",
              data_type: "int",
            }),
          ]);
        }
        return statesResponse(pickerStates);
      });
    });

    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(pickerStates),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    await page.getByRole("checkbox", { name: /Kon/ }).click();
    await expect.element(page.getByText("Will be added")).toBeVisible();
    const apply = page.getByRole("button", {
      name: /Add to project|Remove from project|Apply changes/,
    });
    await apply.click();
    await resolveStarted;

    projectStore.updateField("name", "edited during apply");
    resolveFetch();

    await expect.element(page.getByText("Will be added")).toBeVisible();
    await expect.element(page.getByText("+1 column")).toBeVisible();
    expect(projectStore.draft?.sources).toHaveLength(0);
    expect(projectStore.draft?.name).toBe("edited during apply");
  });

  it("renders committed rows and applies a staged remove only on Apply", async () => {
    projectStore.applyStagedDiff({
      adds: [
        {
          registerVariant: "scb/lisa/individer",
          period: { from: 2010, to: 2015 },
          binding: {
            variable: "scb/lisa/kon",
            type: "opaque",
            display_name: "Kon",
            representation: "Kon",
          },
        },
      ],
    });
    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(pickerStates),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    const kon = page.getByRole("checkbox", { name: /Kon/ });
    await expect.element(kon).toBeChecked();
    await expect.element(page.getByText("In project")).toBeVisible();

    await kon.click();
    await expect.element(kon).not.toBeChecked();
    await expect.element(page.getByText("Will be removed")).toBeVisible();
    await expect.element(page.getByText("-1 column")).toBeVisible();
    expect(projectStore.draft?.sources).toHaveLength(1);

    await page
      .getByRole("button", {
        name: /Add to project|Remove from project|Apply changes/,
      })
      .click();
    await expect.element(page.getByText(/-1 column/)).toBeVisible();
    expect(projectStore.draft?.sources).toHaveLength(0);
  });

  it("does not stage source period replacements from a partial leaf view", async () => {
    projectStore.applyStagedDiff({
      adds: [
        {
          registerVariant: "scb/lisa/individer",
          period: { from: 2010, to: 2015 },
          binding: {
            variable: "scb/lisa/kon",
            type: "opaque",
            display_name: "Kon",
            representation: "Kon",
          },
        },
      ],
    });
    vi.mocked(getCatalogNode).mockResolvedValue({
      states: pickerStates,
    } as never);
    router.navigate("/catalog/scb/lisa/kon?period=2012..2014");

    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(pickerStates),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    await expect
      .element(page.getByText("No staged changes"))
      .not.toBeInTheDocument();
    await expect
      .element(
        page.getByRole("button", {
          name: /Add to project|Remove from project|Apply changes/,
        }),
      )
      .not.toBeInTheDocument();

    expect(projectStore.draft?.sources[0]?.period).toEqual({
      from: 2010,
      to: 2015,
    });
  });

  it("does not stage a source period replacement for an invalid ?period", async () => {
    projectStore.applyStagedDiff({
      adds: [
        {
          registerVariant: "scb/lisa/individer",
          period: { from: 2010, to: 2015 },
          binding: {
            variable: "scb/lisa/kon",
            type: "opaque",
            display_name: "Kon",
            representation: "Kon",
          },
        },
      ],
    });
    router.navigate("/catalog/scb/lisa/kon?period=2020,2019");

    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(pickerStates),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    await expect
      .element(page.getByText("No staged changes"))
      .not.toBeInTheDocument();
    await expect
      .element(
        page.getByRole("button", {
          name: /Add to project|Remove from project|Apply changes/,
        }),
      )
      .not.toBeInTheDocument();
    expect(projectStore.draft?.sources[0]?.period).toEqual({
      from: 2010,
      to: 2015,
    });
  });

  it("does not clamp staged adds with a structurally invalid ?period", async () => {
    router.navigate("/catalog/scb/lisa/kon?period=2020,2019");

    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(pickerStates),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    await page.getByRole("checkbox", { name: /Sni/ }).click();
    await page
      .getByRole("button", {
        name: /Add to project|Remove from project|Apply changes/,
      })
      .click();

    await expect.element(page.getByText("+1 column")).toBeVisible();
    expect(projectStore.draft?.sources[0]?.period).toEqual({
      from: 2018,
      to: 2020,
    });
  });

  // #902: a folded sequential RENAME commits `representation: null`, NOT the latest
  // column — the picker leads the row with the latest column (DINF86) for DISPLAY, but
  // pinning it over the union 1981–1995 window would break the earlier eras (DINF86
  // wasn't delivered before 1986). Null lets per-period resolution pick the right column
  // per year.
  it("a folded rename row commits representation: null (not the latest column)", async () => {
    const renameStates = [
      state({
        state_id: 1,
        variant: "individer",
        delivery_column_name: "DINF",
        valid_from: "1981-01-01",
        valid_to: "1983-12-31",
      }),
      state({
        state_id: 2,
        variant: "individer",
        delivery_column_name: "DINF83",
        valid_from: "1984-01-01",
        valid_to: "1985-12-31",
      }),
      // Contiguous eras (no delivery gap) so the union wire period is a single
      // 1981..1995 span — the rename fold, not an interrupted series.
      state({
        state_id: 3,
        variant: "individer",
        delivery_column_name: "DINF86",
        valid_from: "1986-01-01",
        valid_to: "1995-12-31",
      }),
    ];
    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(renameStates),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    // ONE folded row, led by the latest column DINF86 (the display identity / chip).
    await page.getByRole("checkbox", { name: /DINF86/ }).click();
    await page
      .getByRole("button", {
        name: /Add to project|Remove from project|Apply changes/,
      })
      .click();

    await expect.element(page.getByText(/\+1 column/)).toBeVisible();
    expect(projectStore.draft?.sources[0]).toEqual(
      expect.objectContaining({
        register_variant: "scb/lisa/individer",
        period: { from: 1981, to: 1995 },
        bindings: [
          expect.objectContaining({
            variable: "scb/lisa/kon",
            // NOT "DINF86" — the rename fold commits null so resolution picks per year.
            representation: null,
          }),
        ],
      }),
    );
  });

  // #678 finding 3: an active ?period is HONORED on add — the committed period is
  // the row span INTERSECTED with the window, not the row's full span.
  it("commits the row span intersected with the active ?period (not the full span)", async () => {
    vi.mocked(getCatalogNode).mockResolvedValue({
      states: pickerStates,
    } as never);
    // Kon spans 2010–2015; narrow to 2012..2014.
    router.navigate("/catalog/scb/lisa/kon?period=2012..2014");
    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(pickerStates),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    const kon = page.getByRole("checkbox", { name: /Kon/ });
    await expect.element(kon).toBeVisible();
    await kon.click();
    await page
      .getByRole("button", {
        name: /Add to project|Remove from project|Apply changes/,
      })
      .click();

    await expect.element(page.getByText(/\+1 column/)).toBeVisible();
    expect(projectStore.draft?.sources[0]).toEqual(
      expect.objectContaining({
        period: { from: 2012, to: 2014 },
        bindings: [
          expect.objectContaining({
            variable: "scb/lisa/kon",
            representation: null,
          }),
        ],
      }),
    );
  });

  it("clamps a stale project window to steward bounds before staged add (#1037)", async () => {
    const longSpan = [
      state({
        state_id: 1,
        variant: "individer",
        delivery_column_name: "Kon",
        valid_from: "1996-01-01",
        valid_to: "2026-12-31",
      }),
    ];
    vi.mocked(getCatalogNode).mockResolvedValue(statesResponse(longSpan));
    windowStore.set({ from: 1960, to: 2026 });

    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(longSpan),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: 2000,
      windowMaxYear: 2010,
      vintageYear: 2024,
      enforcePeriodBounds: true,
    });

    const kon = page.getByRole("checkbox", { name: /Kon/ });
    await expect.element(kon).toBeVisible();
    await kon.click();
    await page
      .getByRole("button", {
        name: /Add to project|Remove from project|Apply changes/,
      })
      .click();

    await expect.element(page.getByText(/\+1 column/)).toBeVisible();
    expect(projectStore.draft?.sources[0]).toEqual(
      expect.objectContaining({
        period: { from: 2000, to: 2010 },
      }),
    );
  });

  it("uses the steward-bounded period in the States narrowed note (#1037)", async () => {
    const longSpan = [
      state({
        state_id: 1,
        variant: "individer",
        delivery_column_name: "Kon",
        valid_from: "1996-01-01",
        valid_to: "2026-12-31",
      }),
    ];
    vi.mocked(getCatalogNode).mockResolvedValue(statesResponse(longSpan));
    router.navigate("/catalog/scb/lisa/kon?period=1960..2026");

    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(longSpan),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: 2000,
      windowMaxYear: 2010,
      vintageYear: 2024,
      enforcePeriodBounds: true,
    });

    await expect
      .element(page.getByText(/narrowed to 2000\.\.2010/))
      .toBeVisible();
    expect(document.body.textContent).not.toContain("narrowed to 1960..2026");
    expect(
      vi
        .mocked(getCatalogNode)
        .mock.calls.some(([, p]) => p?.period === "2000..2010"),
    ).toBe(true);
    expect(
      vi
        .mocked(getCatalogNode)
        .mock.calls.some(([, p]) => p?.period === "1960..2026"),
    ).toBe(false);
  });

  it("dims rows whose span does not overlap the active period window", async () => {
    // Narrow to 2018..2020 — the Sni row (2018–2020) overlaps, the Kon row
    // (2010–2015) does not, so Kon's row is dimmed (but still selectable).
    vi.mocked(getCatalogNode).mockResolvedValue({
      states: pickerStates,
    } as never);
    router.navigate("/catalog/scb/lisa/kon?period=2018..2020");

    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(pickerStates),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    const konRow = page.getByRole("checkbox", { name: /Kon/ });
    await expect.element(konRow).toBeVisible();
    // The `dimmed` class is on the row container (.row-btn label), not the checkbox.
    await vi.waitFor(() => {
      const rowBtn = konRow.element().closest(".row-btn");
      if (!rowBtn?.classList.contains("dimmed")) {
        throw new Error("Kon row not yet dimmed");
      }
    });
    // The in-window Sni row is NOT dimmed.
    const sniRow = page
      .getByRole("checkbox", { name: /Sni/ })
      .element()
      .closest(".row-btn");
    expect(sniRow?.classList.contains("dimmed")).toBe(false);
    // A dimmed row stays selectable.
    await konRow.click();
    await expect.element(konRow).toBeChecked();
  });

  it("hoists a constant column to the band context and shows the varying population per row (fordonsreg shape)", async () => {
    // Every representation delivers the CONSTANT column "Sni2002" over the SAME
    // span; only the population (lastbilar/bussar) varies — so the column is
    // hoisted once and the rows show the population.
    const fordonsreg = [
      state({
        state_id: 1,
        variant: "lastbilar",
        delivery_column_name: "Sni2002",
        value_set_version_label: "SNI 2002",
        valid_from: "2003-01-01",
        valid_to: "2015-12-31",
      }),
      state({
        state_id: 2,
        variant: "bussar",
        delivery_column_name: "Sni2002",
        value_set_version_label: "SNI 2002",
        valid_from: "2003-01-01",
        valid_to: "2015-12-31",
      }),
    ];
    render(BindingLeafView, {
      fqidPath: "scb/fordonsreg/naringsgren",
      node: node(fordonsreg, { fqid: "scb/fordonsreg/naringsgren" }),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    // The single-column member leads with its column as the subheading TITLE (a chip);
    // on the LEAF it's a plain <code> (no self-link). Constant value-set context is
    // omitted. The period is NOT in the context — it shows per-row on the right.
    const titleChip = await vi.waitFor(() => {
      const el = document.querySelector(".subhead-title .col-chip");
      if (!el) {
        throw new Error("subhead title chip not yet rendered");
      }
      return el;
    });
    // The chip's leading text node is the column (a trailing ↗ marker only on links).
    expect(titleChip.firstChild?.textContent?.trim()).toBe("Sni2002");
    expect(titleChip.tagName).toBe("CODE");
    expect(document.querySelector(".subhead-context")).toBeNull();
    // Each row still shows its own period on the right-side column.
    const periods = [
      ...document.querySelectorAll(".col-row.nested .period"),
    ].map((el) => el.textContent?.trim());
    expect(periods).toEqual(["2003 – 2015", "2003 – 2015"]);

    // Each row shows the varying POPULATION (not the repeated column) as its primary.
    // (Asserted on the column-list row primaries — a two-variant leaf now also surfaces
    // a Variant FILTER (#908) whose pill checkboxes carry the same variant text, so a
    // bare role+name checkbox query would be ambiguous.)
    const rowPrimaries = [
      ...document.querySelectorAll(".col-row.nested .primary"),
    ].map((el) => el.textContent?.trim());
    expect(rowPrimaries).toContain("lastbilar");
    expect(rowPrimaries).toContain("bussar");
    // The two variants discriminate, so the leaf surfaces the #908 Variant
    // FILTER fieldset (the leaf surface of #908 the picker exposes for a varying-
    // population variable).
    const filterLegends = [
      ...document.querySelectorAll(".dim-filters .dim-filter legend"),
    ].map((el) => el.textContent?.trim());
    expect(filterLegends).toContain("Variant");
    // The constant column is NOT repeated as a per-row label (the populations are the
    // row primaries; no nested row's primary/chip is the column).
    expect(
      [...document.querySelectorAll(".col-row.nested .primary")].some(
        (el) => el.textContent === "Sni2002",
      ),
    ).toBe(false);
  });

  it("renders the delivery column as a prominent CHIP in a column-varies row; the variant primary stays plain (#678)", async () => {
    // Two CO-EXISTING (overlapping) columns on one variable (column VARIES) → each
    // nested row leads with its column rendered as a .col-chip (the selection signal);
    // the value-set qualifier (which varies too) stays plain text, NOT a chip. The
    // windows OVERLAP (the SSYK 3-digit / 5-digit parallel-coding case): a non-
    // overlapping pair would collapse to one rename row (#902).
    const colVaries = [
      state({
        state_id: 1,
        variant: "individer",
        delivery_column_name: "Ssyk3",
        value_set_version_label: "SSYK 3-siffrig",
        valid_from: "2010-01-01",
        valid_to: "2020-12-31",
      }),
      state({
        state_id: 2,
        variant: "individer",
        delivery_column_name: "Ssyk5",
        value_set_version_label: "SSYK 5-siffrig",
        valid_from: "2010-01-01",
        valid_to: "2020-12-31",
      }),
    ];
    render(BindingLeafView, {
      fqidPath: "scb/lisa/yrke",
      node: node(colVaries, { fqid: "scb/lisa/yrke", name: "Yrke" }),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    // Each nested row leads with its column as a chip (mono <code class="col-chip">).
    const chips = await vi.waitFor(() => {
      const els = document.querySelectorAll(".col-row.nested .col-chip");
      if (els.length < 2) {
        throw new Error("column chips not yet rendered");
      }
      return [...els].map((e) => e.textContent?.trim());
    });
    expect(chips).toEqual(["Ssyk3", "Ssyk5"]);
    expect(
      [...document.querySelectorAll(".col-row.nested .col-chip")].every(
        (e) => e.tagName === "CODE",
      ),
    ).toBe(true);
    // The varying value-set qualifier is plain muted text (.sub), NOT a chip.
    const subs = [...document.querySelectorAll(".col-row.nested .sub")].map(
      (e) => e.textContent?.trim(),
    );
    expect(subs).toEqual(["SSYK 3-siffrig", "SSYK 5-siffrig"]);
    expect(document.querySelector(".col-row.nested .sub .col-chip")).toBeNull();
  });

  it("hoists a common value-set STEM to the context and shows per-row suffixes (sni92 shape, #678)", async () => {
    // sni92: the long "Svensk standard för näringsgrensindelning," stem repeats; the
    // picker hoists it once to the subhead context and each row shows only its suffix.
    // Two CO-EXISTING (overlapping-window) columns so they stay separate nested rows
    // (a non-overlapping pair would collapse to one rename row, #902).
    const sni92 = [
      state({
        state_id: 1,
        variant: "individer",
        delivery_column_name: "Sni92A",
        value_set_version_label:
          "Svensk standard för näringsgrensindelning, Aktiviteter",
        valid_from: "2010-01-01",
        valid_to: "2020-12-31",
      }),
      state({
        state_id: 2,
        variant: "individer",
        delivery_column_name: "Sni92B",
        value_set_version_label:
          "Svensk standard för näringsgrensindelning, Branscher",
        valid_from: "2010-01-01",
        valid_to: "2020-12-31",
      }),
    ];
    render(BindingLeafView, {
      fqidPath: "scb/fordonsreg/sni92",
      node: node(sni92, { fqid: "scb/fordonsreg/sni92", name: "Näringsgren" }),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    // The shared stem hoists to the subhead context (quiet text).
    const ctx = await vi.waitFor(() => {
      const el = document.querySelector(".subhead-context .ctx-text");
      if (!el) {
        throw new Error("subhead context not yet rendered");
      }
      return el.textContent ?? "";
    });
    expect(ctx).toContain("Svensk standard för näringsgrensindelning,");
    // Each row shows only its SUFFIX, not the repeated stem.
    const subs = [...document.querySelectorAll(".col-row.nested .sub")].map(
      (e) => e.textContent?.trim(),
    );
    expect(subs).toEqual(["Aktiviteter", "Branscher"]);
  });

  it("renders a 'codings vary' nudge only on a column whose value_set_id changed over time (#678)", async () => {
    // Two columns on one variable: ColA carried value-set 303 then 249 (a coding
    // change → the nudge); ColB carried 100 throughout (stable → no nudge). Keyed on
    // the reliable value_set_id, NOT the label.
    const states = [
      state({
        state_id: 1,
        variant: "v",
        delivery_column_name: "ColA",
        value_set_id: 303,
        value_set_version_label: "MiS 1996:1",
        valid_from: "2019-01-01",
        valid_to: "2019-12-31",
      }),
      state({
        state_id: 2,
        variant: "v",
        delivery_column_name: "ColA",
        value_set_id: 249,
        value_set_version_label: "SUN",
        valid_from: "2020-01-01",
        valid_to: "2022-12-31",
      }),
      state({
        state_id: 3,
        variant: "v",
        delivery_column_name: "ColB",
        value_set_id: 100,
        value_set_version_label: "Stable",
        valid_from: "2019-01-01",
        valid_to: "2022-12-31",
      }),
    ];
    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(states),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    // The ColA row carries the nudge; the ColB row does not. Match each row by its
    // column checkbox, then check for a sibling `.codings-vary` inside the same row.
    const colA = page.getByRole("checkbox", { name: /ColA/ });
    await expect.element(colA).toBeVisible();
    await vi.waitFor(() => {
      const aRow = colA.element().closest("li");
      if (!aRow?.querySelector(".codings-vary")) {
        throw new Error("codings-vary nudge not yet on ColA");
      }
    });
    // Exactly one nudge in the whole picker (ColA only).
    expect(document.querySelectorAll(".rep-picker .codings-vary")).toHaveLength(
      1,
    );
    const colB = page.getByRole("checkbox", { name: /ColB/ }).element();
    expect(colB.closest("li")?.querySelector(".codings-vary")).toBeNull();
    // The nudge carries the accessible pointer-to-detail label.
    const nudge = document.querySelector(".codings-vary");
    expect(nudge?.getAttribute("aria-label")).toBe(
      "Coding changes over time — see the value sets",
    );
    // #905: it's a DEEP LINK (an anchor) to the value-set viewer focused on this
    // ROW — the current leaf path + `?codes=<variant>::<column>#states-heading` (no
    // `band.href` on the binding leaf). The variant ("v") is carried so a column shared
    // across variants isolates the clicked row's coding.
    expect(nudge?.tagName).toBe("A");
    expect(nudge?.getAttribute("href")).toBe(
      "/catalog/scb/lisa/kon?codes=v%3A%3AColA#states-heading",
    );
  });

  it("a ?codes=<column> deep link focuses the value-set viewer on that column's latest coding (#905)", async () => {
    // The other side of the deep link: with `?codes=ColA` in the URL the leaf passes
    // it as `focusColumn`, and the value-set viewer auto-isolates ColA's latest-era
    // coding (value-set 249 / "SUN", not the earlier 303 / "MiS 1996:1").
    const states = [
      state({
        state_id: 1,
        variant: "v",
        delivery_column_name: "ColA",
        value_set_id: 303,
        value_set_version_label: "MiS 1996:1",
        valid_from: "2019-01-01",
        valid_to: "2019-12-31",
      }),
      state({
        state_id: 2,
        variant: "v",
        delivery_column_name: "ColA",
        value_set_id: 249,
        value_set_version_label: "SUN",
        valid_from: "2020-01-01",
        valid_to: "2022-12-31",
      }),
      state({
        state_id: 3,
        variant: "v",
        delivery_column_name: "ColB",
        value_set_id: 100,
        value_set_version_label: "Stable",
        valid_from: "2019-01-01",
        valid_to: "2022-12-31",
      }),
    ];
    router.navigate("/catalog/scb/lisa/kon?codes=ColA");
    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(states),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });
    // The viewer is isolated on ColA's latest coding: the "Used by" detail + the
    // "SUN" heading show, and the union list is hidden.
    await expect.element(page.getByText("Used by")).toBeVisible();
    expect(
      document.querySelector(".vs-detail .vs-heading")?.textContent,
    ).toContain("SUN");
    expect(document.querySelector(".vs-list > li")).toBeNull();
  });

  it("a ?codes=<variant>::<column> deep link isolates the CLICKED variant's coding when a column is shared across variants (#905)", async () => {
    // One delivery column COL delivered by TWO variants with DISTINCT codings:
    // variant "a" → "Coding A" (older), variant "b" → "Coding B" (latest era). The
    // unscoped column lookup would pick B (the latest), but the deep link carries the
    // ROW's variant, so `?codes=a::COL` must isolate A's coding — the deep-link bug.
    const states = [
      state({
        state_id: 1,
        variant: "a",
        delivery_column_name: "COL",
        value_set_id: 100,
        value_set_version_label: "Coding A",
        valid_from: "2015-01-01",
        valid_to: "2018-12-31",
      }),
      state({
        state_id: 2,
        variant: "b",
        delivery_column_name: "COL",
        value_set_id: 200,
        value_set_version_label: "Coding B",
        valid_from: "2019-01-01",
        valid_to: "2022-12-31",
      }),
    ];
    router.navigate("/catalog/scb/lisa/kon?codes=a%3A%3ACOL");
    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(states),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });
    // Isolated on variant "a"'s coding ("Coding A"), NOT "b"'s latest-era "Coding B".
    await expect.element(page.getByText("Used by")).toBeVisible();
    const heading = document.querySelector(
      ".vs-detail .vs-heading",
    )?.textContent;
    expect(heading).toContain("Coding A");
    expect(heading).not.toContain("Coding B");
  });

  it("the single-COLUMN leaf renders ONE compact row led by its COLUMN (#678)", async () => {
    // A single-column leaf has nothing varying, so it merges to ONE compact row. The
    // variable name is already the page <h2>, so the row leads with just its COLUMN
    // chip ("Kon"), NOT a repeated "Kön". The constant register prefix is hoisted off.
    const oneColumn = [
      state({
        state_id: 1,
        variant: "individer",
        delivery_column_name: "Kon",
        valid_from: "2010-01-01",
        valid_to: "2015-12-31",
        value_set_version_label: "1-siffrig",
      }),
    ];
    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(oneColumn),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    // ONE compact single-column row, no subheading.
    const chip = await vi.waitFor(() => {
      const el = document.querySelector(".col-row.single .col-chip");
      if (!el) {
        throw new Error("single-column row not yet rendered");
      }
      return el;
    });
    expect(document.querySelectorAll("li.subhead")).toHaveLength(0);
    // The leaf leads with its COLUMN chip ("Kon"), not the variable name ("Kön").
    expect(chip.firstChild?.textContent?.trim()).toBe("Kon");
    expect(document.querySelector(".col-row.single .primary")).toBeNull();
    expect(
      document.querySelector(".col-row.single")?.textContent,
    ).not.toContain("Kön");
    // The merged row is itself a selectable checkbox (named by its column).
    await expect
      .element(page.getByRole("checkbox", { name: /Kon/ }))
      .toBeVisible();
    // The LEAF passes no `href` → the column chip is a PLAIN <code>, NOT a navigation
    // link (the leaf is already its own page; nav is a group-view affordance only).
    expect(chip.tagName).toBe("CODE");
    expect(document.querySelector(".col-row.single a.col-chip")).toBeNull();
    expect(document.querySelector("a.open-link")).toBeNull();
  });

  it("renders no picker when no state carries a delivery column", async () => {
    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(single),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    await expect
      .element(page.getByRole("heading", { name: "Kön", level: 2 }))
      .toBeVisible();
    expect(document.body.querySelector('[role="checkbox"]')).toBeNull();
  });

  it("demotes Sensitive / Identifier into a 'Technical details' disclosure (#638 PR4)", async () => {
    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(single),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    await expect.element(page.getByText("Technical details")).toBeVisible();
    const disclosure = document.querySelector<HTMLDetailsElement>(
      "details.tech-details",
    );
    expect(disclosure).not.toBeNull();
    expect(disclosure?.open).toBe(false);
    expect(disclosure?.textContent).toContain("Sensitive");
    expect(disclosure?.textContent).toContain("Identifier");
    const promptMeta = [...document.querySelectorAll("dl.meta")].filter(
      (dl) => !dl.closest("details.tech-details"),
    );
    for (const dl of promptMeta) {
      expect(dl.textContent).not.toContain("Sensitive");
      expect(dl.textContent).not.toContain("Identifier");
    }
  });

  it("renders thematic tag chips and recommendation notes", async () => {
    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(single, {
        tags: [
          {
            slug: "income",
            label: "Income & earnings",
            rank: 0,
            starred: true,
            note: "primary fixture measure",
          },
        ],
      } as Partial<BindingNodeData>),
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
    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(singleWithStructural),
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

  it("Apply stays seed-gated (disabled) even when a row is staged, until the seed is present", async () => {
    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(pickerStates),
      regMetaVersion: "",
      steward: "",
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });
    await expect
      .element(page.getByRole("button", { name: "Add to project" }))
      .not.toBeInTheDocument();
    // Selecting a row must NOT enable Apply while the seed is absent.
    await page.getByRole("checkbox", { name: /Kon/ }).click();
    const add = page.getByRole("button", { name: "Add to project" });
    await expect.element(add).toBeDisabled();
  });
});

describe("BindingLeafView period-scoped value-set history (#744)", () => {
  it("uses the period subset for Add while rendering full history with outside-period collapse", async () => {
    const inA = state({
      state_id: 10,
      variant: "individer",
      value_set_id: 10,
      value_set_version_label: "In-period A",
      delivery_column_name: "Kon",
      valid_from: "2007-01-01",
      valid_to: "2007-12-31",
    });
    const inB = state({
      state_id: 11,
      variant: "individer",
      value_set_id: 11,
      value_set_version_label: "In-period B",
      delivery_column_name: "Kon",
      valid_from: "2008-01-01",
      valid_to: "2008-12-31",
    });
    const outside = state({
      state_id: 12,
      variant: "outside-population",
      value_set_id: 12,
      value_set_version_label: "Outside period",
      valid_from: "1990-01-01",
      valid_to: "1990-12-31",
    });
    vi.mocked(getCatalogNode).mockResolvedValue({
      states: [inA, inB],
    } as never);
    router.navigate("/catalog/scb/lisa/kon?period=2007..2008");

    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node([inA, inB, outside]),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    await expect
      .element(page.getByText("1 value set outside this period"))
      .toBeVisible();
    // The picker lists representations over the FULL history (the period window
    // only dims). inA/inB share (individer, Kon) → ONE representation → a single-rep
    // leaf renders FLAT, led by its COLUMN ("Kon"). Selecting it enables Apply.
    const konRow = page.getByRole("checkbox", { name: /Kon/ });
    await expect.element(konRow).toBeVisible();
    await konRow.click();
    await expect
      .element(
        page.getByRole("button", {
          name: /Add to project|Remove from project|Apply changes/,
        }),
      )
      .toBeEnabled();
  });

  it("narrows the value-set list to the active variant modifier, keeping a period-only outside-period scope (Codex P2)", async () => {
    // #905, Codex P2: with `?variant` active the page shows a "Narrowed by" chip and
    // the picker is scoped via `narrowStatesByModifier`. The value-set list MUST match
    // that narrowing — a DIFFERENT variant's same-period coding must NOT appear as an
    // in-scope row. The period-only outside-period collapse (#744) still works, scoped
    // to the narrowed variant.
    const inA = state({
      state_id: 20,
      variant: "individer",
      value_set_id: 20,
      value_set_version_label: "In-period A",
      valid_from: "2007-01-01",
      valid_to: "2007-12-31",
    });
    const inB = state({
      state_id: 21,
      variant: "individer",
      value_set_id: 21,
      value_set_version_label: "In-period B",
      valid_from: "2008-01-01",
      valid_to: "2008-12-31",
    });
    const samePeriodOtherVariant = state({
      state_id: 22,
      variant: "other-population",
      value_set_id: 22,
      value_set_version_label: "Same-period other variant",
      valid_from: "2008-01-01",
      valid_to: "2008-12-31",
    });
    // An individer coding OUTSIDE the period — survives the modifier narrowing and
    // demonstrates the outside-period disclosure still functions for the narrowed
    // variant.
    const outsideIndivider = state({
      state_id: 23,
      variant: "individer",
      value_set_id: 23,
      value_set_version_label: "Outside period",
      valid_from: "1990-01-01",
      valid_to: "1990-12-31",
    });
    vi.mocked(getCatalogNode).mockImplementation(
      async (_fqid, params) =>
        ({
          // The period-only scope (no variant param) returns the same-period rows of
          // ALL variants; the variant-scoped resolve returns only individer's.
          states: params?.variant
            ? [inA, inB]
            : [inA, inB, samePeriodOtherVariant],
        }) as never,
    );
    router.navigate(
      "/catalog/scb/lisa/kon?period=2007..2008&variant=individer",
    );

    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node([inA, inB, samePeriodOtherVariant, outsideIndivider]),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    // The narrowed value-set list shows individer's in-period codings…
    await expect.element(page.getByText("In-period A")).toBeVisible();
    await expect.element(page.getByText("In-period B")).toBeVisible();
    // …the period-only outside-period collapse still works (individer's out-of-period
    // coding)…
    await expect
      .element(page.getByText("1 value set outside this period"))
      .toBeVisible();
    // …and the OTHER variant's same-period coding is absent (Fix 3): the list reflects
    // the active narrowing, not the full history.
    expect(
      [...document.querySelectorAll(".vs-label")].some(
        (el) => el.textContent === "Same-period other variant",
      ),
    ).toBe(false);
  });

  it("shows ALL period codings (every variant) when NO modifier is active", async () => {
    // The control for Fix 3: with no `?variant`/`?value_set_version`, the value-set
    // list is the full period history — every variant's same-period coding shows.
    const inA = state({
      state_id: 24,
      variant: "individer",
      value_set_id: 24,
      value_set_version_label: "In-period A",
      valid_from: "2007-01-01",
      valid_to: "2007-12-31",
    });
    const samePeriodOtherVariant = state({
      state_id: 25,
      variant: "other-population",
      value_set_id: 25,
      value_set_version_label: "Same-period other variant",
      valid_from: "2007-01-01",
      valid_to: "2007-12-31",
    });
    vi.mocked(getCatalogNode).mockResolvedValue({
      states: [inA, samePeriodOtherVariant],
    } as never);
    router.navigate("/catalog/scb/lisa/kon?period=2007");

    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node([inA, samePeriodOtherVariant]),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    await expect.element(page.getByText("In-period A")).toBeVisible();
    await expect
      .element(page.getByText("Same-period other variant"))
      .toBeVisible();
  });

  it("keeps modifier-resolved single-state detail with a broader period-only scope", async () => {
    const picked = state({
      state_id: 30,
      variant: "individer",
      value_set_id: 30,
      value_set_version_label: "Picked",
      valid_from: "2007-01-01",
      valid_to: "2007-12-31",
    });
    const samePeriodOtherVariant = state({
      state_id: 31,
      variant: "other-population",
      value_set_id: 31,
      value_set_version_label: "Same-period other variant",
      valid_from: "2007-01-01",
      valid_to: "2007-12-31",
    });
    vi.mocked(getCatalogNode).mockImplementation(
      async (_fqid, params) =>
        ({
          states: params?.variant ? [picked] : [picked, samePeriodOtherVariant],
        }) as never,
    );
    router.navigate("/catalog/scb/lisa/kon?period=2007&variant=individer");

    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node([picked, samePeriodOtherVariant]),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    await expect
      .element(page.getByText("Value-set version", { exact: true }))
      .toBeVisible();
    expect(document.querySelector(".vs-list")).toBeNull();
  });

  it("keeps Add scoped to the primary resolve when the period-scope fetch fails", async () => {
    const picked = state({
      state_id: 40,
      variant: "individer",
      value_set_id: 40,
      value_set_version_label: "Picked",
      delivery_column_name: "Kon",
      valid_from: "2007-01-01",
      valid_to: "2007-12-31",
    });
    const otherVariant = state({
      state_id: 41,
      variant: "other-population",
      value_set_id: 41,
      value_set_version_label: "Other",
      delivery_column_name: "Sni",
      valid_from: "2007-01-01",
      valid_to: "2007-12-31",
    });
    vi.mocked(getCatalogNode).mockImplementation(async (_fqid, params) => {
      if (params?.variant) {
        return { states: [picked] } as never;
      }
      throw new Error("scope failed");
    });
    router.navigate("/catalog/scb/lisa/kon?period=2007&variant=individer");

    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node([picked, otherVariant]),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    await expect
      .element(page.getByText(/Could not load full period value-set context/))
      .toBeVisible();
    // The picker is independent of the scope-fetch failure: its rows come from
    // `node.states`, so selecting one still enables Apply.
    await page.getByRole("checkbox", { name: /Kon/ }).click();
    await expect
      .element(
        page.getByRole("button", {
          name: /Add to project|Remove from project|Apply changes/,
        }),
      )
      .toBeEnabled();
  });

  it("shows FULL history in the value-set list when a `?period` resolve fails with a stale `?variant` (Fix B)", async () => {
    // #905, Codex P2: when the PRIMARY `?period` resolve fails (a stale/typo `?variant`,
    // a 5xx, a network drop), `states` falls back to `node.states` (full history). The
    // modifier narrowing must NOT then apply — a stale `?variant` would narrow that
    // fallback to empty, defeating the full-history fallback. `valueSetStates`/
    // `valueSetScope` are gated on `!narrowedError`, so the value-set list shows the
    // full history (every variant), not a modifier-narrowed (possibly empty) subset.
    const individerCoding = state({
      state_id: 50,
      variant: "individer",
      value_set_id: 50,
      value_set_version_label: "Individer coding",
      valid_from: "2007-01-01",
      valid_to: "2007-12-31",
    });
    const otherCoding = state({
      state_id: 51,
      variant: "other-population",
      value_set_id: 51,
      value_set_version_label: "Other coding",
      valid_from: "2007-01-01",
      valid_to: "2007-12-31",
    });
    vi.mocked(getCatalogNode).mockImplementation(async (_fqid, params) => {
      // The variant-scoped PRIMARY resolve fails (a stale `?variant=typo`); the
      // period-only scope would succeed but is moot once the primary errors.
      if (params?.variant) {
        throw new Error("422 bad variant");
      }
      return { states: [individerCoding, otherCoding] } as never;
    });
    router.navigate("/catalog/scb/lisa/kon?period=2007&variant=typo");

    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node([individerCoding, otherCoding]),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    // The full-history value-set list shows BOTH variants' codings — the stale variant
    // did NOT narrow the error-fallback to empty.
    await expect.element(page.getByText("Individer coding")).toBeVisible();
    await expect.element(page.getByText("Other coding")).toBeVisible();
  });
});

describe("BindingLeafView member identity from graph focus (#670/#678)", () => {
  const groupedFqid = "scb/lisa/naringsgren-storsta-agi-sni2007g";
  const groupedKey = "naringsgren";
  const groupedNode = node(single, {
    fqid: groupedFqid,
    name: "Näringsgren, största förvärvskälla",
    group: { provider: "scb", register: "lisa", key: groupedKey },
  });

  /** A graph whose focus variable carries the member facets + group label. */
  function focusGraph(
    over: Partial<VariableGraphNode>,
    focusId = "f1",
  ): RelationshipGraph {
    return graph(
      {
        fqid: groupedFqid,
        label: "Näringsgren, största förvärvskälla",
        group_key: groupedKey,
        group_label: "Näringsgren",
        ...over,
      },
      focusId,
    );
  }

  it("renders the member qualifier (facets) and a 'member of ⟨label⟩' link with the correct href", async () => {
    vi.mocked(getBindingGraph).mockResolvedValue(
      focusGraph({
        facets: [
          { axis: "kalla", value: "storsta", label: "Största" },
          { axis: "population", value: "individ", label: "Individ" },
          { axis: "level", value: "grov", label: "Grov" },
          { axis: "metod", value: "standard", label: "Standard" },
        ],
      }) as never,
    );

    render(BindingLeafView, {
      fqidPath: groupedFqid,
      node: groupedNode,
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    // The qualifier is the focus node's facet labels (scope to the identity row —
    // the same facets also render inside the picker graph cluster when graph mode is on).
    await expect
      .element(page.getByText("Största · Individ · Grov · Standard").first())
      .toBeVisible();

    // The context link targets the group subject route from `node.group`.
    const link = page.getByRole("link", {
      name: "Näringsgren",
    });
    await expect.element(link.first()).toBeVisible();
    expect(
      document
        .querySelector(".member-identity .group-context a")
        ?.getAttribute("href"),
    ).toBe("/catalog/group/scb/lisa/naringsgren");
  });

  it("resolves the qualifier from the focus node even when it differs from the leaf fqid (same_as)", async () => {
    // The focus node is keyed on the RESOLVED target; the qualifier still reads its
    // facets, and the slug fallback (if any) reads the LEAF's own fqid.
    vi.mocked(getBindingGraph).mockResolvedValue(
      focusGraph({
        fqid: "scb/rams/inkjan",
        facets: [
          { axis: "kalla", value: "storsta", label: "Största" },
          { axis: "population", value: "individ", label: "Individ" },
          { axis: "level", value: "grov", label: "Grov" },
          { axis: "metod", value: "standard", label: "Standard" },
        ],
      }) as never,
    );

    render(BindingLeafView, {
      fqidPath: groupedFqid,
      node: groupedNode,
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    await expect
      .element(page.getByText("Största · Individ · Grov · Standard").first())
      .toBeVisible();
    expect(
      document.querySelector(".member-identity code.qualifier.slug"),
    ).toBeNull();
  });

  it("a grouped facet-less focus opened via same_as shows the CANONICAL sibling slug, not the alias", async () => {
    // Opened via a same_as alias (the leaf is
    // `.../naringsgren-storsta-agi-sni2007g`), the focus node is keyed on the
    // RESOLVED canonical target. The facet-less slug qualifier must read the focus
    // node's own (canonical) fqid so the alias page and the canonical page show the
    // SAME technical identifier (#670 Codex-P2 parity).
    vi.mocked(getBindingGraph).mockResolvedValue(
      focusGraph({ fqid: "scb/rams/inkjan", facets: [] }) as never,
    );

    render(BindingLeafView, {
      fqidPath: groupedFqid,
      node: groupedNode,
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    const slugEl = await vi.waitFor(() => {
      const el = document.querySelector(".member-identity code.qualifier.slug");
      if (!el) {
        throw new Error("slug qualifier not yet rendered");
      }
      return el;
    });
    // The CANONICAL leaf slug, not the alias
    // `naringsgren-storsta-agi-sni2007g`.
    expect(slugEl.textContent).toBe("inkjan");
  });

  it("a grouped focus with no facets renders the slug qualifier as a code identifier (M10)", async () => {
    vi.mocked(getBindingGraph).mockResolvedValue(
      focusGraph({ facets: [] }) as never,
    );

    render(BindingLeafView, {
      fqidPath: groupedFqid,
      node: groupedNode,
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    const slugEl = await vi.waitFor(() => {
      const el = document.querySelector(".member-identity code.qualifier.slug");
      if (!el) {
        throw new Error("slug qualifier not yet rendered");
      }
      return el;
    });
    expect(slugEl.textContent).toBe("naringsgren-storsta-agi-sni2007g");
    await expect
      .element(page.getByRole("link", { name: "Näringsgren" }).first())
      .toBeVisible();
  });

  it("renders no identity row while the graph is loading (no transient slug flicker)", async () => {
    vi.mocked(getBindingGraph).mockReturnValue(new Promise(() => {}));

    render(BindingLeafView, {
      fqidPath: groupedFqid,
      node: groupedNode,
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    await expect
      .element(
        page.getByRole("heading", {
          name: "Näringsgren, största förvärvskälla",
          level: 2,
        }),
      )
      .toBeVisible();
    expect(document.querySelector(".member-identity")).toBeNull();
  });

  it("an ungrouped variable renders neither qualifier nor group link", async () => {
    // Default beforeEach stubs an empty graph; the plain node has no group.
    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(single),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    await expect
      .element(page.getByRole("heading", { name: "Kön", level: 2 }))
      .toBeVisible();
    expect(document.querySelector(".member-identity")).toBeNull();
    await expect.element(page.getByText(/member of/)).not.toBeInTheDocument();
  });

  it("degrades gracefully when the graph fetch errors (header survives, no qualifier/link)", async () => {
    // The graph fetch is an independent failure domain: an error must NOT blank the
    // leaf — the header (node.name) still renders, the qualifier/link omitted.
    vi.mocked(getBindingGraph).mockRejectedValue(new Error("graph down"));

    render(BindingLeafView, {
      fqidPath: groupedFqid,
      node: groupedNode,
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    await expect
      .element(
        page.getByRole("heading", {
          name: "Näringsgren, största förvärvskälla",
          level: 2,
        }),
      )
      .toBeVisible();
    expect(document.querySelector(".member-identity")).toBeNull();
  });
});
