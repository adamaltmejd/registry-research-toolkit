import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type {
  BindingNodeData,
  RelationshipGraph,
  VariableGraphNode,
  VariableStateModel,
} from "./api";
import {
  getBindingGraph,
  getBindingLineageWarnings,
  getCatalogNode,
  getDocsForVariable,
} from "./api";
import BindingLeafView from "./BindingLeafView.svelte";
import { projectStore } from "./project_store.svelte";
import { router } from "./router.svelte";

// Two surfaces under test:
//   1. the direct representation picker (#678) — the variable's representations
//      list as selectable rows; selecting rows + Add commits the right
//      `addFromCatalog` payloads; out-of-window rows dim; empty selection / no
//      seed disables Add.
//   2. the #670 member identity, now derived from the relationship-graph FOCUS node
//      (#678) — the leaf's single `/graph` fetch feeds both the HistoryGraph
//      renderer AND the header qualifier + "member of ⟨group⟩" link.
//
// The four catalog GETs the leaf + its sibling panels drive (graph / lineage
// warnings / docs / the ?period resolve) are stubbed so nothing hits a real fetch;
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
    value_set_version_label: "",
    value_set_id: null,
    value_set: null,
    is_identifier: false,
    classification_slug: null,
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
  // The graph fetch: an EMPTY graph by default (no nodes) → the HistoryGraph omits
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
  // No `?period` — the embedded states drive the plan.
  window.history.pushState({}, "", "/__reset__");
  router.navigate("/catalog/scb/lisa/kon");
  projectStore.newProject({
    reg_meta_version: "reg_meta/v1.0.0",
    steward: "global",
  });
});

const SEED = { regMetaVersion: "reg_meta/v1.0.0", steward: "global" } as const;

describe("BindingLeafView representation picker (#678)", () => {
  it("lists each representation row with its delivery column + period span", async () => {
    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(pickerStates),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
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
    // same span text also appears in the StatesView usage list).
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

  it("Add is disabled with no selection, enabled once a row is selected", async () => {
    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(pickerStates),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      vintageYear: 2024,
    });

    const add = page.getByRole("button", { name: "Add to project" });
    await expect.element(add).toBeDisabled();

    const konRow = page.getByRole("checkbox", { name: /Kon/ });
    await konRow.click();
    await expect.element(konRow).toBeChecked();
    await expect.element(add).toBeEnabled();
    await expect.element(page.getByText("1 column selected")).toBeVisible();
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

  it("selecting rows + Add commits the right addFromCatalog payloads", async () => {
    const spy = vi.spyOn(projectStore, "addFromCatalog");
    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(pickerStates),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      vintageYear: 2024,
    });

    await page.getByRole("checkbox", { name: /Kon/ }).click();
    await page.getByRole("checkbox", { name: /Sni/ }).click();
    await expect.element(page.getByText("2 columns selected")).toBeVisible();
    await page.getByRole("button", { name: "Add to project" }).click();

    expect(spy).toHaveBeenCalledTimes(2);
    const payloads = spy.mock.calls.map((c) => c[0]);
    expect(payloads).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          registerVariant: "scb/lisa/individer",
          variable: "scb/lisa/kon",
          representation: "Kon",
          resolvedPeriod: "2010..2015",
        }),
        expect.objectContaining({
          registerVariant: "scb/lisa/arbetsstallen",
          variable: "scb/lisa/kon",
          representation: "Sni",
          resolvedPeriod: "2018..2020",
        }),
      ]),
    );
    spy.mockRestore();
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
    const spy = vi.spyOn(projectStore, "addFromCatalog");
    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(renameStates),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      vintageYear: 2024,
    });

    // ONE folded row, led by the latest column DINF86 (the display identity / chip).
    await page.getByRole("checkbox", { name: /DINF86/ }).click();
    await page.getByRole("button", { name: "Add to project" }).click();

    expect(spy).toHaveBeenCalledTimes(1);
    expect(spy.mock.calls[0][0]).toEqual(
      expect.objectContaining({
        registerVariant: "scb/lisa/individer",
        variable: "scb/lisa/kon",
        // NOT "DINF86" — the rename fold commits null so resolution picks per year.
        representation: null,
        resolvedPeriod: "1981..1995",
      }),
    );
    spy.mockRestore();
  });

  // #678 finding 3: an active ?period is HONORED on add — the committed period is
  // the row span INTERSECTED with the window, not the row's full span.
  it("commits the row span intersected with the active ?period (not the full span)", async () => {
    vi.mocked(getCatalogNode).mockResolvedValue({
      states: pickerStates,
    } as never);
    // Kon spans 2010–2015; narrow to 2012..2014.
    router.navigate("/catalog/scb/lisa/kon?period=2012..2014");
    const spy = vi.spyOn(projectStore, "addFromCatalog");

    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(pickerStates),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      vintageYear: 2024,
    });

    const kon = page.getByRole("checkbox", { name: /Kon/ });
    await expect.element(kon).toBeVisible();
    await kon.click();
    await page.getByRole("button", { name: "Add to project" }).click();

    expect(spy).toHaveBeenCalledTimes(1);
    expect(spy.mock.calls[0][0]).toEqual(
      expect.objectContaining({
        variable: "scb/lisa/kon",
        representation: "Kon",
        // The intersection 2012..2014, NOT the row's full 2010..2015 span.
        resolvedPeriod: "2012..2014",
      }),
    );
    spy.mockRestore();
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
      vintageYear: 2024,
    });

    // The single-column member leads with its column as the subheading TITLE (a chip);
    // on the LEAF it's a plain <code> (no self-link). The value set hoists as quiet
    // context text. The period is NOT in the context — it shows per-row on the right.
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
    const ctx = document.querySelector(".subhead-context");
    expect(ctx?.textContent).toContain("SNI 2002");
    // The period is NOT duplicated into the context line.
    expect(ctx?.textContent).not.toContain("2003 – 2015");
    // Each row still shows its own period on the right-side column.
    const periods = [
      ...document.querySelectorAll(".col-row.nested .period"),
    ].map((el) => el.textContent?.trim());
    expect(periods).toEqual(["2003 – 2015", "2003 – 2015"]);

    // Each row shows the varying POPULATION (not the repeated column) as its primary.
    // (Asserted on the column-list row primaries — a two-variant leaf now also surfaces
    // a Population FILTER (#908) whose pill checkboxes carry the same variant text, so a
    // bare role+name checkbox query would be ambiguous.)
    const rowPrimaries = [
      ...document.querySelectorAll(".col-row.nested .primary"),
    ].map((el) => el.textContent?.trim());
    expect(rowPrimaries).toContain("lastbilar");
    expect(rowPrimaries).toContain("bussar");
    // The two populations discriminate, so the leaf surfaces the #908 Population
    // FILTER fieldset (the leaf surface of #908 the picker exposes for a varying-
    // population variable).
    const filterLegends = [
      ...document.querySelectorAll(".dim-filters .dim-filter legend"),
    ].map((el) => el.textContent?.trim());
    expect(filterLegends).toContain("Population");
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

  it("demotes a single state's Data type / Delivery column into its own 'Technical details' disclosure (#638 PR4)", async () => {
    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(singleWithStructural),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      vintageYear: 2024,
    });

    await expect
      .element(page.getByText("Technical details").first())
      .toBeVisible();

    const stateTech = [
      ...document.querySelectorAll<HTMLDetailsElement>("details.tech-details"),
    ].find((d) => d.textContent?.includes("Data type"));
    expect(stateTech).toBeDefined();
    expect(stateTech?.open).toBe(false);
    expect(stateTech?.textContent).toContain("Data type");
    expect(stateTech?.textContent).toContain("Delivery column");
    const stateDetail = stateTech?.closest(".state-detail");
    const promptMeta = [
      ...(stateDetail?.querySelectorAll("dl.meta") ?? []),
    ].find((dl) => !dl.closest("details.tech-details"));
    expect(promptMeta).toBeDefined();
    const promptText = promptMeta?.textContent ?? "";
    expect(promptText).toContain("Variant");
    expect(promptText).toContain("Valid");
    expect(promptText).toContain("Value-set version");
    expect(stateTech?.textContent).not.toContain("Variant");
    expect(stateTech?.textContent).not.toContain("Value-set version");
  });

  it("Add stays seed-gated (disabled) even when a row is selected, until the seed is present", async () => {
    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(pickerStates),
      regMetaVersion: "",
      steward: "",
      vintageYear: 2024,
    });
    const add = page.getByRole("button", { name: "Add to project" });
    await expect.element(add).toBeDisabled();
    // Selecting a row must NOT enable Add while the seed is absent.
    await page.getByRole("checkbox", { name: /Kon/ }).click();
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
      vintageYear: 2024,
    });

    await expect
      .element(page.getByText("1 value set outside this period"))
      .toBeVisible();
    // The picker lists representations over the FULL history (the period window
    // only dims). inA/inB share (individer, Kon) → ONE representation → a single-rep
    // leaf renders FLAT, led by its COLUMN ("Kon"). Selecting it enables Add.
    const konRow = page.getByRole("checkbox", { name: /Kon/ });
    await expect.element(konRow).toBeVisible();
    await konRow.click();
    await expect
      .element(page.getByRole("button", { name: "Add to project" }))
      .toBeEnabled();
  });

  it("uses a period-only scope when a variant modifier is active", async () => {
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
    const outside = state({
      state_id: 23,
      variant: "outside-population",
      value_set_id: 23,
      value_set_version_label: "Outside period",
      valid_from: "1990-01-01",
      valid_to: "1990-12-31",
    });
    vi.mocked(getCatalogNode).mockImplementation(
      async (_fqid, params) =>
        ({
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
      node: node([inA, inB, samePeriodOtherVariant, outside]),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      vintageYear: 2024,
    });

    await expect
      .element(page.getByText("1 value set outside this period"))
      .toBeVisible();
    expect(
      [...document.querySelectorAll(".vs-label")].some(
        (el) => el.textContent === "Same-period other variant",
      ),
    ).toBe(true);
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
      vintageYear: 2024,
    });

    await expect
      .element(page.getByText(/Could not load full period value-set context/))
      .toBeVisible();
    // The picker is independent of the scope-fetch failure: its rows come from
    // `node.states`, so selecting one still enables Add.
    await page.getByRole("checkbox", { name: /Kon/ }).click();
    await expect
      .element(page.getByRole("button", { name: "Add to project" }))
      .toBeEnabled();
  });
});

describe("BindingLeafView member identity from graph focus (#670/#678)", () => {
  const groupedFqid = "scb/lisa/agi1astsni2007g";
  const groupedNode = node(single, {
    fqid: groupedFqid,
    name: "Näringsgren, största förvärvskälla",
    group: { provider: "scb", register: "lisa", key: "naringsgren" },
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
        group_key: "naringsgren",
        group_label: "Näringsgren, största förvärvskälla",
        ...over,
      },
      focusId,
    );
  }

  it("renders the member qualifier (facets) and a 'member of ⟨label⟩' link with the correct href", async () => {
    vi.mocked(getBindingGraph).mockResolvedValue(
      focusGraph({
        facets: [
          { axis: "source", value: "agi", label: "AGI" },
          { axis: "edition", value: "sni2007", label: "2007 SNI edition" },
        ],
      }) as never,
    );

    render(BindingLeafView, {
      fqidPath: groupedFqid,
      node: groupedNode,
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      vintageYear: 2024,
    });

    // The qualifier is the focus node's facet labels (scope to the identity row —
    // the same facets also render inside the HistoryGraph cluster).
    await expect
      .element(page.getByText("AGI · 2007 SNI edition").first())
      .toBeVisible();

    // The context link targets the group subject route from `node.group`.
    const link = page.getByRole("link", {
      name: "Näringsgren, största förvärvskälla",
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
          { axis: "source", value: "agi", label: "AGI" },
          { axis: "edition", value: "sni2007", label: "2007 SNI edition" },
        ],
      }) as never,
    );

    render(BindingLeafView, {
      fqidPath: groupedFqid,
      node: groupedNode,
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      vintageYear: 2024,
    });

    await expect
      .element(page.getByText("AGI · 2007 SNI edition").first())
      .toBeVisible();
    expect(
      document.querySelector(".member-identity code.qualifier.slug"),
    ).toBeNull();
  });

  it("a grouped facet-less focus opened via same_as shows the CANONICAL sibling slug, not the alias", async () => {
    // Opened via a same_as alias (the leaf is `…/agi1astsni2007g`), the focus node
    // is keyed on the RESOLVED canonical target. The facet-less slug qualifier must
    // read the focus node's own (canonical) fqid so the alias page and the
    // canonical page show the SAME technical identifier (#670 Codex-P2 parity).
    vi.mocked(getBindingGraph).mockResolvedValue(
      focusGraph({ fqid: "scb/rams/inkjan", facets: [] }) as never,
    );

    render(BindingLeafView, {
      fqidPath: groupedFqid,
      node: groupedNode,
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      vintageYear: 2024,
    });

    const slugEl = await vi.waitFor(() => {
      const el = document.querySelector(".member-identity code.qualifier.slug");
      if (!el) {
        throw new Error("slug qualifier not yet rendered");
      }
      return el;
    });
    // The CANONICAL leaf slug, not the alias `agi1astsni2007g`.
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
      vintageYear: 2024,
    });

    const slugEl = await vi.waitFor(() => {
      const el = document.querySelector(".member-identity code.qualifier.slug");
      if (!el) {
        throw new Error("slug qualifier not yet rendered");
      }
      return el;
    });
    expect(slugEl.textContent).toBe("agi1astsni2007g");
    await expect
      .element(
        page
          .getByRole("link", { name: "Näringsgren, största förvärvskälla" })
          .first(),
      )
      .toBeVisible();
  });

  it("renders no identity row while the graph is loading (no transient slug flicker)", async () => {
    vi.mocked(getBindingGraph).mockReturnValue(new Promise(() => {}));

    render(BindingLeafView, {
      fqidPath: groupedFqid,
      node: groupedNode,
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
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
