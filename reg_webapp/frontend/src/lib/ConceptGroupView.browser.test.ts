import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type {
  ConceptGroupNodeData,
  GraphState,
  RelationshipGraph,
  VariableGraphNode,
} from "./api";
import { getConceptGroup, getConceptGroupGraph } from "./api";
import ConceptGroupView from "./ConceptGroupView.svelte";
import { projectStore } from "./project_store.svelte";
import { router } from "./router.svelte";
import { windowStore } from "./window.svelte";

// The group page (#678) drives TWO catalog GETs: `getConceptGroup` (members +
// facets) and `getConceptGroupGraph` (the union graph carrying each member's
// states). Mock both; keep the rest of api.ts real (the type exports + router).
//
// The picker is ONE compact, integrated COLUMN list (#678 compact redesign): every
// column is visible (no default collapse, no per-variable card chrome). A
// single-column variable is one selectable row; a multi-column variable is a thin
// subheading (with a per-variable select-all) over its column rows. The LEAF and a
// one-variable group render the SAME compact shape.
vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return {
    ...actual,
    getConceptGroup: vi.fn(),
    getConceptGroupGraph: vi.fn(),
  };
});

const SEED = { regMetaVersion: "1.0.0", steward: "global" } as const;

/** A minimal GraphState — only the fields `pickerRepresentations` reads. */
function gstate(over: Partial<GraphState>): GraphState {
  return {
    state_id: 1,
    representation_run_id: 1,
    variant: "v",
    variant_label: null,
    delivery_column_name: null,
    value_set_version_label: "",
    value_set_id: null,
    valid_from: "2010-01-01",
    valid_to: "2015-12-31",
    classification_slug: null,
    ...over,
  };
}

/** A variable graph node carrying the given fqid + states — a variable's column
 * source. `definition`/`description` default null (the common parallel-column
 * sibling shape); pass them to seed the shared-concept-text dedup. */
function vnode(
  fqid: string,
  states: GraphState[],
  meta: { definition?: string | null; description?: string | null } = {},
): VariableGraphNode {
  return {
    kind: "variable",
    id: fqid,
    fqid,
    label: fqid,
    group_key: null,
    group_label: null,
    facets: [],
    states,
    same_as: [],
    definition: meta.definition ?? null,
    description: meta.description ?? null,
  };
}

function graph(nodes: VariableGraphNode[]): RelationshipGraph {
  return { nodes, edges: [], focus_id: null };
}

function node(
  overrides: Partial<ConceptGroupNodeData> = {},
): ConceptGroupNodeData {
  return {
    kind: "concept-group",
    provider: "scb",
    register: "rams",
    key: "ink",
    label: "Inkomst",
    source: "token",
    axes: [{ name: "month", label: "month" }],
    member: null,
    members: [
      {
        fqid: "scb/rams/inkjan",
        name: "Inkomst januari",
        facets: [{ axis: "month", value: "01", label: "januari" }],
        coverage: null,
      },
      {
        fqid: "scb/rams/inkfeb",
        name: "Inkomst februari",
        facets: [{ axis: "month", value: "02", label: "februari" }],
        coverage: null,
      },
    ],
    ...overrides,
  } as unknown as ConceptGroupNodeData;
}

/** A two-member graph, ONE column each: inkjan delivers `Inkjan` (2010–2015),
 * inkfeb delivers `Inkfeb` (2018–2020). Each single-column member renders as ONE
 * compact row (no subheading). */
function twoSingleColGraph(): RelationshipGraph {
  return graph([
    vnode("scb/rams/inkjan", [
      gstate({
        variant: "individer",
        delivery_column_name: "Inkjan",
        valid_from: "2010-01-01",
        valid_to: "2015-12-31",
      }),
    ]),
    vnode("scb/rams/inkfeb", [
      gstate({
        variant: "individer",
        delivery_column_name: "Inkfeb",
        valid_from: "2018-01-01",
        valid_to: "2020-12-31",
      }),
    ]),
  ]);
}

/** A two-member graph where each member has TWO columns → each renders as a thin
 * subheading over its column rows. */
function twoMultiColGraph(): RelationshipGraph {
  return graph([
    vnode("scb/rams/inkjan", [
      gstate({
        variant: "individer",
        delivery_column_name: "InkjanA",
        valid_from: "2010-01-01",
        valid_to: "2015-12-31",
      }),
      gstate({
        variant: "individer",
        delivery_column_name: "InkjanB",
        valid_from: "2016-01-01",
        valid_to: "2020-12-31",
      }),
    ]),
    vnode("scb/rams/inkfeb", [
      gstate({
        variant: "individer",
        delivery_column_name: "InkfebA",
        valid_from: "2010-01-01",
        valid_to: "2015-12-31",
      }),
      gstate({
        variant: "individer",
        delivery_column_name: "InkfebB",
        valid_from: "2016-01-01",
        valid_to: "2020-12-31",
      }),
    ]),
  ]);
}

beforeEach(() => {
  vi.mocked(getConceptGroup).mockReset();
  vi.mocked(getConceptGroupGraph).mockReset();
  // Default: an empty graph (overridden per case).
  vi.mocked(getConceptGroupGraph).mockResolvedValue(graph([]));
  router.navigate("/catalog/group/scb/rams/ink");
  windowStore.set(null);
  projectStore.newProject({
    reg_meta_version: "reg_meta/v1.0.0",
    steward: "global",
  });
});

function renderGroup(
  props: Partial<{ provider: string; register: string; key: string }> = {},
) {
  return render(ConceptGroupView, {
    provider: "scb",
    register: "rams",
    key: "ink",
    regMetaVersion: SEED.regMetaVersion,
    steward: SEED.steward,
    vintageYear: 2024,
    ...props,
  });
}

describe("ConceptGroupView (#617 + #678 compact column list)", () => {
  it("renders single-column members as compact rows in ONE list (no card chrome)", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(node());
    vi.mocked(getConceptGroupGraph).mockResolvedValue(twoSingleColGraph());

    renderGroup();

    // The group's label heads the page.
    await expect
      .element(page.getByRole("heading", { name: "Inkomst", level: 2 }))
      .toBeVisible();

    // ONE integrated list; each single-column member is a single compact row — no
    // per-variable subheading, no bordered cards.
    const rows = await vi.waitFor(() => {
      const els = document.querySelectorAll(".col-row.single");
      if (els.length < 2) {
        throw new Error("compact rows not yet rendered");
      }
      return els;
    });
    expect(rows).toHaveLength(2);
    expect(document.querySelectorAll("li.subhead")).toHaveLength(0);

    // Each row is a selectable checkbox carrying the member's column.
    await expect
      .element(page.getByRole("checkbox", { name: /Inkomst januari/ }))
      .toBeVisible();
    await expect
      .element(page.getByRole("checkbox", { name: /Inkomst februari/ }))
      .toBeVisible();
  });

  it("selecting columns across two members + Add commits the right per-member payloads", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(node());
    vi.mocked(getConceptGroupGraph).mockResolvedValue(twoSingleColGraph());
    const spy = vi.spyOn(projectStore, "addFromCatalog");

    renderGroup();

    const jan = page.getByRole("checkbox", { name: /Inkomst januari/ });
    const feb = page.getByRole("checkbox", { name: /Inkomst februari/ });
    await expect.element(jan).toBeVisible();
    await jan.click();
    await feb.click();

    // ONE shared footer spanning the whole list: the cross-variable count, in
    // "column" terms.
    await expect.element(page.getByText("2 columns selected")).toBeVisible();
    await page.getByRole("button", { name: "Add to project" }).click();

    expect(spy).toHaveBeenCalledTimes(2);
    const payloads = spy.mock.calls.map((c) => c[0]);
    expect(payloads).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          registerVariant: "scb/rams/individer",
          variable: "scb/rams/inkjan",
          representation: "Inkjan",
          resolvedPeriod: "2010..2015",
        }),
        expect.objectContaining({
          registerVariant: "scb/rams/individer",
          variable: "scb/rams/inkfeb",
          representation: "Inkfeb",
          resolvedPeriod: "2018..2020",
        }),
      ]),
    );
    await expect.element(page.getByText(/Added 2 columns/)).toBeVisible();
    spy.mockRestore();
  });

  // #678 finding 3: an active ?period is HONORED on add (the committed source carries
  // the user's narrowed window, not the row's full span).
  it("commits the row span INTERSECTED with the active ?period, not the full span", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(node());
    vi.mocked(getConceptGroupGraph).mockResolvedValue(twoSingleColGraph());
    // inkjan spans 2010–2015; narrow the group to 2012..2014.
    router.navigate("/catalog/group/scb/rams/ink?period=2012..2014");
    const spy = vi.spyOn(projectStore, "addFromCatalog");

    renderGroup();

    const jan = page.getByRole("checkbox", { name: /Inkomst januari/ });
    await expect.element(jan).toBeVisible();
    await jan.click();
    await page.getByRole("button", { name: "Add to project" }).click();

    expect(spy).toHaveBeenCalledTimes(1);
    expect(spy.mock.calls[0][0]).toEqual(
      expect.objectContaining({
        variable: "scb/rams/inkjan",
        // The committed period is the intersection 2012..2014, NOT the row's full
        // 2010..2015 span.
        resolvedPeriod: "2012..2014",
      }),
    );
    spy.mockRestore();
  });

  it("a MULTI-column member renders a thin subheading over its column rows (all visible)", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(node());
    vi.mocked(getConceptGroupGraph).mockResolvedValue(twoMultiColGraph());

    renderGroup();

    // Each ≥2-column member is a subheading + its rows — visible by default, no
    // collapse toggle.
    const subheads = await vi.waitFor(() => {
      const els = document.querySelectorAll("li.subhead");
      if (els.length < 2) {
        throw new Error("subheadings not yet rendered");
      }
      return els;
    });
    expect(subheads).toHaveLength(2);
    expect(document.querySelector(".band-toggle")).toBeNull();
    expect(document.querySelector("button.expand-all")).toBeNull();

    // All four columns are visible at once (nothing collapsed).
    await expect
      .element(page.getByRole("checkbox", { name: /InkjanA/ }))
      .toBeVisible();
    await expect
      .element(page.getByRole("checkbox", { name: /InkjanB/ }))
      .toBeVisible();
    await expect
      .element(page.getByRole("checkbox", { name: /InkfebA/ }))
      .toBeVisible();
    await expect
      .element(page.getByRole("checkbox", { name: /InkfebB/ }))
      .toBeVisible();
  });

  it("a per-variable select-all grabs every column of that variable", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(node());
    vi.mocked(getConceptGroupGraph).mockResolvedValue(twoMultiColGraph());
    const spy = vi.spyOn(projectStore, "addFromCatalog");

    renderGroup();

    // The inkjan subheading's select-all toggle selects BOTH its columns at once.
    // Members have distinct NAMES here, so the subheading is name-led → the aria
    // label is keyed on the member name.
    const janSelectAll = await vi.waitFor(() => {
      const el = document.querySelector<HTMLInputElement>(
        'input[aria-label="Select all columns of Inkomst januari"]',
      );
      if (!el) {
        throw new Error("inkjan select-all not yet rendered");
      }
      return el;
    });
    janSelectAll.click();

    // Both inkjan columns selected; inkfeb's are not (per-variable scope).
    await expect.element(page.getByText("2 columns selected")).toBeVisible();
    await expect
      .element(page.getByRole("checkbox", { name: /InkjanA/ }))
      .toBeChecked();
    await expect
      .element(page.getByRole("checkbox", { name: /InkfebA/ }))
      .not.toBeChecked();

    await page.getByRole("button", { name: "Add to project" }).click();
    expect(spy).toHaveBeenCalledTimes(2);
    expect(
      spy.mock.calls.every((c) => c[0].variable === "scb/rams/inkjan"),
    ).toBe(true);
    spy.mockRestore();
  });

  it("the global select-all grabs every column of the concept", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(node());
    vi.mocked(getConceptGroupGraph).mockResolvedValue(twoMultiColGraph());

    renderGroup();

    const all = await vi.waitFor(() => {
      const el = document.querySelector<HTMLInputElement>(
        'input[aria-label="Select all columns"]',
      );
      if (!el) {
        throw new Error("global select-all not yet rendered");
      }
      return el;
    });
    all.click();

    // Every column across both variables is selected.
    await expect.element(page.getByText("4 columns selected")).toBeVisible();
    for (const name of [/InkjanA/, /InkjanB/, /InkfebA/, /InkfebB/]) {
      await expect.element(page.getByRole("checkbox", { name })).toBeChecked();
    }
  });

  it("a member with no graph node renders a quiet 'No columns' subheading, not dropped", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(node());
    // Only inkjan has a graph node (single column → a row); inkfeb is absent (0
    // columns → a subheading with the empty marker).
    vi.mocked(getConceptGroupGraph).mockResolvedValue(
      graph([
        vnode("scb/rams/inkjan", [
          gstate({
            variant: "individer",
            delivery_column_name: "Inkjan",
            valid_from: "2010-01-01",
            valid_to: "2015-12-31",
          }),
        ]),
      ]),
    );

    renderGroup();

    await expect
      .element(page.getByRole("checkbox", { name: /Inkomst januari/ }))
      .toBeVisible();
    // inkfeb's quiet "No columns" marker renders (not dropped, no checkbox).
    await expect
      .element(page.getByText("Inkomst februari", { exact: true }))
      .toBeVisible();
    await expect
      .element(page.getByText("No columns", { exact: true }))
      .toBeVisible();
  });

  it("dims a column whose span does not overlap the active period window", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(node());
    vi.mocked(getConceptGroupGraph).mockResolvedValue(twoSingleColGraph());
    // Narrow to 2018..2020 — inkfeb (2018–2020) overlaps, inkjan (2010–2015) does
    // not, so inkjan's row is dimmed (but still selectable).
    router.navigate("/catalog/group/scb/rams/ink?period=2018..2020");

    renderGroup();

    const jan = page.getByRole("checkbox", { name: /Inkomst januari/ });
    await expect.element(jan).toBeVisible();
    // The `dimmed` class is on the row container (.row-btn label), not the checkbox.
    await vi.waitFor(() => {
      const rowBtn = jan.element().closest(".row-btn");
      if (!rowBtn?.classList.contains("dimmed")) {
        throw new Error("inkjan row not yet dimmed");
      }
    });
    const febRow = page
      .getByRole("checkbox", { name: /Inkomst februari/ })
      .element()
      .closest(".row-btn");
    expect(febRow?.classList.contains("dimmed")).toBe(false);
    // A dimmed row stays selectable.
    await jan.click();
    await expect.element(jan).toBeChecked();
  });

  it("dims the SUBHEADING when ALL its columns are out of the active window (#678)", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(node());
    // inkjan: both columns 2010–2020 (fully out of 1980..2004). inkfeb: one column
    // 2000–2004 (IN window), one 2016–2020 (out) → at least one in.
    vi.mocked(getConceptGroupGraph).mockResolvedValue(
      graph([
        vnode("scb/rams/inkjan", [
          gstate({
            variant: "v",
            delivery_column_name: "InkjanA",
            valid_from: "2010-01-01",
            valid_to: "2015-12-31",
          }),
          gstate({
            variant: "v",
            delivery_column_name: "InkjanB",
            valid_from: "2016-01-01",
            valid_to: "2020-12-31",
          }),
        ]),
        vnode("scb/rams/inkfeb", [
          gstate({
            variant: "v",
            delivery_column_name: "InkfebA",
            valid_from: "2000-01-01",
            valid_to: "2004-12-31",
          }),
          gstate({
            variant: "v",
            delivery_column_name: "InkfebB",
            valid_from: "2016-01-01",
            valid_to: "2020-12-31",
          }),
        ]),
      ]),
    );
    router.navigate("/catalog/group/scb/rams/ink?period=1980..2004");

    renderGroup();

    // Wait for both subheadings to render, then check their dim state.
    await vi.waitFor(() => {
      if (document.querySelectorAll("li.subhead").length < 2) {
        throw new Error("subheadings not yet rendered");
      }
    });
    // The members have distinct NAMES → name-led subheading, so the select-all aria
    // label is keyed on the member name.
    const inkjanSub = document
      .querySelector(
        'input[aria-label="Select all columns of Inkomst januari"]',
      )
      ?.closest("li.subhead");
    const inkfebSub = document
      .querySelector(
        'input[aria-label="Select all columns of Inkomst februari"]',
      )
      ?.closest("li.subhead");
    // inkjan: all columns out → the subheading greys.
    expect(inkjanSub?.classList.contains("dimmed")).toBe(true);
    // inkfeb: one column in window → the subheading stays full-strength.
    expect(inkfebSub?.classList.contains("dimmed")).toBe(false);
  });

  it("shows a data-starts-late warning when the window starts before a column's data (#678)", async () => {
    // fordonsreg ?period=1980..2004: data starts 2003, so each in-window row gets a
    // warning by its start year; a column covering the window start does NOT.
    vi.mocked(getConceptGroup).mockResolvedValue(node());
    vi.mocked(getConceptGroupGraph).mockResolvedValue(
      graph([
        // inkjan: data starts 2003 (after the 1980 window start) but is IN window →
        // the warning fires.
        vnode("scb/rams/inkjan", [
          gstate({
            variant: "v",
            delivery_column_name: "Late",
            valid_from: "2003-01-01",
            valid_to: "2004-12-31",
          }),
        ]),
        // inkfeb: data starts 1975 (covers the window start) → no warning.
        vnode("scb/rams/inkfeb", [
          gstate({
            variant: "v",
            delivery_column_name: "Early",
            valid_from: "1975-01-01",
            valid_to: "2004-12-31",
          }),
        ]),
      ]),
    );
    router.navigate("/catalog/group/scb/rams/ink?period=1980..2004");

    renderGroup();

    // The late-start row (inkjan/Late, single column) carries the warning marker.
    const lateRow = await vi.waitFor(() => {
      const cb = page.getByRole("checkbox", { name: /Late/ }).element();
      const row = cb.closest(".row-btn");
      if (!row) {
        throw new Error("Late row not yet rendered");
      }
      return row;
    });
    const warn = lateRow.querySelector(".late-warn");
    expect(warn).not.toBeNull();
    expect(warn?.getAttribute("aria-label")).toBe(
      "Data starts 2003 — your selected period begins 1980",
    );
    // The early-start row (covers the window start) gets NO warning.
    const earlyRow = page
      .getByRole("checkbox", { name: /Early/ })
      .element()
      .closest(".row-btn");
    expect(earlyRow?.querySelector(".late-warn")).toBeNull();
  });

  it("shows NO data-starts-late warning on a FULLY-out-of-window row (it's already dimmed) (#678)", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(node());
    // inkjan's only column is 2010–2015 — entirely AFTER the 1980..2004 window. Its
    // start (2010) is > the window start (1980), but the row is fully out → dimmed,
    // and the warning is suppressed (it's for IN-window rows only).
    vi.mocked(getConceptGroupGraph).mockResolvedValue(
      graph([
        vnode("scb/rams/inkjan", [
          gstate({
            variant: "v",
            delivery_column_name: "Out",
            valid_from: "2010-01-01",
            valid_to: "2015-12-31",
          }),
        ]),
      ]),
    );
    router.navigate("/catalog/group/scb/rams/ink?period=1980..2004");

    renderGroup();

    const outRow = await vi.waitFor(() => {
      const cb = page.getByRole("checkbox", { name: /Out/ }).element();
      const row = cb.closest(".row-btn");
      if (!row) {
        throw new Error("Out row not yet rendered");
      }
      return row;
    });
    // Dimmed (fully out) and NO late-warn marker.
    expect(outRow.classList.contains("dimmed")).toBe(true);
    expect(outRow.querySelector(".late-warn")).toBeNull();
  });

  it("shows NO data-starts-late warning when no period window is set (#678)", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(node());
    vi.mocked(getConceptGroupGraph).mockResolvedValue(twoSingleColGraph());
    // No ?period and no project window → no window → no warnings anywhere.

    renderGroup();

    await expect
      .element(page.getByRole("checkbox", { name: /Inkomst januari/ }))
      .toBeVisible();
    expect(document.querySelector(".late-warn")).toBeNull();
  });

  it("keeps Add seed-gated (disabled) until a column is selected", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(node());
    vi.mocked(getConceptGroupGraph).mockResolvedValue(twoSingleColGraph());

    renderGroup();
    const add = page.getByRole("button", { name: "Add to project" });
    await expect.element(add).toBeVisible();
    await expect.element(add).toBeDisabled();
    await page.getByRole("checkbox", { name: /Inkomst januari/ }).click();
    await expect.element(add).toBeEnabled();
  });

  it("demotes the key, facets, and source into a 'Technical details' disclosure (#638 PR4)", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(node());
    vi.mocked(getConceptGroupGraph).mockResolvedValue(twoSingleColGraph());

    renderGroup();

    await expect.element(page.getByText("Technical details")).toBeVisible();
    const disclosure = document.querySelector<HTMLDetailsElement>(
      "details.tech-details",
    );
    expect(disclosure).not.toBeNull();
    expect(disclosure?.open).toBe(false);
    expect(disclosure?.textContent).toContain("Group");
    expect(disclosure?.textContent).toContain("ink");
    expect(disclosure?.textContent).toContain("Facets");
    expect(disclosure?.textContent).toContain("month");
    expect(disclosure?.textContent).toContain("Source");
    expect(disclosure?.textContent).toContain("token");
    const promptMeta = [...document.querySelectorAll("dl.meta")].filter(
      (dl) => !dl.closest("details.tech-details"),
    );
    expect(promptMeta).toHaveLength(0);
  });

  it("collapses representation members on one fqid into ONE variable (no duplicate rows)", async () => {
    // A representation-member group: two members share `scb/iot/dispink` (distinct
    // delivery columns), the graph node carries both columns' states. The variables
    // dedup by fqid, so the shared variable renders as ONE subheading whose two
    // column rows surface the columns — not two duplicate variables.
    vi.mocked(getConceptGroup).mockResolvedValue(
      node({
        key: "disponibel-inkomst",
        label: "Disponibel inkomst",
        axes: [{ name: "kapitalvinst", label: "Kapitalvinst" }],
        members: [
          {
            fqid: "scb/iot/dispink",
            name: "Disponibel inkomst",
            delivery_column: "dispink_inkl",
            facets: [{ axis: "kapitalvinst", value: "inkl", label: "Inkl." }],
            coverage: null,
          },
          {
            fqid: "scb/iot/dispink",
            name: "Disponibel inkomst",
            delivery_column: "dispink_exkl",
            facets: [{ axis: "kapitalvinst", value: "exkl", label: "Exkl." }],
            coverage: null,
          },
        ],
      } as unknown as Partial<ConceptGroupNodeData>),
    );
    vi.mocked(getConceptGroupGraph).mockResolvedValue(
      graph([
        vnode("scb/iot/dispink", [
          gstate({
            variant: "individer",
            delivery_column_name: "dispink_inkl",
            valid_from: "2010-01-01",
            valid_to: "2020-12-31",
          }),
          gstate({
            variant: "individer",
            delivery_column_name: "dispink_exkl",
            valid_from: "2010-01-01",
            valid_to: "2020-12-31",
          }),
        ]),
      ]),
    );

    renderGroup({
      provider: "scb",
      register: "iot",
      key: "disponibel-inkomst",
    });

    // ONE subheading for the shared-fqid variable, two column rows under it.
    const subheads = await vi.waitFor(() => {
      const els = document.querySelectorAll("li.subhead");
      if (els.length === 0) {
        throw new Error("subheading not yet rendered");
      }
      return els;
    });
    expect(subheads).toHaveLength(1);
    await expect
      .element(page.getByRole("checkbox", { name: /dispink_inkl/ }))
      .toBeVisible();
    await expect
      .element(page.getByRole("checkbox", { name: /dispink_exkl/ }))
      .toBeVisible();
  });

  // ── Adaptive variable identity (#678) ───────────────────────────────────────
  it("a name-constant MIXED group: single-column members are column-led rows, the multi-column member is a column-led subheading", async () => {
    // The moms/naringsgren shape: all members are "Näringsgren" on `scb/moms`. Ng0
    // and Ng1 deliver ONE column each → compact rows led by the column (mono); the
    // sni member delivers TWO columns → a subheading led by its slug.
    const members = [
      {
        fqid: "scb/moms/naringsgren_ng0",
        name: "Näringsgren",
        facets: [],
        coverage: null,
      },
      {
        fqid: "scb/moms/naringsgren_ng1",
        name: "Näringsgren",
        facets: [],
        coverage: null,
      },
      {
        fqid: "scb/moms/naringsgren_sni",
        name: "Näringsgren",
        facets: [],
        coverage: null,
      },
    ];
    vi.mocked(getConceptGroup).mockResolvedValue(
      node({
        provider: "scb",
        register: "moms",
        key: "naringsgren",
        label: "Näringsgren",
        axes: [],
        members,
      } as unknown as Partial<ConceptGroupNodeData>),
    );
    vi.mocked(getConceptGroupGraph).mockResolvedValue(
      graph([
        vnode("scb/moms/naringsgren_ng0", [
          gstate({
            variant: "individer",
            delivery_column_name: "Ng0",
            valid_from: "2010-01-01",
            valid_to: "2015-12-31",
          }),
        ]),
        vnode("scb/moms/naringsgren_ng1", [
          gstate({
            variant: "individer",
            delivery_column_name: "Ng1",
            valid_from: "2010-01-01",
            valid_to: "2015-12-31",
          }),
        ]),
        vnode("scb/moms/naringsgren_sni", [
          gstate({
            variant: "individer",
            delivery_column_name: "Sni92",
            valid_from: "2002-01-01",
            valid_to: "2006-12-31",
          }),
          gstate({
            variant: "individer",
            delivery_column_name: "Sni2007",
            valid_from: "2007-01-01",
            valid_to: "2015-12-31",
          }),
        ]),
      ]),
    );

    renderGroup({ provider: "scb", register: "moms", key: "naringsgren" });

    // Ng0 / Ng1 are single-column compact rows led by the column rendered as a
    // COLUMN CHIP (the prominent selection signal); the constant concept name is NOT
    // repeated on them. In the GROUP view the identity chip is a NAVIGATION LINK to
    // the member's leaf page (band.href set), so it's an <a class="col-chip link">.
    const singleTitles = await vi.waitFor(() => {
      const els = document.querySelectorAll(".col-row.single .col-chip");
      if (els.length < 2) {
        throw new Error("single-column chips not yet rendered");
      }
      // The chip's leading text node is the column name (a trailing ↗ link marker
      // follows it inside the navigable chip).
      return [...els].map((e) => e.firstChild?.textContent?.trim());
    });
    expect(singleTitles).toEqual(["Ng0", "Ng1"]);
    // The identity column chip is a navigable link (an <a>), not a plain <code>.
    const chips = [...document.querySelectorAll(".col-row.single .col-chip")];
    expect(chips.every((e) => e.tagName === "A")).toBe(true);
    expect(chips.map((e) => e.getAttribute("href"))).toEqual([
      "/catalog/scb/moms/naringsgren_ng0",
      "/catalog/scb/moms/naringsgren_ng1",
    ]);

    // The sni member is a subheading (2 columns) led by its slug; its rows are below.
    expect(document.querySelectorAll("li.subhead")).toHaveLength(1);
    const subheadPrimary = document
      .querySelector(".subhead-title .primary")
      ?.textContent?.trim();
    expect(subheadPrimary).toBe("naringsgren_sni");
    await expect
      .element(page.getByRole("checkbox", { name: /Sni92/ }))
      .toBeVisible();
    await expect
      .element(page.getByRole("checkbox", { name: /Sni2007/ }))
      .toBeVisible();
    // The NESTED column chips (sni's two columns) are PLAIN <code>, NOT links — a
    // nested column isn't its own variable, so only the single-column identity chip
    // navigates.
    const nestedChips = [
      ...document.querySelectorAll(".col-row.nested .col-chip"),
    ];
    expect(nestedChips.length).toBe(2);
    expect(nestedChips.every((e) => e.tagName === "CODE")).toBe(true);
    expect(document.querySelector(".col-row.nested a.col-chip")).toBeNull();
  });

  it("a facet group of single-column members leads each compact row with its FACET label (normal weight)", async () => {
    // The moderns-utbildningsniva shape: name constant, a facet axis varies → the
    // facet (specialskola / grundskola) leads each row, not the column.
    vi.mocked(getConceptGroup).mockResolvedValue(
      node({
        provider: "scb",
        register: "forskoleklass",
        key: "utbildning",
        label: "Moderns utbildningsnivå",
        axes: [{ name: "skolform", label: "Skolform" }],
        members: [
          {
            fqid: "scb/forskoleklass/utb_spec",
            name: "Moderns utbildningsnivå",
            facets: [
              { axis: "skolform", value: "spec", label: "specialskola" },
            ],
            coverage: null,
          },
          {
            fqid: "scb/forskoleklass/utb_grund",
            name: "Moderns utbildningsnivå",
            facets: [{ axis: "skolform", value: "grund", label: "grundskola" }],
            coverage: null,
          },
        ],
      } as unknown as Partial<ConceptGroupNodeData>),
    );
    vi.mocked(getConceptGroupGraph).mockResolvedValue(
      graph([
        vnode("scb/forskoleklass/utb_spec", [
          gstate({
            variant: "individer",
            delivery_column_name: "UtbSpec",
            valid_from: "2010-01-01",
            valid_to: "2015-12-31",
          }),
        ]),
        vnode("scb/forskoleklass/utb_grund", [
          gstate({
            variant: "individer",
            delivery_column_name: "UtbGrund",
            valid_from: "2010-01-01",
            valid_to: "2015-12-31",
          }),
        ]),
      ]),
    );

    renderGroup({
      provider: "scb",
      register: "forskoleklass",
      key: "utbildning",
    });

    const titles = await vi.waitFor(() => {
      const els = document.querySelectorAll(".col-row.single .primary");
      if (els.length < 2) {
        throw new Error("rows not yet rendered");
      }
      return [...els].map((e) => e.textContent?.trim());
    });
    expect(titles).toEqual(["specialskola", "grundskola"]);
    // The facet leads as a normal-weight human label (a <span>, not mono <code>).
    expect(
      [...document.querySelectorAll(".col-row.single .primary")].every(
        (e) => e.tagName === "SPAN",
      ),
    ).toBe(true);
  });

  // ── Member → leaf navigation (#678) ─────────────────────────────────────────
  it("a single-column member's COLUMN CHIP is the leaf-navigation link (no separate 'View' link)", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(node());
    vi.mocked(getConceptGroupGraph).mockResolvedValue(twoSingleColGraph());

    renderGroup();

    // The column chip ITSELF is the navigation link to the member's leaf FQID — there
    // is no separate "View ↗" link anymore.
    const janLink = await vi.waitFor(() => {
      const els = [...document.querySelectorAll("a.col-chip.link")];
      const jan = els.find(
        (a) => a.getAttribute("href") === "/catalog/scb/rams/inkjan",
      );
      if (!jan) {
        throw new Error("inkjan column-chip link not yet rendered");
      }
      return jan;
    });
    expect(janLink.tagName).toBe("A");
    // The chip-link is inside the row label (the click-anywhere selection target) but
    // is itself a real <a> (keyboard-navigable; it stops propagation so a nav click
    // never toggles).
    expect(janLink.closest("label.row-btn")).not.toBeNull();
    // The other member's chip links too.
    expect(
      document.querySelector(
        'a.col-chip.link[href="/catalog/scb/rams/inkfeb"]',
      ),
    ).not.toBeNull();
    // No legacy "View ↗" link survives.
    expect(document.querySelector("a.open-link")).toBeNull();
  });

  it("a multi-column member renders its subheading title as a leaf link", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(node());
    vi.mocked(getConceptGroupGraph).mockResolvedValue(twoMultiColGraph());

    renderGroup();

    const titleLink = await vi.waitFor(() => {
      const el = document.querySelector<HTMLAnchorElement>(
        'a.subhead-title[href="/catalog/scb/rams/inkjan"]',
      );
      if (!el) {
        throw new Error("inkjan subheading link not yet rendered");
      }
      return el;
    });
    expect(titleLink.tagName).toBe("A");
    // The select-all checkbox is a SEPARATE control (not inside the link).
    const checkbox = titleLink
      .closest(".subhead-label")
      ?.querySelector('input[type="checkbox"]');
    expect(checkbox).not.toBeNull();
    expect(checkbox?.closest("a")).toBeNull();
  });

  it("clicking a subheading (not the title link) toggles ALL its columns", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(node());
    vi.mocked(getConceptGroupGraph).mockResolvedValue(twoMultiColGraph());

    renderGroup();

    // The WHOLE subheading is a <label> wrapping the select-all checkbox: clicking it
    // (off the title link) toggles every column of that variable.
    const inkjanRow = await vi.waitFor(() => {
      const cb = document.querySelector<HTMLInputElement>(
        'input[aria-label="Select all columns of Inkomst januari"]',
      );
      const label = cb?.closest("label.subhead-label");
      if (!label) {
        throw new Error("inkjan subhead label not yet rendered");
      }
      return label as HTMLLabelElement;
    });
    // Click the label itself (not the title link inside it).
    inkjanRow.click();

    await expect.element(page.getByText("2 columns selected")).toBeVisible();
    await expect
      .element(page.getByRole("checkbox", { name: /InkjanA/ }))
      .toBeChecked();
    await expect
      .element(page.getByRole("checkbox", { name: /InkjanB/ }))
      .toBeChecked();
    // The other variable's columns are untouched.
    await expect
      .element(page.getByRole("checkbox", { name: /InkfebA/ }))
      .not.toBeChecked();
  });

  it("a FULLY-selected variable carries the rust left bar on its subheading; partial does NOT (#678)", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(node());
    vi.mocked(getConceptGroupGraph).mockResolvedValue(twoMultiColGraph());

    renderGroup();

    const inkjanSub = await vi.waitFor(() => {
      const li = document
        .querySelector(
          'input[aria-label="Select all columns of Inkomst januari"]',
        )
        ?.closest("li.subhead");
      if (!li) {
        throw new Error("inkjan subhead not yet rendered");
      }
      return li;
    });
    // Nothing selected → no rust bar.
    expect(inkjanSub.classList.contains("selected")).toBe(false);

    // Select ONE of inkjan's two columns → PARTIAL: still no full rust bar.
    await page.getByRole("checkbox", { name: /InkjanA/ }).click();
    await vi.waitFor(() => {
      if (inkjanSub.classList.contains("selected")) {
        throw new Error("partial selection should NOT show the full rust bar");
      }
    });

    // Select the OTHER → FULLY selected → the rust left bar appears.
    await page.getByRole("checkbox", { name: /InkjanB/ }).click();
    await vi.waitFor(() => {
      if (!inkjanSub.classList.contains("selected")) {
        throw new Error("fully-selected variable should show the rust bar");
      }
    });
  });

  it("a single-column member: the column chip is the title-link; the description toggles all (#678)", async () => {
    // The fordonsreg shape: a single-column member ("SNI2002") with two populations.
    // It leads with its column as the subheading TITLE chip-LINK; the value-set
    // description rides in the context, INSIDE the click-all + hover-all <label>, so
    // clicking the description toggles all the variable's columns (the chip-link itself
    // navigates instead, stopping propagation).
    vi.mocked(getConceptGroup).mockResolvedValue(
      node({
        provider: "scb",
        register: "fordonsreg",
        key: "naringsgren",
        label: "Näringsgren",
        axes: [],
        members: [
          {
            fqid: "scb/fordonsreg/naringsgren",
            name: "Näringsgren",
            facets: [],
            coverage: null,
          },
        ],
      } as unknown as Partial<ConceptGroupNodeData>),
    );
    // Two columns? No — constant column "SNI2002" across two VARIANTS → multi-row,
    // column-constant variable: the column hoists to the context chip, populations
    // vary per row. A value-set label gives the description text.
    vi.mocked(getConceptGroupGraph).mockResolvedValue(
      graph([
        vnode("scb/fordonsreg/naringsgren", [
          gstate({
            variant: "lastbilar",
            delivery_column_name: "SNI2002",
            value_set_version_label:
              "Standard för svensk näringsgrensindelning",
            valid_from: "2003-01-01",
            valid_to: "2015-12-31",
          }),
          gstate({
            variant: "bussar",
            delivery_column_name: "SNI2002",
            value_set_version_label:
              "Standard för svensk näringsgrensindelning",
            valid_from: "2003-01-01",
            valid_to: "2015-12-31",
          }),
        ]),
      ]),
    );

    renderGroup({
      provider: "scb",
      register: "fordonsreg",
      key: "naringsgren",
    });

    // The column is the subheading TITLE — a chip-LINK to the member's leaf.
    const titleChip = await vi.waitFor(() => {
      const el = document.querySelector<HTMLAnchorElement>(
        ".subhead-title a.col-chip.link",
      );
      if (!el) {
        throw new Error("subhead title chip-link not yet rendered");
      }
      return el;
    });
    expect(titleChip.firstChild?.textContent?.trim()).toBe("SNI2002");
    expect(titleChip.getAttribute("href")).toBe(
      "/catalog/scb/fordonsreg/naringsgren",
    );

    // The description rides in the context, INSIDE the hover/click <label>.
    const context = document.querySelector(".subhead-context");
    expect(context?.textContent).toContain(
      "Standard för svensk näringsgrensindelning",
    );
    expect(context?.closest("label.subhead-label")).not.toBeNull();

    // Clicking the DESCRIPTION (part of the select-all label, not the chip-link)
    // toggles ALL the variable's columns.
    (context as HTMLElement).click();
    await expect.element(page.getByText("2 columns selected")).toBeVisible();
    await expect
      .element(page.getByRole("checkbox", { name: /lastbilar/ }))
      .toBeChecked();
    await expect
      .element(page.getByRole("checkbox", { name: /bussar/ }))
      .toBeChecked();
  });

  it("hovering a subheading highlights ALL its column rows (band-hover)", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(node());
    vi.mocked(getConceptGroupGraph).mockResolvedValue(twoMultiColGraph());

    renderGroup();

    // The inkjan column rows (by their column checkboxes) and the inkjan subhead label.
    const janA = await vi.waitFor(() => {
      const cb = page.getByRole("checkbox", { name: /InkjanA/ }).element();
      const rowBtn = cb.closest(".row-btn");
      if (!rowBtn) {
        throw new Error("InkjanA row not yet rendered");
      }
      return rowBtn;
    });
    const janB = page
      .getByRole("checkbox", { name: /InkjanB/ })
      .element()
      .closest(".row-btn") as Element;
    const febA = page
      .getByRole("checkbox", { name: /InkfebA/ })
      .element()
      .closest(".row-btn") as Element;
    const inkjanLabel = document
      .querySelector(
        'input[aria-label="Select all columns of Inkomst januari"]',
      )
      ?.closest("label.subhead-label") as HTMLLabelElement;

    // Normalize first (the real Chromium cursor may already sit over a row from a
    // prior test's click, firing a genuine mouseenter), then test the enter→leave
    // transition deterministically.
    inkjanLabel.dispatchEvent(new MouseEvent("mouseleave", { bubbles: false }));
    await vi.waitFor(() => {
      if (janA.classList.contains("band-hover")) {
        throw new Error("baseline not yet cleared");
      }
    });

    inkjanLabel.dispatchEvent(new MouseEvent("mouseenter", { bubbles: false }));
    await vi.waitFor(() => {
      if (
        !janA.classList.contains("band-hover") ||
        !janB.classList.contains("band-hover")
      ) {
        throw new Error("inkjan rows not yet band-hovered");
      }
    });
    // Only inkjan's rows highlight — NOT the other variable's.
    expect(febA.classList.contains("band-hover")).toBe(false);

    inkjanLabel.dispatchEvent(new MouseEvent("mouseleave", { bubbles: false }));
    await vi.waitFor(() => {
      if (janA.classList.contains("band-hover")) {
        throw new Error("band-hover not cleared on leave");
      }
    });
  });

  it("clicking the subheading TITLE link navigates without toggling selection", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(node());
    vi.mocked(getConceptGroupGraph).mockResolvedValue(twoMultiColGraph());

    renderGroup();

    const titleLink = await vi.waitFor(() => {
      const el = document.querySelector<HTMLAnchorElement>(
        'a.subhead-title[href="/catalog/scb/rams/inkjan"]',
      );
      if (!el) {
        throw new Error("inkjan title link not yet rendered");
      }
      return el;
    });
    // Dispatch a cancelable click on the title link (prevent actual navigation in the
    // test). It stops propagation, so the wrapping label's select-all never fires.
    const evt = new MouseEvent("click", { bubbles: true, cancelable: true });
    evt.preventDefault();
    titleLink.dispatchEvent(evt);

    // No column got selected — the nav link did not toggle the band.
    await expect.element(page.getByText("0 columns selected")).toBeVisible();
    expect(
      document.querySelector<HTMLInputElement>(
        'input[aria-label="Select all columns of Inkomst januari"]',
      )?.checked,
    ).toBe(false);
  });

  it("verifies the fordonsreg/naringsgren shape: the Näringsgren member links to its leaf", async () => {
    // The reported case: /catalog/group/scb/fordonsreg/naringsgren → the Näringsgren
    // member (single column here) links to /catalog/scb/fordonsreg/naringsgren.
    vi.mocked(getConceptGroup).mockResolvedValue(
      node({
        provider: "scb",
        register: "fordonsreg",
        key: "naringsgren",
        label: "Näringsgren",
        axes: [],
        members: [
          {
            fqid: "scb/fordonsreg/naringsgren",
            name: "Näringsgren",
            facets: [],
            coverage: null,
          },
        ],
      } as unknown as Partial<ConceptGroupNodeData>),
    );
    vi.mocked(getConceptGroupGraph).mockResolvedValue(
      graph([
        vnode("scb/fordonsreg/naringsgren", [
          gstate({
            variant: "snoskotrar",
            variant_label: "Snöskotrar",
            delivery_column_name: "Sni",
            valid_from: "2010-01-01",
            valid_to: "2015-12-31",
          }),
        ]),
      ]),
    );

    renderGroup({
      provider: "scb",
      register: "fordonsreg",
      key: "naringsgren",
    });

    // The Näringsgren member's column chip is the leaf link (no "View" link).
    const link = await vi.waitFor(() => {
      const el = document.querySelector<HTMLAnchorElement>(
        'a.col-chip.link[href="/catalog/scb/fordonsreg/naringsgren"]',
      );
      if (!el) {
        throw new Error("naringsgren chip link not yet rendered");
      }
      return el;
    });
    expect(link.tagName).toBe("A");
    expect(document.querySelector("a.open-link")).toBeNull();
  });

  // ── Shared concept definition / description (#678) ───────────────────────────
  it("renders the shared definition/description ONCE at the group level, even though a sibling carries null", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(node());
    // The canonical member (inkjan) carries the shared concept text; the parallel
    // sibling (inkfeb) carries null — the dedup must NOT blank the block, and the
    // single distinct value renders exactly once at the group level.
    vi.mocked(getConceptGroupGraph).mockResolvedValue(
      graph([
        vnode(
          "scb/rams/inkjan",
          [
            gstate({
              variant: "individer",
              delivery_column_name: "Inkjan",
              valid_from: "2010-01-01",
              valid_to: "2015-12-31",
            }),
          ],
          {
            definition: "Annual disposable income of the individual.",
            description: "Summed across all income sources, SCB standard.",
          },
        ),
        vnode("scb/rams/inkfeb", [
          gstate({
            variant: "individer",
            delivery_column_name: "Inkfeb",
            valid_from: "2018-01-01",
            valid_to: "2020-12-31",
          }),
        ]),
      ]),
    );

    renderGroup();

    // The shared block renders ABOVE the Technical details disclosure (not inside it).
    const sharedMeta = await vi.waitFor(() => {
      const els = [...document.querySelectorAll("dl.meta")].filter(
        (dl) => !dl.closest("details.tech-details"),
      );
      if (els.length === 0) {
        throw new Error("shared meta block not yet rendered");
      }
      return els;
    });
    expect(sharedMeta).toHaveLength(1);
    const block = sharedMeta[0];
    // Each label appears exactly once — the null sibling did not add or blank it.
    expect(block.querySelectorAll("dt")).toHaveLength(2);
    const dts = [...block.querySelectorAll("dt")].map((dt) => dt.textContent);
    expect(dts).toEqual(["Definition", "Description"]);
    await expect
      .element(
        page.getByText("Annual disposable income of the individual.", {
          exact: true,
        }),
      )
      .toBeVisible();
    await expect
      .element(
        page.getByText("Summed across all income sources, SCB standard.", {
          exact: true,
        }),
      )
      .toBeVisible();
  });

  it("renders no shared-meta block when every member's definition/description is null", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(node());
    vi.mocked(getConceptGroupGraph).mockResolvedValue(twoSingleColGraph());

    renderGroup();

    // Wait for the page to render (the picker rows), then assert no shared block.
    await expect
      .element(page.getByRole("checkbox", { name: /Inkomst januari/ }))
      .toBeVisible();
    const sharedMeta = [...document.querySelectorAll("dl.meta")].filter(
      (dl) => !dl.closest("details.tech-details"),
    );
    expect(sharedMeta).toHaveLength(0);
  });

  // ── #678 finding 1: a representation group exposes only its MEMBER columns ────
  it("a representation group exposes only its member delivery columns, not the variable's full column set", async () => {
    // The group's members address ONE variable (scb/rams/ink) but only the `IncA`
    // column; the graph node now carries BOTH `IncA` and a NON-member `IncExtra`
    // (the variable's full set). The band must restrict to the member column so the
    // non-member column is never selectable.
    vi.mocked(getConceptGroup).mockResolvedValue(
      node({
        members: [
          {
            fqid: "scb/rams/ink",
            name: "Inkomst",
            delivery_column: "IncA",
            facets: [{ axis: "month", value: "01", label: "januari" }],
            coverage: null,
          },
        ],
      } as unknown as Partial<ConceptGroupNodeData>),
    );
    vi.mocked(getConceptGroupGraph).mockResolvedValue(
      graph([
        vnode("scb/rams/ink", [
          gstate({
            variant: "individer",
            delivery_column_name: "IncA",
            valid_from: "2010-01-01",
            valid_to: "2015-12-31",
          }),
          gstate({
            variant: "individer",
            delivery_column_name: "IncExtra",
            valid_from: "2010-01-01",
            valid_to: "2015-12-31",
          }),
        ]),
      ]),
    );

    renderGroup();

    // The member column renders; the non-member one does NOT.
    await expect
      .element(page.getByRole("checkbox", { name: /IncA/ }))
      .toBeVisible();
    await vi.waitFor(() => {
      const labels = [...document.querySelectorAll(".col-chip")].map((e) =>
        e.textContent?.trim(),
      );
      if (!labels.some((l) => l?.startsWith("IncA"))) {
        throw new Error("IncA not yet rendered");
      }
    });
    const chipTexts = [...document.querySelectorAll(".col-chip")].map((e) =>
      e.textContent?.trim(),
    );
    expect(chipTexts.some((t) => t?.includes("IncExtra"))).toBe(false);
    // Exactly one selectable row (the member column).
    expect(
      document.querySelectorAll('.col-list input[type="checkbox"]'),
    ).toHaveLength(1);
  });

  it("a WHOLE-VARIABLE member (null delivery_column) exposes ALL the variable's columns", async () => {
    // When the member is the whole variable, every column is legitimately
    // selectable → no filter.
    vi.mocked(getConceptGroup).mockResolvedValue(
      node({
        members: [
          {
            fqid: "scb/rams/ink",
            name: "Inkomst",
            delivery_column: null,
            facets: [{ axis: "month", value: "01", label: "januari" }],
            coverage: null,
          },
        ],
      } as unknown as Partial<ConceptGroupNodeData>),
    );
    vi.mocked(getConceptGroupGraph).mockResolvedValue(
      graph([
        vnode("scb/rams/ink", [
          gstate({
            variant: "individer",
            delivery_column_name: "IncA",
            valid_from: "2010-01-01",
            valid_to: "2015-12-31",
          }),
          gstate({
            variant: "individer",
            delivery_column_name: "IncExtra",
            valid_from: "2010-01-01",
            valid_to: "2015-12-31",
          }),
        ]),
      ]),
    );

    renderGroup();

    await expect
      .element(page.getByRole("checkbox", { name: /IncA/ }))
      .toBeVisible();
    await expect
      .element(page.getByRole("checkbox", { name: /IncExtra/ }))
      .toBeVisible();
  });

  // ── #678 finding 5: the member link carries the active group ?period ─────────
  it("a member nav link carries the active group ?period", async () => {
    router.navigate("/catalog/group/scb/rams/ink?period=2018..2020");
    vi.mocked(getConceptGroup).mockResolvedValue(node());
    vi.mocked(getConceptGroupGraph).mockResolvedValue(twoSingleColGraph());

    renderGroup();

    // The chip link keeps the window the user narrowed the group to.
    const janLink = await vi.waitFor(() => {
      const el = document.querySelector<HTMLAnchorElement>(
        'a.col-chip.link[href*="/catalog/scb/rams/inkjan"]',
      );
      if (!el) {
        throw new Error("inkjan chip link not yet rendered");
      }
      return el;
    });
    expect(janLink.getAttribute("href")).toBe(
      "/catalog/scb/rams/inkjan?period=2018..2020",
    );
  });

  it("a member nav link has NO ?period when the group is not narrowed", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(node());
    vi.mocked(getConceptGroupGraph).mockResolvedValue(twoSingleColGraph());

    renderGroup();

    const janLink = await vi.waitFor(() => {
      const el = document.querySelector<HTMLAnchorElement>(
        'a.col-chip.link[href*="/catalog/scb/rams/inkjan"]',
      );
      if (!el) {
        throw new Error("inkjan chip link not yet rendered");
      }
      return el;
    });
    expect(janLink.getAttribute("href")).toBe("/catalog/scb/rams/inkjan");
  });

  // ── #678 finding 6: chip nav goes through the SPA router (no full reload) ─────
  it("clicking a member chip routes through the SPA router (preventDefault, no toggle)", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(node());
    vi.mocked(getConceptGroupGraph).mockResolvedValue(twoSingleColGraph());
    const navSpy = vi.spyOn(router, "navigate");

    renderGroup();

    const janLink = await vi.waitFor(() => {
      const el = document.querySelector<HTMLAnchorElement>(
        'a.col-chip.link[href="/catalog/scb/rams/inkjan"]',
      );
      if (!el) {
        throw new Error("inkjan chip link not yet rendered");
      }
      return el;
    });
    navSpy.mockClear();

    // A plain left click. The handler must preventDefault (so the browser does NOT
    // full-reload) AND navigate via the router — never stopPropagation (which would
    // strand the app-level use:link delegated interception and force a full reload).
    const evt = new MouseEvent("click", {
      bubbles: true,
      cancelable: true,
      button: 0,
    });
    janLink.dispatchEvent(evt);

    expect(evt.defaultPrevented).toBe(true);
    expect(navSpy).toHaveBeenCalledWith("/catalog/scb/rams/inkjan");
    // The click did NOT toggle the row's selection.
    await expect.element(page.getByText("0 columns selected")).toBeVisible();

    navSpy.mockRestore();
  });

  it("a MODIFIER (cmd) click on a member chip is left to the browser (no router nav, no preventDefault)", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(node());
    vi.mocked(getConceptGroupGraph).mockResolvedValue(twoSingleColGraph());
    const navSpy = vi.spyOn(router, "navigate");

    renderGroup();

    const janLink = await vi.waitFor(() => {
      const el = document.querySelector<HTMLAnchorElement>(
        'a.col-chip.link[href="/catalog/scb/rams/inkjan"]',
      );
      if (!el) {
        throw new Error("inkjan chip link not yet rendered");
      }
      return el;
    });
    navSpy.mockClear();

    // A modifier click is deliberately NOT prevented by the component (open-in-new-tab
    // intent → fall through to the browser). But an un-prevented click on a real <a
    // href> would actually navigate the test iframe and disconnect it (flaky CI
    // failure). A document-level bubble probe — registered AFTER Svelte's delegated
    // handler, so it observes the component's (non-)preventDefault — records whether
    // the component prevented it, then prevents the REAL navigation so the iframe
    // survives.
    let componentPrevented = true;
    const probe = (e: Event) => {
      componentPrevented = e.defaultPrevented;
      e.preventDefault();
    };
    document.addEventListener("click", probe);
    try {
      const evt = new MouseEvent("click", {
        bubbles: true,
        cancelable: true,
        button: 0,
        metaKey: true,
      });
      janLink.dispatchEvent(evt);
      // The component left the modifier click to the browser (didn't preventDefault)
      // and did NOT router-navigate.
      expect(componentPrevented).toBe(false);
      expect(navSpy).not.toHaveBeenCalled();
    } finally {
      document.removeEventListener("click", probe);
      navSpy.mockRestore();
    }
  });
});

describe("ConceptGroupView per-column facet labels (#678 finding 4)", () => {
  // A representation group with TWO members on ONE fqid, distinct delivery columns +
  // facets (the inclusive/exclusive disposable-income case): CDISP "Inkl.
  // kapitalvinst", CDISP5 "Exkl. kapitalvinst". The band is built per DISTINCT fqid,
  // so without the facet-per-column map the SECOND member's facet label is lost.
  function twoFacetMembersOneFqid(): ConceptGroupNodeData {
    return node({
      members: [
        {
          fqid: "scb/iot/dispink",
          name: "Disponibel inkomst",
          delivery_column: "CDISP",
          facets: [
            {
              axis: "kapitalvinst",
              value: "inkl",
              label: "Inkl. kapitalvinst",
            },
          ],
          coverage: null,
        },
        {
          fqid: "scb/iot/dispink",
          name: "Disponibel inkomst",
          delivery_column: "CDISP5",
          facets: [
            {
              axis: "kapitalvinst",
              value: "exkl",
              label: "Exkl. kapitalvinst",
            },
          ],
          coverage: null,
        },
      ],
    } as unknown as Partial<ConceptGroupNodeData>);
  }

  function dispinkGraph(): RelationshipGraph {
    // ONE variable node carrying BOTH delivery columns' states (the graph node spans
    // every column of the variable — the deduped-fqid band enumerates them all).
    return graph([
      vnode("scb/iot/dispink", [
        gstate({
          variant: "individer",
          delivery_column_name: "CDISP",
          valid_from: "2010-01-01",
          valid_to: "2020-12-31",
        }),
        gstate({
          variant: "individer",
          delivery_column_name: "CDISP5",
          valid_from: "2010-01-01",
          valid_to: "2020-12-31",
        }),
      ]),
    ]);
  }

  it("shows EACH column's human facet label, not just the technical column name", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(twoFacetMembersOneFqid());
    vi.mocked(getConceptGroupGraph).mockResolvedValue(dispinkGraph());

    renderGroup();

    // Both columns render as picker rows (a multi-column member → a subheading over
    // two column rows).
    await vi.waitFor(() => {
      const cols = [...document.querySelectorAll(".col-chip")].map(
        (e) => e.textContent ?? "",
      );
      if (!cols.some((c) => c.includes("CDISP5"))) {
        throw new Error("CDISP5 column not yet rendered");
      }
    });

    // The LATER member's facet label (CDISP5 → "Exkl. kapitalvinst") reaches its row —
    // the regression dropped it, leaving only the technical column name.
    await expect.element(page.getByText("Exkl. kapitalvinst")).toBeVisible();
    // The first member's facet shows too.
    await expect.element(page.getByText("Inkl. kapitalvinst")).toBeVisible();
  });
});

describe("ConceptGroupView ?member= focus highlight (#678 finding 5)", () => {
  it("marks the band the validated ?member= hint names", async () => {
    // The backend echoes the validated focus slug on `node.member`; the band keyed by
    // the member fqid whose leaf slug is that slug gets the focus marker.
    vi.mocked(getConceptGroup).mockResolvedValue(node({ member: "inkfeb" }));
    vi.mocked(getConceptGroupGraph).mockResolvedValue(twoSingleColGraph());
    router.navigate("/catalog/group/scb/rams/ink?member=inkfeb");

    renderGroup();

    const focused = await vi.waitFor(() => {
      const el = document.querySelector(".col-row.single.focused");
      if (!el) {
        throw new Error("focused band not yet rendered");
      }
      return el;
    });
    // Exactly the inkfeb band is focused (not inkjan).
    expect(document.querySelectorAll(".focused")).toHaveLength(1);
    expect(focused.textContent).toContain("Inkfeb");
  });

  it("marks nothing when there is no ?member= hint", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(node());
    vi.mocked(getConceptGroupGraph).mockResolvedValue(twoSingleColGraph());

    renderGroup();

    await vi.waitFor(() => {
      if (document.querySelectorAll(".col-row.single").length < 2) {
        throw new Error("rows not yet rendered");
      }
    });
    expect(document.querySelectorAll(".focused")).toHaveLength(0);
  });
});
