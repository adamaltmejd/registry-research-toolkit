import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type {
  ConceptGroupNodeData,
  GraphState,
  RelationshipGraph,
  StatesResponse,
  VariableGraphNode,
  VariableStateModel,
} from "./api";
import { getCatalogNode, getConceptGroup, getConceptGroupGraph } from "./api";
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
    getCatalogNode: vi.fn(),
    getConceptGroup: vi.fn(),
    getConceptGroupGraph: vi.fn(),
  };
});

const SEED = { regMetaVersion: "1.0.0", steward: "global" } as const;

/** A minimal GraphState — only the fields `pickerRepresentations` reads. */
function gstate(over: Partial<GraphState>): GraphState {
  return {
    state_id: "1",
    period_scope: "intervals",
    representation_run_id: 1,
    variant: "v",
    variant_label: null,
    delivery_column_name: null,
    value_set_version_label: "",
    value_set_id: null,
    valid_from: "2010-01-01",
    valid_to: "2015-12-31",
    classification_slugs: [],
    ...over,
  };
}

/** A variable graph node carrying the given fqid + states — a variable's column
 * source. `definition`/`description` default null (the common parallel-column
 * sibling shape); pass them to seed the shared-concept-text dedup. */
function vnode(
  fqid: string,
  states: GraphState[],
  meta: {
    definition?: string | null;
    description?: string | null;
    operationalDefinition?: string | null;
    label?: string;
  } = {},
): VariableGraphNode {
  return {
    kind: "variable",
    id: fqid,
    fqid,
    label: meta.label ?? fqid,
    group_key: null,
    group_label: null,
    facets: [],
    states,
    same_as: [],
    definition: meta.definition ?? null,
    description: meta.description ?? null,
    operational_definition: meta.operationalDefinition ?? null,
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

function vstate(over: Partial<VariableStateModel>): VariableStateModel {
  return {
    warning_ids: [],
    state_id: "1",
    period_scope: "intervals",
    variant: "individer",
    variant_label: null,
    register_variant_id: "1",
    valid_from: "2010-01-01",
    valid_to: "2015-12-31",
    data_type: "int",
    data_length: null,
    delivery_column_name: null,
    source_register_text: null,
    provenance: null,
    pooled: false,
    value_set_version_label: "",
    value_set_id: null,
    value_set: null,
    is_identifier: false,
    classifications: [],
    ...over,
  };
}

function statesResponse(states: VariableStateModel[]): StatesResponse {
  return { states } as unknown as StatesResponse;
}

function mockResolveColumns(
  columnsByFqid: Record<string, readonly string[]>,
): void {
  vi.mocked(getCatalogNode).mockImplementation(async (fqid, params) => {
    const columns = columnsByFqid[fqid] ?? [];
    const variant =
      typeof params?.variant === "string" ? params.variant : "individer";
    return statesResponse(
      columns.map((column, index) =>
        vstate({
          state_id: String(index + 1),
          variant,
          delivery_column_name: column,
        }),
      ),
    );
  });
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

/** A one-member graph whose only column is delivered OPEN-ENDED: with no `?period`
 * and no project window a pick on it resolves no finite period (Y-58, the group
 * page's half of the leaf's failure). */
function openEndedGraph(): RelationshipGraph {
  return graph([
    vnode("scb/rams/inkjan", [
      gstate({
        variant: "individer",
        delivery_column_name: "Inkjan",
        valid_from: "2018-01-01",
        valid_to: "9999-12-31",
      }),
    ]),
  ]);
}

/** A two-member graph where each member has TWO genuinely CO-EXISTING (overlapping
 * windows) columns → each renders as a thin subheading over its parallel column rows.
 * The windows OVERLAP (both 2010–2020) on purpose: parallel columns stay co-equal rows,
 * whereas NON-overlapping columns of one variable are a sequential rename that the
 * picker now collapses (#902) — see `renameChainGraph` for that shape. */
function twoMultiColGraph(): RelationshipGraph {
  return graph([
    vnode("scb/rams/inkjan", [
      gstate({
        variant: "individer",
        delivery_column_name: "InkjanA",
        valid_from: "2010-01-01",
        valid_to: "2020-12-31",
      }),
      gstate({
        variant: "individer",
        delivery_column_name: "InkjanB",
        valid_from: "2010-01-01",
        valid_to: "2020-12-31",
      }),
    ]),
    vnode("scb/rams/inkfeb", [
      gstate({
        variant: "individer",
        delivery_column_name: "InkfebA",
        valid_from: "2010-01-01",
        valid_to: "2020-12-31",
      }),
      gstate({
        variant: "individer",
        delivery_column_name: "InkfebB",
        valid_from: "2010-01-01",
        valid_to: "2020-12-31",
      }),
    ]),
  ]);
}

beforeEach(() => {
  vi.mocked(getCatalogNode).mockReset();
  mockResolveColumns({});
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

async function renderGroup(
  props: Partial<{
    provider: string;
    register: string;
    key: string;
    windowMinYear: number;
    windowMaxYear: number;
    vintageYear: number;
    enforcePeriodBounds: boolean;
  }> = {},
) {
  return await render(ConceptGroupView, {
    provider: "scb",
    register: "rams",
    key: "ink",
    regMetaVersion: SEED.regMetaVersion,
    steward: SEED.steward,
    windowMinYear: 1960,
    vintageYear: 2024,
    ...props,
  });
}

describe("ConceptGroupView (#617 + #678 compact column list)", () => {
  it("renders single-column members as rows led by their own names", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(node());
    vi.mocked(getConceptGroupGraph).mockResolvedValue(twoSingleColGraph());

    await renderGroup();

    // The heading names the page kind and the group's label.
    await expect
      .element(
        page.getByRole("heading", {
          name: "Variable group: Inkomst",
          level: 2,
        }),
      )
      .toBeVisible();

    // The two members carry DISTINCT names, so each row leads with its own name
    // (Y-78) rather than sharing a cluster heading.
    await expect.element(page.getByText("Inkomst januari")).toBeVisible();
    await expect.element(page.getByText("Inkomst februari")).toBeVisible();

    // Each row is a selectable checkbox named by the member's delivery COLUMN.
    await expect
      .element(page.getByRole("checkbox", { name: /Inkjan/ }))
      .toBeVisible();
    await expect
      .element(page.getByRole("checkbox", { name: /Inkfeb/ }))
      .toBeVisible();
  });

  it("selecting columns across two members + Apply commits the right staged diff", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(node());
    vi.mocked(getConceptGroupGraph).mockResolvedValue(twoSingleColGraph());
    mockResolveColumns({
      "scb/rams/inkjan": ["Inkjan"],
      "scb/rams/inkfeb": ["Inkfeb"],
    });

    await renderGroup();

    // Distinct names → name-cluster headings (#901); each single-column row's checkbox
    // is named by its delivery COLUMN (the leading identity), not the repeated name.
    const jan = page.getByRole("checkbox", { name: /Inkjan/ });
    const feb = page.getByRole("checkbox", { name: /Inkfeb/ });
    await expect.element(jan).toBeVisible();
    await jan.click();
    await feb.click();

    // ONE shared footer spanning the whole list: the cross-variable count, in
    // "column" terms.
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
          register_variant: "scb/rams/individer",
          bindings: expect.arrayContaining([
            expect.objectContaining({
              variable: "scb/rams/inkjan",
              type: "numeric",
              representation: null,
            }),
            expect.objectContaining({
              variable: "scb/rams/inkfeb",
              type: "numeric",
              representation: null,
            }),
          ]),
        }),
      ]),
    );
  });

  // Y-58, the same failure as the binding leaf's: a group reached without a
  // `?period` narrows nothing, so a pick on an open-ended column has no finite
  // period to author and must be refused rather than written as `period: ""`.
  it("refuses a pick that resolves no finite period, leaving the draft unchanged", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(node());
    vi.mocked(getConceptGroupGraph).mockResolvedValue(openEndedGraph());
    mockResolveColumns({ "scb/rams/inkjan": ["Inkjan"] });

    await renderGroup();

    const jan = page.getByRole("checkbox", { name: /Inkjan/ });
    await expect.element(jan).toBeVisible();
    await jan.click();
    await page.getByRole("button", { name: "Add to project" }).click();

    // Y-77: the refusal names BOTH ways out — the rail's study window and this
    // page's Apply — the same shared string the leaf renders.
    await expect
      .element(
        page.getByText(
          "Apply a period before adding — set the study window in the rail, or press Apply under Period above, then select and add again.",
        ),
      )
      .toBeVisible();
    expect(projectStore.draft?.sources).toHaveLength(0);
    await expect.element(page.getByText(/^Applied/)).not.toBeInTheDocument();

    // Recoverable the same two ways as the leaf's: resolving the group retires the
    // notice, and so does clearing the staging it refused.
    router.navigate("/catalog/group/scb/rams/ink?period=2018");
    await expect
      .element(page.getByText(/Apply a period before adding/))
      .not.toBeInTheDocument();
  });

  // #678 finding 3: an active ?period is HONORED on add (the committed source carries
  // the user's narrowed window, not the row's full span).
  it("commits the row span INTERSECTED with the active ?period, not the full span", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(node());
    vi.mocked(getConceptGroupGraph).mockResolvedValue(twoSingleColGraph());
    mockResolveColumns({ "scb/rams/inkjan": ["Inkjan"] });
    // inkjan spans 2010–2015; narrow the group to 2012..2014.
    router.navigate("/catalog/group/scb/rams/ink?period=2012..2014");

    await renderGroup();

    // The single-column row's checkbox is named by its delivery COLUMN (#901).
    const jan = page.getByRole("checkbox", { name: /Inkjan/ });
    await expect.element(jan).toBeVisible();
    await jan.click();
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
            variable: "scb/rams/inkjan",
            type: "numeric",
            representation: null,
          }),
        ],
      }),
    );
  });

  it("clamps a stale group ?period to steward bounds before staged add (#1037)", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(node());
    vi.mocked(getConceptGroupGraph).mockResolvedValue(
      graph([
        vnode("scb/rams/inkjan", [
          gstate({
            variant: "individer",
            delivery_column_name: "Inkjan",
            valid_from: "1996-01-01",
            valid_to: "2026-12-31",
          }),
        ]),
      ]),
    );
    mockResolveColumns({ "scb/rams/inkjan": ["Inkjan"] });
    router.navigate("/catalog/group/scb/rams/ink?period=1960..2026");

    await renderGroup({
      windowMinYear: 2000,
      windowMaxYear: 2010,
      enforcePeriodBounds: true,
    });

    const jan = page.getByRole("checkbox", { name: /Inkjan/ });
    await expect.element(jan).toBeVisible();
    await jan.click();
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

  it("a per-variable select-all grabs every column of that variable", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(node());
    vi.mocked(getConceptGroupGraph).mockResolvedValue(twoMultiColGraph());

    await renderGroup();

    // The inkjan subheading's select-all toggle selects BOTH its columns at once.
    // Members have distinct NAMES here and neither repeats → no cluster headings
    // (Y-78), so each subheading leads with its own variable name and the aria label
    // is keyed on that name.
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
    await expect.element(page.getByText("+2 columns")).toBeVisible();
    await expect
      .element(page.getByRole("checkbox", { name: /InkjanA/ }))
      .toBeChecked();
    await expect
      .element(page.getByRole("checkbox", { name: /InkfebA/ }))
      .not.toBeChecked();

    await page
      .getByRole("button", {
        name: /Add to project|Remove from project|Apply changes/,
      })
      .click();
    await expect.element(page.getByText(/\+2 columns/)).toBeVisible();
    const variables =
      projectStore.draft?.sources.flatMap((s) =>
        s.bindings.map((b) => b.variable),
      ) ?? [];
    expect(variables).toEqual(["scb/rams/inkjan", "scb/rams/inkjan"]);
  });

  it("pins a representation-grained member even when the final source period resolves a sibling column", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(
      node({
        key: "disp",
        label: "Disponibel inkomst",
        source: "curated",
        axes: [{ name: "kapitalvinst", label: "Kapitalvinst" }],
        members: [
          {
            fqid: "scb/rams/dispink",
            name: "Disponibel inkomst",
            delivery_column: "CDISP",
            facets: [
              {
                axis: "kapitalvinst",
                value: "incl",
                label: "Inkl. kapitalvinst",
              },
            ],
            coverage: null,
          },
          {
            fqid: "scb/rams/dispink",
            name: "Disponibel inkomst",
            delivery_column: "CDISP5",
            facets: [
              {
                axis: "kapitalvinst",
                value: "excl",
                label: "Exkl. kapitalvinst",
              },
            ],
            coverage: null,
          },
        ],
      } as unknown as Partial<ConceptGroupNodeData>),
    );
    vi.mocked(getConceptGroupGraph).mockResolvedValue(
      graph([
        vnode("scb/rams/dispink", [
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
      ]),
    );
    // Regression for #838: resolving the final source period sees only the sibling
    // CDISP. The stored binding must still name the clicked CDISP5 representation so
    // validation reports the coverage gap instead of extracting CDISP.
    mockResolveColumns({ "scb/rams/dispink": ["CDISP"] });

    await renderGroup({ key: "disp" });

    await page.getByRole("checkbox", { name: /CDISP5/ }).click();
    await page
      .getByRole("button", {
        name: /Add to project|Remove from project|Apply changes/,
      })
      .click();

    await expect.element(page.getByText(/\+1 column/)).toBeVisible();
    expect(projectStore.draft?.sources[0]).toEqual(
      expect.objectContaining({
        register_variant: "scb/rams/individer",
        // Exact binding object: a pick writes the resolved type + the pinned
        // representation and no `display_name` (Y-76).
        bindings: [
          {
            variable: "scb/rams/dispink",
            type: "numeric",
            representation: "CDISP5",
          },
        ],
      }),
    );
  });

  it("keeps a lone delivery-column group member on the null representation convention", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(
      node({
        key: "solo-rep",
        label: "Solo representation",
        source: "curated",
        axes: [{ name: "rep", label: "Representation" }],
        members: [
          {
            fqid: "scb/rams/solo",
            name: "Solo representation",
            delivery_column: "SOLO",
            facets: [{ axis: "rep", value: "solo", label: "Solo" }],
            coverage: null,
          },
        ],
      } as unknown as Partial<ConceptGroupNodeData>),
    );
    vi.mocked(getConceptGroupGraph).mockResolvedValue(
      graph([
        vnode("scb/rams/solo", [
          gstate({
            variant: "individer",
            delivery_column_name: "SOLO",
            valid_from: "2010-01-01",
            valid_to: "2020-12-31",
          }),
        ]),
      ]),
    );
    mockResolveColumns({ "scb/rams/solo": ["SOLO"] });

    await renderGroup({ key: "solo-rep" });

    await page.getByRole("checkbox", { name: /SOLO/ }).click();
    await page
      .getByRole("button", {
        name: /Add to project|Remove from project|Apply changes/,
      })
      .click();

    await expect.element(page.getByText(/\+1 column/)).toBeVisible();
    expect(projectStore.draft?.sources[0]?.bindings[0]).toEqual({
      variable: "scb/rams/solo",
      type: "numeric",
      representation: null,
    });
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

    await renderGroup();

    // inkjan's single-column row checkbox is named by its delivery COLUMN (#901),
    // its name leading the row.
    await expect
      .element(page.getByRole("checkbox", { name: /Inkjan/ }))
      .toBeVisible();
    // Both names are distinct and neither repeats → no cluster headings (Y-78).
    expect(document.querySelectorAll(".cluster-head")).toHaveLength(0);
    // inkfeb is not dropped: it renders its (graph-node-less) band with the quiet
    // "No columns" marker and no checkbox.
    await expect
      .element(page.getByText("No columns", { exact: true }))
      .toBeVisible();
  });

  it("renders alias-only representation members as not-delivered disabled rows (#840)", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(
      node({
        key: "disprep",
        label: "Disponibel inkomst",
        source: "curated",
        axes: [{ name: "rep", label: "Representation" }],
        members: [
          {
            fqid: "scb/rams/disp",
            name: "Disponibel inkomst",
            delivery_column: "CDISP",
            facets: [{ axis: "rep", value: "incl", label: "Inkl." }],
            coverage: {
              coverage_from: "1968-01-01",
              coverage_to: "2024-12-31",
              open_ended: false,
              state_count: 1,
            },
          },
          {
            fqid: "scb/rams/disp",
            name: "Disponibel inkomst",
            delivery_column: "CDISP5",
            facets: [{ axis: "rep", value: "excl", label: "Exkl." }],
            coverage: {
              coverage_from: null,
              coverage_to: null,
              open_ended: false,
              state_count: 0,
            },
          },
        ],
      }),
    );
    vi.mocked(getConceptGroupGraph).mockResolvedValue(
      graph([
        vnode("scb/rams/disp", [
          gstate({
            variant: "individer",
            delivery_column_name: "CDISP",
            valid_from: "1968-01-01",
            valid_to: "2024-12-31",
          }),
        ]),
      ]),
    );

    await renderGroup({ key: "disprep" });

    const delivered = page.getByRole("checkbox", { name: /^CDISP(?!5)/ });
    const aliasOnly = page.getByRole("checkbox", { name: /CDISP5/ });
    await expect.element(delivered).toBeVisible();
    await expect.element(delivered).toBeEnabled();
    await expect.element(aliasOnly).toBeVisible();
    await expect.element(aliasOnly).toBeDisabled();
    await expect
      .element(page.getByText("not delivered", { exact: true }))
      .toBeVisible();
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

    await renderGroup();

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

    await renderGroup();

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
          // Two CO-EXISTING (overlapping) columns → a genuine multi-column subheading.
          // A non-overlapping pair would collapse to one rename row (#902).
          gstate({
            variant: "individer",
            delivery_column_name: "Sni92",
            valid_from: "2002-01-01",
            valid_to: "2015-12-31",
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

    await renderGroup({
      provider: "scb",
      register: "moms",
      key: "naringsgren",
    });

    // Ng0 / Ng1 are single-column rows led by their column, which is the link to
    // the member's leaf page; the constant concept name is not repeated on them.
    await expect
      .element(page.getByRole("link", { name: /^Ng0/ }))
      .toHaveAttribute("href", "/catalog/scb/moms/naringsgren_ng0");
    await expect
      .element(page.getByRole("link", { name: /^Ng1/ }))
      .toHaveAttribute("href", "/catalog/scb/moms/naringsgren_ng1");

    // The sni member (2 columns) is a subheading led by its slug, which links to its
    // leaf; its columns are selectable rows below it.
    await expect
      .element(page.getByRole("link", { name: /^naringsgren_sni/ }))
      .toHaveAttribute("href", "/catalog/scb/moms/naringsgren_sni");
    await expect
      .element(page.getByRole("checkbox", { name: /Sni92/ }))
      .toBeVisible();
    await expect
      .element(page.getByRole("checkbox", { name: /Sni2007/ }))
      .toBeVisible();
    // A nested column is not its own variable, so only the identity chip navigates.
    expect(
      page.getByRole("link", { name: /Sni92|Sni2007/ }).elements(),
    ).toEqual([]);
  });

  // ── Name-cluster de-duplication (#901) ──────────────────────────────────────
  it("a HETEROGENEOUS group shows ONE heading per distinct name, bands led by column", async () => {
    // The #901 disponibel-inkomst shape: several members share each of two distinct
    // concept names. Instead of leading every band with the (repeated) name, cluster
    // by name → render each name ONCE as a group heading, and beneath it each band
    // leads with its distinguishing delivery column.
    vi.mocked(getConceptGroup).mockResolvedValue(
      node({
        provider: "scb",
        register: "iot",
        key: "disponibel-inkomst",
        label: "Disponibel inkomst",
        axes: [],
        members: [
          {
            fqid: "scb/iot/dispink_cdisphb",
            name: "Disponibel inkomst",
            facets: [],
            coverage: null,
          },
          {
            fqid: "scb/iot/dispink_dinf",
            name: "Disponibel inkomst, familj",
            facets: [],
            coverage: null,
          },
          {
            fqid: "scb/iot/dispink_cdisp04hb",
            name: "Disponibel inkomst",
            facets: [],
            coverage: null,
          },
        ],
      } as unknown as Partial<ConceptGroupNodeData>),
    );
    vi.mocked(getConceptGroupGraph).mockResolvedValue(
      graph([
        vnode("scb/iot/dispink_cdisphb", [
          gstate({ variant: "individer", delivery_column_name: "CDISPHB" }),
        ]),
        vnode("scb/iot/dispink_dinf", [
          gstate({ variant: "familj", delivery_column_name: "DINF" }),
        ]),
        vnode("scb/iot/dispink_cdisp04hb", [
          gstate({ variant: "individer", delivery_column_name: "CDISP04HB" }),
        ]),
      ]),
    );

    await renderGroup({
      provider: "scb",
      register: "iot",
      key: "disponibel-inkomst",
    });

    // ONE heading per distinct name (first-seen order), the repeated "Disponibel
    // inkomst" collapsed to a single heading.
    const headings = await vi.waitFor(() => {
      const els = document.querySelectorAll(".cluster-head h3");
      if (els.length < 2) {
        throw new Error("cluster headings not yet rendered");
      }
      return [...els].map((h) => h.textContent?.trim());
    });
    expect(headings).toEqual([
      "Disponibel inkomst",
      "Disponibel inkomst, familj",
    ]);

    // Each band leads with its delivery COLUMN chip (the name is hoisted to the
    // heading), so the columns are the visible row identities + checkbox names.
    const chips = [
      ...document.querySelectorAll(".col-row.single .col-chip"),
    ].map((e) => e.firstChild?.textContent?.trim());
    expect(chips).toEqual(["CDISPHB", "CDISP04HB", "DINF"]);
    await expect
      .element(page.getByRole("checkbox", { name: /CDISPHB/ }))
      .toBeVisible();
  });

  it("a HOMOGENEOUS group (all one name) shows NO cluster heading", async () => {
    // The moms/naringsgren shape: every member shares the name → ONE cluster, so no
    // heading is rendered (the name is already the page title) and bands lead with
    // their column — exactly today's behavior, unchanged.
    vi.mocked(getConceptGroup).mockResolvedValue(
      node({
        provider: "scb",
        register: "moms",
        key: "naringsgren",
        label: "Näringsgren",
        axes: [],
        members: [
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
        ],
      } as unknown as Partial<ConceptGroupNodeData>),
    );
    vi.mocked(getConceptGroupGraph).mockResolvedValue(
      graph([
        vnode("scb/moms/naringsgren_ng0", [
          gstate({ variant: "individer", delivery_column_name: "Ng0" }),
        ]),
        vnode("scb/moms/naringsgren_ng1", [
          gstate({ variant: "individer", delivery_column_name: "Ng1" }),
        ]),
      ]),
    );

    await renderGroup({
      provider: "scb",
      register: "moms",
      key: "naringsgren",
    });

    // Bands render, led by their columns; NO cluster heading at all.
    await expect
      .element(page.getByRole("checkbox", { name: /Ng0/ }))
      .toBeVisible();
    expect(document.querySelectorAll(".cluster-head")).toHaveLength(0);
  });

  it("a facet group of single-column members leads each row with its FACET label, once", async () => {
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

    await renderGroup({
      provider: "scb",
      register: "forskoleklass",
      key: "utbildning",
    });

    // The facet leads each row's accessible name. #901: apart from its axis marker
    // ("Skolform: <facet>") the facet is not echoed again in the row's context line.
    for (const [facet, column] of [
      ["specialskola", "UtbSpec"],
      ["grundskola", "UtbGrund"],
    ]) {
      await expect
        .element(
          page.getByRole("checkbox", {
            name: new RegExp(`^${facet}\\s*${column}`),
          }),
        )
        .toBeVisible();
      const echoed = page.getByRole("checkbox", {
        name: new RegExp(`${facet}.*(?<!: )${facet}`),
      });
      expect(echoed.elements()).toEqual([]);
    }
  });

  // ── Member → leaf navigation (#678) ─────────────────────────────────────────
  it("a single-column member's COLUMN CHIP is the leaf-navigation link (no separate 'View' link)", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(node());
    vi.mocked(getConceptGroupGraph).mockResolvedValue(twoSingleColGraph());

    await renderGroup();

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

    await renderGroup();

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

  // #900: when members carry MULTIPLE distinct non-empty definitions/descriptions they
  // DISAGREE — that per-member text must NOT be rendered at the group level (it would
  // misrepresent member text as concept text). The whole shared block is dropped; the
  // per-member text remains reachable on each member's leaf page.
  it("renders NO group-level def/desc when members carry MULTIPLE distinct values (#900)", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(node());
    // The two members carry DIFFERENT definitions AND descriptions — the heterogeneous
    // curated-group shape (#900: disponibel-inkomst's ~14 near-duplicate per-member
    // rows). Members disagree → no single shared value → render nothing.
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
            definition: "Disposable income, January variant.",
            description: "Member-specific January description.",
          },
        ),
        vnode(
          "scb/rams/inkfeb",
          [
            gstate({
              variant: "individer",
              delivery_column_name: "Inkfeb",
              valid_from: "2018-01-01",
              valid_to: "2020-12-31",
            }),
          ],
          {
            definition: "Disposable income, February variant.",
            description: "Member-specific February description.",
          },
        ),
      ]),
    );

    await renderGroup();

    // The page renders (picker rows present), but there is NO group-level shared block.
    await expect
      .element(page.getByRole("checkbox", { name: /Inkjan/ }))
      .toBeVisible();
    const sharedMeta = [...document.querySelectorAll("dl.meta")].filter(
      (dl) => !dl.closest("details.tech-details"),
    );
    expect(sharedMeta).toHaveLength(0);
    // Neither member's divergent text leaked to the group header.
    expect(document.body.textContent).not.toContain(
      "Disposable income, January variant.",
    );
    expect(document.body.textContent).not.toContain(
      "Member-specific February description.",
    );
  });

  // ── Operational definition per member (#892/#932) ────────────────────────────
  // The consumer half of #892: where the shared def/desc (#900) is SUPPRESSED when
  // members disagree, the operational_definition is the OPPOSITE — it is precisely the
  // per-member DISTINGUISHING text, so it renders PER BAND even (especially) when the
  // members differ. This is what lets a researcher tell parallel siblings apart
  // (fordonsreg näringsgren: owner / previous-owner / 2nd-previous-owner).
  it("renders each member's operational_definition per band so parallel siblings are distinguishable (#892)", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(node());
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
          { operationalDefinition: "Owner at year end." },
        ),
        vnode(
          "scb/rams/inkfeb",
          [
            gstate({
              variant: "individer",
              delivery_column_name: "Inkfeb",
              valid_from: "2018-01-01",
              valid_to: "2020-12-31",
            }),
          ],
          {
            operationalDefinition: "Previous owner before the latest transfer.",
          },
        ),
      ]),
    );

    await renderGroup();

    // BOTH members' distinct op-def text renders inline on their own band — NOT
    // deduped away (it's the distinguishing text, not shared concept text).
    await expect
      .element(page.getByText("Owner at year end.", { exact: true }))
      .toBeVisible();
    await expect
      .element(
        page.getByText("Previous owner before the latest transfer.", {
          exact: true,
        }),
      )
      .toBeVisible();
    // It is NOT promoted to the group-level shared-meta block (members disagree).
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
            state_id: "2",
            representation_run_id: 2,
            variant: "individer",
            delivery_column_name: "IncExtra",
            valid_from: "2010-01-01",
            valid_to: "2015-12-31",
          }),
        ]),
      ]),
    );

    await renderGroup();

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

  it("list fallback renders a disabled row for a member with no graph state", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(
      node({
        members: [
          {
            fqid: "scb/rams/empty",
            name: "Empty member",
            delivery_column: "EmptyCol",
            facets: [{ axis: "month", value: "00", label: "empty" }],
            coverage: {
              state_count: 0,
              coverage_from: null,
              coverage_to: null,
              open_ended: false,
            },
          },
          {
            fqid: "scb/rams/live",
            name: "Live member",
            delivery_column: "LiveCol",
            facets: [{ axis: "month", value: "01", label: "live" }],
            coverage: null,
          },
        ],
      } as unknown as Partial<ConceptGroupNodeData>),
    );
    vi.mocked(getConceptGroupGraph).mockResolvedValue({
      nodes: [
        vnode("scb/rams/empty", []),
        vnode("scb/rams/live", [
          gstate({
            variant: "individer",
            delivery_column_name: "LiveCol",
            valid_from: "2010-01-01",
            valid_to: "2020-12-31",
          }),
        ]),
      ],
      edges: [],
      focus_id: null,
    });

    await renderGroup();

    expect(document.querySelector(".graph-picker")).toBeNull();
    const emptyRow = page.getByRole("checkbox", { name: /EmptyCol/ });
    await expect.element(emptyRow).toBeVisible();
    await expect.element(emptyRow).toBeDisabled();
    await expect
      .element(page.getByText("not delivered", { exact: true }))
      .toBeVisible();
    await expect
      .element(page.getByRole("checkbox", { name: /LiveCol/ }))
      .toBeVisible();
  });

  // ── #678 finding 5: the member link carries the active group ?period ─────────
  it("a member nav link carries the active group ?period", async () => {
    router.navigate("/catalog/group/scb/rams/ink?period=2018..2020");
    vi.mocked(getConceptGroup).mockResolvedValue(node());
    vi.mocked(getConceptGroupGraph).mockResolvedValue(twoSingleColGraph());

    await renderGroup();

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

  // ── #678 finding 6: chip nav goes through the SPA router (no full reload) ─────
  it("a plain chip click routes in-app without toggling; a modifier click is left to the browser", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(node());
    vi.mocked(getConceptGroupGraph).mockResolvedValue(twoSingleColGraph());

    await renderGroup();

    const janLink = page.getByRole("link", { name: /^Inkjan/ });
    await expect
      .element(janLink)
      .toHaveAttribute("href", "/catalog/scb/rams/inkjan");
    const groupPath = location.pathname;

    // A modifier click is NOT prevented by the component (open-in-new-tab intent).
    // An un-prevented click on a real <a href> would navigate the test iframe, so a
    // document-level probe — registered AFTER Svelte's delegated handler — records
    // whether the component prevented it, then prevents the real navigation.
    let componentPrevented = true;
    const probe = (e: Event) => {
      componentPrevented = e.defaultPrevented;
      e.preventDefault();
    };
    document.addEventListener("click", probe);
    try {
      janLink.element().dispatchEvent(
        new MouseEvent("click", {
          bubbles: true,
          cancelable: true,
          button: 0,
          metaKey: true,
        }),
      );
    } finally {
      document.removeEventListener("click", probe);
    }
    expect(componentPrevented).toBe(false);
    expect(location.pathname).toBe(groupPath);

    // A plain left click is prevented (no full reload) and routed in-app, and it does
    // not toggle the row's selection.
    const plain = new MouseEvent("click", {
      bubbles: true,
      cancelable: true,
      button: 0,
    });
    janLink.element().dispatchEvent(plain);
    expect(plain.defaultPrevented).toBe(true);
    expect(location.pathname).toBe("/catalog/scb/rams/inkjan");
    await expect
      .element(page.getByRole("checkbox", { name: /Inkjan/ }))
      .not.toBeChecked();
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

    await renderGroup();

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

describe("ConceptGroupView picker dimension filters (#908/#931)", () => {
  function dimensionNode(
    overrides: Partial<ConceptGroupNodeData> = {},
  ): ConceptGroupNodeData {
    return node({
      key: "dimensioned",
      label: "Dimensioned group",
      axes: [{ name: "level", label: "Level" }],
      members: [
        {
          fqid: "scb/rams/old",
          name: "Old",
          facets: [{ axis: "level", value: "old", label: "Old level" }],
          coverage: null,
        },
        {
          fqid: "scb/rams/new",
          name: "New",
          facets: [{ axis: "level", value: "new", label: "New level" }],
          coverage: null,
        },
      ],
      ...overrides,
    } as unknown as Partial<ConceptGroupNodeData>);
  }

  function dimensionGraph(): RelationshipGraph {
    return graph([
      vnode("scb/rams/old", [
        gstate({
          variant: "individer",
          variant_label: "Individer",
          delivery_column_name: "OLD",
          value_set_version_label: "SNI 2002",
        }),
      ]),
      vnode("scb/rams/new", [
        gstate({
          variant: "familj",
          variant_label: "Familj",
          delivery_column_name: "NEW",
          value_set_version_label: "SNI 2007",
        }),
      ]),
    ]);
  }

  async function filterLegends(): Promise<string[]> {
    await vi.waitFor(() => {
      if (document.querySelectorAll(".dim-filters fieldset").length === 0) {
        throw new Error("dimension filters not yet rendered");
      }
    });
    return [...document.querySelectorAll(".dim-filters fieldset legend")].map(
      (el) => el.textContent?.trim() ?? "",
    );
  }

  it("keeps row-level Variant/Coding filters on non-LISA concept groups", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(dimensionNode());
    vi.mocked(getConceptGroupGraph).mockResolvedValue(dimensionGraph());

    await renderGroup({
      provider: "scb",
      register: "rams",
      key: "dimensioned",
    });

    expect(await filterLegends()).toEqual(["Level", "Variant", "Coding"]);
  });

  /** The `person-orgnr` shape: no curated axes at all. */
  function axisLessNode(
    overrides: Partial<ConceptGroupNodeData> = {},
  ): ConceptGroupNodeData {
    return dimensionNode({
      axes: [],
      members: [
        { fqid: "scb/rams/old", name: "Old", facets: [], coverage: null },
        { fqid: "scb/rams/new", name: "New", facets: [], coverage: null },
      ],
      ...overrides,
    });
  }

  it("keeps them on a CURATED group with no axes either (Y-78)", async () => {
    // Suppression is for curated groups whose declared axes ARE the browse facets;
    // with no axes declared there is nothing authoritative to defer to.
    vi.mocked(getConceptGroup).mockResolvedValue(
      axisLessNode({ source: "curated" }),
    );
    vi.mocked(getConceptGroupGraph).mockResolvedValue(dimensionGraph());

    await renderGroup({
      provider: "scb",
      register: "rams",
      key: "dimensioned",
    });

    expect(await filterLegends()).toEqual(["Variant", "Coding"]);
  });

  it("shows only declared axes on curated group pages", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(
      dimensionNode({
        source: "curated",
      }),
    );
    vi.mocked(getConceptGroupGraph).mockResolvedValue(dimensionGraph());

    await renderGroup({
      provider: "scb",
      register: "rams",
      key: "dimensioned",
    });

    expect(await filterLegends()).toEqual(["Level"]);
  });
});

describe("ConceptGroupView ?member= focus highlight (#678 finding 5)", () => {
  it("marks the band the validated ?member= hint names", async () => {
    // The backend echoes the validated focus slug on `node.member`; the band keyed by
    // the member fqid whose leaf slug is that slug gets the focus marker.
    vi.mocked(getConceptGroup).mockResolvedValue(node({ member: "inkfeb" }));
    vi.mocked(getConceptGroupGraph).mockResolvedValue(twoSingleColGraph());
    router.navigate("/catalog/group/scb/rams/ink?member=inkfeb");

    await renderGroup();

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

  it("keeps a focused successor navigable after succession folds to list mode", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(node({ member: "inkfeb" }));
    vi.mocked(getConceptGroupGraph).mockResolvedValue({
      ...twoSingleColGraph(),
      edges: [
        {
          id: "succession:scb/rams/inkjan->scb/rams/inkfeb",
          kind: "succession",
          source: "scb/rams/inkjan",
          target: "scb/rams/inkfeb",
          label: null,
          effective_year: 2018,
        },
      ],
    });
    router.navigate("/catalog/group/scb/rams/ink?member=inkfeb");

    await renderGroup();

    const focusedLink = await vi.waitFor(() => {
      const el = document.querySelector<HTMLAnchorElement>(
        '.col-row.single.focused a.col-chip[href="/catalog/scb/rams/inkfeb"]',
      );
      if (!el) {
        throw new Error("focused member link not yet rendered");
      }
      return el;
    });
    expect(focusedLink.getAttribute("href")).toBe("/catalog/scb/rams/inkfeb");
    expect(document.querySelector(".graph-picker")).toBeNull();
  });
});

describe("ConceptGroupView inter-variable succession fold (#902)", () => {
  /** A two-member group whose members are a succession PAIR: predecessor `old` →
   * successor `new` (effective 2005). Both are members, so the fold collapses them to
   * ONE band (led by `new`) with `old` as history. */
  function successionNode(): ConceptGroupNodeData {
    return node({
      key: "disponibel-inkomst",
      label: "Disponibel inkomst",
      axes: [],
      members: [
        {
          fqid: "scb/iot/dispink-old",
          name: "Disponibel inkomst familj",
          facets: [],
          coverage: null,
        },
        {
          fqid: "scb/iot/dispink-new",
          name: "Disponibel inkomst familj 2004",
          facets: [],
          coverage: null,
        },
      ],
    } as unknown as Partial<ConceptGroupNodeData>);
  }

  /** The graph: a node per member + a succession edge old→new (predecessor→successor,
   * effective 2005). */
  function successionGraph(): RelationshipGraph {
    return {
      nodes: [
        vnode(
          "scb/iot/dispink-old",
          [
            gstate({
              variant: "familj",
              delivery_column_name: "DINFold",
              valid_from: "1999-01-01",
              valid_to: "2004-12-31",
            }),
          ],
          { label: "Disponibel inkomst familj" },
        ),
        vnode(
          "scb/iot/dispink-new",
          [
            gstate({
              variant: "familj",
              delivery_column_name: "DINFnew",
              valid_from: "2005-01-01",
              valid_to: "2020-12-31",
            }),
          ],
          { label: "Disponibel inkomst familj 2004" },
        ),
      ],
      edges: [
        {
          id: "succession:scb/iot/dispink-old->scb/iot/dispink-new",
          kind: "succession",
          source: "scb/iot/dispink-old",
          target: "scb/iot/dispink-new",
          label: null,
          effective_year: 2005,
        },
      ],
      focus_id: null,
    };
  }

  it("keeps a focused superseded predecessor visible in faceted succession groups", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(
      node({
        key: "faceted-succession",
        label: "Faceted succession",
        axes: [{ name: "level", label: "Level" }],
        member: "dispink-old",
        members: [
          {
            fqid: "scb/iot/dispink-old",
            name: "Disponibel inkomst familj",
            facets: [{ axis: "level", value: "old", label: "Old level" }],
            coverage: null,
          },
          {
            fqid: "scb/iot/dispink-new",
            name: "Disponibel inkomst familj 2004",
            facets: [{ axis: "level", value: "new", label: "New level" }],
            coverage: null,
          },
        ],
      } as unknown as Partial<ConceptGroupNodeData>),
    );
    vi.mocked(getConceptGroupGraph).mockResolvedValue(successionGraph());
    router.navigate(
      "/catalog/group/scb/iot/faceted-succession?member=dispink-old",
    );

    await renderGroup({
      provider: "scb",
      register: "iot",
      key: "faceted-succession",
    });

    const focused = await vi.waitFor(() => {
      const el = document.querySelector(
        ".graph-lane.focused, .col-row.single.focused, .subhead.focused",
      );
      if (!el) {
        throw new Error("focused predecessor not rendered");
      }
      return el;
    });
    expect(focused.textContent).toContain("DINFold");
    expect(document.querySelector("details.history")).toBeNull();
  });

  it("folds a predecessor→successor member pair into ONE band led by the LATEST edition", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(successionNode());
    vi.mocked(getConceptGroupGraph).mockResolvedValue(successionGraph());
    router.navigate("/catalog/group/scb/iot/disponibel-inkomst");

    await renderGroup({
      provider: "scb",
      register: "iot",
      key: "disponibel-inkomst",
    });

    // The successor's column is selectable as a co-equal row…
    await expect
      .element(page.getByRole("checkbox", { name: /DINFnew/ }))
      .toBeVisible();
    // …but the superseded predecessor's column is NOT a co-equal top-level row.
    const topLevelColumns = [
      ...document.querySelectorAll(".col-list .row-btn .col-chip"),
    ].map((el) => el.textContent?.replace("↗", "").trim());
    expect(topLevelColumns).toEqual(["DINFnew"]);
    // Graph mode is strictly additive now; folded predecessor cells fall back to the
    // list, with the predecessor reachable through the history disclosure.
    expect(document.querySelector(".graph-picker")).toBeNull();
    expect(
      document.querySelectorAll(".col-list .col-row .row-btn input.cbox"),
    ).toHaveLength(1);
    const historyText =
      document.querySelector("details.history")?.textContent ?? "";
    expect(historyText).toContain("supersedes 1");
    expect(historyText).toContain("edition");
    expect(document.querySelector(".history-rows")?.textContent).toContain(
      "DINFold",
    );
    expect(
      document.querySelector<HTMLAnchorElement>(
        'a.history-link[href="/catalog/scb/iot/dispink-old"]',
      )?.textContent,
    ).toContain("Disponibel inkomst familj");
    expect(document.querySelector(".history-until")?.textContent).toContain(
      "2005",
    );
  });

  it("allows a folded predecessor row to be selected for its era (#926)", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(successionNode());
    vi.mocked(getConceptGroupGraph).mockResolvedValue(successionGraph());
    mockResolveColumns({
      "scb/iot/dispink-old": ["DINFold"],
      "scb/iot/dispink-new": ["DINFnew"],
    });
    router.navigate("/catalog/group/scb/iot/disponibel-inkomst");

    await renderGroup({
      provider: "scb",
      register: "iot",
      key: "disponibel-inkomst",
    });

    await page.getByText("supersedes 1 edition").click();
    await page.getByRole("checkbox", { name: /DINFold/ }).click();
    await expect.element(page.getByText("+1 column")).toBeVisible();
    await page.getByRole("button", { name: "Add to project" }).click();

    await expect.element(page.getByText(/\+1 column/)).toBeVisible();
    expect(projectStore.draft?.sources).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          register_variant: "scb/iot/familj",
          period: { from: 1999, to: 2004 },
          bindings: [
            expect.objectContaining({
              variable: "scb/iot/dispink-old",
              representation: null,
            }),
          ],
        }),
      ]),
    );
  });

  it("keeps faceted succession predecessors selectable from folded history", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(
      node({
        key: "faceted-succession",
        label: "Faceted succession",
        axes: [{ name: "level", label: "Level" }],
        members: [
          {
            fqid: "scb/iot/dispink-old",
            name: "Disponibel inkomst familj",
            facets: [{ axis: "level", value: "old", label: "Old level" }],
            coverage: null,
          },
          {
            fqid: "scb/iot/dispink-new",
            name: "Disponibel inkomst familj 2004",
            facets: [{ axis: "level", value: "new", label: "New level" }],
            coverage: null,
          },
        ],
      } as unknown as Partial<ConceptGroupNodeData>),
    );
    vi.mocked(getConceptGroupGraph).mockResolvedValue(successionGraph());
    router.navigate("/catalog/group/scb/iot/faceted-succession");

    await renderGroup({
      provider: "scb",
      register: "iot",
      key: "faceted-succession",
    });

    await expect
      .element(page.getByRole("checkbox", { name: /DINFnew/ }))
      .toBeVisible();
    expect(document.querySelector(".graph-picker")).toBeNull();
    const topLevelColumns = [
      ...document.querySelectorAll(".col-list .row-btn .col-chip"),
    ].map((el) => el.textContent?.replace("↗", "").trim());
    expect(topLevelColumns).toEqual(["DINFnew"]);

    await page.getByText("supersedes 1 edition").click();
    const predecessor = page.getByRole("checkbox", { name: /DINFold/ });
    await expect.element(predecessor).toBeVisible();
    await predecessor.click();
    await expect.element(page.getByText("+1 column")).toBeVisible();
  });

  it("preserves ?period on folded predecessor history links", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(successionNode());
    vi.mocked(getConceptGroupGraph).mockResolvedValue(successionGraph());
    router.navigate(
      "/catalog/group/scb/iot/disponibel-inkomst?period=2010..2012",
    );

    await renderGroup({
      provider: "scb",
      register: "iot",
      key: "disponibel-inkomst",
    });

    const link = await vi.waitFor(() => {
      const el = document.querySelector<HTMLAnchorElement>(
        'a.history-link[href="/catalog/scb/iot/dispink-old?period=2010..2012"]',
      );
      if (!el) {
        throw new Error("period-carrying predecessor link not yet rendered");
      }
      return el;
    });
    expect(link.getAttribute("href")).toBe(
      "/catalog/scb/iot/dispink-old?period=2010..2012",
    );
  });

  it("does NOT fold when the successor is OUTSIDE the group (partial chain)", async () => {
    // The edge's target is not a group member → the predecessor stays a normal band
    // (only pairs with BOTH endpoints in the group fold).
    vi.mocked(getConceptGroup).mockResolvedValue(
      node({
        key: "g",
        label: "G",
        axes: [],
        members: [
          {
            fqid: "scb/iot/dispink-old",
            name: "Old",
            facets: [],
            coverage: null,
          },
        ],
      } as unknown as Partial<ConceptGroupNodeData>),
    );
    vi.mocked(getConceptGroupGraph).mockResolvedValue({
      nodes: [
        vnode("scb/iot/dispink-old", [
          gstate({
            variant: "familj",
            delivery_column_name: "DINFold",
            valid_from: "1999-01-01",
            valid_to: "2004-12-31",
          }),
        ]),
      ],
      edges: [
        {
          id: "succession:scb/iot/dispink-old->scb/iot/dispink-out",
          kind: "succession",
          source: "scb/iot/dispink-old",
          target: "scb/iot/dispink-out", // NOT a member
          label: null,
          effective_year: 2005,
        },
      ],
      focus_id: null,
    });
    router.navigate("/catalog/group/scb/iot/g");

    await renderGroup({ provider: "scb", register: "iot", key: "g" });

    // The predecessor still renders its own selectable row (not folded away), and there
    // is no history disclosure.
    await expect
      .element(page.getByRole("checkbox", { name: /DINFold/ }))
      .toBeVisible();
    expect(document.querySelector("details.history")).toBeNull();
  });

  it("folds a transitive chain A→B→C into ONE band led by C, history oldest-first", async () => {
    // A→B (effective 2000), B→C (effective 2010). All three are members, so A and B
    // are both superseded and fold away; only C remains as a band, carrying both as
    // history (oldest-first [A, B]).
    vi.mocked(getConceptGroup).mockResolvedValue(
      node({
        key: "disponibel-inkomst",
        label: "Disponibel inkomst",
        axes: [],
        members: [
          {
            fqid: "scb/iot/dispink-a",
            name: "Disponibel inkomst A",
            facets: [],
            coverage: null,
          },
          {
            fqid: "scb/iot/dispink-b",
            name: "Disponibel inkomst B",
            facets: [],
            coverage: null,
          },
          {
            fqid: "scb/iot/dispink-c",
            name: "Disponibel inkomst C",
            facets: [],
            coverage: null,
          },
        ],
      } as unknown as Partial<ConceptGroupNodeData>),
    );
    vi.mocked(getConceptGroupGraph).mockResolvedValue({
      nodes: [
        vnode(
          "scb/iot/dispink-a",
          [
            gstate({
              variant: "familj",
              delivery_column_name: "DINA",
              valid_from: "1995-01-01",
              valid_to: "1999-12-31",
            }),
          ],
          { label: "Disponibel inkomst A" },
        ),
        vnode(
          "scb/iot/dispink-b",
          [
            gstate({
              variant: "familj",
              delivery_column_name: "DINB",
              valid_from: "2000-01-01",
              valid_to: "2009-12-31",
            }),
          ],
          { label: "Disponibel inkomst B" },
        ),
        vnode(
          "scb/iot/dispink-c",
          [
            gstate({
              variant: "familj",
              delivery_column_name: "DINC",
              valid_from: "2010-01-01",
              valid_to: "2020-12-31",
            }),
          ],
          { label: "Disponibel inkomst C" },
        ),
      ],
      edges: [
        {
          id: "succession:scb/iot/dispink-a->scb/iot/dispink-b",
          kind: "succession",
          source: "scb/iot/dispink-a",
          target: "scb/iot/dispink-b",
          label: null,
          effective_year: 2000,
        },
        {
          id: "succession:scb/iot/dispink-b->scb/iot/dispink-c",
          kind: "succession",
          source: "scb/iot/dispink-b",
          target: "scb/iot/dispink-c",
          label: null,
          effective_year: 2010,
        },
      ],
      focus_id: null,
    });
    router.navigate("/catalog/group/scb/iot/disponibel-inkomst");

    await renderGroup({
      provider: "scb",
      register: "iot",
      key: "disponibel-inkomst",
    });

    // Exactly ONE selectable list row remains — the chain head C (A and B folded away).
    await expect
      .element(page.getByRole("checkbox", { name: /DINC/ }))
      .toBeVisible();
    expect(document.querySelector(".graph-picker")).toBeNull();
    expect(
      document.querySelectorAll(".col-list .col-row .row-btn input.cbox"),
    ).toHaveLength(1);
    expect(document.querySelector('input[aria-label*="DINA"]')).toBeNull();
    expect(document.querySelector('input[aria-label*="DINB"]')).toBeNull();

    // The history disclosure surfaces BOTH predecessors transitively, oldest-first.
    const links = await vi.waitFor(() => {
      const found = ["dispink-a", "dispink-b"].map((slug) =>
        document.querySelector<HTMLAnchorElement>(
          `a.history-link[href="/catalog/scb/iot/${slug}"]`,
        ),
      );
      if (found.some((link) => link === null)) {
        throw new Error("predecessor history links not yet rendered");
      }
      return found as HTMLAnchorElement[];
    });
    expect(links.map((a) => a.getAttribute("href"))).toEqual([
      "/catalog/scb/iot/dispink-a",
      "/catalog/scb/iot/dispink-b",
    ]);
  });

  it("omits the 'until <year>' marker when effective_year is null", async () => {
    // A null effective_year (the edge carries no supersession year) must not render a
    // ".history-until" element — the `{#if … != null}` guard suppresses "until null".
    vi.mocked(getConceptGroup).mockResolvedValue(successionNode());
    const g = successionGraph();
    g.edges[0].effective_year = null;
    vi.mocked(getConceptGroupGraph).mockResolvedValue(g);
    router.navigate("/catalog/group/scb/iot/disponibel-inkomst");

    await renderGroup({
      provider: "scb",
      register: "iot",
      key: "disponibel-inkomst",
    });

    // The predecessor still renders as list history…
    const link = await vi.waitFor(() => {
      const el = document.querySelector<HTMLAnchorElement>(
        'a.history-link[href="/catalog/scb/iot/dispink-old"]',
      );
      if (!el) {
        throw new Error("history link not yet rendered");
      }
      return el;
    });
    expect(link.getAttribute("href")).toBe("/catalog/scb/iot/dispink-old");
    // …but with NO year label when the edge carries no effective year.
    expect(document.querySelector(".history-until")).toBeNull();
  });
});
