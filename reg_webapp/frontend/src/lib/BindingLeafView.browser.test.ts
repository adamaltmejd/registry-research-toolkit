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
//   1. the add gate (#638 PR2b) — a node with ≥2 co-existing variants renders the
//      proactive selector + gates Add until picked; a single-variant node doesn't.
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
    related_to: [],
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
    facets: [],
    states: [],
    same_as: [],
    ...over,
  };
  return { nodes: [focus], edges: [], focus_id: focusId };
}

/** Two register variants co-existing over the same window — `choose-variant` when
 * no period bounds them (the gate path). */
const coexisting = [
  state({ state_id: 1, variant: "individer" }),
  state({ state_id: 2, variant: "arbetsstallen" }),
];

/** One variant → `segments` (no population choice). */
const single = [state({ state_id: 1, variant: "individer" })];

const singleWithStructural = [
  state({
    state_id: 1,
    variant: "individer",
    data_type: "char",
    data_length: "1",
    delivery_column_name: "Kon",
  }),
];

const coexistingB = [
  state({ state_id: 3, variant: "foretag" }),
  state({ state_id: 4, variant: "regioner" }),
];

const coexistingSharingVariant = [
  state({ state_id: 5, variant: "individer" }),
  state({ state_id: 6, variant: "regioner" }),
];

const coexistingWithRepChoice = [
  state({
    state_id: 1,
    variant: "individer",
    delivery_column_name: "Kon",
    value_set_version_label: "1-siffrig",
    value_set: [{ code: "1", label: "Man" }],
  }),
  state({
    state_id: 2,
    variant: "individer",
    delivery_column_name: "KonDetalj",
    value_set_version_label: "2-siffrig",
    value_set: [{ code: "01", label: "Man" }],
  }),
  state({ state_id: 3, variant: "arbetsstallen", delivery_column_name: "Sni" }),
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

describe("BindingLeafView add gate (#638 PR2b)", () => {
  it("≥2 co-existing variants render the population selector and gate Add until picked", async () => {
    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(coexisting),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      vintageYear: 2024,
    });

    await expect.element(page.getByText(/pick one to add/i)).toBeVisible();
    const individer = page.getByRole("button", { name: /individer/ });
    const arbetsstallen = page.getByRole("button", { name: /arbetsstallen/ });
    await expect.element(individer).toBeVisible();
    await expect.element(arbetsstallen).toBeVisible();

    const add = page.getByRole("button", { name: "Add to project" });
    await expect.element(add).toBeDisabled();

    await individer.click();
    await expect.element(individer).toHaveAttribute("aria-pressed", "true");
    await expect.element(add).toBeEnabled();
  });

  it("a stale pick is non-member of the new plan's options → Add re-gated until a current option is picked", async () => {
    const { rerender } = render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(coexisting),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      vintageYear: 2024,
    });

    const add = page.getByRole("button", { name: "Add to project" });

    await page.getByRole("button", { name: /individer/ }).click();
    await expect.element(add).toBeEnabled();

    await rerender({ node: node(coexistingB) });

    await expect.element(add).toBeDisabled();
    const foretag = page.getByRole("button", { name: /foretag/ });
    await expect.element(foretag).toBeVisible();

    await foretag.click();
    await expect.element(add).toBeEnabled();
  });

  it("a leaf-identity change re-gates Add even when the new plan shares the picked variant", async () => {
    const { rerender } = render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(coexisting),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      vintageYear: 2024,
    });

    const add = page.getByRole("button", { name: "Add to project" });

    await page.getByRole("button", { name: /individer/ }).click();
    await expect.element(add).toBeEnabled();

    await rerender({
      fqidPath: "scb/lisa/sysstatus",
      node: node(coexistingSharingVariant),
    });

    await expect.element(add).toBeDisabled();
    await page.getByRole("button", { name: /individer/ }).click();
    await expect.element(add).toBeEnabled();
  });

  it("switching population invalidates an in-flight rep prompt (no stale-variant commit)", async () => {
    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(coexistingWithRepChoice),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      vintageYear: 2024,
    });

    await page.getByRole("button", { name: /individer/ }).click();
    await page.getByRole("button", { name: "Add to project" }).click();

    const repChooser = page.getByRole("group", {
      name: "Pick a representation",
    });
    await expect.element(repChooser).toBeVisible();

    await page.getByRole("button", { name: /arbetsstallen/ }).click();
    await expect.element(repChooser).not.toBeInTheDocument();
  });

  it("a single-variant node shows no selector and Add is enabled", async () => {
    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(single),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      vintageYear: 2024,
    });

    const add = page.getByRole("button", { name: "Add to project" });
    await expect.element(add).toBeEnabled();
    expect(
      document.body.querySelector('[aria-label="Pick a register variant"]'),
    ).toBeNull();
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

  it("Add stays seed-gated (disabled) until the deployment seed is present", async () => {
    render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(single),
      regMetaVersion: "",
      steward: "",
      vintageYear: 2024,
    });
    await expect
      .element(page.getByRole("button", { name: "Add to project" }))
      .toBeDisabled();
  });
});

describe("BindingLeafView period-scoped value-set history (#744)", () => {
  it("uses the period subset for Add while rendering full history with outside-period collapse", async () => {
    const inA = state({
      state_id: 10,
      variant: "individer",
      value_set_id: 10,
      value_set_version_label: "In-period A",
      valid_from: "2007-01-01",
      valid_to: "2007-12-31",
    });
    const inB = state({
      state_id: 11,
      variant: "individer",
      value_set_id: 11,
      value_set_version_label: "In-period B",
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
    await expect
      .element(page.getByText(/pick one to add/i))
      .not.toBeInTheDocument();
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
      valid_from: "2007-01-01",
      valid_to: "2007-12-31",
    });
    const otherVariant = state({
      state_id: 41,
      variant: "other-population",
      value_set_id: 41,
      value_set_version_label: "Other",
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
