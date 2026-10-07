import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { GraphState, RelationshipGraph, VariableGraphNode } from "./api";
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
  single,
  state,
  statesResponse,
} from "./binding-leaf-view-test-helpers";
import { projectStore } from "./project_store.svelte";
import { router } from "./router.svelte";
import { windowStore } from "./window.svelte";

// Split from BindingLeafView.browser.test.ts by contract surface: picker graph mounting and graph-focus member identity.
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

function gstate(over: Partial<GraphState>): GraphState {
  return {
    state_id: "1",
    period_scope: "intervals",
    variant: "v",
    variant_label: null,
    representation_run_id: 1,
    valid_from: "2000-01-01",
    valid_to: "2020-12-31",
    value_set_id: null,
    value_set_version_label: "",
    classification_slugs: [],
    delivery_column_name: null,
    ...over,
  };
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
              state_id: "2",
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

    await render(BindingLeafView, {
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
          state_id: String(i + 1),
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

    await render(BindingLeafView, {
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

  it("renders same_as-only graph context after the standalone graph removal", async () => {
    vi.mocked(getBindingGraph).mockResolvedValue({
      nodes: [
        {
          kind: "variable",
          id: "v1",
          fqid: "scb/lisa/kon",
          label: "Kön",
          group_key: null,
          group_label: null,
          definition: null,
          description: null,
          operational_definition: null,
          facets: [],
          states: [],
          same_as: [{ fqid: "scb/lisa/kon-alias", register: "lisa_old" }],
        },
      ],
      edges: [],
      focus_id: "v1",
    } as RelationshipGraph as never);

    await render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(single),
      ...SEED,
      vintageYear: 2024,
    });

    await vi.waitFor(() => {
      const graphText =
        document.querySelector(".rep-picker .graph-picker")?.textContent ?? "";
      if (!graphText.includes("also in") || !graphText.includes("lisa_old")) {
        throw new Error(`same_as graph context not rendered: ${graphText}`);
      }
    });
    expect(document.querySelector(".col-list")).toBeNull();
    const aliasLink = document.querySelector<HTMLAnchorElement>(
      '.graph-sa-chip[href="/catalog/scb/lisa/kon-alias"]',
    );
    expect(aliasLink?.textContent).toBe("lisa_old");
  });

  it("renders edge-less no-column graph runs after the standalone graph removal", async () => {
    vi.mocked(getBindingGraph).mockResolvedValue({
      nodes: [
        {
          kind: "variable",
          id: "v1",
          fqid: "scb/lisa/kon",
          label: "Kön",
          group_key: null,
          group_label: null,
          definition: null,
          description: null,
          operational_definition: null,
          facets: [],
          states: [
            gstate({
              delivery_column_name: null,
              value_set_version_label: "old coding",
              valid_from: "2000-01-01",
              valid_to: "2009-12-31",
            }),
            gstate({
              state_id: "2",
              representation_run_id: 2,
              delivery_column_name: null,
              value_set_version_label: "new coding",
              valid_from: "2010-01-01",
              valid_to: "2020-12-31",
            }),
          ],
          same_as: [],
        },
      ],
      edges: [],
      focus_id: "v1",
    } as RelationshipGraph as never);

    await render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(single),
      ...SEED,
      vintageYear: 2024,
    });

    await vi.waitFor(() => {
      const graphText =
        document.querySelector(".rep-picker .graph-picker")?.textContent ?? "";
      if (
        !graphText.includes("old coding") ||
        !graphText.includes("new coding")
      ) {
        throw new Error(`edge-less run graph not rendered: ${graphText}`);
      }
    });
    expect(
      document.querySelector(".graph-picker input[type='checkbox']"),
    ).toBeNull();
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
          state_id: "2",
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

    await render(BindingLeafView, {
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

    await render(BindingLeafView, {
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

  it("a grouped facet-less focus opened via same_as shows the CANONICAL sibling slug, not the alias", async () => {
    // Opened via a same_as alias (the leaf is
    // `.../naringsgren-storsta-agi-sni2007g`), the focus node is keyed on the
    // RESOLVED canonical target. The facet-less slug qualifier must read the focus
    // node's own (canonical) fqid so the alias page and the canonical page show the
    // SAME technical identifier (#670 Codex-P2 parity).
    vi.mocked(getBindingGraph).mockResolvedValue(
      focusGraph({ fqid: "scb/rams/inkjan", facets: [] }) as never,
    );

    await render(BindingLeafView, {
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

  it("a grouped facet-less focus with one delivery column shows original column casing", async () => {
    vi.mocked(getBindingGraph).mockResolvedValue(
      focusGraph({
        facets: [],
        states: [gstate({ delivery_column_name: "ProdGrpKod" })],
      }) as never,
    );

    await render(BindingLeafView, {
      fqidPath: groupedFqid,
      node: groupedNode,
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    const qualifier = await vi.waitFor(() => {
      const el = document.querySelector(".member-identity code.qualifier.slug");
      if (!el) {
        throw new Error("column qualifier not yet rendered");
      }
      return el;
    });
    expect(qualifier.textContent).toBe("ProdGrpKod");
  });

  it("renders no identity row while the graph is loading (no transient slug flicker)", async () => {
    vi.mocked(getBindingGraph).mockReturnValue(new Promise(() => {}));

    await render(BindingLeafView, {
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
    // A resolved focus node with no group: were it treated as grouped, the leaf
    // slug "kon" would render as the facet-less qualifier.
    vi.mocked(getBindingGraph).mockResolvedValue(
      focusGraph({
        fqid: "scb/lisa/kon",
        label: "Kön",
        group_key: null,
        group_label: null,
        facets: [],
      }) as never,
    );
    await render(BindingLeafView, {
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
    await expect
      .element(page.getByText("kon", { exact: true }))
      .not.toBeInTheDocument();
    await expect.element(page.getByText(/member of/)).not.toBeInTheDocument();
  });

  it("degrades gracefully when the graph fetch errors (header survives, no qualifier/link)", async () => {
    // The graph fetch is an independent failure domain: an error must NOT blank the
    // leaf — the header (node.name) still renders, the qualifier/link omitted.
    vi.mocked(getBindingGraph).mockRejectedValue(new Error("graph down"));

    await render(BindingLeafView, {
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
