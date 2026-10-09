// Split from ConceptGroupView.browser.test.ts by contract surface: picker dimension filters.
// Siblings: ConceptGroupView{,.selection,.labels,.navigation,.filters,.succession}.browser.test.ts.

import { beforeEach, describe, expect, it, vi } from "vitest";
import type { ConceptGroupShow, RelationshipGraph } from "./api";
import { getGraph, getShow, getStates } from "./api";
import {
  graph,
  gstate,
  mockResolveColumns,
  node,
  renderGroup,
  vnode,
} from "./concept-group-view-test-helpers";
import { projectStore } from "./project_store.svelte";
import { router } from "./router.svelte";
import { windowStore } from "./window.svelte";

vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return {
    ...actual,
    getStates: vi.fn(),
    getShow: vi.fn(),
    getGraph: vi.fn(),
  };
});

beforeEach(() => {
  vi.mocked(getStates).mockReset();
  mockResolveColumns({});
  vi.mocked(getShow).mockReset();
  vi.mocked(getGraph).mockReset();
  // Default: an empty graph (overridden per case).
  vi.mocked(getGraph).mockResolvedValue(graph([]));
  router.navigate("/catalog/group/scb/rams/ink");
  windowStore.set(null);
  projectStore.newProject({
    reg_meta_version: "reg_meta/v1.0.0",
    steward: "global",
  });
});

describe("ConceptGroupView picker dimension filters (#908/#931)", () => {
  function dimensionNode(
    overrides: Partial<ConceptGroupShow> = {},
  ): ConceptGroupShow {
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
    } as unknown as Partial<ConceptGroupShow>);
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
    vi.mocked(getShow).mockResolvedValue(dimensionNode());
    vi.mocked(getGraph).mockResolvedValue(dimensionGraph());

    await renderGroup({
      provider: "scb",
      register: "rams",
      key: "dimensioned",
    });

    expect(await filterLegends()).toEqual(["Level", "Variant", "Coding"]);
  });

  /** The `person-orgnr` shape: no curated axes at all. */
  function axisLessNode(
    overrides: Partial<ConceptGroupShow> = {},
  ): ConceptGroupShow {
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
    vi.mocked(getShow).mockResolvedValue(axisLessNode({ source: "curated" }));
    vi.mocked(getGraph).mockResolvedValue(dimensionGraph());

    await renderGroup({
      provider: "scb",
      register: "rams",
      key: "dimensioned",
    });

    expect(await filterLegends()).toEqual(["Variant", "Coding"]);
  });

  it("shows only declared axes on curated group pages", async () => {
    vi.mocked(getShow).mockResolvedValue(
      dimensionNode({
        source: "curated",
      }),
    );
    vi.mocked(getGraph).mockResolvedValue(dimensionGraph());

    await renderGroup({
      provider: "scb",
      register: "rams",
      key: "dimensioned",
    });

    expect(await filterLegends()).toEqual(["Level"]);
  });
});
