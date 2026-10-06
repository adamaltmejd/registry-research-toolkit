import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type {
  BindingNodeData,
  StatesResponse,
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
import { resetCatalogNames } from "./catalog_names.svelte";
import ProjectEditor from "./ProjectEditor.svelte";
import type { Period, Source } from "./project_data";
import { projectStore } from "./project_store.svelte";
import { router } from "./router.svelte";
import { windowStore } from "./window.svelte";

// The common study window (reg_webapp/DESIGN.md → "Common study window"), at the
// two surfaces the decision names: the catalog picker (an add persists the full
// available intersection, or is refused when there is none; committed columns are
// marked against the window) and the /project page (a window edit never rewrites a
// source period, a disjoint source blocks the order, every divergence is marked,
// and "apply overlap to all" rewrites only the sources that have one).

vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return {
    ...actual,
    getDataWarnings: vi.fn().mockResolvedValue([]),
    getCatalogNode: vi.fn(),
    getBindingGraph: vi.fn(),
    getBindingLineageWarnings: vi.fn(),
    getDocsForVariable: vi.fn(),
    getRelatedDocuments: vi.fn(),
  };
});

const SEED = {
  reg_meta_version: "reg_meta/v1.0.0",
  steward: "global",
} as const;

function state(over: Partial<VariableStateModel>): VariableStateModel {
  return {
    warning_ids: [],
    state_id: "1",
    period_scope: "intervals",
    variant: "individer",
    variant_label: null,
    register_variant_id: "1",
    valid_from: "2010-01-01",
    valid_to: "2015-12-31",
    data_type: null,
    data_length: null,
    delivery_column_name: "Kon",
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

function leaf(states: VariableStateModel[]): BindingNodeData {
  return {
    kind: "binding",
    fqid: "scb/lisa/kon",
    name: "Kön",
    definition: null,
    description: null,
    measurement_unit: null,
    is_identifier: false,
    is_sensitive: false,
    register_id: "1",
    variable_id: "1",
    source_register_id: null,
    source_register_text: null,
    states,
    same_as: [],
    lineage: [],
    succession_chain: [],
    via_same_as: null,
  } as unknown as BindingNodeData;
}

/** `Kon` delivered in two eras with a gap: 2005–2008, then 2012–2020. */
const interrupted = [
  state({ state_id: "1", valid_from: "2005-01-01", valid_to: "2008-12-31" }),
  state({ state_id: "2", valid_from: "2012-01-01", valid_to: "2020-12-31" }),
];

/** One source of `scb/lisa/kon` at `period` — what an earlier Add left behind. */
function source(
  name: string,
  registerVariant: string,
  variable: string,
  period: Period,
): Source {
  return {
    name,
    register_variant: registerVariant,
    period,
    bindings: [{ variable, type: "categorical" }],
  };
}

function loadDraft(
  sources: Source[],
  window: { from: number; to: number },
): void {
  projectStore.loadProject({
    schema_version: "3.0.0",
    ...SEED,
    name: "Study",
    sources,
    window,
  });
}

function sourcePeriods(): Period[] {
  return (projectStore.draft?.sources ?? []).map((s) => s.period);
}

beforeEach(() => {
  resetCatalogNames();
  vi.mocked(getCatalogNode).mockReset();
  vi.mocked(getBindingGraph).mockResolvedValue({
    nodes: [],
    edges: [],
    focus_id: null,
  } as never);
  vi.mocked(getBindingLineageWarnings).mockResolvedValue({
    binding: "scb/lisa/kon",
    lineage_warnings: [],
  } as never);
  vi.mocked(getDocsForVariable).mockResolvedValue({
    results: [],
    total_count: 0,
  } as never);
  vi.mocked(getRelatedDocuments).mockResolvedValue({
    kind: "related-documents",
    ingested: true,
    register: "lisa",
    documents: [],
  });
  // Validation answers clean, and every other read the cards make answers empty:
  // what these cases assert is the window, not the catalog names.
  vi.stubGlobal(
    "fetch",
    vi.fn(async () => ({
      ok: true,
      status: 200,
      json: async () => ({ ok: true, issues: [] }),
    })),
  );
  window.history.pushState({}, "", "/__reset__");
  router.navigate("/catalog/scb/lisa/kon");
  windowStore.set(null);
  projectStore.newProject(SEED);
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("common study window — catalog picker", () => {
  function renderLeaf(states: VariableStateModel[]) {
    vi.mocked(getCatalogNode).mockResolvedValue({
      states,
    } as unknown as StatesResponse);
    return render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: leaf(states),
      regMetaVersion: SEED.reg_meta_version,
      steward: SEED.steward,
      windowMinYear: 1960,
      vintageYear: 2024,
    });
  }

  async function addKon(): Promise<void> {
    await page.getByRole("checkbox", { name: /Kon/ }).click();
    await page.getByRole("button", { name: "Add to project" }).click();
  }

  it("persists the full available intersection on add, keeping every disjoint era", async () => {
    windowStore.set({ from: 2006, to: 2015 });
    await renderLeaf(interrupted);

    await addKon();

    await expect.element(page.getByText(/Applied \+1 column/)).toBeVisible();
    expect(sourcePeriods()).toEqual([
      [
        { from: 2006, to: 2008 },
        { from: 2012, to: 2015 },
      ],
    ]);
  });

  it("blocks an add with no overlap and explains it in the picker", async () => {
    windowStore.set({ from: 2017, to: 2020 });
    await renderLeaf([state({})]);

    await addKon();

    await expect
      .element(
        page.getByText(
          "Not added: Kon has no years inside the study window 2017–2020. Untick it, or widen the study window in the rail.",
        ),
      )
      .toBeVisible();
    expect(projectStore.draft?.sources).toEqual([]);
  });

  it("marks a committed column whose source period differs from the window", async () => {
    loadDraft(
      [
        source("LISA", "scb/lisa/individer", "scb/lisa/kon", {
          from: 2010,
          to: 2012,
        }),
      ],
      { from: 2010, to: 2015 },
    );
    await renderLeaf([state({})]);

    await expect
      .element(
        page.getByRole("checkbox", {
          name: /In project, years differ \(source period 2010–2012; study window 2010–2015\)/,
        }),
      )
      .toBeChecked();
  });

  it("marks a committed column the window has moved off as outside it", async () => {
    loadDraft(
      [
        source("LISA", "scb/lisa/individer", "scb/lisa/kon", {
          from: 2010,
          to: 2012,
        }),
      ],
      { from: 2017, to: 2020 },
    );
    await renderLeaf([state({})]);

    await expect
      .element(page.getByText("In project, outside study window"))
      .toBeVisible();
  });
});

describe("common study window — project page", () => {
  function renderProject() {
    return render(ProjectEditor, {
      regMetaVersion: "1.0.0",
      steward: "global",
      providerQualified: false,
    });
  }

  const orderButton = () =>
    page.getByRole("button", { name: "Download order.json" });

  it("keeps a source's period when a window edit leaves it disjoint, and blocks the order", async () => {
    loadDraft(
      [
        source("LISA", "scb/lisa/individer", "scb/lisa/kon", {
          from: 2001,
          to: 2005,
        }),
      ],
      { from: 2001, to: 2005 },
    );
    await renderProject();
    await projectStore.validate();
    await expect.element(orderButton()).toBeEnabled();

    windowStore.set({ from: 2015, to: 2020 });
    await projectStore.validate();

    expect(sourcePeriods()).toEqual([{ from: 2001, to: 2005 }]);
    await expect
      .element(page.getByText(/No years inside the study window 2015–2020/))
      .toBeVisible();
    await expect
      .element(
        page.getByRole("group", {
          name: "Blocking the order: outside the study window (1)",
        }),
      )
      .toBeVisible();
    await expect.element(orderButton()).toBeDisabled();
  });

  it("marks every divergent source and summarises them against the window", async () => {
    loadDraft(
      [
        source("LISA", "scb/lisa/individer", "scb/lisa/kon", {
          from: 2015,
          to: 2020,
        }),
        source("RTB", "scb/rtb/befolkning", "scb/rtb/kommun", {
          from: 2016,
          to: 2018,
        }),
        source("MIDAS", "scb/midas/individer", "scb/midas/kon", 2001),
      ],
      { from: 2015, to: 2020 },
    );
    await renderProject();

    await expect
      .element(
        page.getByText(
          "1 source differs from it; 1 source has no years inside it.",
        ),
      )
      .toBeVisible();
    // The matching source carries no mark; the differing one and the disjoint one do.
    expect(
      page.getByText("Differs from study window 2015–2020").elements(),
    ).toHaveLength(1);
    await expect
      .element(page.getByText(/No years inside the study window 2015–2020/))
      .toBeVisible();
  });

  it("applies the window overlap only to the sources that have one", async () => {
    vi.mocked(getCatalogNode).mockImplementation(async (fqid) => {
      const deliveries: Record<string, [string, string, string, string]> = {
        "scb/lisa": ["scb/lisa/kon", "individer", "2000-01-01", "9999-12-31"],
        "scb/rtb": ["scb/rtb/kommun", "befolkning", "1990-01-01", "2005-12-31"],
      };
      const [variable, variant, from, to] = deliveries[fqid];
      return {
        kind: "register",
        fqid,
        children: [
          {
            kind: "binding",
            fqid: variable,
            deliveries: [
              {
                column: "Col",
                variant,
                period_scope: "intervals",
                windows: [{ valid_from: from, valid_to: to }],
              },
            ],
          },
        ],
      } as never;
    });
    loadDraft(
      [
        source("LISA", "scb/lisa/individer", "scb/lisa/kon", {
          from: 2010,
          to: 2012,
        }),
        source("RTB", "scb/rtb/befolkning", "scb/rtb/kommun", {
          from: 2001,
          to: 2003,
        }),
      ],
      { from: 2015, to: 2020 },
    );
    await renderProject();

    await page.getByRole("button", { name: "Apply window overlap" }).click();
    const dialog = page.getByRole("alertdialog", {
      name: "Apply window overlap to 1 source?",
    });
    await expect.element(dialog).toBeVisible();
    await dialog.getByRole("button", { name: "Apply window overlap" }).click();

    await expect
      .element(
        page.getByText(
          "Applied the window overlap to 1 source. RTB keeps its period: its columns are not delivered in any year of 2015–2020. Remove the source or widen the study window.",
        ),
      )
      .toBeVisible();
    expect(sourcePeriods()).toEqual([
      { from: 2015, to: 2020 },
      { from: 2001, to: 2003 },
    ]);
    // RTB is still disjoint, so the order stays blocked.
    await projectStore.validate();
    await expect.element(orderButton()).toBeDisabled();
  });
});
