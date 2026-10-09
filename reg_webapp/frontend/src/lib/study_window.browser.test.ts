import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { ShowNode, VariableShow, VariableStateModel } from "./api";
import {
  getDocsForVariable,
  getGraph,
  getLineage,
  getRelatedDocuments,
  getShow,
  getStates,
} from "./api";
import BindingLeafView from "./BindingLeafView.svelte";
import { resetCatalogNames } from "./catalog_names.svelte";
import { state as baseState } from "./catalog-test-helpers";
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
    getWarnings: vi.fn().mockResolvedValue([]),
    getShow: vi.fn(),
    getStates: vi.fn(),
    getGraph: vi.fn(),
    getLineage: vi.fn(),
    getDocsForVariable: vi.fn(),
    getRelatedDocuments: vi.fn(),
  };
});

const SEED = {
  reg_meta_version: "reg_meta/v1.0.0",
  steward: "global",
} as const;

function state(over: Partial<VariableStateModel>): VariableStateModel {
  return baseState({
    variant: "individer",
    valid_from: "2010-01-01",
    valid_to: "2015-12-31",
    delivery_column_name: "Kon",
    ...over,
  });
}

/** `scb/lisa/kon`'s `show`: metadata only; its states are a separate read. */
function leaf(): VariableShow {
  return {
    kind: "variable",
    fqid: "scb/lisa/kon",
    name: "Kön",
    definition: null,
    description: null,
    measurement_unit: null,
    operational_definition: null,
    source_register_text: null,
    is_identifier: false,
    is_sensitive: false,
    deprecated: false,
    group: null,
    same_as: [],
    tags: [],
  };
}

/** A register's `show` listing one variable delivered under `variant` from
 * `from` to `to` (in `Col`) — the read the overlap plan works from. */
function registerShow(
  fqid: string,
  variable: string,
  variant: string,
  from: string,
  to: string,
): ShowNode {
  return {
    kind: "register",
    fqid,
    children: [
      {
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
    groups: [],
    tags: [],
    variants: [],
  } as unknown as ShowNode;
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
  vi.mocked(getStates).mockReset();
  // The cards' name reads find no catalog: what these cases assert is the window.
  vi.mocked(getShow).mockReset();
  vi.mocked(getShow).mockRejectedValue(new Error("offline"));
  vi.mocked(getGraph).mockResolvedValue({
    nodes: [],
    edges: [],
    focus_id: null,
  });
  vi.mocked(getLineage).mockResolvedValue({
    edges: [],
    warnings: [],
    registers: [],
  });
  vi.mocked(getDocsForVariable).mockResolvedValue({
    items: [],
    total: 0,
    register_ingested: true,
  });
  vi.mocked(getRelatedDocuments).mockResolvedValue([]);
  // Validation answers clean, and every other read the cards make answers empty:
  // what these cases assert is the window, not the catalog names.
  vi.stubGlobal(
    "fetch",
    vi.fn(async () => ({
      ok: true,
      status: 200,
      json: async () => ({ data: { ok: true, issues: [] }, meta: {} }),
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
    // The `?period` subset read answers the same history the page loaded.
    vi.mocked(getStates).mockResolvedValue(states);
    return render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: leaf(),
      states,
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

  it("blocks a page-period add that falls wholly outside the study window", async () => {
    windowStore.set({ from: 2018, to: 2024 });
    router.navigate("/catalog/scb/lisa/kon?period=1960..1970");
    await renderLeaf([
      state({ valid_from: "1952-01-01", valid_to: "2023-12-31" }),
    ]);

    await addKon();

    await expect
      .element(
        page.getByText(
          "Not added: under the selected period, Kon has no years inside the study window 2018–2024. Choose a period that overlaps the study window, or widen the study window in the rail.",
        ),
      )
      .toBeVisible();
    expect(projectStore.draft?.sources).toEqual([]);
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
    // The row's delivery is outside the window too, but the blocking marker keeps
    // full contrast: the row is not dimmed.
    const row = page.getByRole("checkbox", { name: /Kon/ }).element();
    expect(row.closest(".row-btn")?.classList.contains("dimmed")).toBe(false);
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

  it("leaves a source alone when the catalog lists no delivery for one of its columns", async () => {
    // Fails if the overlap plan stops reading the register's `show` children
    // (package C: `getShow`, variable children without a `kind`).
    vi.mocked(getShow).mockImplementation(async (ref) => {
      if (ref === "scb/lisa") {
        return registerShow(
          "scb/lisa",
          "scb/lisa/kon",
          "individer",
          "2000-01-01",
          "9999-12-31",
        );
      }
      throw new Error("offline");
    });
    loadDraft(
      [
        {
          name: "LISA",
          register_variant: "scb/lisa/individer",
          period: { from: 2010, to: 2012 },
          bindings: [
            { variable: "scb/lisa/kon", type: "categorical" },
            { variable: "scb/lisa/gone", type: "categorical" },
          ],
        },
      ],
      { from: 2015, to: 2020 },
    );
    await renderProject();

    await page.getByRole("button", { name: "Apply window overlap" }).click();

    // Kon alone would narrow the source to 2015–2020; the unread column's years
    // would be lost with it, so the source keeps its period.
    await expect
      .element(
        page.getByText(
          "LISA keeps its period: the catalog lists no delivery for one of its columns, so the overlap can't be worked out. Set the period on the source card.",
        ),
      )
      .toBeVisible();
    expect(page.getByRole("alertdialog").query()).toBeNull();
    expect(sourcePeriods()).toEqual([{ from: 2010, to: 2012 }]);
  });

  it("applies the window overlap only to the sources that have one", async () => {
    // Fails if the overlap plan stops reading the register's `show` children
    // (package C: `getShow`, variable children without a `kind`).
    vi.mocked(getShow).mockImplementation(async (ref) => {
      if (ref === "scb/lisa") {
        return registerShow(
          "scb/lisa",
          "scb/lisa/kon",
          "individer",
          "2000-01-01",
          "9999-12-31",
        );
      }
      if (ref === "scb/rtb") {
        return registerShow(
          "scb/rtb",
          "scb/rtb/kommun",
          "befolkning",
          "1990-01-01",
          "2005-12-31",
        );
      }
      throw new Error("offline");
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
    // Nothing is left for a second press to change (RTB has no overlap), so the
    // action is frozen rather than offered again.
    await expect
      .element(page.getByRole("button", { name: "Apply window overlap" }))
      .toHaveAttribute("aria-disabled", "true");
    // RTB is still disjoint, so the order stays blocked.
    await projectStore.validate();
    await expect.element(orderButton()).toBeDisabled();
  });
});
