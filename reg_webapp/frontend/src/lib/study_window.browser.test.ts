import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { RegisterShow } from "./api";
import {
  getDocsForVariable,
  getGraph,
  getLineage,
  getRelatedDocuments,
  getShow,
  getStates,
} from "./api";
import { registerShow } from "./api-test-helpers";
import { resetCatalogNames } from "./catalog_names.svelte";
import { delivery, variableChild } from "./catalog-node-view-test-helpers";
import ProjectEditor from "./ProjectEditor.svelte";
import type { ProjectSource, ProjectSourcePeriod } from "./project_data";
import { projectStore } from "./project_store.svelte";
import { storedProject } from "./project-store-test-helpers";
import { router } from "./router.svelte";
import { windowStore } from "./window.svelte";

// The common study window (reg_webapp/DESIGN.md → "Common study window") on the
// /project page: a window edit never rewrites a source period, a disjoint source
// blocks the order, every divergence is marked, and "apply overlap to all"
// rewrites only the sources that have one. (The catalog picker's cases went with
// the Svelte binding leaf in React port PR 3.)

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

/** A register's `show` listing `variable` delivered under `variant` from `from` to
 * `to` (null: still delivered) in `Col` — the read the overlap plan works from. */
function deliveringRegister(
  fqid: string,
  variable: string,
  variant: string,
  from: string,
  to: string | null,
): RegisterShow {
  return registerShow({
    fqid,
    variants: [],
    children: [
      variableChild(variable, variable, [delivery(variant, "Col", [from, to])]),
    ],
  });
}

/** One source of `scb/lisa/kon` at `period` — what an earlier Add left behind. */
function source(
  name: string,
  registerVariant: string,
  variable: string,
  period: ProjectSourcePeriod,
): ProjectSource {
  return {
    name,
    register_variant: registerVariant,
    period,
    bindings: [{ variable, type: "categorical" }],
  };
}

function loadDraft(
  sources: ProjectSource[],
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

function sourcePeriods(): ProjectSourcePeriod[] {
  return (storedProject()?.sources ?? []).map((s) => s.period);
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
        return deliveringRegister(
          "scb/lisa",
          "scb/lisa/kon",
          "individer",
          "2000-01-01",
          null,
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
        return deliveringRegister(
          "scb/lisa",
          "scb/lisa/kon",
          "individer",
          "2000-01-01",
          null,
        );
      }
      if (ref === "scb/rtb") {
        return deliveringRegister(
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
