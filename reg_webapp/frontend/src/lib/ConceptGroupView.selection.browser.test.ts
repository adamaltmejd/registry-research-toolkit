// Split from ConceptGroupView.browser.test.ts by contract surface: selection and staged commit.
// Siblings: ConceptGroupView{,.selection,.labels,.navigation,.filters,.succession}.browser.test.ts.

import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import type { ConceptGroupShow, RelationshipGraph } from "./api";
import { getGraph, getShow, getStates } from "./api";
import {
  graph,
  gstate,
  mockResolveColumns,
  node,
  renderGroup,
  twoSingleColGraph,
  vnode,
} from "./concept-group-view-test-helpers";
import { projectStore } from "./project_store.svelte";
import { storedProject } from "./project-store-test-helpers";
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

describe("ConceptGroupView (#617 + #678 compact column list)", () => {
  it("selecting columns across two members + Apply commits the right staged diff", async () => {
    vi.mocked(getShow).mockResolvedValue(node());
    vi.mocked(getGraph).mockResolvedValue(twoSingleColGraph());
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
    expect(storedProject()?.sources).toEqual(
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
    vi.mocked(getShow).mockResolvedValue(node());
    vi.mocked(getGraph).mockResolvedValue(openEndedGraph());
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
    expect(storedProject()?.sources).toHaveLength(0);
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
    vi.mocked(getShow).mockResolvedValue(node());
    vi.mocked(getGraph).mockResolvedValue(twoSingleColGraph());
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
    expect(storedProject()?.sources[0]).toEqual(
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
    vi.mocked(getShow).mockResolvedValue(node());
    vi.mocked(getGraph).mockResolvedValue(
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
    expect(storedProject()?.sources[0]).toEqual(
      expect.objectContaining({
        period: { from: 2000, to: 2010 },
      }),
    );
  });

  it("a per-variable select-all grabs every column of that variable", async () => {
    vi.mocked(getShow).mockResolvedValue(node());
    vi.mocked(getGraph).mockResolvedValue(twoMultiColGraph());

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
      storedProject()?.sources.flatMap((s) =>
        s.bindings.map((b) => b.variable),
      ) ?? [];
    expect(variables).toEqual(["scb/rams/inkjan", "scb/rams/inkjan"]);
  });

  it("pins a representation-grained member even when the final source period resolves a sibling column", async () => {
    vi.mocked(getShow).mockResolvedValue(
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
      } as unknown as Partial<ConceptGroupShow>),
    );
    vi.mocked(getGraph).mockResolvedValue(
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
    expect(storedProject()?.sources[0]).toEqual(
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
    vi.mocked(getShow).mockResolvedValue(
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
      } as unknown as Partial<ConceptGroupShow>),
    );
    vi.mocked(getGraph).mockResolvedValue(
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
    expect(storedProject()?.sources[0]?.bindings[0]).toEqual({
      variable: "scb/rams/solo",
      type: "numeric",
      representation: null,
    });
  });

  // ── #678 finding 1: a representation group exposes only its MEMBER columns ────
  it("a representation group exposes only its member delivery columns, not the variable's full column set", async () => {
    // The group's members address ONE variable (scb/rams/ink) but only the `IncA`
    // column; the graph node now carries BOTH `IncA` and a NON-member `IncExtra`
    // (the variable's full set). The band must restrict to the member column so the
    // non-member column is never selectable.
    vi.mocked(getShow).mockResolvedValue(
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
      } as unknown as Partial<ConceptGroupShow>),
    );
    vi.mocked(getGraph).mockResolvedValue(
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
    vi.mocked(getShow).mockResolvedValue(
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
      } as unknown as Partial<ConceptGroupShow>),
    );
    vi.mocked(getGraph).mockResolvedValue({
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
});
