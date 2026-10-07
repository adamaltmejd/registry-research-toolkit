// Split from ConceptGroupView.browser.test.ts by contract surface: inter-variable succession fold.
// Siblings: ConceptGroupView{,.selection,.labels,.navigation,.filters,.succession}.browser.test.ts.

import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import type { ConceptGroupNodeData, RelationshipGraph } from "./api";
import { getCatalogNode, getConceptGroup, getConceptGroupGraph } from "./api";
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
    getCatalogNode: vi.fn(),
    getConceptGroup: vi.fn(),
    getConceptGroupGraph: vi.fn(),
  };
});

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
