// Split from ConceptGroupView.browser.test.ts by contract surface: definition, description and facet labels.
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

describe("ConceptGroupView (#617 + #678 compact column list)", () => {
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
