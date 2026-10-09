import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import type { ConceptGroupShow } from "./api";
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
import { router } from "./router.svelte";
import { windowStore } from "./window.svelte";

// The group page (#678) drives TWO catalog GETs: `getShow` (members +
// facets) and `getGraph` (the union graph carrying each member's
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

describe("ConceptGroupView (#617 + #678 compact column list)", () => {
  it("renders single-column members as rows led by their own names", async () => {
    vi.mocked(getShow).mockResolvedValue(node());
    vi.mocked(getGraph).mockResolvedValue(twoSingleColGraph());

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

  it("a member with no graph node renders a quiet 'No columns' subheading, not dropped", async () => {
    vi.mocked(getShow).mockResolvedValue(node());
    // Only inkjan has a graph node (single column → a row); inkfeb is absent (0
    // columns → a subheading with the empty marker).
    vi.mocked(getGraph).mockResolvedValue(
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
    vi.mocked(getShow).mockResolvedValue(
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
    vi.mocked(getGraph).mockResolvedValue(
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
    vi.mocked(getShow).mockResolvedValue(node());
    vi.mocked(getGraph).mockResolvedValue(
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
    vi.mocked(getShow).mockResolvedValue(node());
    // inkjan's only column is 2010–2015 — entirely AFTER the 1980..2004 window. Its
    // start (2010) is > the window start (1980), but the row is fully out → dimmed,
    // and the warning is suppressed (it's for IN-window rows only).
    vi.mocked(getGraph).mockResolvedValue(
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
    vi.mocked(getShow).mockResolvedValue(
      node({
        register: "scb/moms",
        key: "naringsgren",
        label: "Näringsgren",
        axes: [],
        members,
      } as unknown as Partial<ConceptGroupShow>),
    );
    vi.mocked(getGraph).mockResolvedValue(
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
    vi.mocked(getShow).mockResolvedValue(
      node({
        register: "scb/iot",
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
      } as unknown as Partial<ConceptGroupShow>),
    );
    vi.mocked(getGraph).mockResolvedValue(
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
    vi.mocked(getShow).mockResolvedValue(
      node({
        register: "scb/moms",
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
      } as unknown as Partial<ConceptGroupShow>),
    );
    vi.mocked(getGraph).mockResolvedValue(
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
    vi.mocked(getShow).mockResolvedValue(
      node({
        register: "scb/forskoleklass",
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
      } as unknown as Partial<ConceptGroupShow>),
    );
    vi.mocked(getGraph).mockResolvedValue(
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
});
