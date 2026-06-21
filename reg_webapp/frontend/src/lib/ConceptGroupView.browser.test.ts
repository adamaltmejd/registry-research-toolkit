import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { ConceptGroupNodeData } from "./api";
import { getConceptGroup } from "./api";
import ConceptGroupView from "./ConceptGroupView.svelte";
import { router } from "./router.svelte";
import { windowStore } from "./window.svelte";

// Mock the single GET the view drives (mirrors DimensionsPanel's api-mock style);
// keep the rest of api.ts real (the type exports + router).
vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return {
    ...actual,
    getConceptGroup: vi.fn(),
  };
});

function node(
  overrides: Partial<ConceptGroupNodeData> = {},
): ConceptGroupNodeData {
  return {
    kind: "concept-group",
    provider: "scb",
    register: "rams",
    key: "ink",
    label: "Inkomst",
    source: "token",
    axes: ["month"],
    member: null,
    members: [
      {
        fqid: "scb/rams/inkjan",
        name: "Inkomst januari",
        facets: [{ axis: "month", value: "01", label: "januari" }],
        coverage: null,
      },
      {
        fqid: "scb/rams/inkfeb",
        name: "Inkomst februari",
        facets: [{ axis: "month", value: "02", label: "februari" }],
        coverage: {
          coverage_from: "2019-01-01",
          coverage_to: "2021-12-31",
          open_ended: false,
          state_count: 1,
        },
      },
    ],
    ...overrides,
  } as unknown as ConceptGroupNodeData;
}

beforeEach(() => {
  vi.mocked(getConceptGroup).mockReset();
  // Reset the URL + the global window so the availability-lens tests start clean
  // (these stores are module singletons shared across cases).
  router.navigate("/catalog/group/scb/rams/ink");
  windowStore.set(null);
});

describe("ConceptGroupView (#617)", () => {
  it("renders the group label + members with facets and coverage", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(node());

    await render(ConceptGroupView, {
      provider: "scb",
      register: "rams",
      key: "ink",
    });

    // The group's label heads the page.
    await expect
      .element(page.getByRole("heading", { name: "Inkomst", level: 2 }))
      .toBeVisible();
    // The single-axis selector labels members by their facet ("januari").
    await expect
      .element(page.getByText("januari", { exact: true }))
      .toBeVisible();
    // Members link to their leaf FQIDs.
    await expect
      .element(page.getByRole("link", { name: /februari/ }))
      .toHaveAttribute("href", "/catalog/scb/rams/inkfeb");
    // The member with coverage shows its study window (the year-collapsed span);
    // the stateless one omits it. Scoped to the member's own coverage span — the
    // PeriodPicker also renders the union coverage in its slider readout.
    await expect
      .element(page.getByText("2019 – 2021", { exact: true }))
      .toBeVisible();
  });
});

describe("ConceptGroupView member selector (#638 PR2a)", () => {
  // A 2-axis (month × rank) group → the matrix selector.
  function matrixNode(): ConceptGroupNodeData {
    return node({
      axes: ["month", "rank"],
      members: [
        {
          fqid: "scb/lisa/agi1inkjan",
          name: null,
          facets: [
            { axis: "month", value: "01", label: "januari" },
            { axis: "rank", value: "1", label: "största" },
          ],
          coverage: null,
        },
        {
          fqid: "scb/lisa/agi2inkjan",
          name: null,
          facets: [
            { axis: "month", value: "01", label: "januari" },
            { axis: "rank", value: "2", label: "näst största" },
          ],
          coverage: null,
        },
      ],
    } as unknown as Partial<ConceptGroupNodeData>);
  }

  it("renders a matrix for ≥2 facet axes with member links", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(matrixNode());

    await render(ConceptGroupView, {
      provider: "scb",
      register: "lisa",
      key: "agi-ink",
    });

    // Axis headers render (row label "januari", column labels "största" / "näst
    // största"). Exact match — "största" is a substring of "näst största".
    await expect
      .element(page.getByRole("columnheader", { name: "största", exact: true }))
      .toBeVisible();
    // A cell member links to its leaf FQID.
    await expect
      .element(page.getByRole("link", { name: "agi2inkjan" }))
      .toHaveAttribute("href", "/catalog/scb/lisa/agi2inkjan");
  });

  it("an edge-group (0-axis, shared-name) member renders its disambiguating leaf slug", async () => {
    // Edge groups have no facet axes and all members share one name (vintages of
    // the same concept), so the list-shape `label` (the name) can't tell two rows
    // apart — the leaf slug must render as a secondary code to disambiguate.
    vi.mocked(getConceptGroup).mockResolvedValue(
      node({
        axes: [],
        members: [
          {
            fqid: "scb/lisa/astsni1",
            name: "Näringsgren, största förvärvskälla",
            facets: [],
            coverage: null,
          },
          {
            fqid: "scb/lisa/astsni2",
            name: "Näringsgren, största förvärvskälla",
            facets: [],
            coverage: null,
          },
        ],
      } as unknown as Partial<ConceptGroupNodeData>),
    );

    await render(ConceptGroupView, {
      provider: "scb",
      register: "lisa",
      key: "astsni",
    });

    // Both same-named members render their distinct leaf slugs (the disambiguator).
    await expect
      .element(page.getByText("astsni1", { exact: true }))
      .toBeVisible();
    await expect
      .element(page.getByText("astsni2", { exact: true }))
      .toBeVisible();
  });

  it("greys a member not delivered across the active window", async () => {
    // A two-member group: feb covers 2019–2021, jan is stateless. With a window
    // of 2010–2012 active, feb's coverage (2019–) doesn't span it → greyed +
    // a "not delivered" note. (jan has no coverage → never greyed.)
    vi.mocked(getConceptGroup).mockResolvedValue(node());
    windowStore.set({ from: 2010, to: 2012 });

    await render(ConceptGroupView, {
      provider: "scb",
      register: "rams",
      key: "ink",
    });

    // The not-delivered note appears for the out-of-window member.
    await expect.element(page.getByText(/not delivered/)).toBeVisible();
    // The greyed member carries the `not-delivered` class on its link.
    const febLink = page.getByRole("link", { name: /februari/ });
    await expect.element(febLink).toBeVisible();
    expect(
      febLink.element().closest("a")?.classList.contains("not-delivered"),
    ).toBe(true);
  });

  it("member links carry the active `?period` into the leaf", async () => {
    // With a year `?period` active (the availability lens narrowed), each member
    // link must carry it to the leaf URL so the leaf opens at the SAME window
    // (continuity, incl. its add-to-project plan). Only `?period` rides along —
    // never the group-specific `?member` focus hint.
    vi.mocked(getConceptGroup).mockResolvedValue(node());
    router.navigate("/catalog/group/scb/rams/ink?period=2019..2020");

    await render(ConceptGroupView, {
      provider: "scb",
      register: "rams",
      key: "ink",
    });

    const febLink = page.getByRole("link", { name: /februari/ });
    await expect.element(febLink).toBeVisible();
    const href = febLink.element().closest("a")?.getAttribute("href") ?? "";
    expect(href).toContain("period=2019..2020");
    expect(href).not.toContain("member=");
  });

  it("member links carry no query when no `?period` is set", async () => {
    // The baseline: browsing the full group (no lens) → plain leaf hrefs, so the
    // leaf opens at full history.
    vi.mocked(getConceptGroup).mockResolvedValue(node());

    await render(ConceptGroupView, {
      provider: "scb",
      register: "rams",
      key: "ink",
    });

    await expect
      .element(page.getByRole("link", { name: /februari/ }))
      .toHaveAttribute("href", "/catalog/scb/rams/inkfeb");
  });

  it("a non-year `?period` suppresses greying even with a project window set", async () => {
    // An explicit non-year `?period` (e.g. `HT2020`) is authoritative: the
    // year-grain lens can't represent it, so it suppresses greying rather than
    // falling back to the project window (mirrors PeriodPicker's subAnnualPeriod
    // gap suppression). feb covers 2019–2021, which does NOT span a 2010–2012
    // window — so with the WINDOW active it would grey, but the non-year period
    // must win and leave it ungreyed.
    vi.mocked(getConceptGroup).mockResolvedValue(node());
    windowStore.set({ from: 2010, to: 2012 });
    router.navigate("/catalog/group/scb/rams/ink?period=HT2020");

    await render(ConceptGroupView, {
      provider: "scb",
      register: "rams",
      key: "ink",
    });

    // The member renders, but ungreyed (no `not-delivered` class, no note).
    const febLink = page.getByRole("link", { name: /februari/ });
    await expect.element(febLink).toBeVisible();
    expect(
      febLink.element().closest("a")?.classList.contains("not-delivered"),
    ).toBe(false);
    await expect
      .element(page.getByText(/not delivered/))
      .not.toBeInTheDocument();
  });
});
