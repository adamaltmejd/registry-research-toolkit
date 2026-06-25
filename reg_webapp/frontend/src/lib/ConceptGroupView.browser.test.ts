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

  it("shows a one-sided 'until <year>' when the coverage start is unknown (#658)", async () => {
    // Latent on the live corpus, but the data model permits an unknown start —
    // null (no finite valid_from) OR the yearless sentinel `0001-01-01` — with a
    // finite end. The known end year must still render, not vanish (the old guard
    // hid the whole line), and never leak the sentinel as "0001 – 2008".
    vi.mocked(getConceptGroup).mockResolvedValue(
      node({
        members: [
          {
            fqid: "scb/rams/inkjan",
            name: "Inkomst januari",
            facets: [{ axis: "month", value: "01", label: "januari" }],
            coverage: {
              coverage_from: "0001-01-01", // yearless sentinel start
              coverage_to: "2008-12-31",
              open_ended: false,
              state_count: 1,
            },
          },
          {
            fqid: "scb/rams/inkfeb",
            name: "Inkomst februari",
            facets: [{ axis: "month", value: "02", label: "februari" }],
            coverage: {
              coverage_from: null, // null start, finite end
              coverage_to: "2010-12-31",
              open_ended: false,
              state_count: 1,
            },
          },
        ],
      } as unknown as Partial<ConceptGroupNodeData>),
    );

    await render(ConceptGroupView, {
      provider: "scb",
      register: "rams",
      key: "ink",
    });

    // Both the sentinel-start and null-start members show their finite end as a
    // one-sided window — not "" and not "0001 – 2008".
    await expect
      .element(page.getByText("until 2008", { exact: true }))
      .toBeVisible();
    await expect
      .element(page.getByText("until 2010", { exact: true }))
      .toBeVisible();
    await expect.element(page.getByText("0001 – 2008")).not.toBeInTheDocument();
  });

  it("demotes the key, facets, and source into a 'Technical details' disclosure (#638 PR4)", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(node());

    await render(ConceptGroupView, {
      provider: "scb",
      register: "rams",
      key: "ink",
    });

    // The disclosure renders with a visible "Technical details" summary, collapsed
    // by default. The demoted rows are in the DOM but NOT visible while collapsed,
    // so assert STRUCTURE (inside the disclosure), not visibility.
    await expect.element(page.getByText("Technical details")).toBeVisible();
    const disclosure = document.querySelector<HTMLDetailsElement>(
      "details.tech-details",
    );
    expect(disclosure).not.toBeNull();
    expect(disclosure?.open).toBe(false);
    // All three build-derivation rows — Group key, Facets, and Source — live
    // INSIDE the disclosure now (#638 PR4 demoted them together).
    expect(disclosure?.textContent).toContain("Group");
    expect(disclosure?.textContent).toContain("ink");
    expect(disclosure?.textContent).toContain("Facets");
    expect(disclosure?.textContent).toContain("month"); // the `node()` axis
    expect(disclosure?.textContent).toContain("Source");
    expect(disclosure?.textContent).toContain("token");
    // The group key's <code> sits inside the disclosure — not in a prominent block.
    // (Exact "ink" matches only the key, not "Inkomst"/the inkjan/inkfeb members.)
    const groupKey = page.getByText("ink", { exact: true }).element();
    expect(groupKey.closest("details.tech-details")).not.toBeNull();
    // There is NO prominent (non-disclosure) `dl.meta` left in the description.
    const promptMeta = [...document.querySelectorAll("dl.meta")].filter(
      (dl) => !dl.closest("details.tech-details"),
    );
    expect(promptMeta).toHaveLength(0);
  });

  it("renders classification groups as first-class member graph pages", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(
      node({
        provider: "class",
        register: null,
        key: "sun",
        label: "Svensk utbildningsnomenklatur (SUN)",
        source: "curated",
        axes: ["dimension"],
        members: [
          {
            fqid: "class/sun2020-inriktning",
            name: "Utbildningsinriktning",
            facets: [
              {
                axis: "dimension",
                value: "inriktning",
                label: "Inriktning",
              },
            ],
            coverage: null,
          },
          {
            fqid: "class/niva-grovv1",
            name: "Utbildningsnivå, grov",
            facets: [
              {
                axis: "dimension",
                value: "niva-grov",
                label: "Aggregat",
              },
            ],
            coverage: null,
          },
        ],
      } as unknown as Partial<ConceptGroupNodeData>),
    );

    await render(ConceptGroupView, {
      provider: "class",
      register: null,
      key: "sun",
    });

    await expect
      .element(
        page.getByRole("heading", {
          name: "Svensk utbildningsnomenklatur (SUN)",
          level: 2,
        }),
      )
      .toBeVisible();
    expect(getConceptGroup).toHaveBeenCalledWith(
      "class",
      null,
      "sun",
      undefined,
    );
    expect(document.querySelector(".period-picker")).toBeNull();
    await expect
      .element(
        page.getByRole("heading", { name: "Classification relationships" }),
      )
      .toBeVisible();
    await expect
      .element(page.getByRole("link", { name: /sun2020-inriktning/ }))
      .toHaveAttribute("href", "/catalog/class/sun2020-inriktning");
    await expect
      .element(page.getByRole("link", { name: /niva-grovv1/ }))
      .toHaveAttribute("href", "/catalog/class/niva-grovv1");
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
