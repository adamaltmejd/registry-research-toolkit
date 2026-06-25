import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { ClassificationGroupNodeData, ConceptGroupNodeData } from "./api";
import {
  getClassificationGroup,
  getClassificationGroupGraph,
  getConceptGroup,
  getConceptGroupGraph,
} from "./api";
import ConceptGroupView from "./ConceptGroupView.svelte";
import { router } from "./router.svelte";
import { windowStore } from "./window.svelte";

// Mock the single GET the view drives (mirrors DimensionsPanel's api-mock style);
// keep the rest of api.ts real (the type exports + router).
vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return {
    ...actual,
    getClassificationGroup: vi.fn(),
    getClassificationGroupGraph: vi.fn(),
    getConceptGroup: vi.fn(),
    getConceptGroupGraph: vi.fn(),
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
  vi.mocked(getClassificationGroup).mockReset();
  vi.mocked(getClassificationGroupGraph).mockReset();
  vi.mocked(getClassificationGroupGraph).mockResolvedValue({
    focus_id: null,
    nodes: [],
    edges: [],
  });
  vi.mocked(getConceptGroup).mockReset();
  vi.mocked(getConceptGroupGraph).mockReset();
  vi.mocked(getConceptGroupGraph).mockResolvedValue({
    focus_id: null,
    nodes: [],
    edges: [],
  });
  // Reset the URL + the global window so the availability-lens tests start clean
  // (these stores are module singletons shared across cases).
  router.navigate("/catalog/group/scb/rams/ink");
  windowStore.set(null);
});

function memberSelector(): HTMLElement {
  const selector = document.querySelector<HTMLElement>(".member-selector");
  expect(selector).not.toBeNull();
  return selector as HTMLElement;
}

function memberLink(name: RegExp): HTMLAnchorElement {
  const link = [
    ...memberSelector().querySelectorAll<HTMLAnchorElement>("a"),
  ].find((item) => name.test(item.textContent ?? ""));
  expect(link).toBeDefined();
  return link as HTMLAnchorElement;
}

describe("ConceptGroupView (#617)", () => {
  it("renders the group label + members with facets and coverage", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(node());
    vi.mocked(getConceptGroupGraph).mockResolvedValue({
      focus_id: null,
      nodes: [
        {
          kind: "variable",
          id: "scb/rams/inkjan",
          fqid: "scb/rams/inkjan",
          label: "januari",
          group_key: "scb/rams/ink",
          states: [
            {
              state_id: 1,
              variant: "_default",
              representation_run_id: 0,
              delivery_column_name: "INKJAN",
              value_set_id: null,
              value_set_version_label: "",
              classification_slug: null,
              valid_from: "2019-01-01",
              valid_to: "2021-12-31",
            },
          ],
          same_as: [],
        },
        {
          kind: "variable",
          id: "scb/rams/inkfeb",
          fqid: "scb/rams/inkfeb",
          label: "februari",
          group_key: "scb/rams/ink",
          states: [
            {
              state_id: 2,
              variant: "_default",
              representation_run_id: 0,
              delivery_column_name: "INKFEB",
              value_set_id: null,
              value_set_version_label: "",
              classification_slug: null,
              valid_from: "2019-01-01",
              valid_to: "2021-12-31",
            },
          ],
          same_as: [],
        },
      ],
      edges: [],
    });

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
      .element(page.getByRole("heading", { name: "Members" }))
      .toBeVisible();
    expect(memberSelector().textContent).toContain("januari");
    // Members link to their leaf FQIDs.
    expect(memberLink(/februari/).getAttribute("href")).toBe(
      "/catalog/scb/rams/inkfeb",
    );
    // The member with coverage shows its study window (the year-collapsed span);
    // the stateless one omits it. Scoped to the member's own coverage span — the
    // PeriodPicker also renders the union coverage in its slider readout.
    await expect
      .element(page.getByText("2019 – 2021", { exact: true }))
      .toBeVisible();
    await expect
      .element(page.getByRole("heading", { name: "Relations" }))
      .toBeVisible();
    expect(
      [...document.querySelectorAll(".history-graph .node-label.in-bar")].map(
        (item) => item.textContent?.trim(),
      ),
    ).toEqual(["INKJAN", "INKFEB"]);
    expect(document.querySelector(".history-graph .axis")).not.toBeNull();
    expect(document.querySelectorAll(".history-graph .detail")).toHaveLength(0);
    expect(document.querySelector(".history-graph .group")).toBeNull();
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

    const trigger = page.getByRole("button", { name: "Technical details" });
    await expect.element(trigger).toBeVisible();
    await expect.element(trigger).toHaveAttribute("aria-expanded", "false");

    await trigger.click();
    await expect.element(trigger).toHaveAttribute("aria-expanded", "true");
    const disclosure = document.querySelector<HTMLElement>(".tech-details");
    expect(disclosure).not.toBeNull();
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
    expect(groupKey.closest(".tech-details")).not.toBeNull();
    // There is NO prominent (non-disclosure) `dl.meta` left in the description.
    const promptMeta = [...document.querySelectorAll("dl.meta")].filter(
      (dl) => !dl.closest(".tech-details"),
    );
    expect(promptMeta).toHaveLength(0);
  });

  it("renders classification groups as first-class member graph pages", async () => {
    vi.mocked(getClassificationGroup).mockResolvedValue({
      kind: "classification-group",
      key: "sun",
      label: "Svensk utbildningsnomenklatur (SUN)",
      source: "curated",
      axes: ["dimension"],
      members: [
        {
          fqid: "class/sun2020-inriktning",
          name: "Utbildningsinriktning",
          facets: [
            { axis: "dimension", value: "inriktning", label: "Inriktning" },
          ],
        },
        {
          fqid: "class/niva-grovv1",
          name: "Utbildningsnivå, grov",
          facets: [
            { axis: "dimension", value: "niva-grov", label: "Aggregat" },
          ],
        },
      ],
    } as unknown as ClassificationGroupNodeData);
    vi.mocked(getClassificationGroupGraph).mockResolvedValue({
      focus_id: null,
      nodes: [
        {
          kind: "classification",
          id: "class/sun1996",
          fqid: "class/sun1996",
          label: "SUN1996",
          group_key: null,
          version_year: 1996,
          is_current: false,
        },
        {
          kind: "classification",
          id: "class/sun2000-inriktning",
          fqid: "class/sun2000-inriktning",
          label: "SUN2000-INRIKTNING",
          group_key: null,
          version_year: 2000,
          is_current: false,
        },
        {
          kind: "classification",
          id: "class/sun2020-inriktning",
          fqid: "class/sun2020-inriktning",
          label: "SUN2020-INRIKTNING",
          group_key: "class/sun",
          version_year: 2020,
          is_current: true,
        },
        {
          kind: "classification",
          id: "class/niva-grovv1",
          fqid: "class/niva-grovv1",
          label: "NIVA-GROV",
          group_key: "class/sun",
          version_year: 2020,
          is_current: true,
        },
      ],
      edges: [
        {
          id: "succession:class/sun1996->class/sun2000-inriktning",
          kind: "succession",
          source: "class/sun1996",
          target: "class/sun2000-inriktning",
          label: null,
        },
        {
          id: "succession:class/sun2000-inriktning->class/sun2020-inriktning",
          kind: "succession",
          source: "class/sun2000-inriktning",
          target: "class/sun2020-inriktning",
          label: null,
        },
      ],
    });

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
    expect(getClassificationGroup).toHaveBeenCalledWith("sun");
    expect(getClassificationGroupGraph).toHaveBeenCalledWith("sun");
    expect(document.querySelector(".period-picker")).toBeNull();
    await expect
      .element(page.getByRole("heading", { name: "Relations" }))
      .toBeVisible();
    expect(
      [...document.querySelectorAll(".history-graph .node-label.in-bar")].map(
        (label) => label.textContent?.trim(),
      ),
    ).toEqual([
      "SUN1996",
      "SUN2000-INRIKTNING",
      "SUN2020-INRIKTNING",
      "NIVA-GROV",
    ]);
    expect(
      [...document.querySelectorAll(".history-graph .node-label.in-bar")].map(
        (label) => label.textContent?.trim(),
      ),
    ).not.toContain("sun");
    expect(document.querySelectorAll(".edges .succession")).toHaveLength(2);
    expect(
      document.querySelector(
        '.member-selector a[href="/catalog/class/sun2020-inriktning"]',
      ),
    ).not.toBeNull();
    expect(
      document.querySelector(
        '.member-selector a[href="/catalog/class/niva-grovv1"]',
      ),
    ).not.toBeNull();
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
      .element(page.getByRole("heading", { name: "Members" }))
      .toBeVisible();
    expect(memberSelector().textContent).toContain("astsni1");
    expect(memberSelector().textContent).toContain("astsni2");
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
    expect(memberLink(/februari/).classList.contains("not-delivered")).toBe(
      true,
    );
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

    await expect
      .element(page.getByRole("heading", { name: "Members" }))
      .toBeVisible();
    const href = memberLink(/februari/).getAttribute("href") ?? "";
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
      .element(page.getByRole("heading", { name: "Members" }))
      .toBeVisible();
    expect(memberLink(/februari/).getAttribute("href")).toBe(
      "/catalog/scb/rams/inkfeb",
    );
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
    await expect
      .element(page.getByRole("heading", { name: "Members" }))
      .toBeVisible();
    expect(memberLink(/februari/).classList.contains("not-delivered")).toBe(
      false,
    );
    await expect
      .element(page.getByText(/not delivered/))
      .not.toBeInTheDocument();
  });
});
