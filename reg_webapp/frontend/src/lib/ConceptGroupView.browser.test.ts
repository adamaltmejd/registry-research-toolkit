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
    axes: [{ name: "month", label: "month" }],
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
});

describe("ConceptGroupView member selector (#638 PR2a)", () => {
  // A 2-axis (month × rank) group → the matrix selector.
  function matrixNode(): ConceptGroupNodeData {
    return node({
      axes: [
        { name: "month", label: "month" },
        { name: "rank", label: "rank" },
      ],
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

  // ── The N-axis facet navigator (#819) ──────────────────────────────────────
  // A >2-axis group can't be a 2D matrix; the iot disposable-income family is
  // enhet × hushållsbegrepp × kapitalvinst. The critical regression the matrix
  // hits: two members on ONE variable (two delivery columns) that share the first
  // two coords and differ only on the third — they collapse into a single 2D cell
  // AND escape the `ungridded` fallback (they cover all declared axes), so the
  // grid DROPS the second. The navigator must reach every member.
  function threeAxisNode(): ConceptGroupNodeData {
    return node({
      key: "disponibel-inkomst",
      label: "Disponibel inkomst",
      source: "curated",
      // #819: axes carry the curator-authored display label distinct from the
      // stable match name (e.g. "Hushållsbegrepp" for `hushallsbegrepp`).
      axes: [
        { name: "enhet", label: "Enhet" },
        { name: "hushallsbegrepp", label: "Hushållsbegrepp" },
        { name: "kapitalvinst", label: "Kapitalvinst" },
      ],
      members: [
        // Two members on the SAME variable + same (enhet, hushållsbegrepp),
        // differing ONLY on kapitalvinst (incl/excl) via distinct delivery
        // columns — the matrix-collapse trap. Both must remain reachable.
        {
          fqid: "scb/iot/dispink",
          name: "Disponibel inkomst",
          delivery_column: "dispink_inkl",
          facets: [
            { axis: "enhet", value: "individ", label: "Individ" },
            { axis: "hushallsbegrepp", value: "vx", label: "Vuxen" },
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
          delivery_column: "dispink_exkl",
          facets: [
            { axis: "enhet", value: "individ", label: "Individ" },
            { axis: "hushallsbegrepp", value: "vx", label: "Vuxen" },
            {
              axis: "kapitalvinst",
              value: "exkl",
              label: "Exkl. kapitalvinst",
            },
          ],
          coverage: null,
        },
        // A third member on a different hushållsbegrepp (so a filter can narrow).
        {
          fqid: "scb/iot/dispinkhb",
          name: "Disponibel inkomst hushåll",
          delivery_column: "dispinkhb_inkl",
          facets: [
            { axis: "enhet", value: "hushall", label: "Hushåll" },
            {
              axis: "hushallsbegrepp",
              value: "vxhb",
              label: "Vuxna i hushåll",
            },
            {
              axis: "kapitalvinst",
              value: "inkl",
              label: "Inkl. kapitalvinst",
            },
          ],
          coverage: null,
        },
      ],
    } as unknown as Partial<ConceptGroupNodeData>);
  }

  it("renders the facet navigator for a >2-axis group, reaching every member (no drop)", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(threeAxisNode());

    await render(ConceptGroupView, {
      provider: "scb",
      register: "iot",
      key: "disponibel-inkomst",
    });

    // No 2D matrix for a >2-axis group — the navigator replaces it.
    expect(document.querySelector("table.facet-matrix")).toBeNull();
    // The count readout reflects the FULL member set (the matrix would have
    // collapsed the two shared-fqid reps into one cell and dropped the second).
    await expect
      .element(page.getByText("Showing 3 of 3 members", { exact: true }))
      .toBeVisible();
    // Three member rows render in the navigator list — none dropped.
    const rows = document.querySelectorAll("ul.members.navigator > li");
    expect(rows).toHaveLength(3);
    // BOTH representations of the shared-fqid variable are present, told apart by
    // their distinct kapitalvinst tags (the matrix-collapse trap). The member
    // facet tags are neutral `Tag` primitives (`.tag`, NOT the `--cat-*` type
    // palette) carrying the value label; scope to the navigator list's tags (the
    // filter fieldsets carry the same labels). The collapsed text includes the
    // axis micro-label prefix (axis identity is TEXT, not hue), so match the
    // value label as a SUBSTRING of each tag.
    const tagText = [
      ...document.querySelectorAll("ul.members.navigator .facet-tags .tag"),
    ].map((p) => p.textContent?.replace(/\s+/g, " ").trim() ?? "");
    expect(tagText.some((t) => t.includes("Inkl. kapitalvinst"))).toBe(true);
    expect(tagText.some((t) => t.includes("Exkl. kapitalvinst"))).toBe(true);
    expect(tagText.some((t) => t.includes("Vuxna i hushåll"))).toBe(true);
    // Axis identity is carried by TEXT, not color-only: each kapitalvinst tag's
    // text names its axis (the a11y + DESIGN-palette fix).
    expect(
      tagText.some((t) => /kapitalvinst/i.test(t) && t.includes("Inkl.")),
    ).toBe(true);
  });

  it("renders one filter fieldset per axis with a pill per value", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(threeAxisNode());

    await render(ConceptGroupView, {
      provider: "scb",
      register: "iot",
      key: "disponibel-inkomst",
    });

    // Wait for the navigator to render (the fetch resolves async).
    await expect
      .element(page.getByText("Showing 3 of 3 members", { exact: true }))
      .toBeVisible();
    // One filter <fieldset> per declared axis (3 axes → 3 fieldsets), each
    // legended by its curator-authored axis LABEL (#819, not the raw match key).
    const fieldsets = [...document.querySelectorAll("fieldset.axis-filter")];
    expect(fieldsets).toHaveLength(3);
    expect(
      fieldsets.map((f) => f.querySelector("legend")?.textContent?.trim()),
    ).toEqual(["Enhet", "Hushållsbegrepp", "Kapitalvinst"]);
    // The kapitalvinst axis exposes a checkbox per distinct value (inkl / exkl).
    const kvFieldset = fieldsets[2];
    const boxes = kvFieldset.querySelectorAll('input[type="checkbox"]');
    expect(boxes).toHaveLength(2);
    const kvLabels = [...kvFieldset.querySelectorAll("label.filter-pill")].map(
      (l) => l.textContent?.trim(),
    );
    // `axisValues` value-sorts (exkl < inkl), so the pills come exkl-first.
    expect(kvLabels).toEqual(["Exkl. kapitalvinst", "Inkl. kapitalvinst"]);
  });

  it("a filter narrows the visible member set without mutating anything", async () => {
    vi.mocked(getConceptGroup).mockResolvedValue(threeAxisNode());

    await render(ConceptGroupView, {
      provider: "scb",
      register: "iot",
      key: "disponibel-inkomst",
    });

    // All 3 members visible initially.
    await expect
      .element(page.getByText("Showing 3 of 3 members", { exact: true }))
      .toBeVisible();

    // Filter to kapitalvinst = exkl: click that axis-filter's "Exkl." pill (the
    // visible <label> wrapping the hidden checkbox). Scoped to the filter
    // fieldsets so it doesn't collide with the member-row pills of the same text.
    const exklLabel = [
      ...document.querySelectorAll("fieldset.axis-filter label.filter-pill"),
    ].find((l) => l.textContent?.trim() === "Exkl. kapitalvinst") as
      | HTMLLabelElement
      | undefined;
    expect(exklLabel).not.toBeUndefined();
    exklLabel?.click();

    // Only the single exkl member survives (AND across axes; the two inkl members
    // drop out) — the filter NARROWS, it never selects/mutates.
    await expect
      .element(page.getByText("Showing 1 of 3 members", { exact: true }))
      .toBeVisible();
    const survivors = [
      ...document.querySelectorAll("ul.members.navigator .facet-tags .tag"),
    ].map((p) => p.textContent?.replace(/\s+/g, " ").trim() ?? "");
    expect(survivors.some((t) => t.includes("Exkl. kapitalvinst"))).toBe(true);
    expect(survivors.some((t) => t.includes("Inkl. kapitalvinst"))).toBe(false);

    // Clearing the filter restores the full set (filter is a narrow-only lens).
    await page.getByRole("button", { name: "Clear filters" }).click();
    await expect
      .element(page.getByText("Showing 3 of 3 members", { exact: true }))
      .toBeVisible();
  });

  it("navigator members carry per-member coverage + out-of-window greying", async () => {
    // The navigator member list (the shared component, fed `memberLink`) must keep
    // the per-member coverage line + the availability-lens greying the matrix /
    // ≤2-axis paths carry — so legacy vintage editions sharing coords are legible
    // by their period, not rendered as identical rows.
    const withCoverage = threeAxisNode();
    // Give one rep a 2019–2021 window and another a 2010–2012 window so a
    // 2019-onward project window greys exactly one.
    withCoverage.members[0].coverage = {
      coverage_from: "2019-01-01",
      coverage_to: "2021-12-31",
      open_ended: false,
      state_count: 1,
    } as ConceptGroupNodeData["members"][number]["coverage"];
    withCoverage.members[2].coverage = {
      coverage_from: "2010-01-01",
      coverage_to: "2012-12-31",
      open_ended: false,
      state_count: 1,
    } as ConceptGroupNodeData["members"][number]["coverage"];
    vi.mocked(getConceptGroup).mockResolvedValue(withCoverage);
    windowStore.set({ from: 2019, to: 2021 });

    await render(ConceptGroupView, {
      provider: "scb",
      register: "iot",
      key: "disponibel-inkomst",
    });

    await expect
      .element(page.getByText("Showing 3 of 3 members", { exact: true }))
      .toBeVisible();
    // The per-member coverage line renders inside the navigator list (the
    // year-collapsed span), not only in the matrix path.
    await expect
      .element(page.getByText("2019 – 2021", { exact: true }))
      .toBeVisible();
    // The 2010–2012 member does NOT span the 2019–2021 window → greyed with the
    // "not delivered" note (the availability lens, preserved in the navigator).
    const greyed = document.querySelector(
      "ul.members.navigator a.not-delivered",
    );
    expect(greyed).not.toBeNull();
    await expect.element(page.getByText(/not delivered/)).toBeVisible();
  });

  it("renders the >2-axis navigator without a duplicate-key error on shared fqids", async () => {
    // The shared-fqid pair (scb/iot/dispink × 2 delivery columns) keyed on fqid
    // alone would throw Svelte's duplicate-key error and drop a member. Keying on
    // (fqid, delivery_column) renders both. A console error would fail-loud here.
    const errors: unknown[] = [];
    const spy = vi.spyOn(console, "error").mockImplementation((...args) => {
      errors.push(args);
    });
    vi.mocked(getConceptGroup).mockResolvedValue(threeAxisNode());

    await render(ConceptGroupView, {
      provider: "scb",
      register: "iot",
      key: "disponibel-inkomst",
    });

    await expect
      .element(page.getByText("Showing 3 of 3 members", { exact: true }))
      .toBeVisible();
    // Both reps of the shared-fqid variable rendered → no member dropped.
    const links = document.querySelectorAll(
      'a[href="/catalog/scb/iot/dispink"]',
    );
    expect(links.length).toBe(2);
    expect(errors).toEqual([]);
    spy.mockRestore();
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
