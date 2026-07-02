import { describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { ConceptGroup } from "./api";
import ConceptGroupRow from "./ConceptGroupRow.svelte";

// ConceptGroupRow's three-way branch (#673 M6): `asLink = href !== undefined &&
// onpick === undefined`. asLink → a <a.group-link> summary, NO <details>;
// otherwise the inline <details.group> (browse-without-href, and pick mode with
// member-pick buttons). The row takes all its data as PROPS — no API mocks.

// A minimal single-axis ("month") group. The label carries a recognizable
// string and the members carry real-shaped leaf FQIDs.
function group(overrides: Partial<ConceptGroup> = {}): ConceptGroup {
  return {
    key: "ink",
    label: "Inkomst",
    source: "token",
    axes: [{ name: "month", label: "month" }],
    members: [
      {
        fqid: "scb/rams/inkjan",
        name: "Inkomst",
        facets: [{ axis: "month", value: "01", label: "januari" }],
      },
      {
        fqid: "scb/rams/inkfeb",
        name: "Inkomst",
        facets: [{ axis: "month", value: "02", label: "februari" }],
      },
    ],
    ...overrides,
  } as unknown as ConceptGroup;
}

describe("ConceptGroupRow (#673 M6)", () => {
  it("href-mode renders a link with the summary, no <details>", async () => {
    const { container } = render(ConceptGroupRow, {
      group: group(),
      href: "/catalog/group/scb/rams/ink",
    });

    // asLink: a single <a.group-link> carrying the group label, pointed at the
    // group subject route — no inline disclosure.
    await expect
      .element(page.getByRole("link", { name: /Inkomst/ }))
      .toHaveAttribute("href", "/catalog/group/scb/rams/ink");
    expect(container.querySelector("details.group")).toBeNull();
    // The shared summary content still renders inside the link: the useful count
    // stays, but browse-link rows omit the noisy group-key slug pill.
    await expect.element(page.getByText("2 variables")).toBeVisible();
    expect(container.querySelector(".group-key")).toBeNull();
  });

  it("pick-mode keeps the inline <details> + member buttons even when href is also passed", async () => {
    const { container } = render(ConceptGroupRow, {
      group: group(),
      href: "/catalog/group/scb/rams/ink",
      onpick: vi.fn(),
    });

    // onpick set → NOT asLink, regardless of href: the inline <details> stays and
    // there is no group-link.
    expect(container.querySelector("a.group-link")).toBeNull();
    expect(container.querySelector("details.group")).not.toBeNull();
    expect(container.querySelector(".group-key")?.textContent?.trim()).toBe(
      "ink",
    );
    // The single-axis pick arm renders members as member-pick buttons (in the DOM
    // even while the <details> is collapsed).
    expect(
      container.querySelectorAll("button.member-pick").length,
    ).toBeGreaterThan(0);
  });

  it("no-href + no-onpick keeps the inline <details> (browse, classification arm)", async () => {
    const { container } = render(ConceptGroupRow, { group: group() });

    // Neither href nor onpick → the existing inline <details> browse, no link.
    expect(container.querySelector("a.group-link")).toBeNull();
    expect(container.querySelector("details.group")).not.toBeNull();
  });

  it("renders facet LABELS for an AXIS-LESS umbrella (axes: [], members carry facets)", async () => {
    // Classification umbrellas are axis-less (`axes: []`, #516) but each member
    // carries a curated `{axis: null, label}` facet. The row must render the
    // facet labels as chips — NOT drop to the bare-slug plain list (regression:
    // the old `axes.length === 1` chip branch fell through to the slug list for
    // an empty `axes`, hiding the curated labels).
    render(ConceptGroupRow, {
      // The axis-less caller (CatalogNodeView) passes `axisNoun([]) === "members"`.
      noun: "members",
      group: group({
        key: "sun",
        label: "Utbildningsnivå",
        axes: [],
        members: [
          {
            fqid: "class/niva-aggregat",
            name: "Nivå – aggregat",
            facets: [
              { axis: null, value: "aggregat", label: "Nivå – aggregat" },
            ],
          },
          {
            fqid: "class/niva-7",
            name: "Nivå – 7 nivåer",
            facets: [{ axis: null, value: "niva7", label: "7 nivåer" }],
          },
        ],
      } as unknown as Partial<ConceptGroup>),
    });

    // The chips carry the curated facet labels and link to the member leaf FQIDs;
    // the bare slug ("niva-aggregat") must NOT be what's shown.
    await page.getByText("2 members").click();
    await expect
      .element(page.getByRole("link", { name: "Nivå – aggregat" }))
      .toHaveAttribute("href", "/catalog/class/niva-aggregat");
    await expect
      .element(page.getByRole("link", { name: "7 nivåer" }))
      .toHaveAttribute("href", "/catalog/class/niva-7");
  });
});

// ── The >2-axis navigator in the register-browse / picker row (#819 PR2) ──────
// The matrix path DROPS members differing only on a 3rd axis (two delivery
// columns of one variable collapse into one 2D cell). ConceptGroupRow renders in
// register browse AND in pick mode (`onpick` set), so the data-loss bug hid most
// of the iot family from browse and made it UNSELECTABLE in pick mode. The shared
// ConceptGroupNavigator must render here too, in BOTH modes, dropping no member.
function threeAxisGroup(): ConceptGroup {
  return {
    key: "disponibel-inkomst",
    label: "Disponibel inkomst",
    source: "curated",
    axes: [
      { name: "enhet", label: "Enhet" },
      { name: "hushallsbegrepp", label: "Hushållsbegrepp" },
      { name: "kapitalvinst", label: "Kapitalvinst" },
    ],
    members: [
      // Two members on the SAME variable (two delivery columns), differing ONLY
      // on kapitalvinst — the exact pair the 2D matrix collapses + drops.
      {
        fqid: "scb/iot/dispink",
        name: "Disponibel inkomst",
        delivery_column: "dispink_inkl",
        facets: [
          { axis: "enhet", value: "individ", label: "Individ" },
          { axis: "hushallsbegrepp", value: "vx", label: "Vuxen" },
          { axis: "kapitalvinst", value: "inkl", label: "Inkl. kapitalvinst" },
        ],
      },
      {
        fqid: "scb/iot/dispink",
        name: "Disponibel inkomst",
        delivery_column: "dispink_exkl",
        facets: [
          { axis: "enhet", value: "individ", label: "Individ" },
          { axis: "hushallsbegrepp", value: "vx", label: "Vuxen" },
          { axis: "kapitalvinst", value: "exkl", label: "Exkl. kapitalvinst" },
        ],
      },
      {
        fqid: "scb/iot/dispinkhb",
        name: "Disponibel inkomst hushåll",
        delivery_column: "dispinkhb_inkl",
        facets: [
          { axis: "enhet", value: "hushall", label: "Hushåll" },
          { axis: "hushallsbegrepp", value: "vxhb", label: "Vuxna i hushåll" },
          { axis: "kapitalvinst", value: "inkl", label: "Inkl. kapitalvinst" },
        ],
      },
    ],
  } as unknown as ConceptGroup;
}

describe("ConceptGroupRow >2-axis navigator (#819 PR2)", () => {
  it("PICK mode renders every member as a pickable button (no matrix collapse/drop)", async () => {
    const onpick = vi.fn();
    const { container } = render(ConceptGroupRow, {
      group: threeAxisGroup(),
      onpick,
    });

    // Pick mode keeps the inline <details>; the >2-axis branch renders the shared
    // navigator, NOT a 2D matrix (which would drop the second shared-fqid rep).
    expect(container.querySelector("table.facet-matrix")).toBeNull();
    // #819 FIX 3: the summary counts DISTINCT variables (2 fqids: dispink ×2 reps
    // + dispinkhb), NOT the 3 representation rows — `members.length` overstated it.
    await page.getByText("2 variables").click();

    // All 3 members render in the navigator list as pick BUTTONS — none dropped.
    const buttons = container.querySelectorAll(
      "ul.members.navigator button.member-pick",
    );
    expect(buttons).toHaveLength(3);
    // The count readout reflects the full set (the matrix would show 2 cells).
    await expect
      .element(page.getByText("Showing 3 of 3 members", { exact: true }))
      .toBeVisible();

    // Each visible member is pickable and emits its member FQID. The two
    // shared-fqid reps both emit (the second is the one the matrix dropped).
    for (const b of buttons) {
      (b as HTMLButtonElement).click();
    }
    expect(onpick).toHaveBeenCalledTimes(3);
    // BOTH delivery columns of scb/iot/dispink were reachable (the dropped rep).
    expect(onpick.mock.calls.filter((c) => c[0] === "scb/iot/dispink")).toEqual(
      [["scb/iot/dispink"], ["scb/iot/dispink"]],
    );
    expect(onpick).toHaveBeenCalledWith("scb/iot/dispinkhb");
  });

  it("axis identity on member tags is carried by TEXT, not color-only", async () => {
    const { container } = render(ConceptGroupRow, {
      group: threeAxisGroup(),
      onpick: vi.fn(),
    });
    await page.getByText("2 variables").click();

    // Member facet tags are neutral `Tag` primitives (`.tag`), NOT the `--cat-*`
    // type palette, and each names its axis as visible TEXT (the a11y + DESIGN
    // palette fix — the value label alone can't convey which axis it's on).
    const tagText = [
      ...container.querySelectorAll("ul.members.navigator .facet-tags .tag"),
    ].map((t) => t.textContent?.replace(/\s+/g, " ").trim() ?? "");
    expect(tagText.some((t) => /enhet/i.test(t) && t.includes("Individ"))).toBe(
      true,
    );
    expect(
      tagText.some((t) => /kapitalvinst/i.test(t) && t.includes("Exkl.")),
    ).toBe(true);
  });

  it("BROWSE mode (no href, no onpick) renders every member as a leaf link", async () => {
    const { container } = render(ConceptGroupRow, { group: threeAxisGroup() });
    expect(container.querySelector("table.facet-matrix")).toBeNull();
    await page.getByText("2 variables").click();

    // Every member is a browse link — the two shared-fqid reps both render (the
    // matrix would have dropped one), so the variable stays reachable in browse.
    const links = container.querySelectorAll("ul.members.navigator a");
    expect(links).toHaveLength(3);
    expect(
      container.querySelectorAll('a[href="/catalog/scb/iot/dispink"]'),
    ).toHaveLength(2);
  });

  it("a per-axis filter narrows the visible members without dropping any from the set", async () => {
    const { container } = render(ConceptGroupRow, {
      group: threeAxisGroup(),
      onpick: vi.fn(),
    });
    await page.getByText("2 variables").click();
    await expect
      .element(page.getByText("Showing 3 of 3 members", { exact: true }))
      .toBeVisible();

    // Filter kapitalvinst = exkl: only the single exkl rep survives (AND across
    // axes). Scope the click to the filter fieldsets (the member rows carry the
    // same label text).
    const exkl = [
      ...container.querySelectorAll("fieldset.axis-filter label.filter-pill"),
    ].find((l) => l.textContent?.trim() === "Exkl. kapitalvinst") as
      | HTMLLabelElement
      | undefined;
    expect(exkl).not.toBeUndefined();
    exkl?.click();

    await expect
      .element(page.getByText("Showing 1 of 3 members", { exact: true }))
      .toBeVisible();
    expect(
      container.querySelectorAll("ul.members.navigator > li"),
    ).toHaveLength(1);
  });

  it("shows each member's delivery column so duplicate-coord members are distinguishable (#819 FIX B)", async () => {
    // The two scb/iot/dispink reps share fqid AND name; in pick mode (where the
    // user is CHOOSING a column) they must be told apart by their delivery column.
    const { container } = render(ConceptGroupRow, {
      group: threeAxisGroup(),
      onpick: vi.fn(),
    });
    await page.getByText("2 variables").click();

    // Each navigator member row carries its delivery column as a subtle code.
    const cols = [
      ...container.querySelectorAll(
        "ul.members.navigator > li code.delivery-column",
      ),
    ].map((c) => c.textContent?.trim());
    expect(cols).toContain("dispink_inkl");
    expect(cols).toContain("dispink_exkl");
    // The two same-fqid rep rows are now distinguishable by their column.
    expect(new Set(cols).size).toBe(cols.length);
  });
});

// ── A ≤2-axis group with COLLIDING coordinates (#819 FIX C) ───────────────────
// The matrix renders only the FIRST member per (row, col) cell, so two members
// sharing a full 2-axis coordinate (representation members distinguished by
// delivery_column) silently DROP one. FIX C routes such a group through the
// navigator instead — keyed on coordinate-uniqueness, not just axes.length > 2.
function twoAxisCollidingGroup(): ConceptGroup {
  return {
    key: "din8",
    label: "Disponibel inkomst (DIN8)",
    source: "curated",
    axes: [
      { name: "enhet", label: "Enhet" },
      { name: "hushallsbegrepp", label: "Hushållsbegrepp" },
    ],
    members: [
      // THREE members on one variable sharing the SAME (enhet, hushallsbegrepp)
      // cell, distinguished only by delivery_column — the DIN83/84/86 case.
      {
        fqid: "scb/iot/din8",
        name: "DIN8",
        delivery_column: "DIN83",
        facets: [
          { axis: "enhet", value: "individ", label: "Individ" },
          { axis: "hushallsbegrepp", value: "vx", label: "Vuxen" },
        ],
      },
      {
        fqid: "scb/iot/din8",
        name: "DIN8",
        delivery_column: "DIN84",
        facets: [
          { axis: "enhet", value: "individ", label: "Individ" },
          { axis: "hushallsbegrepp", value: "vx", label: "Vuxen" },
        ],
      },
      {
        fqid: "scb/iot/din8",
        name: "DIN8",
        delivery_column: "DIN86",
        facets: [
          { axis: "enhet", value: "individ", label: "Individ" },
          { axis: "hushallsbegrepp", value: "vx", label: "Vuxen" },
        ],
      },
    ],
  } as unknown as ConceptGroup;
}

describe("ConceptGroupRow ≤2-axis colliding-coord navigator (#819 FIX C)", () => {
  it("routes a 2-axis group with shared-coordinate members through the navigator, dropping none", async () => {
    const onpick = vi.fn();
    const { container } = render(ConceptGroupRow, {
      group: twoAxisCollidingGroup(),
      onpick,
    });

    // The matrix (which would render only the FIRST of the three colliding members)
    // must NOT be used; the navigator renders all three instead.
    expect(container.querySelector("table.facet-matrix")).toBeNull();
    // One DISTINCT variable (all share fqid scb/iot/din8).
    await page.getByText("1 variables").click();

    const buttons = container.querySelectorAll(
      "ul.members.navigator button.member-pick",
    );
    expect(buttons).toHaveLength(3);
    await expect
      .element(page.getByText("Showing 3 of 3 members", { exact: true }))
      .toBeVisible();

    // All three columns are pickable (the matrix would have dropped two of them),
    // each distinguishable by its delivery column (FIX B).
    const cols = [
      ...container.querySelectorAll(
        "ul.members.navigator > li code.delivery-column",
      ),
    ].map((c) => c.textContent?.trim());
    expect(cols.sort()).toEqual(["DIN83", "DIN84", "DIN86"]);
  });

  it("keeps the matrix for a genuinely unique-coordinate 2-axis group (no regression)", async () => {
    const { container } = render(ConceptGroupRow, {
      group: group({
        key: "agi",
        label: "AGI",
        axes: [
          { name: "month", label: "month" },
          { name: "rank", label: "rank" },
        ],
        members: [
          {
            fqid: "scb/lisa/a",
            name: null,
            facets: [
              { axis: "month", value: "01", label: "jan" },
              { axis: "rank", value: "1", label: "1" },
            ],
          },
          {
            fqid: "scb/lisa/b",
            name: null,
            facets: [
              { axis: "month", value: "01", label: "jan" },
              { axis: "rank", value: "2", label: "2" },
            ],
          },
        ],
      } as unknown as Partial<ConceptGroup>),
    });
    await page.getByText("2 variables").click();
    // Unique coords → the matrix stays (no navigator).
    expect(container.querySelector("table.facet-matrix")).not.toBeNull();
    expect(container.querySelector("ul.members.navigator")).toBeNull();
  });
});
