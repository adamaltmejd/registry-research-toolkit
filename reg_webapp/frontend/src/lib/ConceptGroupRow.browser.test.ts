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
    axes: ["month"],
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
    // The shared summary content still renders inside the link: the count and the
    // group-key badge.
    await expect.element(page.getByText("2 variables")).toBeVisible();
    // The group-key badge renders the key via the shared `Tag` primitive (#828),
    // whose internal label markup adds surrounding whitespace — trim before the
    // text assertion (the key STRING is what matters, not the Tag's layout nodes).
    expect(container.querySelector(".group-key")?.textContent?.trim()).toBe(
      "ink",
    );
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
