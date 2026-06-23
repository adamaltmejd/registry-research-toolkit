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
    expect(container.querySelector(".group-key")?.textContent).toBe("ink");
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
});
