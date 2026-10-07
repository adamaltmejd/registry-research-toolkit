import { describe, expect, it } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { ConceptGroup } from "./api";
import ConceptGroupRow from "./ConceptGroupRow.svelte";

// ConceptGroupRow (#673 M6) links a concept group to its subject page. The row
// takes all its data as PROPS — no API mocks.

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
  it("renders a link to the group page with the distinct-variable count", async () => {
    await render(ConceptGroupRow, {
      group: group(),
      href: "/catalog/group/scb/rams/ink",
    });

    // One link carrying the group label, pointed at the group subject route.
    await expect
      .element(page.getByRole("link", { name: /Inkomst/ }))
      .toHaveAttribute("href", "/catalog/group/scb/rams/ink");
    await expect.element(page.getByText("2 variables")).toBeVisible();
  });
});
