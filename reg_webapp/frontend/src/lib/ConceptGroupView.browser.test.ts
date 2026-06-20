import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { ConceptGroupNodeData } from "./api";
import { getConceptGroup } from "./api";
import ConceptGroupView from "./ConceptGroupView.svelte";

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
    // Members render with their names + FQIDs.
    await expect.element(page.getByText("Inkomst januari")).toBeVisible();
    await expect.element(page.getByText("scb/rams/inkfeb")).toBeVisible();
    // The facet label shows (the inkjan member's "januari" facet).
    await expect
      .element(page.getByText("januari", { exact: true }))
      .toBeVisible();
    // The member with coverage shows its study window; the stateless one omits it.
    await expect.element(page.getByText(/2019.*2021/)).toBeVisible();
  });

  it("shows a not-found message on a 404", async () => {
    const { ApiError } = await import("./api");
    vi.mocked(getConceptGroup).mockRejectedValue(
      new ApiError(404, null, "no concept group"),
    );

    await render(ConceptGroupView, {
      provider: "scb",
      register: "rams",
      key: "nope",
    });

    await expect.element(page.getByText(/Not found/)).toBeVisible();
  });
});
