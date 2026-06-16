import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { DimensionsResponse } from "./api";
import { getBindingDimensions } from "./api";
import DimensionsPanel from "./DimensionsPanel.svelte";

// Mock the single GET the panel drives (mirrors DocMentionsPanel's api-mock
// style); keep the rest of api.ts real (the type exports).
vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return {
    ...actual,
    getBindingDimensions: vi.fn(),
  };
});

// A dimensions envelope; cases override `dimensions`.
function response(
  overrides: Partial<DimensionsResponse> = {},
): DimensionsResponse {
  return {
    binding: "scb/rams/inkjan",
    dimensions: [],
    ...overrides,
  } as unknown as DimensionsResponse;
}

beforeEach(() => {
  vi.mocked(getBindingDimensions).mockReset();
});

describe("DimensionsPanel (#489)", () => {
  it("renders a fetched concept group via ConceptGroupRow", async () => {
    vi.mocked(getBindingDimensions).mockResolvedValue(
      response({
        dimensions: [
          {
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
          },
        ],
      } as unknown as DimensionsResponse),
    );

    await render(DimensionsPanel, { fqidPath: "scb/rams/inkjan" });

    await expect
      .element(page.getByRole("heading", { name: "Variants / dimensions" }))
      .toBeVisible();
    // The group's label + count come from ConceptGroupRow's <summary>.
    await expect.element(page.getByText("Inkomst")).toBeVisible();
    await expect.element(page.getByText("2 variables")).toBeVisible();
  });

  it("omits the whole section when the variable is in no group", async () => {
    vi.mocked(getBindingDimensions).mockResolvedValue(
      response({ dimensions: [] }),
    );

    await render(DimensionsPanel, { fqidPath: "scb/rams/syss" });

    await expect
      .element(page.getByRole("heading", { name: "Variants / dimensions" }))
      .not.toBeInTheDocument();
  });

  it("keeps the section visible with an inline error when the fetch fails", async () => {
    vi.mocked(getBindingDimensions).mockRejectedValue(
      new Error("backend down"),
    );

    await render(DimensionsPanel, { fqidPath: "scb/rams/inkjan" });

    // A fetch failure must NOT silently collapse the section (that would read as
    // "in no group"); it stays visible with the inline error.
    await expect
      .element(page.getByRole("heading", { name: "Variants / dimensions" }))
      .toBeVisible();
    await expect
      .element(page.getByText(/Failed to load dimensions/))
      .toBeVisible();
  });
});
