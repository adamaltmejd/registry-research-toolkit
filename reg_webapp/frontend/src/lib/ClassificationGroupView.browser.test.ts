import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { ClassificationGroupNodeData } from "./api";
import { ApiError, getClassificationGroup } from "./api";
import ClassificationGroupView from "./ClassificationGroupView.svelte";
import { router } from "./router.svelte";

// Mock the single GET the view drives (mirrors ConceptGroupView.browser.test's
// api-mock style); keep the rest of api.ts real (the type exports + router).
vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return {
    ...actual,
    getClassificationGroup: vi.fn(),
  };
});

function node(
  overrides: Partial<ClassificationGroupNodeData> = {},
): ClassificationGroupNodeData {
  return {
    kind: "classification-group",
    key: "sun",
    label: "Svensk utbildningsnomenklatur",
    source: "token",
    axes: ["dimension"],
    members: [
      {
        fqid: "class/niva-test",
        name: "Utbildningsnivå – aggregat",
        facets: [{ axis: "dimension", value: "aggregat", label: "Aggregat" }],
      },
      {
        fqid: "class/sun2020",
        name: "Svensk utbildningsnomenklatur",
        facets: [
          { axis: "dimension", value: "niva", label: "Utbildningsnivå" },
        ],
      },
    ],
    ...overrides,
  } as unknown as ClassificationGroupNodeData;
}

beforeEach(() => {
  vi.mocked(getClassificationGroup).mockReset();
  // Reset the URL so each case starts clean (the router is a module singleton).
  router.navigate("/catalog/group/class/sun");
});

describe("ClassificationGroupView (#756)", () => {
  it("renders the umbrella label + members as facet-labelled links to leaf FQIDs", async () => {
    vi.mocked(getClassificationGroup).mockResolvedValue(node());

    await render(ClassificationGroupView, { key: "sun" });

    // The umbrella label heads the page.
    await expect
      .element(
        page.getByRole("heading", {
          name: "Svensk utbildningsnomenklatur",
          level: 2,
        }),
      )
      .toBeVisible();
    // Members are labelled by their facet ("Utbildningsnivå" / "Aggregat") and
    // link to their classification leaf FQIDs.
    await expect
      .element(page.getByRole("link", { name: "Utbildningsnivå" }))
      .toHaveAttribute("href", "/catalog/class/sun2020");
    await expect
      .element(page.getByRole("link", { name: "Aggregat" }))
      .toHaveAttribute("href", "/catalog/class/niva-test");
  });

  it("demotes key, facets, and source into a 'Technical details' disclosure", async () => {
    vi.mocked(getClassificationGroup).mockResolvedValue(node());

    await render(ClassificationGroupView, { key: "sun" });

    await expect.element(page.getByText("Technical details")).toBeVisible();
    const disclosure = document.querySelector<HTMLDetailsElement>(
      "details.tech-details",
    );
    expect(disclosure).not.toBeNull();
    expect(disclosure?.textContent).toContain("sun");
    expect(disclosure?.textContent).toContain("dimension");
  });

  it("shows a 404 not-found message for an unknown umbrella key", async () => {
    vi.mocked(getClassificationGroup).mockRejectedValue(
      new ApiError(
        404,
        { detail: "no classification group 'nope'" },
        "not found",
      ),
    );

    await render(ClassificationGroupView, { key: "nope" });

    await expect
      .element(page.getByText(/Not found: classification group/))
      .toBeVisible();
  });
});
