import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type {
  ClassificationFamilyNodeData,
  ClassificationGroupNodeData,
} from "./api";
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
    // Classification umbrellas are AXIS-LESS (`axes: []`, #516); each member
    // carries a curated `{axis: null, label}` facet (its short label).
    axes: [],
    members: [
      {
        fqid: "class/niva-test",
        name: "Utbildningsnivå – aggregat",
        facets: [{ axis: null, value: "aggregat", label: "Aggregat" }],
      },
      {
        fqid: "class/sun2020",
        name: "Svensk utbildningsnomenklatur",
        facets: [{ axis: null, value: "niva", label: "Utbildningsnivå" }],
      },
    ],
    ...overrides,
  } as unknown as ClassificationGroupNodeData;
}

function familyNode(
  overrides: Partial<ClassificationFamilyNodeData> = {},
): ClassificationFamilyNodeData {
  return {
    kind: "classification-family",
    key: "ssyk",
    label: "SSYK",
    editions: [
      {
        slug: "ssyk1996",
        fqid: "class/ssyk1996",
        name: "SSYK 1996",
        effective_year: 2012,
        version_year: 1996,
        is_current: false,
        is_self: false,
      },
      {
        slug: "ssyk2012",
        fqid: "class/ssyk2012",
        name: "SSYK 2012",
        effective_year: null,
        version_year: 2012,
        is_current: true,
        is_self: false,
      },
    ],
    ...overrides,
  } as unknown as ClassificationFamilyNodeData;
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

  it("demotes key + source into a 'Technical details' disclosure, OMITTING the Facets row when axis-less", async () => {
    vi.mocked(getClassificationGroup).mockResolvedValue(node());

    await render(ClassificationGroupView, { key: "sun" });

    await expect.element(page.getByText("Technical details")).toBeVisible();
    const disclosure = document.querySelector<HTMLDetailsElement>(
      "details.tech-details",
    );
    expect(disclosure).not.toBeNull();
    expect(disclosure?.textContent).toContain("sun");
    // Axis-less umbrella (`axes: []`) → the "Facets" dt/dd is gated out
    // (`{#if node.axes.length > 0}`), so the disclosure has no Facets row.
    expect(disclosure?.textContent).not.toContain("Facets");
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
      .element(page.getByText(/Not found: classification group or family/))
      .toBeVisible();
  });

  it("renders a succession family as an edition-chain subject page", async () => {
    vi.mocked(getClassificationGroup).mockResolvedValue(familyNode());

    await render(ClassificationGroupView, { key: "ssyk" });

    await expect
      .element(page.getByRole("heading", { name: "SSYK", level: 2 }))
      .toBeVisible();
    await expect
      .element(page.getByRole("link", { name: /SSYK 1996/ }))
      .toHaveAttribute("href", "/catalog/class/ssyk1996");
    await expect
      .element(page.getByRole("link", { name: /SSYK 2012/ }))
      .toHaveAttribute("href", "/catalog/class/ssyk2012");
    await expect.element(page.getByText(/ssyk2012 - current/)).toBeVisible();
  });
});
