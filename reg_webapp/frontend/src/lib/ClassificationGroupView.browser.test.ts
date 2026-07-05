import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type {
  ClassificationFamilyNodeData,
  ClassificationGroupNodeData,
  RelationshipGraph,
} from "./api";
import {
  ApiError,
  getClassificationGroup,
  getClassificationGroupGraph,
} from "./api";
import ClassificationGroupView from "./ClassificationGroupView.svelte";
import { router } from "./router.svelte";

// Mock the GETs the view drives (mirrors ConceptGroupView.browser.test's
// api-mock style); keep the rest of api.ts real (the type exports + router).
vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return {
    ...actual,
    getClassificationGroup: vi.fn(),
    getClassificationGroupGraph: vi.fn(),
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

function groupGraph(): RelationshipGraph {
  return {
    nodes: [
      {
        kind: "classification",
        id: "class/sun1996",
        fqid: "class/sun1996",
        label: "SUN 1996",
        group_key: "class/sun",
        group_label: "Svensk utbildningsnomenklatur",
        version_year: 1996,
        is_current: false,
      },
      {
        kind: "classification",
        id: "class/sun2020",
        fqid: "class/sun2020",
        label: "SUN 2020",
        group_key: "class/sun",
        group_label: "Svensk utbildningsnomenklatur",
        version_year: 2020,
        is_current: true,
      },
      {
        kind: "classification",
        id: "class/niva-test",
        fqid: "class/niva-test",
        label: "Nivå aggregat",
        group_key: "class/sun",
        group_label: "Svensk utbildningsnomenklatur",
        version_year: null,
        is_current: true,
      },
    ],
    edges: [
      {
        id: "succession:class/sun1996->class/sun2020",
        kind: "succession",
        source: "class/sun1996",
        target: "class/sun2020",
        label: null,
        effective_year: 2020,
      },
    ],
    focus_id: null,
  };
}

beforeEach(() => {
  vi.mocked(getClassificationGroup).mockReset();
  vi.mocked(getClassificationGroupGraph).mockReset();
  vi.mocked(getClassificationGroupGraph).mockResolvedValue({
    nodes: [],
    edges: [],
    focus_id: null,
  });
  // Reset the URL so each case starts clean (the router is a module singleton).
  router.navigate("/catalog/group/class/sun");
});

describe("ClassificationGroupView (#756)", () => {
  it("renders the umbrella label + members as facet-labelled links to leaf FQIDs", async () => {
    vi.mocked(getClassificationGroup).mockResolvedValue(node());

    await render(ClassificationGroupView, { key: "sun" });

    // The heading names the page kind and the umbrella label.
    await expect
      .element(
        page.getByRole("heading", {
          name: "Classification group: Svensk utbildningsnomenklatur",
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

  it("renders the classification group relationship graph with historical editions", async () => {
    vi.mocked(getClassificationGroup).mockResolvedValue(node());
    vi.mocked(getClassificationGroupGraph).mockResolvedValue(groupGraph());

    await render(ClassificationGroupView, { key: "sun" });

    expect(getClassificationGroupGraph).toHaveBeenCalledWith("sun");
    await expect
      .element(page.getByRole("heading", { name: "Editions" }))
      .toBeVisible();
    expect(document.querySelector(".classification-editions")).not.toBeNull();
    expect(document.querySelector(".edition-edge")).not.toBeNull();
    expect(
      document.querySelector('a.edition-name[href="/catalog/class/sun1996"]'),
    ).not.toBeNull();
    expect(document.querySelector(".edition-edge-year")?.textContent).toBe(
      "2020",
    );
    expect(document.body.textContent).not.toContain("group:sun");
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
