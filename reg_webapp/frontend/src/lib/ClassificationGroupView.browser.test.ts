import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type {
  ClassificationFamilyNodeData,
  ClassificationGroupNodeData,
  ClassificationNodeData,
  RelationshipGraph,
} from "./api";
import {
  ApiError,
  getCatalogNode,
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
    getCatalogNode: vi.fn(),
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
        short_name: "SSYK1996",
        effective_year: 2012,
        version_year: 1996,
        is_current: false,
        is_self: false,
      },
      {
        slug: "ssyk2012",
        fqid: "class/ssyk2012",
        name: "SSYK 2012",
        short_name: "SSYK2012",
        effective_year: null,
        version_year: 2012,
        is_current: true,
        is_self: false,
      },
    ],
    ...overrides,
  } as unknown as ClassificationFamilyNodeData;
}

function classificationNode(
  overrides: Partial<ClassificationNodeData> = {},
): ClassificationNodeData {
  return {
    kind: "classification",
    fqid: "class/sun2020",
    name: "SUN 2020",
    short_name: "SUN2020",
    edition_chain: [],
    codes: [{ code: "1", label: "Man", level: 1, is_valid: true }],
    dimensions: [],
    derived_from: [],
    derivatives: [],
    ...overrides,
  } as unknown as ClassificationNodeData;
}

function groupGraph(): RelationshipGraph {
  return {
    nodes: [
      {
        kind: "classification",
        id: "class/sun1996",
        fqid: "class/sun1996",
        label: "SUN 1996",
        short_name: "SUN1996",
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
        short_name: "SUN2020",
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
        short_name: "NIVA",
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

function familyGraph(): RelationshipGraph {
  return {
    nodes: [
      {
        kind: "classification",
        id: "class/ssyk1996",
        fqid: "class/ssyk1996",
        label: "Standard för svensk yrkesklassificering 1996",
        short_name: "SSYK1996",
        group_key: "class/ssyk",
        group_label: "SSYK",
        version_year: 1996,
        is_current: false,
      },
      {
        kind: "classification",
        id: "class/ssyk2012",
        fqid: "class/ssyk2012",
        label: "Standard för svensk yrkesklassificering 2012",
        short_name: "SSYK2012",
        group_key: "class/ssyk",
        group_label: "SSYK",
        version_year: 2012,
        is_current: true,
      },
    ],
    edges: [
      {
        id: "succession:class/ssyk1996->class/ssyk2012",
        kind: "succession",
        source: "class/ssyk1996",
        target: "class/ssyk2012",
        label: null,
        effective_year: 2012,
      },
    ],
    focus_id: null,
  };
}

beforeEach(() => {
  vi.mocked(getCatalogNode).mockReset();
  vi.mocked(getClassificationGroup).mockReset();
  vi.mocked(getClassificationGroupGraph).mockReset();
  vi.mocked(getCatalogNode).mockResolvedValue(classificationNode());
  vi.mocked(getClassificationGroupGraph).mockResolvedValue({
    nodes: [],
    edges: [],
    focus_id: null,
  });
  // Reset the URL so each case starts clean (the router is a module singleton).
  router.navigate("/catalog/group/class/sun");
});

describe("ClassificationGroupView (#756)", () => {
  it("renders the umbrella label + members as edition tabs", async () => {
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
    // rendered as tabs. The selected tab's codes are fetched lazily.
    await expect
      .element(page.getByRole("tab", { name: /Utbildningsnivå/ }))
      .toBeVisible();
    await expect
      .element(page.getByRole("tab", { name: /Aggregat/ }))
      .toBeVisible();
    await expect.element(page.getByText("Man")).toBeVisible();
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
    expect(document.querySelector(".edition-year")).toBeNull();
    await expect
      .element(page.getByRole("link", { name: "SUN1996" }))
      .toHaveAttribute("href", "/catalog/class/sun1996");
    await expect
      .element(page.getByRole("tab", { name: /Utbildningsnivå/ }))
      .toHaveAttribute("aria-selected", "true");
    expect(getCatalogNode).toHaveBeenCalledWith("class/sun2020");
    expect(document.body.textContent).not.toContain("group:sun");
  });

  it("defaults to the current group member before future-dated graph successors", async () => {
    vi.mocked(getClassificationGroup).mockResolvedValue(
      node({
        key: "icd",
        label: "ICD",
        members: [
          {
            fqid: "class/icd11",
            name: "ICD-11",
            facets: [{ axis: null, value: "icd11", label: "ICD-11" }],
          },
          {
            fqid: "class/icd10",
            name: "ICD-10",
            facets: [{ axis: null, value: "icd10", label: "ICD-10" }],
          },
        ],
      }),
    );
    vi.mocked(getClassificationGroupGraph).mockResolvedValue({
      nodes: [
        {
          kind: "classification",
          id: "class/icd11",
          fqid: "class/icd11",
          label: "ICD-11",
          short_name: "ICD-11",
          group_key: "class/icd",
          group_label: "ICD",
          version_year: 2027,
          is_current: false,
        },
        {
          kind: "classification",
          id: "class/icd10",
          fqid: "class/icd10",
          label: "ICD-10",
          short_name: "ICD-10",
          group_key: "class/icd",
          group_label: "ICD",
          version_year: 2016,
          is_current: true,
        },
      ],
      edges: [],
      focus_id: null,
    });
    vi.mocked(getCatalogNode).mockResolvedValue(
      classificationNode({
        fqid: "class/icd10",
        name: "ICD-10",
        short_name: "ICD10",
        codes: [
          { code: "A", label: "Current diagnosis", level: 1, is_valid: true },
        ],
      }),
    );

    await render(ClassificationGroupView, { key: "icd" });

    await expect
      .element(page.getByRole("tab", { name: /ICD-10/ }))
      .toHaveAttribute("aria-selected", "true");
    await expect
      .element(page.getByRole("tab", { name: /ICD-11/ }))
      .toHaveAttribute("aria-selected", "false");
    await expect.element(page.getByText("Current diagnosis")).toBeVisible();
    expect(getCatalogNode).toHaveBeenCalledWith("class/icd10");
  });

  it("renders a succession family as an edition-chain subject page", async () => {
    vi.mocked(getClassificationGroup).mockResolvedValue(familyNode());
    vi.mocked(getClassificationGroupGraph).mockResolvedValue(familyGraph());
    vi.mocked(getCatalogNode).mockResolvedValue(
      classificationNode({
        fqid: "class/ssyk2012",
        name: "SSYK 2012",
        short_name: "SSYK2012",
        codes: [{ code: "9", label: "Yrke", level: 1, is_valid: true }],
      }),
    );

    await render(ClassificationGroupView, { key: "ssyk" });

    await expect
      .element(page.getByRole("heading", { name: "SSYK", level: 2 }))
      .toBeVisible();
    await expect
      .element(page.getByRole("tab", { name: /SSYK 1996/ }))
      .toBeVisible();
    await expect
      .element(page.getByRole("tab", { name: /SSYK 2012/ }))
      .toHaveAttribute("aria-selected", "true");
    await expect.element(page.getByText(/ssyk2012 - current/)).toBeVisible();
    await expect
      .element(page.getByRole("heading", { name: "Editions" }))
      .toBeVisible();
    await expect
      .element(page.getByRole("link", { name: "SSYK1996" }))
      .toHaveAttribute("href", "/catalog/class/ssyk1996");
    expect(document.querySelector(".edition-year")).toBeNull();
    await vi.waitFor(() => {
      expect(document.querySelector(".code-label")?.textContent).toBe("Yrke");
    });
    expect(getClassificationGroupGraph).toHaveBeenCalledWith("ssyk");
  });

  it("defaults to the current family edition before future-dated successors", async () => {
    vi.mocked(getClassificationGroup).mockResolvedValue(
      familyNode({
        key: "icd",
        label: "ICD",
        editions: [
          {
            slug: "icd11",
            fqid: "class/icd11",
            name: "ICD-11",
            short_name: "ICD-11",
            effective_year: null,
            version_year: 2027,
            is_current: false,
            is_self: false,
          },
          {
            slug: "icd10",
            fqid: "class/icd10",
            name: "ICD-10",
            short_name: "ICD-10",
            effective_year: 2027,
            version_year: 2016,
            is_current: true,
            is_self: false,
          },
        ],
      }),
    );
    vi.mocked(getCatalogNode).mockResolvedValue(
      classificationNode({
        fqid: "class/icd10",
        name: "ICD-10",
        short_name: "ICD10",
        codes: [
          { code: "A", label: "Current diagnosis", level: 1, is_valid: true },
        ],
      }),
    );

    await render(ClassificationGroupView, { key: "icd" });

    await expect
      .element(page.getByRole("tab", { name: /ICD-10/ }))
      .toHaveAttribute("aria-selected", "true");
    await expect
      .element(page.getByRole("tab", { name: /ICD-11/ }))
      .toHaveAttribute("aria-selected", "false");
    await expect.element(page.getByText(/icd10 - current/)).toBeVisible();
    await expect.element(page.getByText("Current diagnosis")).toBeVisible();
    expect(getCatalogNode).toHaveBeenCalledWith("class/icd10");
  });

  it("uses the active member FQID without re-fetching the initial classification node", async () => {
    const active = classificationNode({
      fqid: "class/niva-test",
      name: "Nivå aggregat",
      short_name: "NIVA",
      codes: [{ code: "A", label: "Aggregatnivå", level: 1, is_valid: true }],
    });
    vi.mocked(getClassificationGroup).mockResolvedValue(node());

    await render(ClassificationGroupView, {
      key: "sun",
      activeFqid: "class/niva-test",
      initialActiveNode: active,
    });

    await expect
      .element(page.getByRole("tab", { name: /Aggregat/ }))
      .toHaveAttribute("aria-selected", "true");
    await expect.element(page.getByText("Aggregatnivå")).toBeVisible();
    expect(getCatalogNode).not.toHaveBeenCalled();
  });

  it("renders related classifications for an active grouped classification node", async () => {
    const active = classificationNode({
      fqid: "class/niva-test",
      name: "Nivå aggregat",
      short_name: "NIVA",
      codes: [{ code: "A", label: "Aggregatnivå", level: 1, is_valid: true }],
      derived_from: [
        {
          fqid: "class/sun2020",
          slug: "sun2020",
          short_name: "SUN2020",
          name: "Svensk utbildningsnomenklatur",
          note: "Grouped source classification",
        },
      ],
      derivatives: [
        {
          fqid: "class/niva-extra",
          slug: "niva-extra",
          short_name: "NIVA extra",
          name: "Extra grouped derivative",
          note: null,
        },
      ],
    });
    vi.mocked(getClassificationGroup).mockResolvedValue(node());

    await render(ClassificationGroupView, {
      key: "sun",
      activeFqid: "class/niva-test",
      initialActiveNode: active,
    });

    await expect
      .element(page.getByRole("heading", { name: "Related classifications" }))
      .toBeVisible();
    await expect.element(page.getByText("Derived from")).toBeVisible();
    await expect
      .element(page.getByRole("link", { name: "SUN2020" }))
      .toHaveAttribute("href", "/catalog/class/sun2020");
    await expect
      .element(page.getByText("Grouped source classification"))
      .toBeVisible();
    await expect
      .element(page.getByText("Derived classifications"))
      .toBeVisible();
    await expect
      .element(page.getByRole("link", { name: "NIVA extra" }))
      .toHaveAttribute("href", "/catalog/class/niva-extra");
    expect(getCatalogNode).not.toHaveBeenCalled();
  });

  it("renders related classifications for an active family edition node", async () => {
    vi.mocked(getClassificationGroup).mockResolvedValue(familyNode());
    vi.mocked(getCatalogNode).mockResolvedValue(
      classificationNode({
        fqid: "class/ssyk2012",
        name: "SSYK 2012",
        short_name: "SSYK2012",
        codes: [{ code: "9", label: "Yrke", level: 1, is_valid: true }],
        derived_from: [
          {
            fqid: "class/ssyk1996",
            slug: "ssyk1996",
            short_name: "SSYK1996",
            name: "SSYK 1996",
            note: "Derived family predecessor",
          },
        ],
        derivatives: [
          {
            fqid: "class/ssyk-derived",
            slug: "ssyk-derived",
            short_name: "SSYK derived",
            name: "Derived occupational classification",
            note: null,
          },
        ],
      }),
    );

    await render(ClassificationGroupView, { key: "ssyk" });

    await expect
      .element(page.getByRole("heading", { name: "Related classifications" }))
      .toBeVisible();
    await expect
      .element(page.getByRole("link", { name: "SSYK1996" }))
      .toHaveAttribute("href", "/catalog/class/ssyk1996");
    await expect
      .element(page.getByText("Derived family predecessor"))
      .toBeVisible();
    await expect
      .element(page.getByRole("link", { name: "SSYK derived" }))
      .toHaveAttribute("href", "/catalog/class/ssyk-derived");
  });
});
