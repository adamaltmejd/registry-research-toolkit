import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type {
  ClassificationCodeModel,
  ClassificationFamilyShow,
  ClassificationGroupShow,
  ClassificationShow,
  RelationshipGraph,
  ShowNode,
} from "./api";
import { ApiError, getGraph, getShow, getValues } from "./api";
import ClassificationGroupView from "./ClassificationGroupView.svelte";
import { router } from "./router.svelte";

// Mock the GETs the view drives (mirrors ConceptGroupView.browser.test's
// api-mock style); keep the rest of api.ts real (the type exports + router).
// `show` answers the group ref with `groupShow` (or rejects with `groupError`) and
// any other ref with `memberShow`; `values` answers a classification ref with
// its registered codes, else `DEFAULT_CODES`.
vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return {
    ...actual,
    getShow: vi.fn(),
    getGraph: vi.fn(),
    getValues: vi.fn(),
  };
});

const DEFAULT_CODES: ClassificationCodeModel[] = [
  { code: "1", label: "Man", level: 1, is_valid: true },
];
let groupShow: ShowNode | null = null;
let groupError: Error | null = null;
let memberShow: ShowNode | null = null;
const codesByFqid = new Map<string, ClassificationCodeModel[]>();

function showGroup(shown: ShowNode): void {
  groupShow = shown;
}
function showMember(shown: ShowNode): void {
  memberShow = shown;
}

function node(
  overrides: Partial<ClassificationGroupShow> = {},
): ClassificationGroupShow {
  return {
    kind: "classification_group",
    fqid: `group/class/${overrides.key ?? "sun"}`,
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
  };
}

function familyNode(
  overrides: Partial<ClassificationFamilyShow> = {},
): ClassificationFamilyShow {
  return {
    kind: "classification_family",
    fqid: `group/class/${overrides.key ?? "ssyk"}`,
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
  };
}

/** A classification `show` node; `codes` registers what `values` answers for its
 * FQID (the codes are no longer embedded on the node). */
function classificationNode({
  codes,
  ...overrides
}: Partial<ClassificationShow> & {
  codes?: ClassificationCodeModel[];
} = {}): ClassificationShow {
  const shown: ClassificationShow = {
    kind: "classification",
    fqid: "class/sun2020",
    name: "SUN 2020",
    short_name: "SUN2020",
    family: null,
    dimensions: [],
    derived_from: [],
    derivatives: [],
    variables: [],
    ...overrides,
  };
  if (codes) {
    codesByFqid.set(shown.fqid, codes);
  }
  return shown;
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
  groupShow = null;
  groupError = null;
  codesByFqid.clear();
  memberShow = classificationNode();
  vi.mocked(getShow).mockReset();
  vi.mocked(getShow).mockImplementation(async (ref) => {
    if (ref?.startsWith("group/")) {
      if (groupError) throw groupError;
      if (groupShow) return groupShow;
    } else if (memberShow) {
      return memberShow;
    }
    throw new Error(`unexpected show ${ref}`);
  });
  vi.mocked(getValues).mockReset();
  vi.mocked(getValues).mockImplementation(async (ref) => {
    const items = codesByFqid.get(ref) ?? DEFAULT_CODES;
    return { items, next_cursor: null, total: items.length };
  });
  vi.mocked(getGraph).mockReset();
  vi.mocked(getGraph).mockResolvedValue({
    nodes: [],
    edges: [],
    focus_id: null,
  });
  // Reset the URL so each case starts clean (the router is a module singleton).
  router.navigate("/catalog/group/class/sun");
});

describe("ClassificationGroupView (#756)", () => {
  it("renders the umbrella label + members as edition tabs", async () => {
    showGroup(node());

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
    showGroup(node());

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
    groupError = new ApiError(
      404,
      { error: { code: "not_found", message: "no group/class/nope" } },
      "not found",
    );

    await render(ClassificationGroupView, { key: "nope" });

    await expect
      .element(page.getByText(/Not found: classification group or family/))
      .toBeVisible();
  });

  it("defaults to the current group member before future-dated graph successors", async () => {
    showGroup(
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
    vi.mocked(getGraph).mockResolvedValue({
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
    showMember(
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
  });

  it("renders a succession family as an edition-chain subject page", async () => {
    showGroup(familyNode());
    vi.mocked(getGraph).mockResolvedValue(familyGraph());
    showMember(
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
  });

  it("keeps lower content stable when a branched edition graph exceeds the reserved slot", async () => {
    let resolveGraph: (value: RelationshipGraph) => void = () => {};
    const editions = [
      ["root", 1990, false],
      ["left", 2000, false],
      ["right", 2001, false],
      ["current", 2010, true],
    ] as const;
    showGroup(
      familyNode({
        key: "branch",
        label: "Branched editions",
        editions: editions.map(([slug, versionYear, isCurrent]) => ({
          slug,
          fqid: `class/${slug}`,
          name: slug,
          short_name: slug,
          effective_year: isCurrent ? null : versionYear + 1,
          version_year: versionYear,
          is_current: isCurrent,
          is_self: false,
        })),
      }),
    );
    vi.mocked(getGraph).mockImplementation(
      () =>
        new Promise((resolve) => {
          resolveGraph = resolve;
        }),
    );
    showMember(
      classificationNode({
        fqid: "class/current",
        derived_from: [
          {
            fqid: "class/root",
            short_name: "root",
            name: "root",
            note: null,
          },
        ],
      }),
    );

    await render(ClassificationGroupView, { key: "branch" });

    const valueSet = page.getByRole("region", { name: "Value set" });
    const related = page.getByRole("region", {
      name: "Related classifications",
    });
    await expect.element(valueSet).toBeVisible();
    await expect.element(related).toBeVisible();
    const valueSetBefore = valueSet.element().getBoundingClientRect().top;
    const relatedBefore = related.element().getBoundingClientRect().top;

    resolveGraph({
      nodes: editions.map(([slug, versionYear, isCurrent]) => ({
        kind: "classification" as const,
        id: `class/${slug}`,
        fqid: `class/${slug}`,
        label: slug,
        short_name: slug,
        group_key: "class/branch",
        group_label: "Branched editions",
        version_year: versionYear,
        is_current: isCurrent,
      })),
      edges: [
        ["root", "left"],
        ["root", "right"],
        ["left", "current"],
        ["right", "current"],
      ].map(([source, target]) => ({
        id: `succession:class/${source}->class/${target}`,
        kind: "succession" as const,
        source: `class/${source}`,
        target: `class/${target}`,
        label: null,
        effective_year: null,
      })),
      focus_id: null,
    });

    const graph = page.getByRole("region", { name: "Editions" });
    await expect.element(graph).toBeVisible();
    expect(graph.element().scrollHeight).toBeGreaterThan(
      graph.element().clientHeight,
    );
    expect(
      Math.abs(valueSet.element().getBoundingClientRect().top - valueSetBefore),
    ).toBeLessThan(2);
    expect(
      Math.abs(related.element().getBoundingClientRect().top - relatedBefore),
    ).toBeLessThan(2);
  });

  it("collapses the reserved graph row when the focused member has no succession edges", async () => {
    let resolveGraph: (value: RelationshipGraph) => void = () => {};
    const active = classificationNode({
      fqid: "class/niva-test",
      name: "Nivå aggregat",
      short_name: "NIVA",
    });
    showGroup(node());
    vi.mocked(getGraph).mockImplementation(
      () =>
        new Promise((resolve) => {
          resolveGraph = resolve;
        }),
    );

    const { container } = await render(ClassificationGroupView, {
      key: "sun",
      activeFqid: "class/niva-test",
      initialActiveNode: active,
    });

    const valueSet = page.getByRole("region", { name: "Value set" });
    await expect.element(valueSet).toBeVisible();
    const reservedTop = valueSet.element().getBoundingClientRect().top;
    expect(container.querySelector(".reserve-edition-graph")).not.toBeNull();

    resolveGraph(groupGraph());

    await vi.waitFor(() => {
      expect(container.querySelector(".reserve-edition-graph")).toBeNull();
    });
    expect(
      reservedTop - valueSet.element().getBoundingClientRect().top,
    ).toBeGreaterThan(100);
    expect(
      page.getByRole("heading", { name: "Editions" }).elements(),
    ).toHaveLength(0);
  });

  it("collapses the reserved graph row after the graph request fails", async () => {
    let rejectGraph: (reason: Error) => void = () => {};
    showGroup(familyNode());
    vi.mocked(getGraph).mockImplementation(
      () =>
        new Promise((_, reject) => {
          rejectGraph = reject;
        }),
    );
    showMember(classificationNode({ fqid: "class/ssyk2012" }));

    const { container } = await render(ClassificationGroupView, {
      key: "ssyk",
    });

    const valueSet = page.getByRole("region", { name: "Value set" });
    await expect.element(valueSet).toBeVisible();
    const reservedTop = valueSet.element().getBoundingClientRect().top;
    expect(container.querySelector(".reserve-edition-graph")).not.toBeNull();

    rejectGraph(new Error("graph unavailable"));

    await vi.waitFor(() => {
      expect(container.querySelector(".reserve-edition-graph")).toBeNull();
    });
    expect(
      reservedTop - valueSet.element().getBoundingClientRect().top,
    ).toBeGreaterThan(100);
  });

  it("keeps related classifications after the value set while the graph resolves", async () => {
    let resolveGraph: (value: RelationshipGraph) => void = () => {};
    showGroup(familyNode());
    vi.mocked(getGraph).mockImplementation(
      () =>
        new Promise((resolve) => {
          resolveGraph = resolve;
        }),
    );
    showMember(
      classificationNode({
        fqid: "class/ssyk2012",
        derived_from: [
          {
            fqid: "class/ssyk1996",
            short_name: "SSYK1996",
            name: "SSYK 1996",
            note: null,
          },
        ],
      }),
    );

    await render(ClassificationGroupView, { key: "ssyk" });

    const valueSet = page.getByRole("region", { name: "Value set" });
    const related = page.getByRole("region", {
      name: "Related classifications",
    });
    await expect.element(valueSet).toBeVisible();
    await expect.element(related).toBeVisible();
    const valueSetBefore = valueSet.element().getBoundingClientRect();
    const relatedBefore = related.element().getBoundingClientRect();
    expect(relatedBefore.top).toBeGreaterThanOrEqual(valueSetBefore.bottom);

    resolveGraph(familyGraph());

    await expect
      .element(page.getByRole("heading", { name: "Editions" }))
      .toBeVisible();
    const valueSetAfter = valueSet.element().getBoundingClientRect();
    const relatedAfter = related.element().getBoundingClientRect();
    expect(relatedAfter.top).toBeGreaterThanOrEqual(valueSetAfter.bottom);
    expect(Math.abs(valueSetAfter.top - valueSetBefore.top)).toBeLessThan(2);
    expect(Math.abs(relatedAfter.top - relatedBefore.top)).toBeLessThan(2);
  });

  it("defaults to the current family edition before future-dated successors", async () => {
    showGroup(
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
    showMember(
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
  });

  it("uses the active member FQID without re-fetching the initial classification node", async () => {
    const active = classificationNode({
      fqid: "class/niva-test",
      name: "Nivå aggregat",
      short_name: "NIVA",
      codes: [{ code: "A", label: "Aggregatnivå", level: 1, is_valid: true }],
    });
    showGroup(node());

    await render(ClassificationGroupView, {
      key: "sun",
      activeFqid: "class/niva-test",
      initialActiveNode: active,
    });

    await expect
      .element(page.getByRole("tab", { name: /Aggregat/ }))
      .toHaveAttribute("aria-selected", "true");
    await expect.element(page.getByText("Aggregatnivå")).toBeVisible();
    expect(getShow).not.toHaveBeenCalledWith("class/niva-test");
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
          short_name: "SUN2020",
          name: "Svensk utbildningsnomenklatur",
          note: "Grouped source classification",
        },
      ],
      derivatives: [
        {
          fqid: "class/niva-extra",
          short_name: "NIVA extra",
          name: "Extra grouped derivative",
          note: null,
        },
      ],
    });
    showGroup(node());

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
    expect(getShow).not.toHaveBeenCalledWith("class/niva-test");
  });
});
