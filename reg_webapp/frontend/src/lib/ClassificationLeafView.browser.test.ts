import { beforeEach, describe, expect, it } from "vite-plus/test";
import { page } from "vite-plus/test/browser";
import { render } from "vitest-browser-svelte";
import type { ClassificationShow, RelationshipGraph } from "./api";
import { stubApi } from "./api-test-helpers";
import ClassificationLeafView from "./ClassificationLeafView.svelte";

// The classification leaf rendered through the unified SubjectView shell (#638 PR1).
// The codes are the `values` facet; the picker surface is the #906 compact edition
// DAG, fetched from `/api/graph/<fqid>` — stubbed empty here so it omits itself (its
// own failure domain; the leaf renders regardless). This guards the shell wiring: the
// title (nodeLabel = name), the short-name meta dl, and the codes panel.

/** Answer the leaf's two reads: the edition graph with `graph` (empty by
 * default) and the codes with two first-level codes. */
function serveGraph(
  graph: RelationshipGraph = { nodes: [], edges: [], focus_id: null },
) {
  stubApi((url) => {
    if (url.pathname.startsWith("/api/graph/")) {
      return graph;
    }
    if (!url.pathname.startsWith("/api/values/")) {
      return undefined;
    }
    return {
      items: [
        { code: "1", label: "Förgymnasial", level: 1, is_valid: true },
        { code: "3", label: "Eftergymnasial", level: 1, is_valid: true },
      ],
      next_cursor: null,
      total: 2,
    };
  });
}

function node(overrides: Partial<ClassificationShow> = {}): ClassificationShow {
  return {
    kind: "classification",
    fqid: "class/sun2020",
    name: "Svensk utbildningsnomenklatur",
    short_name: "SUN2020",
    family: null,
    dimensions: [],
    derived_from: [],
    derivatives: [],
    variables: [],
    ...overrides,
  };
}

beforeEach(() => {
  serveGraph();
});

describe("ClassificationLeafView (#638 shell)", () => {
  // Fails if the leaf stops mounting the codes panel or the shell's title/meta.
  it("renders the title, short-name meta, and the codes panel", async () => {
    await render(ClassificationLeafView, { node: node() });

    // The shell's title is nodeLabel(node) = the classification name.
    await expect
      .element(
        page.getByRole("heading", {
          name: "Svensk utbildningsnomenklatur",
          level: 2,
        }),
      )
      .toBeVisible();
    // The fqid header.
    await expect.element(page.getByText("class/sun2020")).toBeVisible();
    // The description meta dl: the Short name term + value. `exact` on the value —
    // a non-exact "SUN2020" also substring-matches the fqid <code>class/sun2020</code>.
    await expect
      .element(page.getByText("Short name", { exact: true }))
      .toBeVisible();
    await expect
      .element(page.getByText("SUN2020", { exact: true }))
      .toBeVisible();
    // The codes panel renders inside the shell.
    await expect
      .element(page.getByRole("heading", { name: "Codes" }))
      .toBeVisible();
    await expect.element(page.getByText("Förgymnasial")).toBeVisible();
  });

  it("renders the compact classification edition graph when the graph fetch has editions", async () => {
    serveGraph({
      nodes: [
        {
          kind: "classification",
          id: "sun1996",
          fqid: "class/sun1996",
          label: "SUN 1996",
          group_key: "sun",
          version_year: 1996,
          is_current: false,
        },
        {
          kind: "classification",
          id: "sun2020",
          fqid: "class/sun2020",
          label: "SUN 2020",
          group_key: "sun",
          version_year: 2020,
          is_current: true,
        },
        {
          kind: "classification",
          id: "niva-test",
          fqid: "class/niva-test",
          label: "Nivå aggregat",
          group_key: "sun",
          version_year: null,
          is_current: true,
        },
      ],
      edges: [
        {
          id: "sun1996-sun2020",
          kind: "succession",
          source: "sun1996",
          target: "sun2020",
          label: null,
          effective_year: 2019,
        },
      ],
      focus_id: "sun2020",
    });

    await render(ClassificationLeafView, { node: node() });

    await expect
      .element(page.getByRole("heading", { name: "Editions" }))
      .toBeVisible();
    expect(document.querySelector(".classification-editions")).not.toBeNull();
    expect(document.querySelector(".edition-edge")).not.toBeNull();
    expect(
      document.querySelector('a.edition-name[href="/catalog/class/sun1996"]'),
    ).not.toBeNull();
    await expect.element(page.getByText("2019", { exact: true })).toBeVisible();
    expect(document.body.textContent).not.toContain("Nivå aggregat");
    expect(document.querySelector(".history-graph")).toBeNull();
  });

  it("omits sibling-only succession chains when the viewed edition has no edge", async () => {
    serveGraph({
      nodes: [
        {
          kind: "classification",
          id: "sun1996",
          fqid: "class/sun1996",
          label: "SUN 1996",
          group_key: "sun",
          version_year: 1996,
          is_current: false,
        },
        {
          kind: "classification",
          id: "sun2020",
          fqid: "class/sun2020",
          label: "SUN 2020",
          group_key: "sun",
          version_year: 2020,
          is_current: true,
        },
        {
          kind: "classification",
          id: "niva-test",
          fqid: "class/niva-test",
          label: "Nivå aggregat",
          group_key: "sun",
          version_year: null,
          is_current: true,
        },
      ],
      edges: [
        {
          id: "sun1996-sun2020",
          kind: "succession",
          source: "sun1996",
          target: "sun2020",
          label: null,
          effective_year: 2020,
        },
      ],
      focus_id: "niva-test",
    });

    await render(ClassificationLeafView, {
      node: node({
        fqid: "class/niva-test",
        name: "Nivå aggregat",
        short_name: "NIVA",
      }),
    });

    await expect
      .element(page.getByRole("heading", { name: "Editions" }))
      .not.toBeInTheDocument();
    expect(document.querySelector(".classification-editions")).toBeNull();
    expect(document.body.textContent).not.toContain("SUN 1996");
  });

  // Fails if the leaf stops mounting the related-classification links. Their
  // content (both lists, notes, hrefs) is pinned once, in ClassificationGroupView.
  it("mounts the related-classification links", async () => {
    await render(ClassificationLeafView, {
      node: node({
        fqid: "class/ks87-p",
        name: "Klassifikation av sjukdomar 1987, primärvård",
        short_name: "KS87-P",
        derived_from: [
          {
            fqid: "class/icd-9-ks87",
            short_name: "ICD-9-KS87",
            name: "Klassifikation av sjukdomar 1987",
            note: "Primary-care setting variant of ICD-9-KS87",
          },
        ],
      }),
    });

    await expect
      .element(page.getByRole("heading", { name: "Related classifications" }))
      .toBeVisible();
  });
});
