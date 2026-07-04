import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { ClassificationNodeData } from "./api";
import { getBindingGraph } from "./api";
import ClassificationLeafView from "./ClassificationLeafView.svelte";

// The classification leaf rendered through the unified SubjectView shell (#638 PR1).
// The codes are EMBEDDED on the node (render synchronously); the picker surface is
// the #906 compact edition DAG, fetched via `getBindingGraph(node.fqid)` — stubbed
// empty here so it omits itself (its own failure domain; the leaf renders regardless).
// This guards the shell wiring: the title (nodeLabel = name), the short-name meta dl,
// and the embedded codes panel.

vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return { ...actual, getBindingGraph: vi.fn() };
});

function node(
  overrides: Partial<ClassificationNodeData> = {},
): ClassificationNodeData {
  return {
    kind: "classification",
    fqid: "class/sun2020",
    name: "Svensk utbildningsnomenklatur",
    short_name: "SUN2020",
    edition_chain: [],
    codes: [
      { code: "1", label: "Förgymnasial", level: 1, is_valid: true },
      { code: "3", label: "Eftergymnasial", level: 1, is_valid: true },
    ],
    dimensions: [],
    derived_from: [],
    derivatives: [],
    ...overrides,
  } as unknown as ClassificationNodeData;
}

beforeEach(() => {
  vi.mocked(getBindingGraph).mockReset();
  vi.mocked(getBindingGraph).mockResolvedValue({
    nodes: [],
    edges: [],
    focus_id: null,
  } as never);
});

describe("ClassificationLeafView (#638 shell)", () => {
  it("renders the title, short-name meta, and the embedded codes panel", async () => {
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
    // The embedded value-set codes panel renders inside the shell.
    await expect
      .element(page.getByRole("heading", { name: "Codes" }))
      .toBeVisible();
    await expect.element(page.getByText("Förgymnasial")).toBeVisible();
  });

  it("renders the compact classification edition graph when the graph fetch has editions", async () => {
    vi.mocked(getBindingGraph).mockResolvedValue({
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
    } as never);

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
    vi.mocked(getBindingGraph).mockResolvedValue({
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
    } as never);

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

  it("omits the codes panel when the edition carries no codes", async () => {
    await render(ClassificationLeafView, { node: node({ codes: [] }) });

    // The leaf still renders (title + short name), but the codes panel omits itself.
    await expect
      .element(
        page.getByRole("heading", {
          name: "Svensk utbildningsnomenklatur",
          level: 2,
        }),
      )
      .toBeVisible();
    await expect
      .element(page.getByRole("heading", { name: "Codes" }))
      .not.toBeInTheDocument();
  });

  it("renders non-temporal derived-from classification references", async () => {
    await render(ClassificationLeafView, {
      node: node({
        fqid: "class/ks87-p",
        name: "Klassifikation av sjukdomar 1987, primärvård",
        short_name: "KS87-P",
        derived_from: [
          {
            fqid: "class/icd-9-ks87",
            slug: "icd-9-ks87",
            short_name: "ICD-9-KS87",
            name: "Klassifikation av sjukdomar 1987",
            note: "Primary-care setting variant of ICD-9-KS87",
          },
        ],
        derivatives: [
          {
            fqid: "class/ks87-p-extra",
            slug: "ks87-p-extra",
            short_name: "KS87-P extra",
            name: "Extra derived classification",
            note: null,
          },
        ],
      }),
    });

    await expect
      .element(page.getByRole("heading", { name: "Related classifications" }))
      .toBeVisible();
    await expect.element(page.getByText("Derived from")).toBeVisible();
    await expect
      .element(page.getByRole("link", { name: "ICD-9-KS87" }))
      .toHaveAttribute("href", "/catalog/class/icd-9-ks87");
    await expect
      .element(page.getByText("Primary-care setting variant of ICD-9-KS87"))
      .toBeVisible();
    await expect
      .element(page.getByText("Derived classifications"))
      .toBeVisible();
    await expect
      .element(page.getByRole("link", { name: "KS87-P extra" }))
      .toHaveAttribute("href", "/catalog/class/ks87-p-extra");
  });
});
