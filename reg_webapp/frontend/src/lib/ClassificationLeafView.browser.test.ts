import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { ClassificationNodeData } from "./api";
import { getCatalogNode } from "./api";
import ClassificationLeafView from "./ClassificationLeafView.svelte";

// The classification leaf rendered through the unified SubjectView shell (#638 PR1).
// Most surfaces are EMBEDDED on the node (codes / dimensions / edition_chain). When a
// classification belongs to a concept group, the view fetches the group members'
// classification leaves so the graph matches the group page. This guards the
// shell wiring: the title (nodeLabel = name), the short-name meta dl, and that the
// embedded codes panel renders inside the shell. The panels' own behaviour is
// covered by their dedicated suites.

vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return {
    ...actual,
    getCatalogNode: vi.fn(),
  };
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
    ...overrides,
  } as unknown as ClassificationNodeData;
}

function fetchedClassification(
  overrides: Partial<ClassificationNodeData>,
): ClassificationNodeData {
  return {
    kind: "classification",
    fqid: "class/example",
    name: "Example",
    short_name: "EXAMPLE",
    codes: [],
    dimensions: [],
    edition_chain: [],
    edition_edges: [],
    ...overrides,
  } as unknown as ClassificationNodeData;
}

beforeEach(() => {
  vi.mocked(getCatalogNode).mockReset();
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

  it("renders the same concept-group graph as the group page, focused on the viewed member", async () => {
    vi.mocked(getCatalogNode).mockImplementation((fqidPath) => {
      if (fqidPath === "class/sun2020-inriktning") {
        return Promise.resolve(
          fetchedClassification({
            fqid: "class/sun2020-inriktning",
            name: "SUN 2020 — inriktning",
            short_name: "SUN2020-INRIKTNING",
            edition_chain: [
              {
                slug: "sun1996",
                fqid: "class/sun1996",
                name: "SUN 1996",
                effective_year: 2000,
                is_self: false,
                is_current: false,
              },
              {
                slug: "sun2000-inriktning",
                fqid: "class/sun2000-inriktning",
                name: "SUN 2000 — inriktning",
                effective_year: 2020,
                is_self: false,
                is_current: false,
              },
              {
                slug: "sun2020-inriktning",
                fqid: "class/sun2020-inriktning",
                name: "SUN 2020 — inriktning",
                effective_year: null,
                is_self: true,
                is_current: true,
              },
            ],
            edition_edges: [
              {
                predecessor_slug: "sun1996",
                predecessor_fqid: "class/sun1996",
                successor_slug: "sun2000-inriktning",
                successor_fqid: "class/sun2000-inriktning",
                effective_year: 2000,
                note: null,
              },
              {
                predecessor_slug: "sun2000-inriktning",
                predecessor_fqid: "class/sun2000-inriktning",
                successor_slug: "sun2020-inriktning",
                successor_fqid: "class/sun2020-inriktning",
                effective_year: 2020,
                note: null,
              },
            ],
          }),
        );
      }
      return Promise.resolve(
        fetchedClassification({
          fqid: "class/niva-grovv1",
          name: "Utbildningsnivå, grov",
          short_name: "NIVA-GROV",
        }),
      );
    });

    await render(ClassificationLeafView, {
      node: node({
        fqid: "class/niva-grovv1",
        name: "Utbildningsnivå, grov",
        short_name: "NIVA-GROV",
        codes: [],
        edition_chain: [],
        edition_edges: [],
        dimensions: [
          {
            key: "sun",
            label: "Svensk utbildningsnomenklatur (SUN)",
            source: "curated",
            axes: ["dimension"],
            members: [
              {
                fqid: "class/sun2020-inriktning",
                name: "Utbildningsinriktning",
                facets: [
                  {
                    axis: "dimension",
                    value: "inriktning",
                    label: "Inriktning",
                  },
                ],
              },
              {
                fqid: "class/niva-grovv1",
                name: "Utbildningsnivå, grov",
                facets: [
                  {
                    axis: "dimension",
                    value: "niva-grov",
                    label: "Aggregat",
                  },
                ],
              },
            ],
          },
        ],
      } as unknown as Partial<ClassificationNodeData>),
    });

    await expect
      .element(
        page.getByRole("heading", { name: "Classification relationships" }),
      )
      .toBeVisible();
    expect(
      [...document.querySelectorAll(".history-graph .node-label.in-bar")].map(
        (label) => label.textContent?.trim(),
      ),
    ).toEqual([
      "sun1996",
      "sun2000-inriktning",
      "sun2020-inriktning",
      "niva-grovv1",
    ]);
    expect(
      [...document.querySelectorAll(".history-graph .node-label.in-bar")].map(
        (label) => label.textContent?.trim(),
      ),
    ).not.toContain("sun");
    expect(document.querySelectorAll(".edges .succession")).toHaveLength(2);
    expect(document.querySelectorAll(".edges .member")).toHaveLength(0);
    expect(document.querySelector(".node.self title")?.textContent).toBe(
      "niva-grovv1",
    );
  });

  it("uses the current edition to find the group graph for a historical member", async () => {
    const sunGroup: NonNullable<ClassificationNodeData["dimensions"]>[number] =
      {
        key: "sun",
        label: "Svensk utbildningsnomenklatur (SUN)",
        source: "curated",
        axes: ["dimension"],
        members: [
          {
            fqid: "class/sun-inriktning2020",
            name: "Utbildningsinriktning",
            facets: [
              {
                axis: "dimension",
                value: "inriktning",
                label: "Inriktning",
              },
            ],
          },
          {
            fqid: "class/niva-grovv1",
            name: "Utbildningsnivå, grov",
            facets: [
              {
                axis: "dimension",
                value: "niva-grov",
                label: "Aggregat",
              },
            ],
          },
        ],
      };
    vi.mocked(getCatalogNode).mockImplementation((fqidPath) => {
      if (fqidPath === "class/sun-inriktning2020") {
        return Promise.resolve(
          fetchedClassification({
            fqid: "class/sun-inriktning2020",
            name: "SUN 2020 — inriktning",
            short_name: "SUN2020-INRIKTNING",
            dimensions: [sunGroup],
            edition_chain: [
              {
                slug: "sun1996",
                fqid: "class/sun1996",
                name: "SUN 1996",
                effective_year: 2000,
                is_self: false,
                is_current: false,
              },
              {
                slug: "sun-inriktning2000",
                fqid: "class/sun-inriktning2000",
                name: "SUN 2000 — inriktning",
                effective_year: 2020,
                is_self: false,
                is_current: false,
              },
              {
                slug: "sun-inriktning2020",
                fqid: "class/sun-inriktning2020",
                name: "SUN 2020 — inriktning",
                effective_year: null,
                is_self: true,
                is_current: true,
              },
            ],
            edition_edges: [
              {
                predecessor_slug: "sun1996",
                predecessor_fqid: "class/sun1996",
                successor_slug: "sun-inriktning2000",
                successor_fqid: "class/sun-inriktning2000",
                effective_year: 2000,
                note: null,
              },
              {
                predecessor_slug: "sun-inriktning2000",
                predecessor_fqid: "class/sun-inriktning2000",
                successor_slug: "sun-inriktning2020",
                successor_fqid: "class/sun-inriktning2020",
                effective_year: 2020,
                note: null,
              },
            ],
          }),
        );
      }
      return Promise.resolve(
        fetchedClassification({
          fqid: "class/niva-grovv1",
          name: "Utbildningsnivå, grov",
          short_name: "NIVA-GROV",
        }),
      );
    });

    await render(ClassificationLeafView, {
      node: node({
        fqid: "class/sun-inriktning2000",
        name: "SUN 2000 — inriktning",
        short_name: "SUN2000-INRIKTNING",
        codes: [],
        dimensions: [],
        edition_chain: [
          {
            slug: "sun1996",
            fqid: "class/sun1996",
            name: "SUN 1996",
            effective_year: 2000,
            is_self: false,
            is_current: false,
          },
          {
            slug: "sun-inriktning2000",
            fqid: "class/sun-inriktning2000",
            name: "SUN 2000 — inriktning",
            effective_year: 2020,
            is_self: true,
            is_current: false,
          },
          {
            slug: "sun-inriktning2020",
            fqid: "class/sun-inriktning2020",
            name: "SUN 2020 — inriktning",
            effective_year: null,
            is_self: false,
            is_current: true,
          },
        ],
        edition_edges: [
          {
            predecessor_slug: "sun1996",
            predecessor_fqid: "class/sun1996",
            successor_slug: "sun-inriktning2000",
            successor_fqid: "class/sun-inriktning2000",
            effective_year: 2000,
            note: null,
          },
          {
            predecessor_slug: "sun-inriktning2000",
            predecessor_fqid: "class/sun-inriktning2000",
            successor_slug: "sun-inriktning2020",
            successor_fqid: "class/sun-inriktning2020",
            effective_year: 2020,
            note: null,
          },
        ],
      } as unknown as Partial<ClassificationNodeData>),
    });

    await expect
      .element(
        page.getByRole("heading", { name: "Classification relationships" }),
      )
      .toBeVisible();
    await vi.waitFor(() => {
      expect(
        [...document.querySelectorAll(".history-graph .node-label.in-bar")].map(
          (label) => label.textContent?.trim(),
        ),
      ).toContain("niva-grovv1");
    });

    expect(
      [...document.querySelectorAll(".history-graph .node-label.in-bar")].map(
        (label) => label.textContent?.trim(),
      ),
    ).toEqual([
      "sun1996",
      "sun-inriktning2000",
      "sun-inriktning2020",
      "niva-grovv1",
    ]);
    expect(document.querySelectorAll(".edges .succession")).toHaveLength(2);
    expect(document.querySelector(".node.self title")?.textContent).toBe(
      "sun-inriktning2000",
    );
  });
});
