import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { ClassificationNodeData } from "./api";
import { getCatalogGraph } from "./api";
import ClassificationLeafView from "./ClassificationLeafView.svelte";

vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return {
    ...actual,
    getCatalogGraph: vi.fn(),
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
    edition_edges: [],
    codes: [
      { code: "1", label: "Förgymnasial", level: 1, is_valid: true },
      { code: "3", label: "Eftergymnasial", level: 1, is_valid: true },
    ],
    dimensions: [],
    ...overrides,
  } as unknown as ClassificationNodeData;
}

beforeEach(() => {
  vi.mocked(getCatalogGraph).mockReset();
  vi.mocked(getCatalogGraph).mockResolvedValue({
    focus_id: null,
    nodes: [],
    edges: [],
  });
});

describe("ClassificationLeafView (#638 shell)", () => {
  it("renders the title, short-name meta, and the embedded codes panel", async () => {
    await render(ClassificationLeafView, { node: node() });

    await expect
      .element(
        page.getByRole("heading", {
          name: "Svensk utbildningsnomenklatur",
          level: 2,
        }),
      )
      .toBeVisible();
    await expect.element(page.getByText("class/sun2020")).toBeVisible();
    await expect
      .element(page.getByText("Short name", { exact: true }))
      .toBeVisible();
    await expect
      .element(page.getByText("SUN2020", { exact: true }))
      .toBeVisible();
    await expect
      .element(page.getByRole("heading", { name: "Codes" }))
      .toBeVisible();
    await expect.element(page.getByText("Förgymnasial")).toBeVisible();
  });

  it("omits the codes panel when the edition carries no codes", async () => {
    await render(ClassificationLeafView, { node: node({ codes: [] }) });

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

  it("renders the API graph for the viewed classification and highlights focus", async () => {
    vi.mocked(getCatalogGraph).mockResolvedValue({
      focus_id: "class/sun2020-inriktning",
      nodes: [
        {
          kind: "classification",
          id: "class/sun2000-inriktning",
          fqid: "class/sun2000-inriktning",
          label: "SUN2000-INRIKTNING",
          group_key: "class/sun",
          version_year: 2000,
          is_current: false,
        },
        {
          kind: "classification",
          id: "class/sun2020-inriktning",
          fqid: "class/sun2020-inriktning",
          label: "SUN2020-INRIKTNING",
          group_key: "class/sun",
          version_year: 2020,
          is_current: true,
        },
      ],
      edges: [
        {
          id: "succession:class/sun2000-inriktning->class/sun2020-inriktning",
          kind: "succession",
          source: "class/sun2000-inriktning",
          target: "class/sun2020-inriktning",
          label: "derived:vintage_chain",
        },
      ],
    });

    await render(ClassificationLeafView, {
      node: node({
        fqid: "class/sun2020-inriktning",
        name: "SUN 2020 - inriktning",
        short_name: "SUN2020-INRIKTNING",
      }),
    });

    expect(getCatalogGraph).toHaveBeenCalledWith("class/sun2020-inriktning");
    expect(
      [...document.querySelectorAll(".history-graph .node-label.in-bar")].map(
        (item) => item.textContent?.trim(),
      ),
    ).toEqual(["SUN2000-INRIKTNING", "SUN2020-INRIKTNING"]);
    expect(
      document.querySelector(
        'a[href="/catalog/class/sun2020-inriktning"] .node.self',
      ),
    ).not.toBeNull();
  });
});
