import { describe, expect, it } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { ClassificationNodeData } from "./api";
import ClassificationLeafView from "./ClassificationLeafView.svelte";

// The classification leaf rendered through the unified SubjectView shell (#638 PR1).
// Everything is EMBEDDED on the node (codes / dimensions / edition_chain), so the
// view + its panels render synchronously — no fetch, no mocking. This guards the
// shell wiring: the title (nodeLabel = name), the short-name meta dl, and that the
// embedded codes panel renders inside the shell. The panels' own behaviour is
// covered by their dedicated suites.

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

  it("shows concept-group sibling relationships for aggregate classifications", async () => {
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
    ).toEqual(["niva-grovv1", "sun", "sun2020-inriktning"]);
  });
});
