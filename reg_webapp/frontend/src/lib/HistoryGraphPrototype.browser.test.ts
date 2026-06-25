import { describe, expect, it } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { GraphState, RelationshipGraph } from "./api";
import HistoryGraphPrototype from "./HistoryGraphPrototype.svelte";

function state(over: Partial<GraphState>): GraphState {
  return {
    state_id: 1,
    variant: "_default",
    representation_run_id: 0,
    delivery_column_name: "AGI01",
    value_set_id: null,
    value_set_version_label: "",
    classification_slug: null,
    valid_from: "2019-01-01",
    valid_to: null,
    ...over,
  };
}

const variableGraph: RelationshipGraph = {
  focus_id: "scb/lisa/agi1astsni2007u",
  nodes: [
    {
      kind: "variable",
      id: "scb/lisa/agi1astsni2007g",
      fqid: "scb/lisa/agi1astsni2007g",
      label: "AGI2007G",
      group_key: "scb/lisa/agi1astsni2007",
      states: [
        state({
          state_id: 10,
          delivery_column_name: "AGI1AstSNI2007G",
          representation_run_id: 0,
          valid_from: "2019-01-01",
          valid_to: "2019-01-31",
        }),
        state({
          state_id: 10,
          delivery_column_name: "AGI1AstSNI2007GExtra",
          representation_run_id: 0,
          valid_from: "2019-02-01",
          valid_to: "2019-02-28",
        }),
      ],
      same_as: [],
    },
    {
      kind: "variable",
      id: "scb/lisa/agi1astsni2007u",
      fqid: "scb/lisa/agi1astsni2007u",
      label: "AGI2007U",
      group_key: "scb/lisa/agi1astsni2007",
      states: [
        state({
          state_id: 20,
          delivery_column_name: "AGI1AstSNI2007U",
          representation_run_id: 0,
          valid_from: "2019-01-01",
          valid_to: null,
        }),
        state({
          state_id: 21,
          delivery_column_name: "AGI1AstSNI2007U",
          representation_run_id: 1,
          valid_from: "2020-01-01",
          value_set_version_label: "SNI2007",
        }),
      ],
      same_as: [],
    },
  ],
  edges: [
    {
      id: "related:scb/lisa/agi1astsni2007g--scb/lisa/agi1astsni2007u",
      kind: "related",
      source: "scb/lisa/agi1astsni2007g",
      target: "scb/lisa/agi1astsni2007u",
      label: "split_sibling",
    },
  ],
};

const classificationGraph: RelationshipGraph = {
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
};

describe("HistoryGraphPrototype", () => {
  it("renders API variable graph labels and links inside the graph", async () => {
    await render(HistoryGraphPrototype, {
      graph: variableGraph,
      vintageYear: 2024,
    });

    await expect
      .element(page.getByRole("heading", { name: "Relations" }))
      .toBeVisible();
    await expect.element(page.getByText("AGI1AstSNI2007U")).toBeVisible();
    await expect
      .element(page.getByRole("link", { name: "Open AGI2007U" }))
      .toHaveAttribute("href", "/catalog/scb/lisa/agi1astsni2007u");
    await expect.element(page.getByText("2 columns")).toBeVisible();
    expect(
      document.querySelector(".node.variable title")?.textContent,
    ).toContain("Columns: AGI1AstSNI2007G, AGI1AstSNI2007GExtra");
    expect(
      document.querySelector(".edges path.related title")?.textContent,
    ).toBe("split_sibling");
    expect(document.querySelector(".column-slice")).toBeNull();
  });

  it("omits empty graph payloads", async () => {
    await render(HistoryGraphPrototype, {
      graph: { focus_id: null, nodes: [], edges: [] },
    });

    expect(document.querySelector(".history-graph")).toBeNull();
  });

  it("renders classification graphs without a time axis", async () => {
    await render(HistoryGraphPrototype, { graph: classificationGraph });

    expect(document.querySelector(".axis")).toBeNull();
    await expect.element(page.getByText("SUN2000-INRIKTNING")).toBeVisible();
    await expect.element(page.getByText("SUN2020-INRIKTNING")).toBeVisible();
    expect(document.querySelectorAll("path.succession")).toHaveLength(1);
  });
});
