import { describe, expect, it } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import HistoryGraphPrototype from "./HistoryGraphPrototype.svelte";
import type { HistoryGraph } from "./history_graph";

const neutralBarStyle = {
  fill: "rgb(245, 245, 245)",
  stroke: "none",
  strokeWidth: "1.2px",
};

function barStyle(selector: string): typeof neutralBarStyle {
  const bar = document.querySelector<SVGRectElement>(selector);
  if (!bar) {
    throw new Error(`Missing graph bar for selector ${selector}`);
  }
  const style = getComputedStyle(bar);
  return {
    fill: style.fill,
    stroke: style.stroke,
    strokeWidth: style.strokeWidth,
  };
}

const graph: HistoryGraph = {
  mode: "variable",
  title: "Variable relationships",
  nodeGrain: "entity-with-column-slices",
  dataContract: "client-stitch-prototype",
  nodes: [
    {
      id: "scb/lisa/agi",
      kind: "variable",
      label: "Monthly income",
      fqid: "scb/lisa/agi",
      from: 2019,
      to: null,
      self: true,
      current: true,
      detail: "facets: month",
      columns: [
        {
          id: "_default:AGI01",
          label: "AGI01",
          variant: "_default",
          from: 2019,
          to: null,
          stateIds: [1],
        },
        {
          id: "_default:AGI02",
          label: "AGI02",
          variant: "_default",
          from: 2019,
          to: null,
          stateIds: [2],
        },
        {
          id: "_default:AGI03",
          label: "AGI03",
          variant: "_default",
          from: 2019,
          to: null,
          stateIds: [3],
        },
        {
          id: "_default:AGI04",
          label: "AGI04",
          variant: "_default",
          from: 2019,
          to: null,
          stateIds: [4],
        },
      ],
    },
  ],
  edges: [],
  warnings: ["prototype gap"],
};

const standaloneClassificationGraph: HistoryGraph = {
  mode: "classification",
  title: "Classification relationships",
  nodeGrain: "entity-with-column-slices",
  dataContract: "client-stitch-prototype",
  nodes: [
    {
      id: "class/sun2020",
      kind: "classification",
      label: "SUN2020",
      fqid: "class/sun2020",
      from: 2020,
      to: 2020,
      self: true,
      current: true,
      columns: [],
    },
  ],
  edges: [],
  warnings: [],
};

const variableGroupGraph: HistoryGraph = {
  mode: "group",
  title: "Variable relationships",
  nodeGrain: "entity-with-column-slices",
  dataContract: "client-stitch-prototype",
  nodes: [
    {
      id: "scb/lisa/agi1astsni2007g",
      kind: "group-member",
      label: "agi1astsni2007g",
      detail: "Näringsgren",
      fqid: "scb/lisa/agi1astsni2007g",
      from: 2019,
      to: 2023,
      columns: [],
    },
    {
      id: "scb/lisa/agi1astsni2007u",
      kind: "group-member",
      label: "agi1astsni2007u",
      detail: "Näringsgren",
      fqid: "scb/lisa/agi1astsni2007u",
      from: 2019,
      to: 2023,
      self: true,
      columns: [],
    },
  ],
  edges: [],
  warnings: ["group graph gap"],
};

const classificationGraph: HistoryGraph = {
  mode: "classification",
  title: "Classification relationships",
  nodeGrain: "entity-with-column-slices",
  dataContract: "client-stitch-prototype",
  nodes: [
    {
      id: "class/sun1996",
      kind: "classification",
      label: "SUN1996",
      fqid: "class/sun1996",
      from: 1996,
      to: 1996,
      self: true,
      current: false,
      columns: [],
    },
    {
      id: "class/sun2000-inriktning",
      kind: "classification",
      label: "SUN2000-INRIKTNING",
      fqid: "class/sun2000-inriktning",
      from: 2000,
      to: 2000,
      self: false,
      current: false,
      columns: [],
    },
    {
      id: "class/sun2020-inriktning",
      kind: "classification",
      label: "SUN2020-INRIKTNING",
      fqid: "class/sun2020-inriktning",
      from: 2020,
      to: 2020,
      self: false,
      current: true,
      columns: [],
    },
  ],
  edges: [
    {
      id: "classification:sun1996->sun2000-inriktning",
      kind: "succession",
      from: "class/sun1996",
      to: "class/sun2000-inriktning",
      fromYear: 2000,
      toYear: 2000,
      label: null,
    },
    {
      id: "classification:sun2000-inriktning->sun2020-inriktning",
      kind: "succession",
      from: "class/sun2000-inriktning",
      to: "class/sun2020-inriktning",
      fromYear: 2020,
      toYear: 2020,
      label: null,
    },
  ],
  warnings: [],
};

describe("HistoryGraphPrototype", () => {
  it("renders the graph, column count, and contract gaps", async () => {
    await render(HistoryGraphPrototype, { graph });

    await expect
      .element(page.getByRole("heading", { name: "Variable relationships" }))
      .toBeVisible();
    await expect
      .element(page.getByText("variable", { exact: true }))
      .toBeVisible();
    await expect.element(page.getByText("Monthly income")).toBeVisible();
    await expect.element(page.getByText("4 columns")).toBeVisible();
    expect(document.querySelector(".legend")).toBeNull();
    await expect.element(page.getByText("Contract gaps")).toBeVisible();
  });

  it("omits a graph with only one plain entity", async () => {
    await render(HistoryGraphPrototype, {
      graph: standaloneClassificationGraph,
    });

    expect(document.querySelector(".history-graph")).toBeNull();
  });

  it("renders coverage-only variable group graphs when they have multiple members", async () => {
    await render(HistoryGraphPrototype, {
      graph: variableGroupGraph,
      vintageYear: 2024,
    });

    await expect
      .element(page.getByRole("heading", { name: "Variable relationships" }))
      .toBeVisible();
    expect(
      [...document.querySelectorAll(".node-label.in-bar")].map((node) =>
        node.textContent?.trim(),
      ),
    ).toEqual(["agi1astsni2007g", "agi1astsni2007u"]);
    expect(document.querySelector(".axis")).not.toBeNull();
    expect(document.querySelector(".detail")).toBeNull();
    expect(barStyle(".node.group-member:not(.self) .bar")).toEqual(
      neutralBarStyle,
    );
    expect(document.querySelectorAll(".node.self")).toHaveLength(1);
    expect(
      document.querySelector(
        'a[href="/catalog/scb/lisa/agi1astsni2007u"] .node.self',
      ),
    ).not.toBeNull();
    expect(document.querySelector(".legend")).toBeNull();
    await expect
      .element(page.getByRole("button", { name: "Contract gaps" }))
      .toBeVisible();
  });

  it("renders classifications through the shared graph surface without a timeline axis", async () => {
    await render(HistoryGraphPrototype, {
      graph: classificationGraph,
    });

    await expect
      .element(
        page.getByRole("heading", { name: "Classification relationships" }),
      )
      .toBeVisible();
    expect(
      [...document.querySelectorAll(".node-label.in-bar")].map((node) =>
        node.textContent?.trim(),
      ),
    ).toEqual(["SUN1996", "SUN2000-INRIKTNING", "SUN2020-INRIKTNING"]);
    await expect.element(page.getByText("succession")).toBeVisible();
    expect(document.querySelector(".edition-svg")).toBeNull();
    expect(document.querySelector(".axis")).toBeNull();
    expect(document.querySelectorAll(".edges .succession")).toHaveLength(2);
    expect(
      barStyle(".node.classification:not(.self):not(.current) .bar"),
    ).toEqual(neutralBarStyle);
    expect(
      document
        .querySelector('a[href="/catalog/class/sun2000-inriktning"]')
        ?.querySelector(".node-label")
        ?.textContent?.trim(),
    ).toBe("SUN2000-INRIKTNING");
  });
});
