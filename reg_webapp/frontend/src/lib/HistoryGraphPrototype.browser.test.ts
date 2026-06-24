import { describe, expect, it } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import HistoryGraphPrototype from "./HistoryGraphPrototype.svelte";
import type { HistoryGraph } from "./history_graph";

const graph: HistoryGraph = {
  mode: "variable",
  title: "History graph prototype",
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
  title: "Classification editions",
  nodeGrain: "entity-with-column-slices",
  dataContract: "client-stitch-prototype",
  nodes: [
    {
      id: "class/sun2020",
      kind: "classification",
      label: "sun2020",
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

const classificationGraph: HistoryGraph = {
  mode: "classification",
  title: "Classification editions",
  nodeGrain: "entity-with-column-slices",
  dataContract: "client-stitch-prototype",
  nodes: [
    {
      id: "class/sun1996",
      kind: "classification",
      label: "sun1996",
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
      label: "sun2000-inriktning",
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
      label: "sun2020-inriktning",
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
  it("renders the graph, column count, legend, and contract gaps", async () => {
    await render(HistoryGraphPrototype, { graph });

    await expect
      .element(page.getByRole("heading", { name: "History graph prototype" }))
      .toBeVisible();
    await expect.element(page.getByText("variable")).toBeVisible();
    await expect.element(page.getByText("Monthly income")).toBeVisible();
    await expect.element(page.getByText("4 columns")).toBeVisible();
    await expect.element(page.getByText("succession")).toBeVisible();
    await expect.element(page.getByText("Contract gaps")).toBeVisible();
  });

  it("omits a graph with only one plain entity", async () => {
    await render(HistoryGraphPrototype, {
      graph: standaloneClassificationGraph,
    });

    expect(document.querySelector(".history-graph")).toBeNull();
  });

  it("renders classifications through the shared graph surface without a timeline axis", async () => {
    await render(HistoryGraphPrototype, {
      graph: classificationGraph,
    });

    await expect
      .element(page.getByRole("heading", { name: "Classification editions" }))
      .toBeVisible();
    expect(
      [...document.querySelectorAll(".node-label.in-bar")].map((node) =>
        node.textContent?.trim(),
      ),
    ).toEqual(["sun1996", "sun2000-inriktning", "sun2020-inriktning"]);
    await expect.element(page.getByText("succession")).toBeVisible();
    expect(document.querySelector(".edition-svg")).toBeNull();
    expect(document.querySelector(".axis")).toBeNull();
    expect(document.querySelectorAll(".edges .succession")).toHaveLength(2);
  });
});
