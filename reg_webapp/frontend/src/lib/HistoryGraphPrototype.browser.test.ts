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
});
