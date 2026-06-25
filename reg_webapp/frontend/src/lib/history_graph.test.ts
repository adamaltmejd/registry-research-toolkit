import { describe, expect, it } from "vitest";
import type { GraphState, RelationshipGraph } from "./api";
import {
  hasRenderableHistoryGraph,
  historyGraphFromRelationshipGraph,
  historyGraphYears,
} from "./history_graph";

function state(over: Partial<GraphState>): GraphState {
  return {
    state_id: 1,
    variant: "_default",
    representation_run_id: 0,
    delivery_column_name: "COL",
    value_set_id: null,
    value_set_version_label: "",
    classification_slug: null,
    valid_from: "2010-01-01",
    valid_to: null,
    ...over,
  };
}

function variableGraph(
  states: GraphState[],
  timeDomain?: RelationshipGraph["time_domain"],
): RelationshipGraph {
  return {
    focus_id: "scb/lisa/agi",
    time_domain: timeDomain,
    nodes: [
      {
        kind: "variable",
        id: "scb/lisa/agi",
        fqid: "scb/lisa/agi",
        label: "AGI",
        group_key: "scb/lisa/agi1",
        states,
        same_as: [],
      },
    ],
    edges: [],
  };
}

describe("relationship graph render adapter", () => {
  it("renders variable representation runs as in-node column cells", () => {
    const graph = historyGraphFromRelationshipGraph(
      variableGraph([
        state({
          state_id: 1,
          representation_run_id: 0,
          delivery_column_name: "AGI01",
          valid_from: "2019-01-01",
          valid_to: "2019-12-31",
        }),
        state({
          state_id: 2,
          representation_run_id: 1,
          delivery_column_name: "AGI02",
          valid_from: "2020-01-01",
          valid_to: null,
          value_set_version_label: "SUN2020",
        }),
      ]),
    );

    expect(graph.dataContract).toBe("reg-meta-relationship-graph");
    expect(graph.mode).toBe("variable");
    expect(graph.nodes[0].self).toBe(true);
    expect(graph.nodes[0].columns.map((column) => column.label)).toEqual([
      "AGI01",
      "AGI02",
    ]);
    expect(graph.nodes[0].columns.map((column) => column.columnLabels)).toEqual(
      [["AGI01"], ["AGI02"]],
    );
    expect(graph.nodes[0].columns[1].valueSetLabel).toBe("SUN2020");
  });

  it("keeps same-run alias columns available to the renderer", () => {
    const graph = historyGraphFromRelationshipGraph(
      variableGraph([
        state({
          state_id: 1,
          representation_run_id: 0,
          delivery_column_name: "AGIJan",
          valid_from: "2019-01-01",
          valid_to: "2019-01-31",
        }),
        state({
          state_id: 1,
          representation_run_id: 0,
          delivery_column_name: "AGIFeb",
          valid_from: "2019-02-01",
          valid_to: "2019-02-28",
        }),
      ]),
    );

    expect(graph.nodes[0].columns).toHaveLength(1);
    expect(graph.nodes[0].columns[0].columnLabels).toEqual([
      "AGIJan",
      "AGIFeb",
    ]);
    expect(graph.nodes[0].columns[0].label).toBe("AGIJan · AGIFeb");
  });

  it("uses the register time domain when the API provides one", () => {
    const graph = historyGraphFromRelationshipGraph(
      variableGraph(
        [
          state({
            valid_from: "2019-01-01",
            valid_to: "2019-12-31",
          }),
        ],
        {
          kind: "register",
          provider: "scb",
          register: "lisa",
          coverage_from: "1990-01-01",
          coverage_to: null,
          open_ended: true,
        },
      ),
    );

    expect(historyGraphYears(graph, 2024)).toEqual({ min: 1990, max: 2024 });
  });

  it("uses API focus and point years for classification graphs", () => {
    const graph = historyGraphFromRelationshipGraph({
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

    expect(graph.mode).toBe("classification");
    expect(graph.nodes.find((node) => node.self)?.id).toBe(
      "class/sun2020-inriktning",
    );
    expect(graph.nodes.find((node) => node.current)?.id).toBe(
      "class/sun2020-inriktning",
    );
    expect(historyGraphYears(graph)).toEqual({ min: 2000, max: 2020 });
  });

  it("lets reg_meta decide whether a graph renders", () => {
    const empty = historyGraphFromRelationshipGraph({
      focus_id: null,
      nodes: [],
      edges: [],
    });

    expect(hasRenderableHistoryGraph(empty)).toBe(false);
  });
});
