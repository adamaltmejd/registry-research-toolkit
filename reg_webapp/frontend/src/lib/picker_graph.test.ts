import { describe, expect, it } from "vitest";
import type {
  ClassificationGraphNode,
  GraphState,
  RelationshipGraph,
  VariableGraphNode,
} from "./api";
import {
  classificationDagLayout,
  clustersOf,
  graphEdgeVisibleInGraph,
  resolveEdges,
} from "./picker_graph";

function state(over: Partial<GraphState> = {}): GraphState {
  return {
    state_id: "1",
    period_scope: "intervals",
    variant: "v",
    variant_label: null,
    representation_run_id: 1,
    valid_from: "2010-01-01",
    valid_to: "2010-12-31",
    value_set_id: null,
    value_set_version_label: "",
    classification_slugs: [],
    delivery_column_name: "Col",
    variant_family: null,
    variant_family_label: null,
    ...over,
  };
}

function variableNode(
  over: Partial<VariableGraphNode> = {},
): VariableGraphNode {
  return {
    kind: "variable",
    id: "v1",
    fqid: "scb/lisa/kon",
    label: "Kön",
    group_key: null,
    group_label: null,
    definition: null,
    description: null,
    operational_definition: null,
    facets: [],
    states: [],
    same_as: [],
    ...over,
  };
}

function classificationNode(
  over: Partial<ClassificationGraphNode> = {},
): ClassificationGraphNode {
  return {
    kind: "classification",
    id: "c1",
    fqid: "class/sun2020",
    label: "SUN 2020",
    short_name: "SUN2020",
    group_key: "sun",
    version_year: 2020,
    is_current: true,
    ...over,
  };
}

describe("clustersOf — group_key clustering (Fork B)", () => {
  it("titles a classification umbrella cluster by its group_label heading (#794 P3)", () => {
    // Classification umbrella members now carry `group_label` (the curated group's
    // display label) — the cluster heading must use it (not stay null), so a
    // SUN/related-granularities umbrella reads under a real title.
    const a = classificationNode({
      id: "c1",
      group_key: "class/sun",
      group_label: "SUN — Svensk utbildningsnomenklatur",
      version_year: 2000,
    });
    const b = classificationNode({
      id: "c2",
      group_key: "class/sun",
      group_label: "SUN — Svensk utbildningsnomenklatur",
      version_year: 2020,
    });
    const clusters = clustersOf({ nodes: [a, b], edges: [], focus_id: null });
    expect(clusters).toHaveLength(1);
    expect(clusters[0].label).toBe("SUN — Svensk utbildningsnomenklatur");
  });

  it("leaves a headless classification cluster (member carries no group_label) null", () => {
    // A non-member spine edition pulled in by the chain walk carries no group_label
    // → the cluster heading stays null (no spurious title).
    const a = classificationNode({
      id: "c1",
      group_key: null,
      group_label: null,
      version_year: 2000,
    });
    const clusters = clustersOf({ nodes: [a], edges: [], focus_id: null });
    expect(clusters[0].label).toBeNull();
  });

  it("gives each null-group_key node its OWN singleton cluster (no heading)", () => {
    const a = classificationNode({ id: "a", group_key: null });
    const b = classificationNode({ id: "b", group_key: null });
    const clusters = clustersOf({ nodes: [a, b], edges: [], focus_id: null });
    expect(clusters).toHaveLength(2);
    expect(clusters.every((c) => c.label === null)).toBe(true);
  });

  it("orders a cluster's classification members by version_year, oldest first", () => {
    const newer = classificationNode({ id: "c2", version_year: 2020 });
    const older = classificationNode({ id: "c1", version_year: 2000 });
    // Pass newest-first; expect ordered oldest-first.
    const clusters = clustersOf({
      nodes: [newer, older],
      edges: [],
      focus_id: null,
    });
    // Both share group_key "sun" → one cluster, ordered by year.
    expect(clusters[0].nodes.map((n) => n.node.id)).toEqual(["c1", "c2"]);
  });
});

describe("resolveEdges", () => {
  it("resolves edge ids to nodes and drops edges with a missing endpoint", () => {
    const a = variableNode({ id: "a" });
    const b = variableNode({ id: "b" });
    const graph: RelationshipGraph = {
      nodes: [a, b],
      edges: [
        { id: "e1", kind: "succession", source: "a", target: "b", label: null },
        // dangling: "z" isn't a node → dropped.
        { id: "e2", kind: "succession", source: "a", target: "z", label: null },
      ],
      focus_id: "a",
    };
    const resolved = resolveEdges(graph);
    expect(resolved).toHaveLength(1);
    expect(resolved[0].source.id).toBe("a");
    expect(resolved[0].target.id).toBe("b");
  });

  it("requires representation edges to match scoped columns and variant", () => {
    const node = variableNode({
      id: "v1",
      states: [
        state({
          variant: "energy",
          delivery_column_name: "BorgNr",
          representation_run_id: 1,
        }),
        state({
          variant: "energy",
          delivery_column_name: "PersOrgNr",
          representation_run_id: 2,
        }),
        state({
          variant: "vehicles",
          delivery_column_name: "BorgNr",
          representation_run_id: 3,
        }),
      ],
    });
    const scoped = {
      id: "rep",
      kind: "succession" as const,
      source: "v1",
      target: "v1",
      label: null,
      source_column: "borgnr",
      target_column: "persorgnr",
      variant: "energy",
    };
    const graph: RelationshipGraph = {
      nodes: [node],
      edges: [scoped],
      focus_id: "v1",
    };
    expect(graphEdgeVisibleInGraph(scoped, graph)).toBe(true);

    const filtered: RelationshipGraph = {
      ...graph,
      nodes: [
        {
          ...node,
          states: node.states.filter((s) => s.variant === "vehicles"),
        },
      ],
    };
    expect(graphEdgeVisibleInGraph(scoped, filtered)).toBe(false);
    expect(resolveEdges(filtered)).toHaveLength(0);
  });
});

describe("classificationDagLayout — compact edition DAG (#906)", () => {
  it("opens rows only for branching ranks, preserving edition order", () => {
    const base = classificationNode({
      id: "base",
      label: "Base",
      version_year: 1990,
      is_current: false,
    });
    const branchA = classificationNode({
      id: "branch-a",
      label: "Branch A",
      version_year: 2000,
      is_current: false,
    });
    const branchB = classificationNode({
      id: "branch-b",
      label: "Branch B",
      version_year: 2001,
      is_current: false,
    });
    const graph: RelationshipGraph = {
      nodes: [branchB, base, branchA],
      edges: [
        {
          id: "base-a",
          kind: "succession",
          source: "base",
          target: "branch-a",
          label: null,
        },
        {
          id: "base-b",
          kind: "succession",
          source: "base",
          target: "branch-b",
          label: null,
        },
      ],
      focus_id: "branch-a",
    };
    const cluster = clustersOf(graph)[0];
    const points = cluster.nodes.filter((n) => n.kind === "classification");
    const layout = classificationDagLayout(points, resolveEdges(graph));

    expect(layout.rows).toBe(2);
    expect(layout.columns).toBe(2);
    expect(
      layout.nodes.map((node) => [node.point.node.id, node.column, node.row]),
    ).toEqual([
      ["base", 0, 0],
      ["branch-a", 1, 0],
      ["branch-b", 1, 1],
    ]);
  });
});
