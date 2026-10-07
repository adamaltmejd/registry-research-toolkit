import { describe, expect, it } from "vitest";
import type {
  ClassificationGraphNode,
  GraphState,
  RelationshipGraph,
  VariableGraphNode,
} from "./api";
import {
  axisTicks,
  cellsOf,
  classificationDagLayout,
  clustersOf,
  graphEdgeVisibleInGraph,
  resolveEdges,
  type YearScale,
  yearScaleOf,
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

describe("cellsOf — representation-run grouping", () => {
  it("fuses consecutive states sharing a run into ONE cell, spanning their window", () => {
    const node = variableNode({
      states: [
        state({
          state_id: "1",
          representation_run_id: 1,
          value_set_version_label: "1-siffrig",
          valid_from: "2010-01-01",
          valid_to: "2010-12-31",
        }),
        state({
          state_id: "2",
          representation_run_id: 1,
          value_set_version_label: "1-siffrig",
          valid_from: "2011-01-01",
          valid_to: "2011-12-31",
        }),
      ],
    });
    const cells = cellsOf(node);
    expect(cells).toHaveLength(1);
    expect(cells[0].label).toBe("1-siffrig");
    expect(cells[0].window).toBe("2010 – 2011");
    expect(cells[0].columns).toEqual(["Col"]);
  });

  it("labels by classification slug, then delivery column, when no version label", () => {
    const slug = cellsOf(
      variableNode({
        states: [
          state({
            value_set_version_label: "",
            classification_slugs: ["sun2020"],
          }),
        ],
      }),
    );
    expect(slug[0].label).toBe("sun2020");

    const col = cellsOf(
      variableNode({
        states: [
          state({
            value_set_version_label: "",
            classification_slugs: [],
            delivery_column_name: "Kon",
          }),
        ],
      }),
    );
    expect(col[0].label).toBe("Kon");
  });
});

describe("yearScaleOf — shared time axis (#678 rework)", () => {
  it("extends the ceiling to the catalog vintage for an open-ended cell only", () => {
    const v = variableNode({
      id: "v",
      states: [state({ valid_from: "2010-01-01", valid_to: null })], // open-ended
    });
    const scale = yearScaleOf({ nodes: [v], edges: [], focus_id: null }, 2024);
    // Open-ended → vintage extends the max; the finite start anchors the min.
    expect(scale).toMatchObject({ minYear: 2010, maxYear: 2024 });
    expect(scale?.ceilingFromVintage).toBe(true);
  });

  it("does NOT let the vintage shrink a finite max, and ignores it when no cell is open", () => {
    const v = variableNode({
      id: "v",
      states: [state({ valid_from: "2000-01-01", valid_to: "2018-12-31" })],
    });
    // A vintage past the finite max but no open-ended cell → max stays finite.
    const scale = yearScaleOf({ nodes: [v], edges: [], focus_id: null }, 2024);
    expect(scale).toMatchObject({ maxYear: 2018, ceilingFromVintage: false });
  });

  it("returns null when NO node is datable (every bound open/unknown, no year)", () => {
    const v = variableNode({
      id: "v",
      states: [state({ valid_from: null, valid_to: null })],
    });
    const undated = classificationNode({ id: "c", version_year: null });
    expect(
      yearScaleOf({ nodes: [v, undated], edges: [], focus_id: null }),
    ).toBeNull();
  });

  it("renders an axis for a one-sided finite window: unknown start, finite end", () => {
    // Every cell is `valid_from: null` (unknown start) but ends at a known year. A
    // single finite bound seeds BOTH ends of `noteYear`, so the graph is datable
    // (NOT the axis-less fallback): the degenerate widening gives it scale width
    // and the finite year is anchored. (Regression guard for #794 P2's concern —
    // the one-sided finite case already yields a usable axis.)
    const v = variableNode({
      id: "v",
      states: [state({ valid_from: null, valid_to: "2008-12-31" })],
    });
    const scale = yearScaleOf({ nodes: [v], edges: [], focus_id: null });
    expect(scale).not.toBeNull();
    expect(scale?.ceilingFromVintage).toBe(false);
    expect(Number.isFinite(scale?.minYear)).toBe(true);
    expect(Number.isFinite(scale?.maxYear)).toBe(true);
    // The finite endpoint (2008) is on the axis, and there ARE year ticks.
    const ticks = axisTicks(scale as YearScale);
    expect(ticks.length).toBeGreaterThan(0);
    expect(ticks.some((t) => t.year === 2008)).toBe(true);
  });

  it("renders an axis for a multi-cell one-sided finite window (all unknown starts)", () => {
    // Two cells, both unknown-start but finite-end (2005, 2008). The finite ends
    // span the scale — still datable, still an axis.
    const v = variableNode({
      id: "v",
      states: [
        state({
          representation_run_id: 0,
          valid_from: null,
          valid_to: "2005-12-31",
        }),
        state({
          representation_run_id: 1,
          value_set_version_label: "next",
          valid_from: null,
          valid_to: "2008-12-31",
        }),
      ],
    });
    const scale = yearScaleOf({ nodes: [v], edges: [], focus_id: null });
    expect(scale).toMatchObject({
      minYear: 2005,
      maxYear: 2008,
      ceilingFromVintage: false,
    });
  });
});

describe("clustersOf — sub-row packing on the shared axis", () => {
  it("packs time-overlapping cells onto distinct sub-rows (rowCount > 1)", () => {
    // Two runs whose windows OVERLAP (co-existing variants) must not collide.
    const node = variableNode({
      id: "v",
      states: [
        state({
          representation_run_id: 1,
          variant: "a",
          valid_from: "2000-01-01",
          valid_to: "2010-12-31",
        }),
        state({
          representation_run_id: 2,
          variant: "b",
          valid_from: "2005-01-01",
          valid_to: "2015-12-31",
        }),
      ],
    });
    const scale = yearScaleOf({ nodes: [node], edges: [], focus_id: null });
    const clusters = clustersOf(
      { nodes: [node], edges: [], focus_id: null },
      scale,
    );
    const lane = clusters[0].nodes[0];
    expect(lane.kind).toBe("variable");
    if (lane.kind === "variable") {
      expect(lane.rowCount).toBe(2);
      expect(lane.cells.map((c) => c.row)).toEqual([0, 1]);
    }
  });

  it("packs cells whose RENDERED widths overlap to separate rows (min-width footprint)", () => {
    // #794 P3: two SHORT runs only ~1 raw year apart (one ending 2010, the next
    // starting 2011) don't overlap in raw years — but the renderer floors each cell
    // to CELL_MIN_W (~6.7 years wide at PX_PER_YEAR), so they paint overlapping.
    // Packing must respect the rendered footprint and push them to distinct rows.
    const node = variableNode({
      id: "v",
      states: [
        state({
          representation_run_id: 1,
          value_set_version_label: "a",
          valid_from: "2009-01-01",
          valid_to: "2010-12-31",
        }),
        state({
          representation_run_id: 2,
          value_set_version_label: "b",
          valid_from: "2011-01-01",
          valid_to: "2012-12-31",
        }),
      ],
    });
    const scale = yearScaleOf({ nodes: [node], edges: [], focus_id: null });
    const lane = clustersOf(
      { nodes: [node], edges: [], focus_id: null },
      scale,
    )[0].nodes[0];
    expect(lane.kind).toBe("variable");
    if (lane.kind === "variable") {
      // The first cell's padded footprint (2009 + ~6.7 ≈ 2015.7) covers 2011, so the
      // second can't share row 0.
      expect(lane.rowCount).toBe(2);
      expect(lane.cells.map((c) => c.row)).toEqual([0, 1]);
    }
  });

  it("does NOT over-split a short cell whose footprint clears the next cell's start", () => {
    // Guard against the min-width fix splitting genuinely non-overlapping cells: a
    // single-year run at 2000 (footprint ends ~2006.7) and the next starting 2008
    // clear each other → one row. (The first cell ends 2000 but its ~6.7-year
    // footprint still ends before 2008.)
    const node = variableNode({
      id: "v",
      states: [
        state({
          representation_run_id: 1,
          value_set_version_label: "a",
          valid_from: "2000-01-01",
          valid_to: "2000-12-31",
        }),
        state({
          representation_run_id: 2,
          value_set_version_label: "b",
          valid_from: "2008-01-01",
          valid_to: "2008-12-31",
        }),
      ],
    });
    const scale = yearScaleOf({ nodes: [node], edges: [], focus_id: null });
    const lane = clustersOf(
      { nodes: [node], edges: [], focus_id: null },
      scale,
    )[0].nodes[0];
    if (lane.kind === "variable") {
      expect(lane.rowCount).toBe(1);
      expect(lane.cells.every((c) => c.row === 0)).toBe(true);
    }
  });
});

describe("clustersOf — group_key clustering (Fork B)", () => {
  it("clusters nodes sharing a group_key under one cluster with the group_label", () => {
    const a = variableNode({
      id: "a",
      group_key: "g",
      group_label: "Group G",
    });
    const b = variableNode({ id: "b", group_key: "g", group_label: "Group G" });
    const clusters = clustersOf({ nodes: [a, b], edges: [], focus_id: null });
    expect(clusters).toHaveLength(1);
    expect(clusters[0].label).toBe("Group G");
    expect(clusters[0].nodes.map((n) => n.node.id)).toEqual(["a", "b"]);
  });

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
    const a = variableNode({ id: "a", group_key: null });
    const b = variableNode({ id: "b", group_key: null });
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
